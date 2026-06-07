"""engine/execution_mixin.py — 交易执行 + VWAP/自适应定价 + Binance 查询"""
from __future__ import annotations
import logging
import time
import asyncio
import traceback
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set, Any

if TYPE_CHECKING:
    pass

import aiohttp
import binance_futures
from telegram_handler import tg_notifier
from engine.models import ArbitrageState

logger = logging.getLogger(__name__)


class ExecutionMixin:
    """Mixin: 交易验证+执行 + VWAP/自适应定价 + Binance 仓位查询"""

    @staticmethod
    def _entry_pnl_metrics(sim_res: dict, actual_res: dict,
                           settle_fee: float, funding_fee: float) -> dict:
        """开仓记录/日志使用的 PnL 口径。

        有 maker 字段时显式使用 maker 口径；否则保持旧的 all-taker 口径兼容。
        """
        def _f(_data: dict, _key: str, _default: float = 0.0) -> float:
            try:
                return float((_data or {}).get(_key, _default) or 0.0)
            except Exception:
                return float(_default)

        _settle = float(settle_fee or 0.0)
        _funding = float(funding_fee or 0.0)
        _maker_ready = (
            isinstance(sim_res, dict) and isinstance(actual_res, dict)
            and sim_res.get('maker_net_profit') is not None
            and sim_res.get('maker_open_fee_usd') is not None
            and actual_res.get('maker_net_profit') is not None
            and actual_res.get('maker_open_fee_usd') is not None
        )
        if _maker_ready:
            _sim_open_fee = _f(sim_res, 'maker_open_fee_usd')
            _actual_open_fee = _f(actual_res, 'maker_open_fee_usd')
            _sim_full_net = _f(sim_res, 'maker_net_profit')
            _actual_full_net = _f(actual_res, 'maker_net_profit')
            _basis = 'maker'
        else:
            _sim_open_fee = _f(sim_res, 'total_fee')
            _actual_open_fee = _f(actual_res, 'total_fee')
            _sim_full_net = _f(sim_res, 'net_profit') - _settle - _funding
            _actual_full_net = _f(actual_res, 'net_profit') - _settle - _funding
            _basis = 'legacy'

        return {
            'basis': _basis,
            'sim_open_fee': _sim_open_fee,
            'actual_open_fee': _actual_open_fee,
            'sim_total_fee': _sim_open_fee + _settle + _funding,
            'actual_total_fee': _actual_open_fee + _settle + _funding,
            'sim_full_net': _sim_full_net,
            'actual_full_net': _actual_full_net,
            'slippage_usd': _sim_full_net - _actual_full_net,
        }

    def _attach_actual_maker_entry_metrics(
            self, actual_res: dict, future_price: Decimal, call_price: Decimal,
            put_price: Decimal, anchor_type: str, settle_fee: float,
            funding_fee: float) -> None:
        """给实际成交结果补 maker 口径字段，不覆盖 simulate_trade 旧字段语义。"""
        if not isinstance(actual_res, dict):
            return
        _anchor_type = str(anchor_type or '').lower()
        if _anchor_type == 'call':
            _maker_option_price, _taker_option_price = call_price, put_price
        elif _anchor_type == 'put':
            _maker_option_price, _taker_option_price = put_price, call_price
        else:
            return
        try:
            _maker_open_fee = self._estimate_scan_maker_open_fee_usd(
                Decimal(str(future_price)), Decimal(str(_taker_option_price)),
                self.trade_amount, option_maker_price=Decimal(str(_maker_option_price)))
            _gross = Decimal(str(actual_res.get(
                'gross_profit_usd', actual_res.get('gross_profit', 0)) or 0))
            _maker_net = (
                _gross - _maker_open_fee -
                Decimal(str(settle_fee or 0.0)) -
                Decimal(str(funding_fee or 0.0)))
            actual_res['maker_open_fee_usd'] = float(_maker_open_fee)
            actual_res['maker_net_profit'] = float(_maker_net)
        except Exception as _e:
            logger.info(f"实际成交 maker 口径计算失败: {_e}")

    async def _retry_confirm_entry_prices(self, state: ArbitrageState,
                                           order_ids: list, exec_params: dict,
                                           is_cross_exchange: bool,
                                           trade_result: Optional[dict] = None):
        """🌟 P1-18 配套: 后台周期重试 REST 查询真实成交价

        当开仓时 WS+5s 重试都失败, prices_confirmed 保持 False, 硬止损门禁跳过此仓位。
        本任务异步重试, 成功恢复后将 prices_confirmed 升级为 True, 恢复正常风控。
        失败 10 次 (10 分钟) 后放弃, 依赖到期结算或人工 /stop_all 兜底。
        """
        if not order_ids or len(order_ids) != 3:
            return
        call_id, put_id, future_id = order_ids
        _anchor_type = ''
        _anchor_weighted_avg = Decimal('0')
        if isinstance(trade_result, dict):
            _anchor_type = str(trade_result.get('anchor_type', '') or '').lower()
            try:
                _anchor_weighted_avg = Decimal(str(
                    trade_result.get('anchor_weighted_avg', '0') or '0'))
            except Exception:
                _anchor_weighted_avg = Decimal('0')
        _max_attempts = 10
        _interval_sec = 60.0
        _log_prefix = f"[{state.expiry_strike}]"

        for _attempt in range(1, _max_attempts + 1):
            try:
                await asyncio.sleep(_interval_sec)
                # 已被其他路径升级 (例如 monitor REST 同步) → 无需继续
                if state.prices_confirmed and not getattr(state, '_entry_prices_estimated', False):
                    logger.info(f"{_log_prefix}✅ 成交价已被其他路径确认, 重试任务退出")
                    return
                # 仓位已被清掉 (如 emergency_dump 或交割) → 无需继续
                if state.state != 'position_open':
                    logger.info(f"{_log_prefix}📦 仓位已关闭 (state={state.state}), 重试任务退出")
                    return

                c_order = self.client.get_order_by_id(call_id)
                p_order = self.client.get_order_by_id(put_id)
                f_order = self.client.get_order_by_id(future_id) if not is_cross_exchange else None
                _c_ok = c_order and c_order.average_price > 0
                _p_ok = p_order and p_order.average_price > 0
                _f_ok = True if is_cross_exchange else (f_order and f_order.average_price > 0)

                if _c_ok and _p_ok and _f_ok:
                    _real_f = state.binance_entry_price if is_cross_exchange else f_order.average_price
                    _real_c = c_order.average_price
                    _real_p = p_order.average_price
                    if _anchor_weighted_avg > 0:
                        if _anchor_type == 'call':
                            _real_c = _anchor_weighted_avg
                        elif _anchor_type == 'put':
                            _real_p = _anchor_weighted_avg
                    state.entry_prices = {
                        'future': _real_f,
                        'call': _real_c,
                        'put': _real_p,
                    }
                    state.prices_confirmed = True
                    # 清理降级标记, 恢复正常硬止损门禁
                    if hasattr(state, '_entry_prices_estimated'):
                        state._entry_prices_estimated = False
                    await self._save_state_to_redis(state)
                    logger.info(
                        f"{_log_prefix}✅ 后台重试第 {_attempt} 次成功确认成交价, "
                        f"恢复正常硬止损门禁")
                    asyncio.create_task(tg_notifier.send_async(
                        f"✅ {state.combo_id} 成交价后台重试成功\n"
                        f"已恢复正常硬止损门禁"))
                    return
                else:
                    logger.info(
                        f"{_log_prefix}后台重试 {_attempt}/{_max_attempts}: "
                        f"c_ok={_c_ok} p_ok={_p_ok} f_ok={_f_ok}")
            except Exception as _e:
                logger.info(f"{_log_prefix}后台重试 {_attempt} 异常: {_e}")
                # 单次异常不中断, 继续下一轮

        # 10 次仍失败 → Telegram 告警, 提示人工介入
        logger.error(
            f"{_log_prefix}❌ 成交价后台重试 {_max_attempts} 次全部失败, "
            f"prices_confirmed 保持 False, 依赖到期结算或人工 /stop_all")
        asyncio.create_task(tg_notifier.send_async(
            f"❌ {state.combo_id} 成交价后台重试全部失败 ({_max_attempts} 次)\n"
            f"硬止损门禁持续关闭, 建议人工 /stop_all 或等到期结算"))

    @staticmethod
    def _append_unique_pipe_ids(existing: str, ids: list) -> str:
        """Append non-empty IDs to a pipe-separated audit field without duplicating."""
        _seen = []
        for _raw in (existing or '').split('|'):
            _id = str(_raw).strip()
            if _id and _id not in _seen:
                _seen.append(_id)
        for _raw in ids or []:
            _id = str(_raw).strip()
            if _id and _id != 'UNCONFIRMED' and _id not in _seen:
                _seen.append(_id)
        return '|'.join(_seen)

    @staticmethod
    def _capture_binance_open_ids(state: ArbitrageState, *bn_results: Any) -> None:
        """Capture one or more Binance opening order IDs for exchange-level PnL audit."""
        try:
            _ids = []
            for _res in bn_results:
                if not _res:
                    continue
                if isinstance(_res, dict):
                    _oid = _res.get('orderId')
                    if _oid:
                        _ids.append(_oid)
                    _order_ids = _res.get('orderIds') or []
                    if isinstance(_order_ids, (list, tuple)):
                        _ids.extend(_order_ids)
                else:
                    _ids.append(_res)
            if not _ids:
                return
            state.binance_order_id = ExecutionMixin._append_unique_pipe_ids(
                getattr(state, 'binance_order_id', '') or '', _ids)
        except Exception:
            # 审计字段丢失不应影响交易流程
            pass

    @staticmethod
    def _capture_binance_close_ids(state: ArbitrageState, bn_result: Optional[dict]) -> None:
        """🌟 P2 审计: 从 _close_binance_hedge 的返回值里提取 TWAP 分片 orderIds
        保存到 state.binance_close_order_id (多 ID 以 '|' 分隔持久化)。

        所有调用 `_close_binance_hedge` 的路径在 bn_result 有效时都应调用此方法,
        否则 Future_ID 的平仓部分会空缺, _format_future_id 会退化为只有开仓 ID。

        设计: 多次调用会追加(例如先 delivery 部分关单失败, 再 emergency 补关),
        避免覆盖丢失前一次的 orderIds。
        """
        try:
            if not isinstance(bn_result, dict):
                return
            _ids = bn_result.get('orderIds') or []
            if not _ids:
                return
            _existing = getattr(state, 'binance_close_order_id', '') or ''
            state.binance_close_order_id = ExecutionMixin._append_unique_pipe_ids(_existing, _ids)
        except Exception:
            # 审计字段丢失不应影响交易流程
            pass

    @staticmethod
    def _format_future_id(state: ArbitrageState) -> str:
        """🌟 P2 审计: 格式化 Future_ID = 开仓ID|平仓ID(s)
        多分片开仓/平仓在 state 内以管道分隔，记录时转成逗号。
        - 开仓和平仓都存在 → "open_id1,open_id2|close_id1,close_id2"
        - 仅开仓 → "open_id1,open_id2"
        - 仅平仓 (理论不会有) → "|close_id"
        - 两者都空 → "UNCONFIRMED"
        """
        _open = (getattr(state, 'binance_order_id', '') or '').strip()
        _close = (getattr(state, 'binance_close_order_id', '') or '').strip()
        # 统一把 state 里的 '|' 分隔转成逗号 (state 存储侧为 pipe, 记录内部用 comma 避免歧义)
        _open_fmt = _open.replace('|', ',')
        _close_fmt = _close.replace('|', ',')
        if _open_fmt and _close_fmt:
            return f"{_open_fmt}|{_close_fmt}"
        if _open_fmt:
            return _open_fmt
        if _close_fmt:
            return f"|{_close_fmt}"
        return 'UNCONFIRMED'

    def _check_position_lock(self, expiry: str, strike: Decimal) -> bool:
        """检查持仓锁"""
        # ==== 优先检查本地刚刚加上的内存锁 ====
        if (expiry, strike) in self.position_locks:
            return True
        # 检查是否有持仓
        combination = self.arbitrage_combinations.get((expiry, strike))
        if not combination:
            return True

        # 只检查期权持仓 (期货是多组合共享的，不能作为单组合锁定依据)
        for instrument in [combination['call'], combination['put']]:
            position = self.client.positions.get(instrument)
            if position and position.size != Decimal('0'):
                return True

        return False

    async def _calculate_vwap(self, instrument_name: str, side: str, required_amount: Decimal) -> Optional[Decimal]:
        """计算指定深度的成交量加权平均价 (VWAP)。
        required_amount 的单位取决于合约类型：期货=USD，期权=BTC/ETH。"""
        try:
            book = self.client.local_orderbooks.get(instrument_name)
            if not book:
                return None
            # 丢包后 OrderBook 标记为无效，拒绝使用脏数据做 VWAP
            if not book.is_valid:
                return None

            levels = book.get_top_levels(action_side=side)

            accumulated_amount = Decimal('0')
            accumulated_value = Decimal('0')

            for price, amount in levels:
                remaining = required_amount - accumulated_amount

                if amount >= remaining:
                    accumulated_amount += remaining
                    accumulated_value += remaining * price
                    break
                else:
                    accumulated_amount += amount
                    accumulated_value += amount * price

            if accumulated_amount < required_amount:
                return None

            return accumulated_value / accumulated_amount

        except Exception as e:
            logger.error(f"VWAP 计算失败: {e}")
            return None

    async def _calculate_three_leg_vwap(
        self, strategy_type: str,
        future_name: str, call_name: str, put_name: str,
        option_amount: Decimal, future_amount_usd: Decimal
    ) -> Optional[Dict[str, Decimal]]:
        """三腿 VWAP 一次性计算（全 Taker 假设）。
        sell_future_buy_synthetic: 卖期货(bids) + 买Call(asks) + 卖Put(bids)
        buy_future_sell_synthetic: 买期货(asks) + 卖Call(bids) + 买Put(asks)
        任一腿深度不足返回 None。"""
        if strategy_type == 'sell_future_buy_synthetic':
            f_side, c_side, p_side = 'sell', 'buy', 'sell'
        else:
            f_side, c_side, p_side = 'buy', 'sell', 'buy'

        f_vwap = await self._calculate_vwap(future_name, f_side, future_amount_usd)
        c_vwap = await self._calculate_vwap(call_name, c_side, option_amount)
        p_vwap = await self._calculate_vwap(put_name, p_side, option_amount)

        if f_vwap is None or c_vwap is None or p_vwap is None:
            return None

        return {'future': f_vwap, 'call': c_vwap, 'put': p_vwap}

    def _compute_dynamic_aggression(self, bid: Decimal, ask: Decimal, ref_price: Decimal) -> Decimal:
        """🌟 Spread 自适应 Maker 激进度

        公式: aggression = upper - clamp((spread_usd - 50) / 3000, 0, 0.20)

        - upper = self.maker_price_aggression (config 可调, 默认 0.8)
        - spread_usd = (ask - bid) × ref_price  (单腿期权 bid-ask 价差, 折算 USD)

        效果:
          spread_usd ≤ 50  → aggression = upper     (流动性极好, 挂接近对手方)
          spread_usd = 500 → aggression = upper-0.15 (流动性一般, 有让步空间)
          spread_usd ≥ 3050→ aggression = upper-0.20 (流动性差, 保留 Maker 利润空间)

        Args:
            bid: 期权买一价 (BTC)
            ask: 期权卖一价 (BTC)
            ref_price: 参考价 (BTC→USD 转换, 如 Binance mid_price)

        Returns:
            Decimal: 动态 aggression (范围 0.60 ~ 1.0)
        """
        try:
            upper = Decimal(str(getattr(self, 'maker_price_aggression', 0.8)))
            if bid <= 0 or ask <= 0 or ref_price <= 0:
                return upper  # 数据不全时退回固定值
            spread_usd = (ask - bid) * ref_price
            # clamp((spread_usd - 50) / 3000, 0, 0.20)
            decay = (spread_usd - Decimal('50')) / Decimal('3000')
            decay = max(Decimal('0'), min(Decimal('0.20'), decay))
            return upper - decay
        except Exception:
            return Decimal(str(getattr(self, 'maker_price_aggression', 0.8)))

    async def _calculate_adaptive_price(self, instrument_name: str, side: str, required_amount: Decimal) -> Optional[Decimal]:
        """自适应 Taker 定价：首档深度够就用首档价（更精确），不够才穿档 VWAP（更保守）。
        避免深度充足时 VWAP 多档穿透导致的过度悲观估算。"""
        try:
            book = self.client.local_orderbooks.get(instrument_name)
            if not book or not book.is_valid:
                return None

            levels = book.get_top_levels(action_side=side)
            if not levels:
                return None

            # 首档深度 >= 所需量 → 直接用首档价（实际成交不会穿档）
            first_price, first_amount = levels[0]
            if first_amount >= required_amount:
                return first_price

            # 首档不够 → 完整 VWAP 穿档计算
            return await self._calculate_vwap(instrument_name, side, required_amount)
        except Exception as e:
            logger.error(f"自适应定价失败 {instrument_name}: {e}")
            return None

    def _check_processing_lock(self, expiry: str, strike: Decimal) -> bool:
        """检查处理锁（冷却锁）"""
        return (expiry, strike) in self.processing_opportunities

    def _estimate_round_settle_fee_usd(self, future_price: Decimal, call_ref: Decimal,
                                       put_ref: Decimal, amount: Decimal = None) -> Decimal:
        """估算该组合到期结算总费用（Deribit 交割费 + Binance 平仓费，含 premium cap）。"""
        if amount is None:
            amount = self.trade_amount
        if future_price <= 0 or amount <= 0:
            return Decimal('0')

        call_ref = max(call_ref, Decimal('0'))
        put_ref = max(put_ref, Decimal('0'))
        fee_calc = self.trade_executor.fee_calculator if getattr(self, 'trade_executor', None) else self.fee_calculator

        del_c_btc = fee_calc.calculate_delivery_fee(future_price, call_ref, amount, is_option=True)
        del_p_btc = fee_calc.calculate_delivery_fee(future_price, put_ref, amount, is_option=True)
        delivery_usd = (del_c_btc + del_p_btc) * future_price

        if getattr(self, 'binance_fee_calc', None) is not None:
            _bn_rate = Decimal(str(getattr(self.binance_fee_calc, 'taker_rate', Decimal('0.0004'))))
            bn_close_usd = future_price * amount * _bn_rate
        else:
            bn_close_usd = binance_futures.BinanceFeeCalculator.calculate_fee_usdt(
                future_price, amount, is_taker=True, tier="standard")
        return delivery_usd + bn_close_usd

    async def _verify_and_execute_task(self, opportunity_info: Dict):
        """
        并发任务单元 (终极版)：
        1. 获取最新价格
        2. 应用 Deribit 动态阶梯 Tick Size
        3. 智能三级定价 (中间价 -> 插队价 -> 盘口价)
        4. VWAP 深度护城河校验
        """
        expiry_strike = opportunity_info['expiry_strike']
        expiry, strike = expiry_strike

        # 防竞态：任务创建与暂停信号之间可能存在间隙，入口处再次校验
        if self.trading_paused or not self.running:
            return

        if self._check_processing_lock(expiry, strike) or self._check_position_lock(expiry, strike):
            return

        # 🌟 连续失败递增冷却：防止对同一"幻影机会"无限重试
        _cooldown_until = self._combo_cooldown_until.get(expiry_strike, 0)
        if time.time() < _cooldown_until:
            return

        self.processing_opportunities.add(expiry_strike)
        _bn_margin_reserved = False
        _bn_needed = Decimal('0')

        try:
            combination = self.arbitrage_combinations.get(expiry_strike)
            if not combination: return

            future_ticker = self.client.tickers.get(combination['future'])
            call_ticker = self.client.tickers.get(combination['call'])
            put_ticker = self.client.tickers.get(combination['put'])

            _is_cross_exchange = bool(combination.get('binance_future'))
            if _is_cross_exchange:
                if not all([call_ticker, put_ticker]): return
                if any(t.bid <= 0 or t.ask <= 0 for t in [call_ticker, put_ticker]): return
            else:
                if not all([future_ticker, call_ticker, put_ticker]): return
                if any(t.bid <= 0 or t.ask <= 0 for t in [future_ticker, call_ticker, put_ticker]): return

            # 行情过期检测：DTE 越短 Gamma 越大，需要更新鲜的行情
            try:
                from datetime import datetime, timezone, timedelta
                _raw_dt_s = datetime.strptime(expiry, "%d%b%y")
                _exp_dt_s = _raw_dt_s.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                _dte_h_s = (_exp_dt_s - datetime.now(timezone.utc)).total_seconds() / 3600
            except Exception:
                _dte_h_s = 48.0
            if _dte_h_s <= 4:
                _stale_threshold = 10
            elif _dte_h_s <= 24:
                _stale_threshold = 20
            else:
                _stale_threshold = 30
            _now = time.time()
            _verify_tickers = [('call', call_ticker), ('put', put_ticker)]
            if not _is_cross_exchange and future_ticker:
                _verify_tickers.insert(0, ('future', future_ticker))
            for _t_name, _t_data in _verify_tickers:
                if hasattr(_t_data, 'timestamp') and _t_data.timestamp > 0:
                    _age = _now - _t_data.timestamp
                    if _age > _stale_threshold:
                        logger.debug(f"[{expiry}-{strike}] 跳过：{_t_name} 行情已过期 {_age:.0f}s (>{_stale_threshold}s)")
                        return

            log_prefix = f"[{expiry.zfill(7)}-{strike}]"
            # ================= 🌟 核心修复 1：获取期权合约的真实信息 =================
            call_info = await self.client.get_instrument_info(combination['call'])
            put_info = await self.client.get_instrument_info(combination['put'])
            # ================= 🌟 核心修复 2：基于真实API报文的动态 Tick 计算 =================
            # 直接调用类级静态方法，解析 tick_size_steps（BTC/ETH 分档 tick）
            _get_dynamic_tick = self._get_dynamic_tick
            # =================================================================

            default_size = Decimal('1') if self.target_currency == 'ETH' else Decimal('10')
            contract_size = self.contract_sizes.get(combination['future'], default_size)

            # 验证阶段: 距到期不足 min_option_dte_hours 不开新仓
            _h_to_exp = 48.0  # 默认值，防止 try 块异常时后续引用未赋值
            try:
                from datetime import datetime, timezone, timedelta
                _raw_dt = datetime.strptime(expiry, "%d%b%y")
                _expiry_dt = _raw_dt.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                _h_to_exp = (_expiry_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                _min_dte_hours = float(getattr(self, 'min_option_dte_hours', 12))
                if _h_to_exp <= _min_dte_hours:
                    logger.debug(f"{log_prefix} 跳过：距到期仅 {_h_to_exp:.1f} 小时 (<= {_min_dte_hours}h)，不开新仓")
                    return
            except Exception:
                pass

            # ===== 跨所: 获取 Binance 期货价格 (验证阶段) =====
            bn_symbol = combination.get('binance_future', '')
            bn_type = combination.get('binance_future_type', '')
            _verify_threshold = self.min_profit_threshold
            _bn_ready, _bn_reason = self._binance_market_ready(bn_symbol, max_age_sec=float(_stale_threshold))
            if not _bn_ready:
                logger.debug(f"{log_prefix}跳过：Binance 市场未就绪 ({_bn_reason})")
                return
            bn_ob = self.binance_ws.order_books.get(bn_symbol) if self.binance_ws else None
            if not bn_ob or bn_ob.mid_price is None or bn_ob.mid_price <= 0:
                return
            if bn_ob.update_time and (time.time() - bn_ob.update_time) > 30:
                logger.debug(f"{log_prefix}跳过：Binance 盘口过期 {time.time() - bn_ob.update_time:.0f}s")
                return
            bn_bid = bn_ob.best_bid
            bn_ask = bn_ob.best_ask
            if not bn_bid or not bn_ask or bn_bid <= 0 or bn_ask <= 0:
                return

            # ===== funding 费用预估（按方向保留符号） =====
            _verify_funding_long_usd = Decimal('0')
            _verify_funding_short_usd = Decimal('0')
            if bn_type == "perpetual" and self.binance_ws:
                _vf_rate = self.binance_ws.funding_rates.get(bn_symbol, Decimal('0'))
                if abs(_vf_rate) > self.binance_max_funding_rate:
                    _vf_dir = "多头付费，空头收益" if _vf_rate > 0 else "空头付费，多头收益"
                    logger.info(f"{log_prefix}⚠️ funding={float(_vf_rate)*100:+.4f}%({_vf_dir}，超警戒，由利润门槛过滤)")
                _vf_pos_val = bn_ob.mid_price * self.trade_amount
                try:
                    _vf_hours = max(_h_to_exp, 8)
                except Exception:
                    _vf_hours = 48
                _vf_raw = binance_futures.BinanceFeeCalculator.estimate_funding_cost_usdt(
                    _vf_pos_val, _vf_rate, _vf_hours)
                _verify_funding_long_usd = _vf_raw
                _verify_funding_short_usd = -_vf_raw

            premium_btc_1 = call_ticker.bid - put_ticker.ask
            syn_buy_price = (premium_btc_1 * bn_bid) + strike
            spread_1 = bn_bid - syn_buy_price

            premium_btc_2 = call_ticker.ask - put_ticker.bid
            syn_sell_price = (premium_btc_2 * bn_ask) + strike
            spread_2 = syn_sell_price - bn_ask

            target_type = None
            final_net_profit = Decimal('0')
            exec_params = {}
            future_amount_usd = Decimal('0')
            _selected_funding_usd = Decimal('0')
            _selected_settle_fee_usd = Decimal('0')
            c_spread = call_ticker.ask - call_ticker.bid if call_ticker.ask > 0 and call_ticker.bid > 0 else Decimal('999')
            p_spread = put_ticker.ask - put_ticker.bid if put_ticker.ask > 0 and put_ticker.bid > 0 else Decimal('999')

            # ================= 策略 1: 卖期货买合成 =================
            if spread_1 * self.trade_amount > _verify_threshold:
                future_amount_usd = self.trade_amount * bn_bid
                future_amount_usd = (future_amount_usd / contract_size).quantize(Decimal('1'),
                                                                                 rounding='ROUND_HALF_UP') * contract_size

                c_base, p_base = call_ticker.bid, put_ticker.ask

                # 🌟 修复：传入真实合约信息
                c_tick = _get_dynamic_tick(c_base, call_info)
                p_tick = _get_dynamic_tick(p_base, put_info)

                # 方案 A: 激进价 (靠近对手方，提高被扫概率)
                # 策略1: sell_future_buy_synthetic → buy Call, sell Put
                # Call buy: base=bid, 对手方=ask, 激进=bid + aggr*(ask-bid)
                # Put sell: base=ask, 对手方=bid, 激进=ask - aggr*(ask-bid)
                # 🌟 使用 spread 自适应 aggression (maker_price_aggression 为上限)
                _aggr_c = self._compute_dynamic_aggression(call_ticker.bid, call_ticker.ask, bn_ob.mid_price)
                _aggr_p = self._compute_dynamic_aggression(put_ticker.bid, put_ticker.ask, bn_ob.mid_price)
                c_aggr_raw = call_ticker.bid + (call_ticker.ask - call_ticker.bid) * _aggr_c
                p_aggr_raw = put_ticker.ask - (put_ticker.ask - put_ticker.bid) * _aggr_p
                _aggr = _aggr_c  # 用于日志显示

                c_mid = self.client._adjust_to_tick_size(c_aggr_raw, _get_dynamic_tick(c_aggr_raw, call_info))
                p_mid = self.client._adjust_to_tick_size(p_aggr_raw, _get_dynamic_tick(p_aggr_raw, put_info))
                # 🌟 Maker 防穿透: tick 对齐后若 buy>=ask 或 sell<=bid，回退一个 tick 确保挂单
                if c_mid >= call_ticker.ask:
                    c_mid = call_ticker.ask - _get_dynamic_tick(call_ticker.ask, call_info)
                if p_mid <= put_ticker.bid:
                    p_mid = put_ticker.bid + _get_dynamic_tick(put_ticker.bid, put_info)

                # 方案 B: 插队价 (这部分无需修改，保留原样)
                c_jump = c_base + c_tick if (call_ticker.ask - c_base) > c_tick else c_base
                p_jump = p_base - p_tick if (p_base - put_ticker.bid) > p_tick else p_base

                prices_to_test = [
                    (c_mid, p_mid, f"🎯 激进价({_aggr:.0%})"),
                    (c_jump, p_jump, "🚀 插队价-优先排队"),
                    (c_base, p_base, "🐢 盘口价-被动等待")
                ]

                for test_c, test_p, strategy_name in prices_to_test:
                    # 🌟 Deribit 价格限制过滤：跳过超出交易所允许范围的定价方案
                    if call_ticker.min_price > 0 and test_c < call_ticker.min_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Call 价格 {test_c} < 交易所下限 {call_ticker.min_price}")
                        continue
                    if call_ticker.max_price > 0 and test_c > call_ticker.max_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Call 价格 {test_c} > 交易所上限 {call_ticker.max_price}")
                        continue
                    if put_ticker.min_price > 0 and test_p < put_ticker.min_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Put 价格 {test_p} < 交易所下限 {put_ticker.min_price}")
                        continue
                    if put_ticker.max_price > 0 and test_p > put_ticker.max_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Put 价格 {test_p} > 交易所上限 {put_ticker.max_price}")
                        continue
                    current_sim = await self.trade_executor.simulate_trade('sell_future_buy_synthetic',
                                                                           bn_bid, test_c, test_p, strike,
                                                                           future_amount_usd, self.trade_amount)
                    _verify_settle_fee_usd = self._estimate_round_settle_fee_usd(
                        bn_ob.mid_price, test_c, test_p, self.trade_amount)
                    _call_is_anchor = c_spread >= p_spread
                    _taker_option_price = test_p if _call_is_anchor else test_c
                    _maker_option_price = test_c if _call_is_anchor else test_p
                    _maker_open_fee_usd = self._estimate_scan_maker_open_fee_usd(
                        bn_bid, _taker_option_price, self.trade_amount,
                        option_maker_price=_maker_option_price)
                    _reference_profit = (
                        Decimal(str(current_sim['net_profit'])) -
                        _verify_settle_fee_usd - _verify_funding_short_usd)
                    profit = (
                        Decimal(str(current_sim.get('gross_profit_usd', current_sim.get('gross_profit', 0)))) -
                        _maker_open_fee_usd - _verify_settle_fee_usd - _verify_funding_short_usd)
                    if profit >= _verify_threshold:
                        logger.info(
                            f"{log_prefix}=== 开仓利润明细 [sell_future_buy_synthetic] {strategy_name} ===\n"
                            f"  期货: sell @ Binance bid={bn_bid} | 面值={future_amount_usd} USD\n"
                            f"  Call:  buy  @ {test_c} (bid={call_ticker.bid} ask={call_ticker.ask})\n"
                            f"  Put:   sell @ {test_p} (bid={put_ticker.bid} ask={put_ticker.ask})\n"
                            f"  Premium(C-P): {test_c - test_p:.6f} BTC | Synthetic: {(test_c - test_p) * bn_bid + strike:.2f} USD\n"
                            f"  Spread: {bn_bid - ((test_c - test_p) * bn_bid + strike):.2f} USD\n"
                            f"  毛利: {current_sim['gross_profit']:.2f} | Maker口径开仓费: {_maker_open_fee_usd:.2f} "
                            f"| 结算+Funding: {_verify_settle_fee_usd + _verify_funding_short_usd:.2f} "
                            f"| 净利: {profit:.2f} USD | 保守口径: {_reference_profit:.2f} USD"
                        )
                        target_type, final_net_profit, sim_res = 'sell_future_buy_synthetic', profit, current_sim
                        _selected_funding_usd = _verify_funding_short_usd
                        _selected_settle_fee_usd = _verify_settle_fee_usd
                        exec_params = {'future_price': bn_bid, 'call_price': test_c, 'put_price': test_p}
                        break

            # ================= 策略 2: 买期货卖合成 =================
            # 🌟 BUG修复: elif → if，独立评估两个策略方向，选利润更高的执行
            # 旧代码用 elif 导致策略1的乐观快速检查一旦通过就阻断策略2，
            # 即使策略2利润高10倍也永远无法被评估（如 03APR26-2025 组合：策略1=$2 vs 策略2=$23）
            if spread_2 * self.trade_amount > _verify_threshold:
                _fut_usd_2 = self.trade_amount * bn_ask
                _fut_usd_2 = (_fut_usd_2 / contract_size).quantize(Decimal('1'),
                                                                                 rounding='ROUND_HALF_UP') * contract_size

                c_base, p_base = call_ticker.ask, put_ticker.bid

                # 🌟 修复：传入真实合约信息
                c_tick = _get_dynamic_tick(c_base, call_info)
                p_tick = _get_dynamic_tick(p_base, put_info)

                # 方案 A: 激进价 (靠近对手方，提高被扫概率)
                # 策略2: buy_future_sell_synthetic → sell Call, buy Put
                # Call sell: base=ask, 对手方=bid, 激进=ask - aggr*(ask-bid)
                # Put buy: base=bid, 对手方=ask, 激进=bid + aggr*(ask-bid)
                # 🌟 使用 spread 自适应 aggression (maker_price_aggression 为上限)
                _aggr_c = self._compute_dynamic_aggression(call_ticker.bid, call_ticker.ask, bn_ob.mid_price)
                _aggr_p = self._compute_dynamic_aggression(put_ticker.bid, put_ticker.ask, bn_ob.mid_price)
                c_aggr_raw = call_ticker.ask - (call_ticker.ask - call_ticker.bid) * _aggr_c
                p_aggr_raw = put_ticker.bid + (put_ticker.ask - put_ticker.bid) * _aggr_p
                _aggr = _aggr_c  # 用于日志显示

                c_mid = self.client._adjust_to_tick_size(c_aggr_raw, _get_dynamic_tick(c_aggr_raw, call_info))
                p_mid = self.client._adjust_to_tick_size(p_aggr_raw, _get_dynamic_tick(p_aggr_raw, put_info))
                # 🌟 Maker 防穿透: tick 对齐后若 sell<=bid 或 buy>=ask，回退一个 tick 确保挂单
                if c_mid <= call_ticker.bid:
                    c_mid = call_ticker.bid + _get_dynamic_tick(call_ticker.bid, call_info)
                if p_mid >= put_ticker.ask:
                    p_mid = put_ticker.ask - _get_dynamic_tick(put_ticker.ask, put_info)

                # 方案 B: 插队价 (这部分无需修改，保留原样)
                c_jump = c_base - c_tick if (c_base - call_ticker.bid) > c_tick else c_base
                p_jump = p_base + p_tick if (put_ticker.ask - p_base) > p_tick else p_base

                prices_to_test = [
                    (c_mid, p_mid, f"🎯 激进价({_aggr:.0%})"),
                    (c_jump, p_jump, "🚀 插队价-优先排队"),
                    (c_base, p_base, "🐢 盘口价-被动等待")
                ]

                for test_c, test_p, strategy_name in prices_to_test:
                    # 🌟 Deribit 价格限制过滤：跳过超出交易所允许范围的定价方案
                    if call_ticker.min_price > 0 and test_c < call_ticker.min_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Call 价格 {test_c} < 交易所下限 {call_ticker.min_price}")
                        continue
                    if call_ticker.max_price > 0 and test_c > call_ticker.max_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Call 价格 {test_c} > 交易所上限 {call_ticker.max_price}")
                        continue
                    if put_ticker.min_price > 0 and test_p < put_ticker.min_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Put 价格 {test_p} < 交易所下限 {put_ticker.min_price}")
                        continue
                    if put_ticker.max_price > 0 and test_p > put_ticker.max_price:
                        logger.debug(f"{log_prefix}跳过 {strategy_name}: Put 价格 {test_p} > 交易所上限 {put_ticker.max_price}")
                        continue
                    current_sim = await self.trade_executor.simulate_trade('buy_future_sell_synthetic',
                                                                           bn_ask, test_c, test_p, strike,
                                                                           _fut_usd_2, self.trade_amount)
                    _verify_settle_fee_usd = self._estimate_round_settle_fee_usd(
                        bn_ob.mid_price, test_c, test_p, self.trade_amount)
                    _call_is_anchor = c_spread >= p_spread
                    _taker_option_price = test_p if _call_is_anchor else test_c
                    _maker_option_price = test_c if _call_is_anchor else test_p
                    _maker_open_fee_usd = self._estimate_scan_maker_open_fee_usd(
                        bn_ask, _taker_option_price, self.trade_amount,
                        option_maker_price=_maker_option_price)
                    _reference_profit = (
                        Decimal(str(current_sim['net_profit'])) -
                        _verify_settle_fee_usd - _verify_funding_long_usd)
                    profit = (
                        Decimal(str(current_sim.get('gross_profit_usd', current_sim.get('gross_profit', 0)))) -
                        _maker_open_fee_usd - _verify_settle_fee_usd - _verify_funding_long_usd)
                    if profit >= _verify_threshold and profit > final_net_profit:
                        logger.info(
                            f"{log_prefix}=== 开仓利润明细 [buy_future_sell_synthetic] {strategy_name} ===\n"
                            f"  期货: buy  @ Binance ask={bn_ask} | 面值={_fut_usd_2} USD\n"
                            f"  Call:  sell @ {test_c} (bid={call_ticker.bid} ask={call_ticker.ask})\n"
                            f"  Put:   buy  @ {test_p} (bid={put_ticker.bid} ask={put_ticker.ask})\n"
                            f"  Premium(C-P): {test_c - test_p:.6f} BTC | Synthetic: {(test_c - test_p) * bn_ask + strike:.2f} USD\n"
                            f"  Spread: {(test_c - test_p) * bn_ask + strike - bn_ask:.2f} USD\n"
                            f"  毛利: {current_sim['gross_profit']:.2f} | Maker口径开仓费: {_maker_open_fee_usd:.2f} "
                            f"| 结算+Funding: {_verify_settle_fee_usd + _verify_funding_long_usd:.2f} "
                            f"| 净利: {profit:.2f} USD | 保守口径: {_reference_profit:.2f} USD"
                        )
                        target_type, final_net_profit, sim_res = 'buy_future_sell_synthetic', profit, current_sim
                        _selected_funding_usd = _verify_funding_long_usd
                        _selected_settle_fee_usd = _verify_settle_fee_usd
                        exec_params = {'future_price': bn_ask, 'call_price': test_c, 'put_price': test_p}
                        break

            # ================= 期权 Taker 腿 VWAP + 期货 VWAP 联合校验 =================
            if target_type:
                # 净仓模式护栏：同一 Binance symbol 禁止同时持有反向组合（避免归因错配）
                if not self.binance_dual_side_mode:
                    _tracked_signed = self._build_tracked_binance_signed().get(bn_symbol, Decimal('0'))
                    _target_signed = self.trade_amount if target_type == 'buy_future_sell_synthetic' else -self.trade_amount
                    if _tracked_signed != 0 and ((_tracked_signed > 0) != (_target_signed > 0)):
                        logger.warning(
                            f"{log_prefix}放弃: Binance {bn_symbol} 已有反向净仓 {_tracked_signed:+f}，"
                            f"目标方向 {_target_signed:+f}，为避免单向模式错配，跳过本次开仓")
                        return

                # 1. 确定 Taker 腿方向
                c_spread = call_ticker.ask - call_ticker.bid if call_ticker.ask > 0 and call_ticker.bid > 0 else Decimal('999')
                p_spread = put_ticker.ask - put_ticker.bid if put_ticker.ask > 0 and put_ticker.bid > 0 else Decimal('999')
                t_opt_ticker = put_ticker if c_spread >= p_spread else call_ticker
                t_opt_side = 'sell' if (target_type == 'sell_future_buy_synthetic' and t_opt_ticker == put_ticker) or \
                                       (target_type == 'buy_future_sell_synthetic' and t_opt_ticker == call_ticker) else 'buy'
                t_opt_name = combination['put'] if t_opt_ticker == put_ticker else combination['call']

                # 2. 期权 Taker 腿累积深度 VWAP（替代旧的单档 1.2x 检查）
                t_opt_vwap = await self._calculate_vwap(t_opt_name, t_opt_side, self.trade_amount)
                if t_opt_vwap is None:
                    logger.info(f"{log_prefix}放弃: 期权Taker腿 {t_opt_name} 累积深度不足 {self.trade_amount} {self.target_currency}")
                    return

                # 3. 跨所: Binance 期货价格作为 VWAP (Binance 深度足够，直接用盘口价)
                vwap_f_price = bn_bid if target_type == 'sell_future_buy_synthetic' else bn_ask

                # 4. 联合重模拟：期货 VWAP + 期权 Taker VWAP（Maker 腿保持挂单价）
                if t_opt_ticker == call_ticker:
                    vwap_call = t_opt_vwap
                    vwap_put = exec_params['put_price']
                else:
                    vwap_call = exec_params['call_price']
                    vwap_put = t_opt_vwap

                # 🌟 Bug#19 修复：用 VWAP 期货价格重算合约数量，避免模拟使用过时的 future_amount_usd
                future_amount_usd = (self.trade_amount * vwap_f_price / contract_size).quantize(
                    Decimal('1'), rounding='ROUND_HALF_UP') * contract_size

                final_sim_res = await self.trade_executor.simulate_trade(
                    strategy_type=target_type,
                    future_price=vwap_f_price,
                    call_price=vwap_call,
                    put_price=vwap_put,
                    strike=strike,
                    future_amount_usd=future_amount_usd,
                    option_btc_amount=self.trade_amount
                )
                _vwap_settle_fee_usd = self._estimate_round_settle_fee_usd(
                    bn_ob.mid_price, vwap_call, vwap_put, self.trade_amount)
                _vwap_taker_option_price = t_opt_vwap
                _vwap_maker_option_price = vwap_put if t_opt_ticker == call_ticker else vwap_call
                _vwap_maker_open_fee_usd = self._estimate_scan_maker_open_fee_usd(
                    vwap_f_price, _vwap_taker_option_price, self.trade_amount,
                    option_maker_price=_vwap_maker_option_price)
                _vwap_reference_net_profit = (
                    Decimal(str(final_sim_res['net_profit'])) -
                    _vwap_settle_fee_usd - _selected_funding_usd)
                vwap_net_profit = (
                    Decimal(str(final_sim_res.get('gross_profit_usd', final_sim_res.get('gross_profit', 0)))) -
                    _vwap_maker_open_fee_usd - _vwap_settle_fee_usd - _selected_funding_usd)

                if vwap_net_profit < _verify_threshold:
                    logger.info(
                        f"{log_prefix}放弃: VWAP穿透测试失败。滑点将利润从 {final_net_profit:.2f} "
                        f"拖至 {vwap_net_profit:.2f} (保守口径 {_vwap_reference_net_profit:.2f})")
                    return

                # 5. 通过！更新为 VWAP 验证后的真实预期利润
                final_net_profit = vwap_net_profit
                final_sim_res['maker_net_profit'] = float(vwap_net_profit)
                final_sim_res['maker_open_fee_usd'] = float(_vwap_maker_open_fee_usd)
                final_sim_res['reference_net_profit'] = float(_vwap_reference_net_profit)
                sim_res = final_sim_res
                _selected_settle_fee_usd = _vwap_settle_fee_usd
                exec_params['future_price'] = vwap_f_price
                if t_opt_ticker == call_ticker:
                    exec_params['call_price'] = t_opt_vwap
                else:
                    exec_params['put_price'] = t_opt_vwap
                logger.info(f"{log_prefix}VWAP验证通过: Maker口径预期净利润 {final_net_profit:.2f} USD "
                            f"(保守口径 {_vwap_reference_net_profit:.2f}, "
                            f"期货VWAP={vwap_f_price}, 期权Taker VWAP={t_opt_vwap})")
            # =======================================================================

                # ================= 🚨 终极防双开锁：执行前最后一次原子确认 =================
                ## 在经历了耗时的 VWAP 计算和各种 await 之后，有可能其他协程已经抢先建仓
                # 所以在发射真金白银的前一毫秒，只需校验【真实持仓锁】即可！(绝对不能再查 processing_lock，因为那是当前任务自己加的)
                if self._check_position_lock(expiry, strike):
                    logger.warning(f"{log_prefix} 🚨 拦截到并发冲突！当前组合在测算期间已被建仓，终止发射！")
                    return
                # =========================================================================

                # ================= 保证金预检：防止 Anchor 成交后 Taker 被拒 =================
                # --- Deribit 保证金预检 ---
                try:
                    _margin_resp = await self.client.send_request({
                        "jsonrpc": "2.0",
                        "id": self.client._get_next_request_id(),
                        "method": "private/get_account_summary",
                        "params": {"currency": self.target_currency}
                    }, is_private=True)
                    if 'result' in _margin_resp:
                        _avail = Decimal(str(_margin_resp['result'].get('available_funds', 0)))
                        _equity = Decimal(str(_margin_resp['result'].get('equity', 0)))
                        _init_margin = Decimal(str(_margin_resp['result'].get('initial_margin', 0)))
                        # PM 模式下 short option 保证金较高，用 15% 期权 + 3% 期货 + 2x 缓冲
                        _opt_notional = self.trade_amount
                        _fut_notional = future_amount_usd / (bn_ask if bn_ask > 0 else Decimal('67000'))
                        _est_margin = _opt_notional * Decimal('0.15') + _fut_notional * Decimal('0.03')
                        _margin_buffer = _est_margin * Decimal('2.0')

                        if _avail < _margin_buffer:
                            logger.error(
                                f"{log_prefix}🚨 [Deribit 保证金不足] 可用: {_avail:.4f} "
                                f"< 所需: {_margin_buffer:.4f}（含 2x 缓冲）| 权益: {_equity:.4f} | 已用: {_init_margin:.4f}")
                            # 保证金不足 → 撤单 + 清裸腿 + 暂停系统
                            asyncio.create_task(self._margin_emergency_shutdown(
                                "Deribit",
                                f"可用: {_avail:.4f} {self.target_currency} < 所需: {_margin_buffer:.4f}"))
                            return
                        logger.info(f"{log_prefix}Deribit 保证金预检通过: 可用={_avail:.4f} 所需={_margin_buffer:.4f}")
                except Exception as _me:
                    # 🌟 P2-6 修复: 预检失败时阻塞开仓 (保守策略)
                    # 若 Deribit 会拒单则无害, 但若此时 Binance 对冲已执行则产生裸腿
                    logger.warning(f"{log_prefix}Deribit 保证金预检失败，跳过本次开仓: {_me}")
                    return

                # --- Binance 余额预检（含并发预留） ---
                if bn_symbol and self.binance_ws and self.binance_connected:
                    try:
                        _bn_acct = await self.binance_ws.get_account_info()
                        if _bn_acct:
                            _bn_avail = Decimal(str(_bn_acct.get('availableBalance', '0')))
                            _bn_total = Decimal(str(_bn_acct.get('totalMarginBalance', '0')))
                            _bn_price = bn_ask if bn_ask > 0 else Decimal('67000')
                            _bn_needed = self.trade_amount * _bn_price * Decimal('0.05') * Decimal('2.0')
                            _bn_effective = _bn_avail - self._bn_reserved_margin

                            if _bn_avail <= 0:
                                logger.warning(f"{log_prefix}⚠️ Binance 余额查询返回异常值: {_bn_avail}，跳过本组合")
                                return
                            if _bn_avail < _bn_needed:
                                # 真实余额不足 → 系统级紧急停机
                                logger.error(
                                    f"{log_prefix}🚨 [Binance 余额不足] 可用: {_bn_avail:.2f} USDT "
                                    f"< 所需: {_bn_needed:.2f} | 总余额: {_bn_total:.2f}")
                                asyncio.create_task(self._margin_emergency_shutdown(
                                    "Binance",
                                    f"可用: {_bn_avail:.2f} USDT < 所需: {_bn_needed:.2f} USDT"))
                                return
                            if _bn_effective < _bn_needed:
                                # 真实余额够但并发预留占满 → 仅跳过本次，不 shutdown
                                logger.warning(
                                    f"{log_prefix}⚠️ [Binance 并发预留] 可用: {_bn_avail:.2f} "
                                    f"- 已预留: {self._bn_reserved_margin:.2f} = 有效: {_bn_effective:.2f} USDT "
                                    f"< 所需: {_bn_needed:.2f}，跳过本组合")
                                return
                            self._bn_reserved_margin += _bn_needed
                            _bn_margin_reserved = True
                            logger.info(f"{log_prefix}Binance 余额预检通过: 有效={_bn_effective:.2f} "
                                        f"所需={_bn_needed:.2f} 预留后总占={self._bn_reserved_margin:.2f}")
                        else:
                            logger.warning(f"{log_prefix}Binance 余额预检无有效响应，跳过本次开仓")
                            return
                    except Exception as _bme:
                        logger.warning(f"{log_prefix}Binance 余额预检失败，跳过本次开仓: {_bme}")
                        return
                # =========================================================================

                # 原有代码：加上处理锁
                _bn_final_ready, _bn_final_reason = self._binance_market_ready(
                    bn_symbol, max_age_sec=5.0,
                    orderbook_max_age_sec=5.0, mark_max_age_sec=10.0, last_max_age_sec=20.0)
                if not _bn_final_ready:
                    logger.warning(f"{log_prefix}Binance 最终门禁失败 ({_bn_final_reason})，取消开仓")
                    return

                state = ArbitrageState(expiry_strike=expiry_strike, state='executing', strategy_type=target_type)
                self.arbitrage_states[expiry_strike] = state

                trade_result = await self.trade_executor.execute_arbitrage_trade(
                    strategy_type=target_type,
                    future_symbol=combination['future'],
                    call_symbol=combination['call'],
                    put_symbol=combination['put'],
                    strike=strike,
                    btc_amount=self.trade_amount,
                    future_price=exec_params['future_price'],
                    call_price=exec_params['call_price'],
                    put_price=exec_params['put_price'],
                    is_maker_anchor=False,
                    max_wait_time=self.max_wait_time,
                    min_profit_threshold=_verify_threshold,
                    post_anchor_min_profit_usd=getattr(self, 'post_anchor_min_profit_usd', Decimal('12')),
                    rollback_ioc_aggressive_ticks=getattr(self, 'rollback_ioc_aggressive_ticks', 100),
                    log_prefix=log_prefix,
                    binance_symbol=bn_symbol,
                    binance_future_type=bn_type,
                    funding_deduction_usd=_selected_funding_usd,
                )

                if trade_result.get('success'):
                    self.trades_executed += 1
                    logger.info(f"{log_prefix}✅ 并发交易成功: {expiry_strike} ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ")
                    # 注意：此时仅 Deribit 三腿执行成功，Binance 对冲尚未确认。
                    # 状态保持 executing，待 Binance 对冲确认后再切换为 position_open，
                    # 避免"未完成对冲"提前落盘为已开仓。
                    state.state = 'executing'
                    state.last_update = time.time()
                    # ====== 【新增】记录该篮子的独立开仓成本 ======
                    state.entry_amount = self.trade_amount
                    state.future_size_usd = future_amount_usd  # <--- 锁定该笔套利专属的期货面值
                    state.combo_id = f"{expiry}-{strike}-{int(time.time())}"  # 🌟 本地订单ID
                    state.entry_prices = {
                        'future': exec_params['future_price'],  # 暂用预期价，后续可替换为查单后的真实均价
                        'call': exec_params['call_price'],
                        'put': exec_params['put_price']
                    }
                    state.prices_confirmed = False  # 标记：预期价未确认，禁止平仓决策
                    # ===== 跨所: 保存 Binance 对冲信息到状态 =====
                    state.binance_future_symbol = bn_symbol
                    state.binance_future_type = bn_type
                    _bn_position_side = 'LONG' if target_type == "buy_future_sell_synthetic" else 'SHORT'
                    state.binance_position_side = _bn_position_side if self.binance_dual_side_mode else ''
                    if trade_result.get('binance_order_id'):
                        self._capture_binance_open_ids(state, trade_result['binance_order_id'])
                        state.binance_entry_price = trade_result.get('binance_entry_price', Decimal('0'))
                        state.binance_filled_qty = trade_result.get('binance_filled_qty', Decimal('0'))
                        if state.binance_filled_qty > 0:
                            state.binance_open_qty = state.binance_filled_qty
                    self.position_locks.add(expiry_strike)
                    # ================= 🌟 新增：先按 executing 记录到 Redis，防止崩溃丢单 =================
                    await self._save_state_to_redis(state)
                    # =================================================================

                    # ===== 跨所: Binance 对冲 + 关闭 Deribit 期货 =====
                    _bn_hedge_ok = False  # 提前初始化，成交对账需要判断
                    _bn_recheck_failed = False
                    if bn_symbol and self.binance_executor and self.binance_connected:
                        # 二次余额预检：anchor 成交后、hedge 前再查一次，捕获预检→执行间的余额变化
                        try:
                            _bn_acct2 = await self.binance_ws.get_account_info()
                            if _bn_acct2:
                                _bn_avail2 = Decimal(str(_bn_acct2.get('availableBalance', '0')))
                                _bn_price2 = bn_ask if bn_ask > 0 else Decimal('75000')
                                _bn_needed2 = self.trade_amount * _bn_price2 * Decimal('0.05') * Decimal('2.0')
                                if _bn_avail2 < _bn_needed2:
                                    logger.error(
                                        f"{log_prefix}🚨 [Binance 二次预检] 余额不足: 可用={_bn_avail2:.2f} "
                                        f"< 所需={_bn_needed2:.2f}，跳过对冲走回滚路径")
                                    asyncio.create_task(tg_notifier.send_error_async(
                                        f"🚨 {log_prefix} Binance 二次预检余额不足 "
                                        f"(可用={_bn_avail2:.2f} < 需={_bn_needed2:.2f})，回滚期权",
                                        "hedge_margin_recheck"))
                                    _bn_recheck_failed = True
                                else:
                                    logger.info(f"{log_prefix}Binance 二次预检通过: 可用={_bn_avail2:.2f} 所需={_bn_needed2:.2f}")
                        except Exception as _recheck_e:
                            logger.warning(f"{log_prefix}Binance 二次预检异常（不阻塞对冲）: {_recheck_e}")

                    if bn_symbol and self.binance_executor and self.binance_connected and not _bn_recheck_failed:
                        # 1. 在 Binance 开期货对冲
                        _bn_side = "SELL" if target_type == "sell_future_buy_synthetic" else "BUY"
                        try:
                            _bn_result = await self.binance_executor.hedge_order(
                                symbol=bn_symbol,
                                side=_bn_side,
                                quantity=self.trade_amount,
                                order_type=self.binance_hedge_order_type,
                                max_slippage_usd=self.binance_max_slippage_usd,
                                position_side=state.binance_position_side or None,
                            )
                            # 兼容 IOC 语义: EXPIRED + executedQty>0 等价于部分成交
                            if _bn_result:
                                _st = str(_bn_result.get('status', '')).upper()
                                _ex = Decimal(str(_bn_result.get('executedQty', '0')))
                                if _st == 'EXPIRED' and _ex > 0:
                                    _bn_result = dict(_bn_result)
                                    _bn_result['status'] = 'PARTIALLY_FILLED'
                                    logger.warning(
                                        f"{log_prefix}⚠️ Binance 返回 EXPIRED 但已成交 {_ex}，"
                                        f"按 PARTIALLY_FILLED 继续处理")
                            # 🌟 防御性查单: 如果 REST 返回非 FILLED (如 NEW/PARTIALLY_FILLED)，
                            # 等待后通过查单 API 确认最终状态，避免市价单已成交但被误判为失败
                            if _bn_result and _bn_result.get('status') not in ('FILLED', 'PARTIALLY_FILLED'):
                                _bn_oid = _bn_result.get('orderId')
                                if _bn_oid:
                                    logger.info(f"{log_prefix}⏳ Binance 订单状态={_bn_result.get('status')}，等待确认中...")
                                    await asyncio.sleep(2)
                                    _query = await self.binance_executor.ws_client._rest_request(
                                        "GET", "/fapi/v1/order",
                                        {"symbol": bn_symbol, "orderId": _bn_oid}, signed=True)
                                    if _query:
                                        _bn_result = _query
                                        _q_st = str(_bn_result.get('status', '')).upper()
                                        _q_ex = Decimal(str(_bn_result.get('executedQty', '0')))
                                        if _q_st == 'EXPIRED' and _q_ex > 0:
                                            _bn_result = dict(_bn_result)
                                            _bn_result['status'] = 'PARTIALLY_FILLED'
                                        logger.info(f"{log_prefix}🔍 查单确认: 状态={_query.get('status')} 均价={_query.get('avgPrice')}")

                            if _bn_result and _bn_result.get('status') == 'FILLED':
                                _bn_avg = Decimal(str(_bn_result.get('avgPrice', '0')))
                                _bn_filled = Decimal(str(_bn_result.get('executedQty', '0')))
                                logger.info(f"{log_prefix}✅ Binance 对冲全额成交: {_bn_side} {_bn_filled} {bn_symbol} @ {_bn_avg}")
                                self._capture_binance_open_ids(state, _bn_result)
                                state.binance_entry_price = _bn_avg
                                state.binance_filled_qty = _bn_filled
                                state.binance_open_qty = _bn_filled
                                if _bn_avg > 0 and _bn_filled > 0:
                                    state.future_size_usd = (_bn_avg * _bn_filled).quantize(Decimal('0.01'))
                                _bn_hedge_ok = True
                            elif _bn_result and _bn_result.get('status') == 'PARTIALLY_FILLED':
                                # 部分成交 → 追单剩余量，防止裸腿敞口
                                _bn_filled_1 = Decimal(str(_bn_result.get('executedQty', '0')))
                                _bn_avg_1 = Decimal(str(_bn_result.get('avgPrice', '0')))
                                _bn_remaining = self.trade_amount - _bn_filled_1
                                logger.warning(f"{log_prefix}⚠️ Binance 对冲部分成交: {_bn_filled_1}/{self.trade_amount}，追单剩余 {_bn_remaining}")
                                _bn_info = getattr(self.binance_executor, 'contract_info', {}).get(bn_symbol, {})
                                _bn_step = Decimal(str(_bn_info.get('step_size', '0.001')))
                                _bn_min_qty = Decimal(str(_bn_info.get('min_qty', _bn_step if _bn_step > 0 else Decimal('0.001'))))
                                _bn_remaining_floor = max(_bn_min_qty, self.trade_amount * Decimal('0.01'))
                                if _bn_remaining >= _bn_remaining_floor:
                                    _retry = await self.binance_executor.place_market_order(
                                        bn_symbol, _bn_side, _bn_remaining,
                                        reduce_only=False, position_side=state.binance_position_side or None)
                                    if _retry and _retry.get('status') == 'FILLED':
                                        _bn_filled_2 = Decimal(str(_retry.get('executedQty', '0')))
                                        _bn_avg_2 = Decimal(str(_retry.get('avgPrice', '0')))
                                        _bn_total = _bn_filled_1 + _bn_filled_2
                                        _bn_wavg = (_bn_avg_1 * _bn_filled_1 + _bn_avg_2 * _bn_filled_2) / _bn_total if _bn_total > 0 else _bn_avg_1
                                        self._capture_binance_open_ids(state, _bn_result, _retry)
                                        state.binance_entry_price = _bn_wavg
                                        state.binance_filled_qty = _bn_total
                                        state.binance_open_qty = _bn_total
                                        if _bn_total > 0 and _bn_wavg > 0:
                                            state.future_size_usd = (_bn_total * _bn_wavg).quantize(Decimal('0.01'))
                                        _bn_hedge_ok = True
                                        logger.info(f"{log_prefix}✅ Binance 追单成功: 总计 {_bn_total} @ 加权均价 {_bn_wavg:.2f}")
                                    else:
                                        # 追单失败/部分成交 → 关闭所有已成交量，标记失败
                                        _retry_filled_partial = Decimal(str(_retry.get('executedQty', '0'))) if _retry else Decimal('0')
                                        _total_to_close = _bn_filled_1 + _retry_filled_partial
                                        # 保存已成交总量，供后续回滚兜底继续平仓
                                        self._capture_binance_open_ids(state, _bn_result, _retry)
                                        state.binance_entry_price = _bn_avg_1
                                        state.binance_filled_qty = _total_to_close
                                        state.binance_open_qty = _total_to_close
                                        logger.error(f"{log_prefix}❌ Binance 追单失败，关闭全部已成交 {_total_to_close} (首笔{_bn_filled_1}+追单{_retry_filled_partial}) 并回滚")
                                        _close_side = "BUY" if _bn_side == "SELL" else "SELL"
                                        if _total_to_close > Decimal('0'):
                                            _close_res = await self.binance_executor.place_market_order(
                                                bn_symbol, _close_side, _total_to_close, reduce_only=True,
                                                position_side=state.binance_position_side or None)
                                            if _close_res and _close_res.get('status') == 'FILLED':
                                                state.binance_filled_qty = Decimal('0')
                                                state.binance_open_qty = Decimal('0')
                                            elif _close_res and _close_res.get('status') == 'PARTIALLY_FILLED':
                                                _closed_qty = Decimal(str(_close_res.get('executedQty', '0')))
                                                state.binance_filled_qty = max(_total_to_close - _closed_qty, Decimal('0'))
                                                state.binance_open_qty = state.binance_filled_qty
                                        asyncio.create_task(tg_notifier.send_error_async(
                                            f"🚨 {log_prefix} Binance 对冲失败，已反向关闭 {_total_to_close}", "hedge_partial_rollback"))
                                else:
                                    # 剩余量极小（小于交易所最小单位或小于本单1%），接受部分成交
                                    self._capture_binance_open_ids(state, _bn_result)
                                    state.binance_entry_price = _bn_avg_1
                                    state.binance_filled_qty = _bn_filled_1
                                    state.binance_open_qty = _bn_filled_1
                                    if _bn_avg_1 > 0 and _bn_filled_1 > 0:
                                        state.future_size_usd = (_bn_avg_1 * _bn_filled_1).quantize(Decimal('0.01'))
                                    _bn_hedge_ok = True
                                    logger.info(
                                        f"{log_prefix}✅ Binance 部分成交 {_bn_filled_1}，"
                                        f"剩余 {_bn_remaining} < 阈值 {_bn_remaining_floor}，按最小残量忽略")
                            else:
                                logger.error(f"{log_prefix}❌ Binance 对冲失败 (状态={_bn_result.get('status') if _bn_result else 'None'})")
                        except Exception as _bn_err:
                            logger.error(f"{log_prefix}❌ Binance 对冲异常: {_bn_err}")
                    elif bn_symbol and not _bn_recheck_failed:
                        # Binance 断连/executor 不可用 → 跨所模式无法对冲
                        logger.error(f"{log_prefix}❌ Binance 不可用 (executor={bool(self.binance_executor)}, "
                                    f"connected={self.binance_connected})，跨所对冲无法执行")

                    # 🌟 安全护栏（移到条件块外部）: 跨所模式下 Deribit 期货已跳过,
                    # 如果 Binance 对冲也失败(含断连跳过), 期权裸露无对冲 → 必须回滚
                    if bn_symbol and not _bn_hedge_ok:
                        logger.error(f"{log_prefix}🚨 Binance 对冲失败且无 Deribit 期货后备, 回滚 Deribit 期权!")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 {log_prefix} Binance 对冲失败, 紧急回滚期权", "hedge_fail_rollback"))
                        # 根据 Binance 错误码判断失败原因
                        _bn_err_code = getattr(self.binance_executor, '_last_order_error', None)
                        if _bn_err_code == -2019:
                            # -2019 = Margin is insufficient → 持久暂停，必须充值后 /start
                            if not getattr(self, '_margin_shutdown_active', False):
                                asyncio.create_task(self._margin_emergency_shutdown(
                                    "Binance", f"对冲失败: -2019 保证金不足"))
                        else:
                            # 其他原因（网络/API限频/滑点等）→ 临时暂停60秒后自动恢复
                            self._add_pause("Binance对冲临时失败")
                            asyncio.create_task(self._hedge_fail_auto_recover(log_prefix))
                        if self._is_deribit_settlement_core_window():
                            self._add_pause("结算窗口")
                            self._add_pause("锚定腿回滚失败")
                            state.state = 'failed'
                            state.last_update = time.time()
                            await self._save_state_to_redis(state)
                            self.position_locks.discard(expiry_strike)
                            logger.warning(
                                f"{log_prefix}⏸️ core_settlement_binance_hedge_rollback_deferred: "
                                f"Binance 对冲失败但 Deribit core window 已开始，"
                                f"不发送期权回滚单，窗口结束后由 ghost/integrity 重新对账处理")
                            return
                        try:
                            _combo = self.arbitrage_combinations.get(expiry_strike)
                            if _combo:
                                _c_pos = self.client.positions.get(_combo['call'])
                                _p_pos = self.client.positions.get(_combo['put'])
                                _c_ok = True
                                _p_ok = True
                                if _c_pos and _c_pos.size != 0:
                                    _c_close = 'sell' if _c_pos.size > 0 else 'buy'
                                    _c_order = await self.client.place_order(
                                        _combo['call'], abs(_c_pos.size), _c_close,
                                        'market', reduce_only=True, log_prefix=f"{log_prefix}[回滚]")
                                    _c_ok = _c_order is not None
                                if _p_pos and _p_pos.size != 0:
                                    _p_close = 'sell' if _p_pos.size > 0 else 'buy'
                                    _p_order = await self.client.place_order(
                                        _combo['put'], abs(_p_pos.size), _p_close,
                                        'market', reduce_only=True, log_prefix=f"{log_prefix}[回滚]")
                                    _p_ok = _p_order is not None
                                if _c_ok and _p_ok:
                                    logger.info(f"{log_prefix}✅ Deribit 期权已回滚")
                                else:
                                    _failed_legs = []
                                    if not _c_ok:
                                        _failed_legs.append('Call')
                                    if not _p_ok:
                                        _failed_legs.append('Put')
                                    logger.error(
                                        f"{log_prefix}❌ 期权回滚下单失败: {', '.join(_failed_legs)}, "
                                        f"等待幽灵检测兜底")
                        except Exception as _rb_err:
                            logger.error(f"{log_prefix}❌ 期权回滚失败: {_rb_err}, 需人工处理!")
                        # 再次兜底：若 Binance 仍有残余仓位，循环重试平仓并保留状态追踪
                        _bn_residual_threshold = Decimal('0.0001')
                        if state.binance_future_symbol and state.binance_filled_qty > _bn_residual_threshold:
                            logger.warning(
                                f"{log_prefix}⚠️ 回滚后检测到 Binance 残余仓位: {state.binance_future_symbol} "
                                f"qty={state.binance_filled_qty}，启动兜底平仓")
                            for _bn_try in range(3):
                                _rb_bn_res = await self._close_binance_hedge(state, emergency=True)
                                # 🌟 P2 回归修复: 开仓回滚残余平仓的 close IDs 也记录, 保持 state 审计完整
                                if _rb_bn_res:
                                    self._capture_binance_close_ids(state, _rb_bn_res)
                                if state.binance_filled_qty <= _bn_residual_threshold:
                                    break
                                await asyncio.sleep(1)
                            if state.binance_filled_qty > _bn_residual_threshold:
                                self._add_pause("Binance残余仓位")
                                asyncio.create_task(tg_notifier.send_error_async(
                                    f"🚨 {log_prefix} Binance 残余仓位未清空!\n"
                                    f"合约: {state.binance_future_symbol}\n"
                                    f"剩余数量: {state.binance_filled_qty}\n"
                                    f"系统已自动暂停，请人工介入处理。", "binance_residual_risk"))
                        state.state = 'failed'
                        # 仅在确认 Binance 已无残余仓位时清空标识，防止失联
                        if state.binance_filled_qty <= Decimal('0.0001'):
                            state.binance_future_symbol = ''
                            state.binance_future_type = ''
                            state.binance_position_side = ''
                            state.binance_open_qty = Decimal('0')
                        self.position_locks.discard(expiry_strike)

                    # Binance 对冲确认成功后，才标记为 position_open
                    if state.state != 'failed':
                        state.state = 'position_open'
                        state.last_update = time.time()
                        # 记录开仓时基差 (Binance成交价 - Deribit指数)，用于监控基差恶化
                        try:
                            _idx_r = await self.client.send_request({
                                "jsonrpc": "2.0",
                                "id": self.client._get_next_request_id(),
                                "method": "public/get_index_price",
                                "params": {"index_name": f"{self.target_currency.lower()}_usd"}
                            })
                            _idx_p = Decimal(str(_idx_r['result'].get('index_price', 0))) if _idx_r and 'result' in _idx_r else Decimal('0')
                            if _idx_p > 0 and state.binance_entry_price > 0:
                                state.entry_basis_usd = float(state.binance_entry_price - _idx_p)
                                logger.info(f"{log_prefix}📈 开仓基差记录: Binance={state.binance_entry_price:.1f} - 指数={_idx_p:.1f} = {state.entry_basis_usd:+.1f} USD")
                        except Exception as _eb_err:
                            logger.info(f"{log_prefix}开仓基差获取失败: {_eb_err}")
                            state.entry_basis_usd = None
                    await self._save_state_to_redis(state)

                    # 回滚后跳过成交对账（期权已平、无需记录）
                    if state.state == 'failed':
                        return

                    _rec_settle_fee = 0  # 预初始化，防止异常路径 finally 兜底时未定义
                    _rec_funding_fee = float(_selected_funding_usd)  # funding 预扣与主路径/兜底路径统一
                    try:
                        await asyncio.sleep(1.5)
                        order_ids = trade_result.get('orders', [])
                        if len(order_ids) == 3:
                            call_id, put_id, future_id = order_ids
                            _anchor_type = str(trade_result.get('anchor_type', '') or '').lower()
                            _anchor_order_ids = [
                                str(_oid).strip()
                                for _oid in (trade_result.get('anchor_order_ids') or [])
                                if str(_oid).strip()
                            ]
                            _anchor_order_id_str = ','.join(_anchor_order_ids)
                            try:
                                _anchor_weighted_avg = Decimal(str(
                                    trade_result.get('anchor_weighted_avg', '0') or '0'))
                            except Exception:
                                _anchor_weighted_avg = Decimal('0')
                            # 🌟 P1-17: 保存 Deribit 期权 order_id 到 state, 供结算/紧急审计
                            try:
                                state.call_order_id = (
                                    _anchor_order_id_str
                                    if _anchor_type == 'call' and _anchor_order_id_str
                                    else str(call_id or '')
                                )
                                state.put_order_id = (
                                    _anchor_order_id_str
                                    if _anchor_type == 'put' and _anchor_order_id_str
                                    else str(put_id or '')
                                )
                            except Exception:
                                pass
                            c_order, p_order, f_order = self.client.get_order_by_id(
                                call_id), self.client.get_order_by_id(put_id), self.client.get_order_by_id(future_id)

                            actual_c_price = c_order.average_price if c_order and c_order.average_price > 0 else \
                            exec_params['call_price']
                            actual_p_price = p_order.average_price if p_order and p_order.average_price > 0 else \
                            exec_params['put_price']
                            if _anchor_weighted_avg > 0:
                                if _anchor_type == 'call':
                                    actual_c_price = _anchor_weighted_avg
                                    logger.info(
                                        f"{log_prefix}📊 Anchor Call 聚合均价覆盖: "
                                        f"{_anchor_order_id_str or call_id} -> {actual_c_price}")
                                elif _anchor_type == 'put':
                                    actual_p_price = _anchor_weighted_avg
                                    logger.info(
                                        f"{log_prefix}📊 Anchor Put 聚合均价覆盖: "
                                        f"{_anchor_order_id_str or put_id} -> {actual_p_price}")
                            actual_f_price = f_order.average_price if f_order and f_order.average_price > 0 else \
                            exec_params['future_price']

                            # 🌟 跨所修复: Binance 对冲成功时，用 Binance 成交价替代 Deribit 远期价
                            # Deribit 期货已被平仓(由 Binance 替代)，实际对冲成本是 Binance 价格
                            if _bn_hedge_ok and state.binance_entry_price > 0:
                                logger.info(f"{log_prefix}📊 对账价格源: Binance {state.binance_entry_price} (替代 Deribit {actual_f_price})")
                                actual_f_price = state.binance_entry_price

                            # ====== 用真实成交价覆盖账本，确保后续平仓计算 100% 绝对精准 ======
                            state.entry_prices = {
                                'future': actual_f_price,
                                'call': actual_c_price,
                                'put': actual_p_price
                            }
                            state.prices_confirmed = True  # 真实成交价已确认，允许平仓决策
                            # ================= 🌟 新增：拿到真实均价后，覆盖更新 Redis =================
                            await self._save_state_to_redis(state)
                            # =======================================================================

                            actual_res = await self.trade_executor.simulate_trade(
                                strategy_type=target_type, future_price=actual_f_price, call_price=actual_c_price,
                                put_price=actual_p_price, strike=strike, future_amount_usd=future_amount_usd,
                                option_btc_amount=self.trade_amount
                            )

                            # 生成策略方向的中文描述
                            if target_type == 'sell_future_buy_synthetic':
                                strategy_desc = "卖期货 + 买Call + 卖Put"
                                actual_premium = actual_c_price - actual_p_price
                                actual_synthetic = (actual_premium * actual_f_price) + strike
                                actual_spread = actual_f_price - actual_synthetic
                            else:
                                strategy_desc = "买期货 + 卖Call + 买Put"
                                actual_premium = actual_c_price - actual_p_price
                                actual_synthetic = (actual_premium * actual_f_price) + strike
                                actual_spread = actual_synthetic - actual_f_price

                            _f_exchange = "Binance" if _bn_hedge_ok else "Deribit"
                            _bn_perp_sym = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
                            # 预估结算费+funding 提前计算，日志/记录/警告统一使用全成本口径
                            _rec_settle_fee = float(self._estimate_round_settle_fee_usd(
                                actual_f_price, actual_c_price, actual_p_price, self.trade_amount))
                            if _rec_settle_fee <= 0:
                                _rec_settle_fee = float(_selected_settle_fee_usd)
                            _rec_funding_fee = float(_selected_funding_usd)
                            self._attach_actual_maker_entry_metrics(
                                actual_res, actual_f_price, actual_c_price, actual_p_price,
                                _anchor_type, _rec_settle_fee, _rec_funding_fee)
                            _entry_metrics = self._entry_pnl_metrics(
                                sim_res, actual_res, _rec_settle_fee, _rec_funding_fee)
                            _actual_full_net = _entry_metrics['actual_full_net']
                            _sim_full_net = _entry_metrics['sim_full_net']
                            logger.info(
                                f"{log_prefix}{'='*50}\n"
                                f"{log_prefix}=== 成交确认 [{strategy_desc}] ===\n"
                                f"{log_prefix}  期权: Deribit {combination['call']} / {combination['put']}\n"
                                f"{log_prefix}  期货: {_f_exchange} {_bn_perp_sym if _bn_hedge_ok else combination['future']} | 面值: {future_amount_usd} USD\n"
                                f"{log_prefix}  数量: {self.trade_amount} {self.target_currency}\n"
                                f"{log_prefix}  ---- 三腿成交对账 ----\n"
                                f"{log_prefix}  期货({_f_exchange}): 预期={exec_params['future_price']} -> 实际={actual_f_price} (偏差={actual_f_price - exec_params['future_price']:.2f} USD)\n"
                                f"{log_prefix}  Call(Deribit):  预期={exec_params['call_price']} -> 实际={actual_c_price} (偏差={actual_c_price - exec_params['call_price']:.6f} BTC)\n"
                                f"{log_prefix}  Put(Deribit):   预期={exec_params['put_price']} -> 实际={actual_p_price} (偏差={actual_p_price - exec_params['put_price']:.6f} BTC)\n"
                                f"{log_prefix}  ---- 利润核算（全成本净利 = 开仓费 + 结算费 + funding 预扣） ----\n"
                                f"{log_prefix}  实际Premium(C-P): {actual_premium:.6f} BTC | 合成价: {actual_synthetic:.2f} USD | 价差: {actual_spread:.2f} USD\n"
                                f"{log_prefix}  全成本净利({_entry_metrics['basis']}): 模拟={_sim_full_net:.2f} -> 实际={_actual_full_net:.2f} USD | 滑点损失: {_entry_metrics['slippage_usd']:.2f} USD\n"
                                f"{log_prefix}  手续费: 开仓={_entry_metrics['actual_open_fee']:.2f} + 结算={_rec_settle_fee:.2f} + funding={_rec_funding_fee:.2f} = {_entry_metrics['actual_total_fee']:.2f} USD\n"
                                f"{log_prefix}{'='*50}"
                            )

                            _negative_action = str(getattr(self, 'post_fill_negative_action', 'hold')).lower()
                            if _negative_action not in ('hold', 'rollback'):
                                _negative_action = 'hold'
                            _negative_open = _actual_full_net < 0

                            # 🚨 开仓净利润复核：负净利默认告警继续持有；可配置为立即回滚
                            if _negative_open:
                                if _negative_action == 'rollback':
                                    logger.error(
                                        f"{log_prefix}🚨 开仓全成本净利为负 {_actual_full_net:.2f} USD，"
                                        f"按策略执行立即回滚")
                                    asyncio.create_task(tg_notifier.send_error_async(
                                        f"🚨 {log_prefix} 开仓净利为负!\n"
                                        f"全成本净利: {_actual_full_net:.2f} USD\n"
                                        f"动作: 立即回滚 (post_fill_negative_action=rollback)",
                                        "negative_open_rollback"))
                                else:
                                    logger.warning(f"{log_prefix}⚠️ 开仓全成本净利为负: {_actual_full_net:.2f} USD，将等待到期结算")
                                    asyncio.create_task(tg_notifier.send_error_async(
                                        f"⚠️ {log_prefix} 开仓净利为负!\n"
                                        f"全成本净利: {_actual_full_net:.2f} USD\n"
                                        f"开仓费: {_entry_metrics['actual_open_fee']:.2f} | 结算费预估: {_rec_settle_fee:.2f}\n"
                                        f"将继续持有等待到期结算", "negative_open_pnl"))

                            record = {
                                '订单ID': state.combo_id,
                                '成交时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                '策略方向': target_type, '到期日': expiry, '行权价': float(strike),
                                '标的': self.target_currency,
                                '期权数量': float(self.trade_amount), '期货面值(USD)': float(future_amount_usd),
                                '模拟_Future价格': float(exec_params['future_price']),
                                '实际_Future均价': float(actual_f_price),
                                '模拟_Call价格': float(exec_params['call_price']),
                                '实际_Call均价': float(actual_c_price),
                                '模拟_Put价格': float(exec_params['put_price']), '实际_Put均价': float(actual_p_price),
                                '模拟_手续费(USD)': _entry_metrics['sim_total_fee'],
                                '实际_手续费(USD)': _entry_metrics['actual_total_fee'],
                                '开仓手续费(USD)': round(_entry_metrics['actual_open_fee'], 4),
                                '预估结算手续费(USD)': round(float(_rec_settle_fee), 4),
                                '已实现funding(USD)': 0.0,
                                '模拟_净利润(USD)': _sim_full_net,
                                '实际_净利润(USD)': _actual_full_net,
                                '滑点与偏差损失(USD)': _entry_metrics['slippage_usd'],
                                'Call_ID': getattr(state, 'call_order_id', '') or call_id or 'UNCONFIRMED',
                                'Put_ID': getattr(state, 'put_order_id', '') or put_id or 'UNCONFIRMED',
                                # 🌟 P2 修复: 跨所模式下 Deribit 期货腿被跳过, future_id 常为空
                                # 应优先使用 Binance 开仓 orderId (state.binance_order_id 已在 hedge 流程设置)
                                'Future_ID': (future_id
                                              or self._format_future_id(state)
                                              or 'UNCONFIRMED'),
                                '交易类型': '开仓', '平仓原因': '',
                            }
                            await self._enqueue_trade_record(record)
                            state._record_written = True
                            if _negative_open and _negative_action == 'rollback':
                                _f_pos = self.client.positions.get(combination['future'])
                                _c_pos = self.client.positions.get(combination['call'])
                                _p_pos = self.client.positions.get(combination['put'])
                                await self._emergency_dump_all(state, combination, _f_pos, _c_pos, _p_pos)
                                return
                            asyncio.create_task(tg_notifier.send_async(
                                f"✅ 【套利开仓成功】\n"
                                f"期货: {combination['future']}\n"
                                f"Call: {combination['call']}\n"
                                f"Put: {combination['put']}\n"
                                f"全成本净利: {_actual_full_net:.2f} USD"
                            ))
                    except Exception as e:
                        logger.error(f"成交价确认出错: {e}")
                        # 确认失败时，降级等 5 秒后重试一次
                        try:
                            await asyncio.sleep(5)
                            order_ids = trade_result.get('orders', [])
                            if len(order_ids) == 3:
                                call_id, put_id, future_id = order_ids
                                _anchor_type = str(trade_result.get('anchor_type', '') or '').lower()
                                try:
                                    _anchor_weighted_avg = Decimal(str(
                                        trade_result.get('anchor_weighted_avg', '0') or '0'))
                                except Exception:
                                    _anchor_weighted_avg = Decimal('0')
                                c_order = self.client.get_order_by_id(call_id)
                                p_order = self.client.get_order_by_id(put_id)
                                f_order = self.client.get_order_by_id(future_id)
                                # 🌟 跨所修复: 跨所模式无 Deribit 期货订单，跳过 f_order 检查
                                _is_cross_exchange = _bn_hedge_ok and state.binance_entry_price > 0
                                _c_ok = c_order and c_order.average_price > 0
                                _p_ok = p_order and p_order.average_price > 0
                                _f_ok = (f_order and f_order.average_price > 0) if not _is_cross_exchange else True
                                if _c_ok and _p_ok and _f_ok:
                                    _retry_f_price = state.binance_entry_price if _is_cross_exchange else f_order.average_price
                                    _retry_c_price = c_order.average_price
                                    _retry_p_price = p_order.average_price
                                    if _anchor_weighted_avg > 0:
                                        if _anchor_type == 'call':
                                            _retry_c_price = _anchor_weighted_avg
                                        elif _anchor_type == 'put':
                                            _retry_p_price = _anchor_weighted_avg
                                    state.entry_prices = {
                                        'future': _retry_f_price,
                                        'call': _retry_c_price,
                                        'put': _retry_p_price
                                    }
                                    state.prices_confirmed = True
                                    await self._save_state_to_redis(state)
                                    logger.info(f"{log_prefix}成交价确认重试成功")
                        except Exception as retry_e:
                            logger.error(f"{log_prefix}成交价确认重试也失败: {retry_e}")
                    finally:
                        # 🌟 P1-18 修复: 成交价无法确认时, 不再强制 prices_confirmed=True
                        # 理由: 违反 CLAUDE.md "prices_confirmed 门禁" 约束
                        #       (成交价未确认前禁止平仓决策)
                        #       预期价 vs 实际价的偏差虽小, 但硬止损是绝对阈值判断,
                        #       错误基准 → 错误 PnL → 可能提前或滞后触发强平
                        # 改为: 保留 entry_prices 降级值 (供监控/Telegram 显示, 不做平仓决策)
                        #        prices_confirmed 保持 False, 下游硬止损/Gamma 按门禁约束跳过
                        #        启动后台 REST 重试 (每 60s 一次, 最多 10 次), 恢复后升级为 True
                        #        5 分钟仍失败 → Telegram 告警, 等到期结算 或 手动 /stop_all 兜底
                        if not state.prices_confirmed:
                            # 跨所模式优先用 Binance 实际均价补齐 future 入场价, 避免监控显示偏差
                            _fallback_f = state.entry_prices.get('future', exec_params.get('future_price', Decimal('0')))
                            if state.binance_entry_price > 0:
                                _fallback_f = state.binance_entry_price
                            state.entry_prices['future'] = _fallback_f
                            state.entry_prices.setdefault('call', exec_params.get('call_price', Decimal('0')))
                            state.entry_prices.setdefault('put', exec_params.get('put_price', Decimal('0')))
                            try:
                                _anchor_type_fb = str(trade_result.get('anchor_type', '') or '').lower()
                                _anchor_avg_fb = Decimal(str(
                                    trade_result.get('anchor_weighted_avg', '0') or '0'))
                                if _anchor_avg_fb > 0:
                                    if _anchor_type_fb == 'call':
                                        state.entry_prices['call'] = _anchor_avg_fb
                                    elif _anchor_type_fb == 'put':
                                        state.entry_prices['put'] = _anchor_avg_fb
                            except Exception:
                                pass
                            # 降级标记: 仅用于监控显示, 下游硬止损检查会跳过此仓位
                            state._entry_prices_estimated = True
                            logger.warning(
                                f"{log_prefix}⚠️ 成交价确认失败, 保持 prices_confirmed=False (门禁约束)\n"
                                f"  降级 future入场价={_fallback_f} (仅监控显示, 不做硬止损决策)\n"
                                f"  后台 REST 重试中, 如 5 分钟后仍失败请手动 /stop_all")
                            # 异步 Telegram 告警 + 后台重试升级
                            asyncio.create_task(tg_notifier.send_async(
                                f"⚠️ {state.combo_id} 成交价确认失败\n"
                                f"已降级监控显示但禁止硬止损 (门禁保护)\n"
                                f"后台将重试 REST 确认, 如 5 分钟后仍失败请 /stop_all\n"
                                f"或等待到期结算自动了结"))
                            asyncio.create_task(self._retry_confirm_entry_prices(
                                state, trade_result.get('orders', []),
                                exec_params, _bn_hedge_ok, trade_result))
                            await self._save_state_to_redis(state)
                        # 🌟 兜底: 无论成交价确认成功或降级，必须写入开仓记录
                        if state.state == 'position_open' and not getattr(state, '_record_written', False):
                            try:
                                _fb_f = state.entry_prices.get('future', exec_params.get('future_price', Decimal('0')))
                                _fb_c = state.entry_prices.get('call', exec_params.get('call_price', Decimal('0')))
                                _fb_p = state.entry_prices.get('put', exec_params.get('put_price', Decimal('0')))
                                # 修复: 主路径异常时 _rec_settle_fee 仍为0，用可用价格重新计算
                                if _rec_settle_fee == 0 and _fb_f > 0:
                                    _rec_settle_fee = float(self._estimate_round_settle_fee_usd(
                                        _fb_f, _fb_c, _fb_p, self.trade_amount))
                                _fb_res = await self.trade_executor.simulate_trade(
                                    strategy_type=target_type, future_price=_fb_f, call_price=_fb_c,
                                    put_price=_fb_p, strike=strike, future_amount_usd=future_amount_usd,
                                    option_btc_amount=self.trade_amount)
                                self._attach_actual_maker_entry_metrics(
                                    _fb_res, _fb_f, _fb_c, _fb_p,
                                    str(trade_result.get('anchor_type', '') or ''),
                                    _rec_settle_fee, _rec_funding_fee)
                                _fb_metrics = self._entry_pnl_metrics(
                                    sim_res, _fb_res, _rec_settle_fee, _rec_funding_fee)
                                _fb_sim_full_net = _fb_metrics['sim_full_net']
                                _fb_actual_full_net = _fb_metrics['actual_full_net']
                                _fb_record = {
                                    '订单ID': state.combo_id,
                                    '成交时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                    '策略方向': target_type, '到期日': expiry, '行权价': float(strike),
                                    '标的': self.target_currency,
                                    '期权数量': float(self.trade_amount), '期货面值(USD)': float(future_amount_usd),
                                    '模拟_Future价格': float(exec_params.get('future_price', 0)),
                                    '实际_Future均价': float(_fb_f),
                                    '模拟_Call价格': float(exec_params.get('call_price', 0)),
                                    '实际_Call均价': float(_fb_c),
                                    '模拟_Put价格': float(exec_params.get('put_price', 0)),
                                    '实际_Put均价': float(_fb_p),
                                    '模拟_手续费(USD)': _fb_metrics['sim_total_fee'],
                                    '实际_手续费(USD)': _fb_metrics['actual_total_fee'],
                                    '开仓手续费(USD)': round(_fb_metrics['actual_open_fee'], 4),
                                    '预估结算手续费(USD)': round(float(_rec_settle_fee), 4),
                                    '已实现funding(USD)': 0.0,
                                    '模拟_净利润(USD)': _fb_sim_full_net,
                                    '实际_净利润(USD)': _fb_actual_full_net,
                                    '滑点与偏差损失(USD)': _fb_metrics['slippage_usd'],
                                    'Call_ID': '', 'Put_ID': '', 'Future_ID': '',  # 占位, 下方 S1 修复会覆盖
                                    '交易类型': '开仓', '平仓原因': '',
                                }
                                # 🌟 S1 修复 (P1-17 回归补): 优先读 state.* (主路径已存),
                                # 否则读 trade_result (主路径早期异常时可能残缺),
                                # 最后用 'UNCONFIRMED' 占位保留审计可见性, 不写空字符串
                                def _pick_id(_state_val, _orders_list, _idx):
                                    if _state_val and str(_state_val).strip():
                                        return str(_state_val)
                                    try:
                                        _tr_val = (_orders_list or [])[_idx]
                                        if _tr_val and str(_tr_val).strip():
                                            return str(_tr_val)
                                    except (IndexError, TypeError):
                                        pass
                                    return 'UNCONFIRMED'
                                _tr_orders = trade_result.get('orders', []) if isinstance(trade_result, dict) else []
                                _fb_record['Call_ID'] = _pick_id(getattr(state, 'call_order_id', None), _tr_orders, 0)
                                _fb_record['Put_ID'] = _pick_id(getattr(state, 'put_order_id', None), _tr_orders, 1)
                                _fb_future_id = future_id or self._format_future_id(state)
                                if not _fb_future_id or _fb_future_id == 'UNCONFIRMED':
                                    _fb_future_id = _pick_id(None, _tr_orders, 2)
                                _fb_record['Future_ID'] = _fb_future_id
                                await self._enqueue_trade_record(_fb_record)
                                state._record_written = True
                                logger.info(f"{log_prefix}💾 兜底落盘成功 (降级价格)")
                            except Exception as _fb_e:
                                logger.error(f"{log_prefix}❌ 兜底落盘也失败: {_fb_e}")
                else:
                    state.state = 'failed'
            else:
                logger.info(f"{log_prefix}执行时机会消失: 降级排队也无法满足利润")

        except Exception as e:
            logger.error(f"并发任务异常 {expiry_strike}: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"{expiry_strike} -> {str(e)[:200]}", "task_exec_error"))
        finally:
            if _bn_margin_reserved and _bn_needed > 0:
                self._bn_reserved_margin = max(Decimal('0'), self._bn_reserved_margin - _bn_needed)
                logger.info(f"[{expiry_strike}] Binance 保证金预留释放: -{_bn_needed:.2f}，剩余预留: {self._bn_reserved_margin:.2f}")
            if expiry_strike in self.processing_opportunities:
                self.processing_opportunities.discard(expiry_strike)
            # 🌟 连续失败递增冷却：开仓成功时清零，失败时递增冷却
            _state = self.arbitrage_states.get(expiry_strike)
            if _state and _state.state in ('position_open', 'executing'):
                # 开仓成功 → 清除冷却
                self._combo_fail_count.pop(expiry_strike, None)
                self._combo_cooldown_until.pop(expiry_strike, None)
            else:
                # 未开仓（机会消失/VWAP失败/超时撤单）→ 递增冷却
                _fails = self._combo_fail_count.get(expiry_strike, 0) + 1
                self._combo_fail_count[expiry_strike] = _fails
                # 冷却时间: 5s → 15s → 45s (上限)
                _cooldown_secs = min(45, 5 * (3 ** (_fails - 1)) if _fails <= 3 else 45)
                self._combo_cooldown_until[expiry_strike] = time.time() + _cooldown_secs
                if _fails >= 3:
                    logger.info(f"[{expiry}-{strike}] 连续失败 {_fails} 次，冷却 {_cooldown_secs}s")

    async def _get_binance_actual_position(self, symbol: str, position_side: str = "") -> Tuple[Decimal, str, Decimal, bool]:
        """获取 Binance 真实持仓（WS 优先，REST 降级）

        Returns:
            (quantity, side, entry_price, known)
            known=False 表示查询失败/未知，不能据此判定仓位为 0
        """
        if not symbol:
            return Decimal('0'), '', Decimal('0'), True
        _target_ps = (position_side or "").upper()

        # 1) WS 快照优先（最快）
        try:
            if self.binance_ws:
                if _target_ps in ("LONG", "SHORT"):
                    _ws_pos = self.binance_ws.positions_by_side.get((symbol, _target_ps))
                else:
                    _ws_pos = self.binance_ws.positions.get(symbol)
                if _ws_pos and _ws_pos.quantity > 0:
                    return (
                        Decimal(str(_ws_pos.quantity)),
                        _target_ps if _target_ps in ("LONG", "SHORT") else _ws_pos.side,
                        Decimal(str(getattr(_ws_pos, 'entry_price', 0))),
                        True
                    )
        except Exception:
            pass

        # 2) REST 兜底（更可靠）
        # 仅当拿到有效 positionRisk 响应时，才允许把 0 仓位判定为 known=True
        _risk_rows = None
        _queried = False
        _ws_confirmed = False
        _rest_confirmed = False
        try:
            if self.binance_ws:
                _queried = True
                if _target_ps in ("LONG", "SHORT"):
                    _rows_ws = await self.binance_ws.get_position_risk_all(symbol)
                    if isinstance(_rows_ws, list):
                        _ws_confirmed = True
                        _risk_rows = [r for r in _rows_ws if isinstance(r, dict)]
                else:
                    _risk = await self.binance_ws.get_position_risk(symbol)
                    if isinstance(_risk, dict) and _risk.get("code") is None:
                        _ws_confirmed = True
                        _risk_rows = [_risk]
        except Exception as _e:
            logger.info(f"[Binance持仓对账] WS客户端 REST 查询失败 {symbol}: {_e}")

        # WS 未确认成功（含失败/超时/错误响应）时，继续用直连 REST 二次确认
        if (_risk_rows is None or not _ws_confirmed) and getattr(self, 'binance_auth', None):
            _session = None
            try:
                _queried = True
                _session = aiohttp.ClientSession(
                    headers=self.binance_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                _risk_resp = await self._binance_rest_fallback(
                    self.binance_auth, _session, "GET", "/fapi/v2/positionRisk",
                    {"symbol": symbol}, signed=True)
                _risk_rows = []
                if isinstance(_risk_resp, list):
                    _rest_confirmed = True
                    for _r in _risk_resp:
                        if isinstance(_r, dict) and _r.get("symbol") == symbol:
                            _risk_rows.append(_r)
                elif isinstance(_risk_resp, dict):
                    if _risk_resp.get("code") is not None:
                        logger.info(f"[Binance持仓对账] REST 返回错误: {_risk_resp}")
                    elif _risk_resp.get("symbol") == symbol:
                        _rest_confirmed = True
                        _risk_rows.append(_risk_resp)
            except Exception as _e:
                logger.info(f"[Binance持仓对账] 直连 REST 查询失败 {symbol}: {_e}")
            finally:
                if _session:
                    await _session.close()

        if not _risk_rows:
            # 只有拿到有效响应并确认无仓位时，才能返回 known=True
            if _ws_confirmed or _rest_confirmed:
                return Decimal('0'), '', Decimal('0'), True
            if _queried:
                logger.warning(f"[Binance持仓对账] {symbol} 仓位查询失败/未确认，返回 unknown，等待重试")
            return Decimal('0'), '', Decimal('0'), False

        try:
            # 指定 LONG/SHORT 时，按分腿返回；否则返回净仓位
            if _target_ps in ("LONG", "SHORT"):
                for _risk in _risk_rows:
                    _ps = str(_risk.get("positionSide", "BOTH")).upper()
                    _pos_amt = Decimal(str(_risk.get("positionAmt", "0")))
                    if _ps in ("LONG", "SHORT"):
                        if _ps != _target_ps:
                            continue
                        if abs(_pos_amt) == 0:
                            continue
                        _entry = Decimal(str(_risk.get("entryPrice", "0")))
                        return abs(_pos_amt), _target_ps, _entry, True
                    # 单向模式兼容
                    if _ps == "BOTH" and _pos_amt != 0:
                        _side = "LONG" if _pos_amt > 0 else "SHORT"
                        if _side != _target_ps:
                            continue
                        _entry = Decimal(str(_risk.get("entryPrice", "0")))
                        return abs(_pos_amt), _target_ps, _entry, True
                return Decimal('0'), _target_ps, Decimal('0'), True

            _signed = Decimal('0')
            _entry_ref = Decimal('0')
            for _risk in _risk_rows:
                _pos_amt = Decimal(str(_risk.get("positionAmt", "0")))
                if _pos_amt == 0:
                    continue
                _ps = str(_risk.get("positionSide", "BOTH")).upper()
                if _ps == "LONG":
                    _signed += abs(_pos_amt)
                    _entry_ref = Decimal(str(_risk.get("entryPrice", "0")))
                elif _ps == "SHORT":
                    _signed -= abs(_pos_amt)
                    _entry_ref = Decimal(str(_risk.get("entryPrice", "0")))
                else:
                    _signed += _pos_amt
                    _entry_ref = Decimal(str(_risk.get("entryPrice", "0")))

            if _signed == 0:
                return Decimal('0'), '', Decimal('0'), True
            _side = "LONG" if _signed > 0 else "SHORT"
            return abs(_signed), _side, _entry_ref, True
        except Exception:
            return Decimal('0'), '', Decimal('0'), False

    async def _get_binance_realized_funding_usd(self, symbol: str, start_ms: int, end_ms: int) -> Optional[Decimal]:
        """获取 Binance 指定时间窗的已实现 funding 净收益（USDT）

        返回:
            Decimal: 成功获取（可为0/正/负）
            None: 查询失败或不可用
        """
        if not symbol or end_ms <= start_ms:
            return Decimal('0')

        _log_throttle = max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0)
        _cool_key = (symbol, "FUNDING_FEE")
        if not hasattr(self, '_binance_income_failure_log_ts'):
            self._binance_income_failure_log_ts = {}
        if not hasattr(self, '_binance_income_locks'):
            self._binance_income_locks = {}

        _lock = self._binance_income_locks.get(_cool_key)
        if _lock is None:
            _lock = asyncio.Lock()
            self._binance_income_locks[_cool_key] = _lock

        async with _lock:
            def _mark_income_failure(_reason: str) -> None:
                _ts = time.time()
                _last_log = self._binance_income_failure_log_ts.get(_cool_key, 0.0)
                if _ts - _last_log >= _log_throttle:
                    logger.warning(f"[Funding实收] {symbol} 查询失败: {_reason}")
                    self._binance_income_failure_log_ts[_cool_key] = _ts

            _params = {
                "symbol": symbol,
                "incomeType": "FUNDING_FEE",
                "startTime": int(start_ms),
                "endTime": int(end_ms),
                "limit": 1000
            }
            _income_rows = None

            # 路径A：复用 Binance WS 客户端签名 REST
            try:
                if self.binance_ws:
                    _income_rows = await self.binance_ws._rest_request(
                        "GET", "/fapi/v1/income", _params, signed=True)
            except Exception as _e:
                logger.debug(f"[Funding实收] WS客户端查询失败: {_e}")

            # 路径B：直连 REST 降级
            if _income_rows is None and getattr(self, 'binance_auth', None):
                _session = None
                try:
                    _session = aiohttp.ClientSession(
                        headers=self.binance_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                    _signed = self.binance_auth.sign(dict(_params))
                    _income_rows = await self._binance_rest_fallback(
                        self.binance_auth, _session, "GET", "/fapi/v1/income", _signed, signed=False)
                except Exception as _e:
                    logger.debug(f"[Funding实收] 直连REST查询失败: {_e}")
                finally:
                    if _session:
                        await _session.close()

            if _income_rows is None:
                _mark_income_failure("REST无可用响应")
                return None
            if isinstance(_income_rows, dict) and _income_rows.get('code'):
                _mark_income_failure(str(_income_rows))
                return None
            if not isinstance(_income_rows, list):
                _mark_income_failure(f"响应类型异常: {type(_income_rows).__name__}")
                return None

            _total = Decimal('0')
            try:
                for _row in _income_rows:
                    if not isinstance(_row, dict):
                        continue
                    if _row.get("incomeType") and _row.get("incomeType") != "FUNDING_FEE":
                        continue
                    _total += Decimal(str(_row.get("income", "0")))
                return _total
            except Exception as _e:
                logger.info(f"[Funding实收] 汇总失败: {_e}")
                _mark_income_failure(f"汇总失败: {_e}")
                return None

    async def _get_binance_realized_commission_usd(self, symbol: str, start_ms: int, end_ms: int) -> Optional[Decimal]:
        """获取 Binance 指定时间窗的已实现交易佣金净额（USDT）

        返回:
            Decimal: 成功获取（通常为负值，表示手续费支出）
            None: 查询失败或不可用
        """
        if not symbol or end_ms <= start_ms:
            return Decimal('0')

        _params = {
            "symbol": symbol,
            "incomeType": "COMMISSION",
            "startTime": int(start_ms),
            "endTime": int(end_ms),
            "limit": 1000
        }
        _income_rows = None

        try:
            if self.binance_ws:
                _income_rows = await self.binance_ws._rest_request(
                    "GET", "/fapi/v1/income", _params, signed=True)
        except Exception as _e:
            logger.debug(f"[手续费实收] WS客户端查询失败: {_e}")

        if _income_rows is None and getattr(self, 'binance_auth', None):
            _session = None
            try:
                _session = aiohttp.ClientSession(
                    headers=self.binance_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                _signed = self.binance_auth.sign(dict(_params))
                _income_rows = await self._binance_rest_fallback(
                    self.binance_auth, _session, "GET", "/fapi/v1/income", _signed, signed=False)
            except Exception as _e:
                logger.debug(f"[手续费实收] 直连REST查询失败: {_e}")
            finally:
                if _session:
                    await _session.close()

        if _income_rows is None:
            return None
        if isinstance(_income_rows, dict) and _income_rows.get('code'):
            logger.warning(f"[手续费实收] 查询返回错误: {_income_rows}")
            return None
        if not isinstance(_income_rows, list):
            return None

        _total = Decimal('0')
        try:
            for _row in _income_rows:
                if not isinstance(_row, dict):
                    continue
                if _row.get("incomeType") and _row.get("incomeType") != "COMMISSION":
                    continue
                _total += Decimal(str(_row.get("income", "0")))
            return _total
        except Exception as _e:
            logger.info(f"[手续费实收] 汇总失败: {_e}")
            return None

    def _get_closing_lock(self, state: ArbitrageState) -> asyncio.Lock:
        """🌟 B 修复: 获取/创建 per-combo 平仓互斥锁
        任何写 Binance 或 Deribit 平仓单的路径都应在 async with 中执行,
        避免 monitor_positions 触发的 settlement 与 emergency_dump 并发冲突
        """
        key = state.expiry_strike
        if key not in self._combo_closing_locks:
            self._combo_closing_locks[key] = asyncio.Lock()
        return self._combo_closing_locks[key]
