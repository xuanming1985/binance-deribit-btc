"""trade_executor.py — 交易执行器 (从 binance-deribit.py 提取)"""
import asyncio
import time
import logging
import random
import traceback
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import binance_futures
from utils import FastJSON
from engine.models import MarketData, Position, Order
from deribit_client import EnhancedDeribitWebSocketClient
from fee_calculator import FeeCalculator
from telegram_handler import tg_notifier

json = FastJSON()
logger = logging.getLogger(__name__)


class TradeExecutor:
    """交易执行器，支持实盘交易"""

    def __init__(self, client: EnhancedDeribitWebSocketClient, fee_calculator: FeeCalculator):
        self.client = client
        self.fee_calculator = fee_calculator
        self.order_history = []
        self.emergency_stop = False  # 紧急停止标志，stop_all 时设为 True
        self._stop_signal_logged = False  # stop 暂停信号日志冷却标志
        self.binance_ws = None  # 跨所: Binance WS 引用，用于利润守卫获取实时价格
        self._binance_fee_calc = None  # 跨所: Binance 费率计算器 (引擎初始化后绑定)
        self._binance_executor = None  # 跨所: Binance 执行器
        self._binance_hedge_order_type = 'MARKET'  # 硬编码市价单
        self._binance_max_slippage_usd = Decimal('5')  # hedge_order接口需要
        self.engine = None  # 运行时由 RealTimeArbitrageEngine 注入，用于读取风控参数
        self.tier_stats = {
            'T1_fill': 0, 'T2_fill': 0, 'T3_fill': 0,
            'cancel_profit': 0, 'cancel_timeout': 0,
            'rollback_verify': 0, 'total': 0,
        }

    @staticmethod
    def generate_trade_label(strategy: str, expiry: str, strike: Decimal) -> str:
        """生成交易标签"""
        timestamp = int(time.time() * 1000)
        label = f"arb_{strategy[:3]}_{expiry}_{strike}_{timestamp}"
        return label

    def _get_binance_exec_price(self, binance_symbol: str, strategy_type: str,
                                max_age_sec: float = 5.0) -> Optional[Decimal]:
        """获取跨所执行/风控使用的 Binance 有效盘口价。

        返回值:
        - sell_future_buy_synthetic: best_bid
        - buy_future_sell_synthetic: best_ask
        若盘口不存在/价格无效/更新过期则返回 None。
        """
        if not binance_symbol or not self.binance_ws:
            return None
        try:
            ob = self.binance_ws.order_books.get(binance_symbol)
            if not ob or ob.mid_price is None or ob.mid_price <= 0:
                return None
            if ob.update_time and (time.time() - ob.update_time) > max_age_sec:
                return None
            if strategy_type == 'sell_future_buy_synthetic':
                return ob.best_bid if ob.best_bid and ob.best_bid > 0 else None
            return ob.best_ask if ob.best_ask and ob.best_ask > 0 else None
        except Exception:
            return None

    def _binance_market_ready(
            self, binance_symbol: str, max_age_sec: float = 5.0,
            orderbook_max_age_sec: float = None,
            mark_max_age_sec: float = None,
            last_max_age_sec: float = None) -> Tuple[bool, str]:
        """执行层最终门禁：避免 Binance 行情断线后仍发 Deribit 锚定腿。"""
        if not binance_symbol:
            return True, "no_binance_symbol"
        if self.engine and hasattr(self.engine, "_binance_market_ready"):
            return self.engine._binance_market_ready(
                binance_symbol, max_age_sec=max_age_sec,
                orderbook_max_age_sec=orderbook_max_age_sec,
                mark_max_age_sec=mark_max_age_sec,
                last_max_age_sec=last_max_age_sec)
        ob_age_limit = float(orderbook_max_age_sec if orderbook_max_age_sec is not None else max_age_sec)
        mark_age_limit = float(mark_max_age_sec if mark_max_age_sec is not None else max_age_sec)
        last_age_limit = float(last_max_age_sec if last_max_age_sec is not None else max_age_sec)
        if not self.binance_ws:
            return False, "missing_ws"
        try:
            if not self.binance_ws.connected:
                return False, "ws_disconnected"
        except Exception:
            return False, "ws_state_unknown"
        now = time.time()
        ob = self.binance_ws.order_books.get(binance_symbol)
        if not ob or ob.mid_price is None or ob.mid_price <= 0:
            return False, "orderbook_invalid"
        if not ob.update_time or (now - ob.update_time) > ob_age_limit:
            return False, "orderbook_stale"
        mark = self.binance_ws.mark_prices.get(binance_symbol, Decimal("0"))
        mark_ts = self.binance_ws.mark_price_update_times.get(binance_symbol, 0.0)
        if mark <= 0 or not mark_ts or (now - mark_ts) > mark_age_limit:
            return False, "mark_stale"
        last = self.binance_ws.last_prices.get(binance_symbol, Decimal("0"))
        last_ts = self.binance_ws.last_price_update_times.get(binance_symbol, 0.0)
        if last <= 0 or not last_ts or (now - last_ts) > last_age_limit:
            return False, "last_stale"
        return True, "ok"

    def _deribit_core_settlement_active(self) -> bool:
        """True when Deribit core settlement window allows only cancel/read/log."""
        engine = getattr(self, 'engine', None) or getattr(tg_notifier, 'engine', None)
        if not engine or not hasattr(engine, '_is_deribit_settlement_core_window'):
            return False
        try:
            return bool(engine._is_deribit_settlement_core_window())
        except Exception:
            return False

    def _mark_core_settlement_deferred(self, reason: str, pause_reason: str = None,
                                       anchor_order_id: str = None) -> None:
        """Record a deferred Deribit action without sending new orders."""
        engine = getattr(self, 'engine', None) or getattr(tg_notifier, 'engine', None)
        if not engine:
            return
        try:
            engine._add_pause("结算窗口")
            if pause_reason:
                engine._add_pause(pause_reason)
            if anchor_order_id:
                pending = getattr(engine, '_anchor_settlement_core_pending_orders', set())
                pending.add(str(anchor_order_id))
                engine._anchor_settlement_core_pending_orders = pending
            logger.warning(f"⏸️ [Deribit核心结算窗口] {reason}; 不发送自动下单/平仓，窗口结束后重新对账处理")
        except Exception as exc:
            logger.warning(f"[Deribit核心结算窗口] 标记延后处理失败: {exc}")

    def _get_binance_fee_rate(self, is_taker: bool = True) -> Optional[Decimal]:
        """读取 Binance 费率（优先实例实时值，兜底静态 tier）"""
        _calc = getattr(self, '_binance_fee_calc', None)
        if _calc is None:
            return None
        try:
            if is_taker:
                _r = getattr(_calc, 'taker_rate', None)
            else:
                _r = getattr(_calc, 'maker_rate', None)
            if _r is not None:
                return Decimal(str(_r))
        except Exception:
            pass
        try:
            _tier = getattr(_calc, 'tier', 'standard')
            _rates = binance_futures.BinanceFeeCalculator.FEE_TIERS.get(
                _tier, binance_futures.BinanceFeeCalculator.FEE_TIERS["standard"]
            )
            return Decimal(str(_rates[1] if is_taker else _rates[0]))
        except Exception:
            return None

    def _calculate_binance_fee_usdt(self, price: Decimal, quantity: Decimal, is_taker: bool = True) -> Decimal:
        """按当前 Binance 费率计算手续费(USDT)"""
        if price <= 0 or quantity <= 0:
            return Decimal('0')
        _rate = self._get_binance_fee_rate(is_taker=is_taker)
        if _rate is None:
            # 兜底沿用原静态实现
            _tier = self._binance_fee_calc.tier if self._binance_fee_calc is not None else "standard"
            return binance_futures.BinanceFeeCalculator.calculate_fee_usdt(
                price, quantity, is_taker=is_taker, tier=_tier
            )
        return price * quantity * _rate

    def _calculate_binance_fee_btc(self, price: Decimal, quantity: Decimal, is_taker: bool = True) -> Decimal:
        """按当前 Binance 费率计算手续费(BTC)"""
        if price <= 0 or quantity <= 0:
            return Decimal('0')
        _fee_usdt = self._calculate_binance_fee_usdt(price, quantity, is_taker=is_taker)
        return _fee_usdt / price

    async def simulate_trade(self,
                             strategy_type: str,
                             future_price: Decimal,
                             call_price: Decimal,
                             put_price: Decimal,
                             strike: Decimal,
                             future_amount_usd: Decimal,
                             option_btc_amount: Decimal,
                             ) -> Dict:
        """模拟交易并计算利润 (Maker1 + Taker2 模式)"""
        trades = []
        # ================= 升级修改 =================
        # 为了配合 Anchor Leg 策略，模拟计算时采取"最保守估计"
        # 假设期货和两条期权腿全部按照 Taker (吃单) 缴纳最贵的手续费
        # 这样实盘利润只会大于等于这里的模拟利润，绝不亏损！
        future_is_taker = True
        call_is_taker = True
        put_is_taker = True

        # 跨所: 期货对冲腿在 Binance，费用由 BinanceFeeCalculator 计算
        if hasattr(self, '_binance_fee_calc') and self._binance_fee_calc is not None:
            future_fee = self._calculate_binance_fee_btc(
                future_price, option_btc_amount, is_taker=True)
        else:
            future_fee = self.fee_calculator.calculate_future_fee(
                future_price, future_amount_usd, is_taker=future_is_taker)
        call_fee = self.fee_calculator.calculate_option_fee(future_price, call_price, option_btc_amount,
                                                            is_taker=call_is_taker)
        put_fee = self.fee_calculator.calculate_option_fee(future_price, put_price, option_btc_amount,
                                                           is_taker=put_is_taker)

        total_fee_btc = future_fee + call_fee + put_fee

        total_fee_usd_est = total_fee_btc * future_price  # 先估算成 USD

        # ================= 🌟 修复：升级为大资金动态异常拦截 =================
        # 计算该笔交易的总名义本金价值 (USD)
        notional_value_usd = option_btc_amount * future_price

        # 动态容忍度：取 50 刀 和 名义本金的 0.5% 之间的最大值
        # 这样小资金测试时有 50 刀保护，大资金时允许合理范围内的手续费
        dynamic_fee_limit = max(Decimal('50.0'), notional_value_usd * Decimal('0.005'))

        if total_fee_usd_est >= dynamic_fee_limit:
            logger.error(
                f"【异常拦截】计算出的手续费: {total_fee_usd_est:.2f} USD ({total_fee_btc} {self.client.target_currency}) "
                f"超过动态安全上限 {dynamic_fee_limit:.2f} USD，已跳过此交易计算。")
            asyncio.create_task(tg_notifier.send_async(
                f"【异常拦截】计算出的手续费: {total_fee_usd_est:.2f} USD ({total_fee_btc} {self.client.target_currency}) "
                f"超过动态安全上限 {dynamic_fee_limit:.2f} USD，已跳过此交易计算。"))
            return {
                'timestamp': time.time(),
                'strategy_type': strategy_type,
                'future_contracts': 0.0,
                'option_btc_amount': 0.0,
                'gross_profit_usd': 0.0,
                'gross_profit': 0.0,
                'total_fee': float(total_fee_usd_est),
                'net_profit': 0.0,
                'trades': [],
                'error': 'Abnormal fee detected'
            }

        total_fee_usd = total_fee_btc * future_price
        option_premium_net_btc = call_price - put_price
        option_premium_usd = option_premium_net_btc * future_price
        synthetic_price_usd = strike + option_premium_usd
        spread = synthetic_price_usd - future_price

        if strategy_type == 'sell_future_buy_synthetic':
            gross_profit_usd = -spread * option_btc_amount
        elif strategy_type == 'buy_future_sell_synthetic':
            gross_profit_usd = spread * option_btc_amount
        else:
            raise ValueError(f"Unknown strategy_type: {strategy_type}")

        # 净利 = 毛利 - 费用（execution_risk_penalty 已移除：三档递进开仓每档实时重获行情，漂移风险已覆盖）
        net_profit = gross_profit_usd - total_fee_usd

        trade_record = {
            'timestamp': time.time(),
            'strategy_type': strategy_type,
            'future_contracts': float(future_amount_usd),  # 字典键名不改，内容填 USD 即可
            'option_btc_amount': float(option_btc_amount),
            'gross_profit_usd': float(gross_profit_usd),
            'gross_profit': float(gross_profit_usd),
            'total_fee': float(total_fee_usd),
            'net_profit': float(net_profit),
            'trades': trades
        }
        self.order_history.append(trade_record)
        # ====== 内存保护：仅保留最近 5000 条记录 (🌟 ARCH-3 修复: 1000→5000) ======
        if len(self.order_history) > 5000:
            self.order_history = self.order_history[-5000:]
        return trade_record

    async def execute_arbitrage_trade(self, strategy_type: str, future_symbol: str, call_symbol: str, put_symbol: str,
                                      strike: Decimal, btc_amount: Decimal, log_prefix: str = "", **kwargs) -> Dict:
        prefix = f"{log_prefix}" if log_prefix else ""
        try:
            logger.info(f"{prefix}启动执行:{strategy_type}, 数量: {btc_amount} {self.client.target_currency}")

            future_info = await self.client.get_instrument_info(future_symbol)
            option_info = await self.client.get_instrument_info(call_symbol)
            if not future_info or not option_info:
                return {'error': '获取合约信息失败', 'success': False}

            ref_price = kwargs.get('future_price')
            if not ref_price:
                f_ticker = self.client.tickers.get(future_symbol)
                ref_price = f_ticker.bid if 'sell_future' in strategy_type else f_ticker.ask
            contract_size = Decimal(str(future_info.get('contract_size', 10)))
            # 🌟 平仓修复：使用开仓时锁定的期货面值，避免价格波动导致合约张数不一致
            future_amount_override = kwargs.get('future_amount_usd_override')
            if future_amount_override and future_amount_override > 0:
                future_order_amount = future_amount_override
            else:
                future_order_amount = (btc_amount * ref_price / contract_size).quantize(Decimal('1'), rounding='ROUND_HALF_UP') * contract_size

            option_min = Decimal(str(option_info.get('min_trade_amount', '0.1')))
            if btc_amount < option_min:
                return {'error': f'数量 {btc_amount} 低于期权最小单位', 'success': False}

            label = self.generate_trade_label(strategy_type, future_symbol.split('-')[1], strike)

            return await self._execute_maker2_taker1_strategy(
                strategy_type, future_symbol, call_symbol, put_symbol, strike, future_order_amount, btc_amount,
                kwargs.get('future_price'),  # 🌟 修复：在此处加上 future_price 的透传
                kwargs.get('call_price'),
                kwargs.get('put_price'), label,
                kwargs.get('max_wait_time', 15), kwargs.get('min_profit_threshold', Decimal('10.0')),
                kwargs.get('is_exit', False),
                log_prefix=log_prefix,
                binance_symbol=kwargs.get('binance_symbol', ''),
                funding_deduction_usd=kwargs.get('funding_deduction_usd', Decimal('0')),
                post_anchor_min_profit_usd=kwargs.get('post_anchor_min_profit_usd', Decimal('12')),
                rollback_ioc_aggressive_ticks=kwargs.get('rollback_ioc_aggressive_ticks', 100),
            )
        except Exception as e:
            logger.error(f"{prefix}执行异常: {e}")
            return {'error': str(e), 'success': False}

    # ======================================================================
    # 🌟 Fix: _calculate_vwap / _calculate_adaptive_price 原本只定义在
    # RealTimeArbitrageEngine 上, 但 _execute_maker2_taker1_strategy 的 T3
    # 分支(line ~3090)用 self._calculate_adaptive_price 调用, self 是
    # TradeExecutor → AttributeError。由于 TradeExecutor 和 Engine 都持有
    # 同一个 client (EnhancedDeribitWebSocketClient), body 可以原样复用。
    # 此处复制一份保持向后兼容, 不改动 Engine 里那份已有调用点。
    # ======================================================================

    async def _calculate_vwap(self, instrument_name: str, side: str, required_amount: Decimal) -> Optional[Decimal]:
        """计算指定深度的成交量加权平均价 (VWAP)。
        required_amount 的单位取决于合约类型: 期货=USD, 期权=BTC/ETH。"""
        try:
            book = self.client.local_orderbooks.get(instrument_name)
            if not book or not book.is_valid:
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
            logger.error(f"[TradeExecutor] VWAP 计算失败 {instrument_name}: {e}")
            return None

    async def _calculate_adaptive_price(self, instrument_name: str, side: str, required_amount: Decimal) -> Optional[Decimal]:
        """自适应 Taker 定价: 首档深度够就用首档价(更精确), 不够才穿档 VWAP(更保守)"""
        try:
            book = self.client.local_orderbooks.get(instrument_name)
            if not book or not book.is_valid:
                return None
            levels = book.get_top_levels(action_side=side)
            if not levels:
                return None
            first_price, first_amount = levels[0]
            if first_amount >= required_amount:
                return first_price
            return await self._calculate_vwap(instrument_name, side, required_amount)
        except Exception as e:
            logger.error(f"[TradeExecutor] 自适应定价失败 {instrument_name}: {e}")
            return None

    async def _execute_maker2_taker1_strategy(self, strategy_type: str, future_symbol: str, call_symbol: str,
                                              put_symbol: str, strike: Decimal, f_amount: Decimal,
                                              o_amount: Decimal,
                                              future_price: Decimal, c_price: Decimal, p_price: Decimal, label: str,
                                              max_wait_time: int = 15,
                                              min_profit_threshold: Decimal = Decimal('10.0'),
                                              is_exit: bool = False, log_prefix: str = "",
                                              binance_symbol: str = "",
                                              funding_deduction_usd: Decimal = Decimal('0'),
                                              post_anchor_min_profit_usd: Decimal = Decimal('12'),
                                              rollback_ioc_aggressive_ticks: int = 100) -> Dict:
        prefix = f"{log_prefix}" if log_prefix else ""
        try:
            if strategy_type == 'sell_future_buy_synthetic':
                c_side, p_side, f_side = 'buy', 'sell', 'sell'
            else:
                c_side, p_side, f_side = 'sell', 'buy', 'buy'

            # 👑 保留原版核心：动态计算 Spread，挑选流动性最差的腿作为 Maker 锚定
            c_spread = self._get_spread(call_symbol)
            p_spread = self._get_spread(put_symbol)

            if c_spread >= p_spread:
                anchor_symbol, anchor_side, anchor_price = call_symbol, c_side, c_price
                taker_symbol, taker_side = put_symbol, p_side
                anchor_type = 'call'
            else:
                anchor_symbol, anchor_side, anchor_price = put_symbol, p_side, p_price
                taker_symbol, taker_side = call_symbol, c_side
                anchor_type = 'put'

            _bn_ready, _bn_reason = self._binance_market_ready(
                binance_symbol, max_age_sec=5.0,
                orderbook_max_age_sec=5.0, mark_max_age_sec=10.0, last_max_age_sec=20.0)
            if not _bn_ready:
                logger.warning(f"{prefix}Binance 最终门禁失败 ({_bn_reason})，禁止发 Deribit 锚定腿")
                return {'error': f'Binance market not ready: {_bn_reason}', 'success': False}

            logger.info(f"{prefix}确定锚定腿(Maker): {anchor_symbol} ({anchor_type}), 方向: {anchor_side}, 挂单价: {anchor_price}")
            logger.info(f"{prefix}Taker腿: {taker_symbol} + {future_symbol}, 待锚定成交后 IOC 发射")

            a_info = await self.client.get_instrument_info(anchor_symbol)
            a_tick = self.client._get_dynamic_tick(anchor_price, a_info) if a_info else Decimal('0.0001')

            if self._deribit_core_settlement_active():
                self._mark_core_settlement_deferred(
                    "core_settlement_before_anchor_order", pause_reason="结算窗口")
                return {'success': False, 'error': 'core_settlement_before_anchor_order'}

            anchor_order = await self.client.place_order(anchor_symbol, o_amount, anchor_side, 'limit', anchor_price, label,
                                                         is_maker=True, log_prefix=log_prefix,reduce_only=is_exit)
            if not anchor_order: return {'success': False, 'error': '锚定单发送失败'}

            start_time, anchor_filled, current_anchor_price = time.time(), False, anchor_price
            anchor_order_id = anchor_order.order_id

            # ================= 🌟 修改：用真实时间戳替代 loop_counter =================
            last_rest_check_time = start_time

            # ================= 🌟 开仓三档递进定价 (T1/T2 等比, T3 触发式) =================
            # 时间分配: T1 50% / T2 50% / T3 无固定时长(触发式)
            # T3 不等待时间, 达到 T2 结束后持续每 0.5s 测试全 Taker 对手价利润:
            #   - 利润 ≥ min_profit_threshold → 立即改单为 Taker 对手价执行
            #   - 否则继续等待 (Smart Pennying 仍在后续循环中工作)
            # 注意: T3 阶段仍然受 max_wait_time 总时长约束, 超时仍会撤单
            _tier_names = ['🧲 T1-中间价', '🚀 T2-插队价', '💰 T3-触发执行']
            _t1_dur = int(max_wait_time * 0.5)
            _t2_dur = max(max_wait_time - _t1_dur, 5)  # T2 用剩余时间
            _t3_dur = 0  # T3 不占用固定时长, 只要达标立即执行
            _tier_durations = [_t1_dur, _t2_dur, _t3_dur]
            _current_tier = 0
            _tier_deadline = start_time + _tier_durations[0]
            # 各档统一使用原始门槛: 改价后利润仍须≥门槛才继续，低于门槛直接撤单
            _tier_profit_factors = [Decimal('1.0'), Decimal('1.0'), Decimal('1.0')]
            logger.info(f"{prefix}📊 三档递进已启动: T1={_t1_dur}s / T2={_t2_dur}s / T3=触发式 (总 {max_wait_time}s)")
            _cancel_reason = 'timeout'

            def _estimate_open_fee_usd(_f_price: Decimal, _c_price: Decimal, _p_price: Decimal,
                                       anchor_as_taker: bool = False) -> Decimal:
                """开仓费估算（含期权 premium cap）
                anchor_as_taker: T3 对手价执行时锚定腿以 Taker 成交, 需按 Taker 费率计算
                """
                if _f_price <= 0:
                    return Decimal('0')
                _future_notional = o_amount * _f_price
                if self._binance_fee_calc is not None:
                    _f_fee_btc = self._calculate_binance_fee_btc(
                        _f_price, o_amount, is_taker=True)
                else:
                    _f_fee_btc = self.fee_calculator.calculate_future_fee(_f_price, _future_notional, is_taker=True)
                if is_exit or anchor_as_taker:
                    _c_is_taker = True
                    _p_is_taker = True
                else:
                    _c_is_taker = (anchor_type != 'call')
                    _p_is_taker = (anchor_type != 'put')
                _c_fee_btc = self.fee_calculator.calculate_option_fee(
                    _f_price, _c_price, o_amount, is_taker=_c_is_taker)
                _p_fee_btc = self.fee_calculator.calculate_option_fee(
                    _f_price, _p_price, o_amount, is_taker=_p_is_taker)
                return (_f_fee_btc + _c_fee_btc + _p_fee_btc) * _f_price

            def _estimate_settle_fee_usd(_f_price: Decimal, _c_ref: Decimal, _p_ref: Decimal) -> Decimal:
                """结算费估算（期权交割费含 premium cap + Binance 平仓费）"""
                if _f_price <= 0:
                    return Decimal('0')
                _del_c_btc = self.fee_calculator.calculate_delivery_fee(_f_price, _c_ref, o_amount, is_option=True)
                _del_p_btc = self.fee_calculator.calculate_delivery_fee(_f_price, _p_ref, o_amount, is_option=True)
                _delivery_usd = (_del_c_btc + _del_p_btc) * _f_price
                _bn_close_usd = self._calculate_binance_fee_usdt(
                    _f_price, o_amount, is_taker=True)
                return _delivery_usd + _bn_close_usd

            async def _watch_l2_rollback_order(_order, _symbol: str, _target_qty: Decimal, _leg_tag: str) -> Decimal:
                """等待短时成交窗口，超时未成则撤单并返回残余数量"""
                if not _order or not getattr(_order, 'order_id', ''):
                    return _target_qty
                _eng = getattr(self, 'engine', None)
                _timeout_sec = max(float(getattr(_eng, 'rollback_l2_watch_seconds', 3.0)), 1.0)
                _dust = Decimal('0.0001')
                try:
                    await asyncio.sleep(_timeout_sec)
                    _latest = self.client.get_order_by_id(_order.order_id) or _order
                    _filled = Decimal(str(getattr(_latest, 'filled_amount', 0)))
                    _left = max(_target_qty - _filled, Decimal('0'))
                    if _left > _dust:
                        try:
                            await self.client.cancel_order(_order.order_id, log_prefix=f"{log_prefix}[L2超时撤单]")
                        except Exception as _ce:
                            logger.warning(f"{prefix}⚠️ [{_leg_tag}] L2超时撤单失败: {_symbol} id={_order.order_id} err={_ce}")
                        _latest_after_cancel = self.client.get_order_by_id(_order.order_id) or _latest
                        _filled_after_cancel = Decimal(str(getattr(_latest_after_cancel, 'filled_amount', _filled)))
                        _left = max(_target_qty - max(_filled, _filled_after_cancel), Decimal('0'))
                        return _left
                    return Decimal('0')
                except Exception as _we:
                    logger.info(f"{prefix}[{_leg_tag}] L2超时监控异常: {_we}")
                    return _target_qty

            def _round_to_tick(_price: Decimal, _tick: Decimal) -> Decimal:
                if _tick <= 0:
                    return _price
                return Decimal(str(int(round(_price / _tick)))) * _tick

            def _clamp_to_band(_price: Decimal, _ticker) -> Decimal:
                if _ticker:
                    if _ticker.min_price > 0 and _price < _ticker.min_price:
                        _price = _ticker.min_price
                    if _ticker.max_price > 0 and _price > _ticker.max_price:
                        _price = _ticker.max_price
                return max(_price, a_tick)

            def _rollback_ioc_price(_side: str, _ticks: int, _ref_price: Decimal) -> Decimal:
                _ticker = self.client.tickers.get(anchor_symbol)
                if _ticker and ((_ticker.ask > 0 and _side == 'buy') or (_ticker.bid > 0 and _side == 'sell')):
                    _base = Decimal(str(_ticker.ask if _side == 'buy' else _ticker.bid))
                else:
                    _base = Decimal(str(_ref_price or current_anchor_price or anchor_price))
                    if _base <= 0 and _ticker:
                        _base = max(
                            Decimal(str(getattr(_ticker, 'bid', 0))),
                            Decimal(str(getattr(_ticker, 'ask', 0))),
                            Decimal(str(getattr(_ticker, 'mark_price', 0) or 0)))
                _offset = a_tick * Decimal(str(max(int(_ticks), 0)))
                _raw = _base + _offset if _side == 'buy' else _base - _offset
                return _clamp_to_band(_round_to_tick(max(_raw, a_tick), a_tick), _ticker)

            async def _rollback_anchor_with_guard(_qty: Decimal, _reason: str, _label_prefix: str,
                                                  _ref_price: Decimal) -> Decimal:
                """锚定腿回滚: IOC → 更激进 IOC → L2 兜底，返回最终残余数量。"""
                _dust = Decimal('0.0001')
                _qty = Decimal(str(_qty or 0))
                if _qty <= _dust:
                    return Decimal('0')
                if self._deribit_core_settlement_active():
                    self._mark_core_settlement_deferred(
                        f"core_settlement_anchor_rollback_deferred({_reason})",
                        pause_reason="锚定腿回滚失败",
                        anchor_order_id=anchor_order_id)
                    return _qty
                _close_side = 'sell' if anchor_side == 'buy' else 'buy'
                _ticks = max(1, int(rollback_ioc_aggressive_ticks or 100))
                _price1 = _rollback_ioc_price(_close_side, _ticks, _ref_price)
                logger.warning(
                    f"{prefix}🚑 锚定腿回滚({_reason}): {anchor_symbol} {_close_side} {_qty} "
                    f"@ {_price1} (IOC {_ticks} ticks)")
                _rb1 = await self.client.place_order(
                    anchor_symbol, _qty, _close_side, 'limit', price=_price1,
                    label=f"{_label_prefix}_{label}", log_prefix=log_prefix,
                    reduce_only=not is_exit, time_in_force="immediate_or_cancel")
                _remaining = _qty - (_rb1.filled_amount if _rb1 else Decimal('0'))

                if _remaining > _dust:
                    _ticks2 = max(_ticks * 5, 500)
                    _price2 = _rollback_ioc_price(_close_side, _ticks2, _ref_price)
                    logger.warning(
                        f"{prefix}⚠️ 锚定腿回滚 IOC 未满额, 残余={_remaining}, "
                        f"二次重试 @ {_price2} ({_ticks2} ticks)")
                    _rb2 = await self.client.place_order(
                        anchor_symbol, _remaining, _close_side, 'limit', price=_price2,
                        label=f"{_label_prefix}2_{label}", log_prefix=log_prefix,
                        reduce_only=not is_exit, time_in_force="immediate_or_cancel")
                    _remaining -= (_rb2.filled_amount if _rb2 else Decimal('0'))

                if _remaining > _dust:
                    _l2_ticker = self.client.tickers.get(anchor_symbol)
                    if _l2_ticker and ((_l2_ticker.ask > 0 and _close_side == 'buy') or
                                       (_l2_ticker.bid > 0 and _close_side == 'sell')):
                        _l2_raw = Decimal(str(_l2_ticker.ask if _close_side == 'buy' else _l2_ticker.bid))
                        _l2_price = _clamp_to_band(_round_to_tick(_l2_raw, a_tick), _l2_ticker)
                        try:
                            _l2_order = await asyncio.wait_for(
                                self.client.place_order(
                                    anchor_symbol, _remaining, _close_side, 'limit', price=_l2_price,
                                    label=f"l2a_{_label_prefix}_{label}", log_prefix=log_prefix,
                                    reduce_only=not is_exit),
                                timeout=5.0)
                            if _l2_order and _l2_order.order_id:
                                logger.warning(
                                    f"{prefix}🔄 [L2-兜底挂单] 锚定腿 {anchor_symbol} "
                                    f"残余={_remaining} @ {_l2_price} (id={_l2_order.order_id})")
                                _remaining = await _watch_l2_rollback_order(
                                    _l2_order, anchor_symbol, _remaining, "锚定腿")
                        except Exception as _l2_err:
                            logger.error(f"{prefix}🚨 [L2-兜底失败] 锚定腿 {anchor_symbol}: {_l2_err}")

                if _remaining > _dust:
                    logger.error(f"{prefix}🚨 锚定腿回滚未完成! 残余={_remaining}")
                    if tg_notifier.engine:
                        tg_notifier.engine._add_pause("锚定腿回滚失败")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 锚定腿回滚未完成\n{anchor_symbol} 残余: {_remaining}\n"
                        f"原因: {_reason}\n系统已暂停新开仓，请立即检查交易所持仓。",
                        "anchor_rollback_incomplete"))
                else:
                    logger.info(f"{prefix}✅ 锚定腿回滚完成: {anchor_symbol} {_qty}")
                return max(_remaining, Decimal('0'))
            # ===================================================================

            _fallback_used = False
            _orig_partial_qty = Decimal('0')
            _orig_partial_avg = Decimal('0')
            _anchor_order_ids = [anchor_order_id]
            _anchor_weighted_avg = Decimal('0')
            _anchor_filled_qty = Decimal('0')

            while time.time() - start_time < max_wait_time:
                current_time = time.time()
                latest_order = self.client.get_order_by_id(anchor_order_id) or anchor_order
                status, filled_qty = latest_order.status, latest_order.filled_amount

                # 👑 机构级升级：基于真实时间的 3 秒兜底扫测 (防 WS 漏单)
                # 🌟 修复：从 1.5 秒改为 3 秒，降低匹配引擎负载（5 个并发锚定腿 × 1.5s = 3.3 req/s 接近 Deribit 匹配引擎上限）
                if status == 'open' and (current_time - last_rest_check_time > 3.0):
                    last_rest_check_time = current_time
                    try:
                        msg = {
                            "jsonrpc": "2.0",
                            "id": self.client._get_next_request_id(),
                            "method": "private/get_order_state",
                            "params": {"order_id": anchor_order_id}
                        }
                        resp = await self.client.send_request(msg, is_private=True, timeout=2.0)
                        if 'result' in resp:
                            real_state = resp['result'].get('order_state')
                            real_filled = Decimal(str(resp['result'].get('filled_amount', 0)))
                            real_avg = Decimal(str(resp['result'].get('average_price', 0) or 0))
                            real_price_raw = resp['result'].get('price')
                            real_price = Decimal(str(real_price_raw)) if real_price_raw not in (None, '') else None
                            avg_changed = real_avg > 0 and real_avg != latest_order.average_price
                            price_changed = real_price is not None and real_price != latest_order.price

                            if real_state != status or real_filled != filled_qty or avg_changed or price_changed:
                                logger.info(
                                    f"{prefix}[状态纠正] 捕获到 WS 漏单！真实状态:{real_state}, "
                                    f"真实成交:{real_filled}, 均价:{real_avg}")
                                latest_order.status = real_state
                                latest_order.filled_amount = real_filled
                                if real_avg > 0:
                                    latest_order.average_price = real_avg
                                if real_price is not None:
                                    latest_order.price = real_price
                                if hasattr(self.client, '_store_order_snapshot'):
                                    self.client._store_order_snapshot(latest_order)
                                status = real_state
                                filled_qty = real_filled
                    except Exception:
                        pass

                if filled_qty >= o_amount or status == 'filled':
                    anchor_filled = True
                    break

                # 🚨 紧急停止检查：stop_all 触发时立即退出，不降级开新仓
                if self.emergency_stop:
                    logger.warning(f"{prefix}🚨 检测到紧急停止信号，放弃等待锚定腿")
                    break

                # 🚨 优雅退出检查：running=False (SIGINT/SIGTERM) 时立即退出等待循环
                if hasattr(tg_notifier, 'engine') and not tg_notifier.engine.running:
                    logger.warning(f"{prefix}🛑 检测到系统退出信号，放弃等待锚定腿")
                    break

                # 🚨 stop 命令检查：trading_paused 时退出等待循环（仅开仓，平仓不受影响）
                # 平仓本身就是解决风险敞口的手段，不能因暂停而阻止平仓，否则形成死锁
                if not is_exit and hasattr(tg_notifier, 'engine') and tg_notifier.engine.trading_paused:
                    if not hasattr(self, '_stop_signal_logged') or not self._stop_signal_logged:
                        logger.warning(f"{prefix}🛑 检测到 stop 暂停信号，放弃等待锚定腿")
                        self._stop_signal_logged = True
                    break

                # 👑 保留原版核心：盘中遇到被拒单/撤销时的降级吃单策略
                if status in ['rejected', 'cancelled']:
                    # 🚨 紧急停止检查：被拒单时如果有 emergency_stop，不降级开新仓
                    if self.emergency_stop:
                        logger.warning(f"{prefix}🚨 锚定单被拒且检测到紧急停止信号，放弃降级吃单")
                        break
                    if self._deribit_core_settlement_active():
                        self._mark_core_settlement_deferred(
                            "core_settlement_fallback_ioc_deferred",
                            pause_reason=("锚定腿回滚失败" if filled_qty > 0 else None),
                            anchor_order_id=anchor_order_id)
                        return {'success': False, 'error': 'core_settlement_fallback_ioc_deferred'}

                    logger.warning(f"{prefix}⚠️ 锚定单被拒(撞上更优价格), 立刻降级侵略性限价单吃单！")
                    if anchor_side == 'buy':
                        fallback_price = anchor_price + (a_tick * 5)
                    else:
                        fallback_price = max(anchor_price - (a_tick * 5), a_tick)

                    fallback_order = await self.client.place_order(anchor_symbol, o_amount - filled_qty, anchor_side,
                                                                   'limit', price=fallback_price, label=label,
                                                                   log_prefix=log_prefix, reduce_only=is_exit,
                                                                   time_in_force="immediate_or_cancel")
                    if fallback_order:
                        _orig_anchor_id = anchor_order_id
                        anchor_order_id = fallback_order.order_id
                        # ================= 修复：验证 fallback IOC 是否真正成交 =================
                        fb_filled = fallback_order.filled_amount
                        # REST 二次确认（防 WS 延迟）
                        await asyncio.sleep(0.3)
                        try:
                            fb_msg = {"jsonrpc": "2.0", "id": self.client._get_next_request_id(),
                                      "method": "private/get_order_state", "params": {"order_id": anchor_order_id}}
                            fb_resp = await self.client.send_request(fb_msg, is_private=True, timeout=2.0)
                            if 'result' in fb_resp:
                                fb_filled = Decimal(str(fb_resp['result'].get('filled_amount', 0)))
                                fb_avg = Decimal(str(fb_resp['result'].get('average_price', 0) or 0))
                                fallback_order.filled_amount = fb_filled
                                if fb_avg > 0:
                                    fallback_order.average_price = fb_avg
                                if hasattr(self.client, '_store_order_snapshot'):
                                    self.client._store_order_snapshot(fallback_order)
                        except Exception:
                            pass
                        if fb_filled >= o_amount - filled_qty:
                            anchor_filled = True
                            _fallback_used = True
                            _orig_order = self.client.get_order_by_id(_orig_anchor_id)
                            _orig_partial_qty = filled_qty
                            _orig_partial_avg = (_orig_order.average_price
                                                 if _orig_order and _orig_order.average_price > 0
                                                 else anchor_price)
                            _anchor_order_ids = (
                                [_orig_anchor_id, fallback_order.order_id]
                                if _orig_partial_qty > 0 else [fallback_order.order_id]
                            )
                            logger.info(f"{prefix}✅ 降级 IOC 单全额成交: {fb_filled} (原单部分成交: {filled_qty} @ {_orig_partial_avg})")
                        else:
                            logger.warning(f"{prefix}⚠️ 降级 IOC 单成交不足: {fb_filled}/{o_amount - filled_qty}，放弃执行")
                            # 回滚已成交的部分
                            total_filled_so_far = filled_qty + fb_filled
                            if total_filled_so_far > 0:
                                await _rollback_anchor_with_guard(
                                    total_filled_so_far,
                                    "fallback_ioc_partial",
                                    "fb",
                                    fallback_price)
                            return {'success': False, 'error': 'Fallback IOC partial, anchor rollback attempted'}
                        # =================================================================
                    break

                # ================= 🌟 三档递进 =================
                # T1→T2: 到达 _tier_deadline 时切换 (固定时长)
                # T2→T3: T2 结束后一次性测试全 Taker 对手价利润:
                #   - 达标 → 立即改单为对手价 (相当于 Taker, 应立即成交)
                #   - 不达标 → 终止本轮等待, 撤单并返回 (本轮放弃, 等下轮扫描)
                if status == 'open' and _current_tier == 0 and time.time() >= _tier_deadline:
                    # === T1 → T2 切换 ===
                    _current_tier = 1
                    _tier_deadline = time.time() + _tier_durations[1]

                    _t_ticker = self.client.tickers.get(anchor_symbol)
                    if _t_ticker and _t_ticker.bid > 0 and _t_ticker.ask > 0:
                        _t_dyn_tick = self.client._get_dynamic_tick(Decimal(str(_t_ticker.bid)), a_info)
                        # T2 插队价: bid+1tick(买) / ask-1tick(卖)
                        if anchor_side == 'buy':
                            _np = Decimal(str(_t_ticker.bid)) + _t_dyn_tick
                        else:
                            _np = max(Decimal(str(_t_ticker.ask)) - _t_dyn_tick, _t_dyn_tick)
                        _np_tick = self.client._get_dynamic_tick(_np, a_info)
                        _np = self.client._adjust_to_tick_size(_np, _np_tick)
                        if _t_ticker.min_price > 0 and _np < _t_ticker.min_price:
                            _np = _t_ticker.min_price
                        if _t_ticker.max_price > 0 and _np > _t_ticker.max_price:
                            _np = _t_ticker.max_price

                        # 验证 T2 利润
                        _t2_f_price = self._get_binance_exec_price(
                            binance_symbol, strategy_type, max_age_sec=5.0) if binance_symbol else None
                        if binance_symbol and _t2_f_price is None:
                            logger.warning(f"{prefix}⚠️ Binance 盘口不可用/过期，无法验证 T2 利润, 终止等待")
                            break
                        if _t2_f_price is None and not binance_symbol:
                            _tf = self.client.tickers.get(future_symbol)
                            if _tf:
                                _t2_f_price = _tf.bid if strategy_type == 'sell_future_buy_synthetic' else _tf.ask
                        _tc = self.client.tickers.get(call_symbol)
                        _tp = self.client.tickers.get(put_symbol)
                        if _t2_f_price and _tc and _tp:
                            _cpr = _np if anchor_type == 'call' else (_tc.ask if c_side == 'buy' else _tc.bid)
                            _ppr = _np if anchor_type == 'put' else (_tp.ask if p_side == 'buy' else _tp.bid)
                            _t2_gross = ((_t2_f_price - ((_cpr - _ppr) * _t2_f_price + strike)) * o_amount
                                         if strategy_type == 'sell_future_buy_synthetic'
                                         else (((_cpr - _ppr) * _t2_f_price + strike) - _t2_f_price) * o_amount)
                            _t2_fee = _estimate_open_fee_usd(_t2_f_price, _cpr, _ppr)
                            _t2_settle = _estimate_settle_fee_usd(_t2_f_price, _cpr, _ppr)
                            _t2_profit = _t2_gross - _t2_fee - _t2_settle - funding_deduction_usd
                            if _t2_profit < min_profit_threshold:
                                logger.info(
                                    f"{prefix}📊 T2 切换: {_np} 预估利润 {_t2_profit:.2f} < 门槛 {min_profit_threshold:.2f} USD, 放弃")
                                _cancel_reason = 'profit'
                                break
                            # 达标 → 改价
                            edit_ok = await self.client.edit_order(
                                anchor_order_id, amount=o_amount, price=_np, log_prefix=log_prefix)
                            if edit_ok:
                                current_anchor_price = _np
                                anchor_price = _np
                                logger.info(f"{prefix}📊 切换至 🚀 T2-插队价 @ {_np} (等待 {_tier_durations[1]}s)")
                            else:
                                logger.warning(f"{prefix}⚠️ T2 改价失败, 保持 {current_anchor_price}")
                    else:
                        logger.warning(f"{prefix}⚠️ T1→T2 档位切换时行情缺失, 保持当前档位")

                elif status == 'open' and _current_tier == 1 and time.time() >= _tier_deadline:
                    # === T2 → T3 切换: 一次性测试全 Taker 对手价 ===
                    _current_tier = 2
                    _t3_ticker = self.client.tickers.get(anchor_symbol)
                    if not (_t3_ticker and _t3_ticker.bid > 0 and _t3_ticker.ask > 0):
                        logger.debug(f"{prefix}📊 T3 测试: 行情缺失, 本轮放弃")
                        break

                    # 对手价 (Taker)
                    if anchor_side == 'buy':
                        _t3_np = Decimal(str(_t3_ticker.ask))
                    else:
                        _t3_np = Decimal(str(_t3_ticker.bid))
                    _t3_np_tick = self.client._get_dynamic_tick(_t3_np, a_info)
                    _t3_np = self.client._adjust_to_tick_size(_t3_np, _t3_np_tick)
                    if _t3_ticker.min_price > 0 and _t3_np < _t3_ticker.min_price:
                        _t3_np = _t3_ticker.min_price
                    if _t3_ticker.max_price > 0 and _t3_np > _t3_ticker.max_price:
                        _t3_np = _t3_ticker.max_price

                    # 测试全 Taker 利润
                    _t3_f_price = self._get_binance_exec_price(
                        binance_symbol, strategy_type, max_age_sec=5.0) if binance_symbol else None
                    if binance_symbol and _t3_f_price is None:
                        logger.warning(f"{prefix}⚠️ T3 测试: Binance 盘口不可用, 本轮放弃")
                        break
                    if _t3_f_price is None and not binance_symbol:
                        _t3_tf = self.client.tickers.get(future_symbol)
                        if _t3_tf:
                            _t3_f_price = _t3_tf.bid if strategy_type == 'sell_future_buy_synthetic' else _t3_tf.ask
                    _t3_tc = self.client.tickers.get(call_symbol)
                    _t3_tp = self.client.tickers.get(put_symbol)
                    if not (_t3_f_price and _t3_tc and _t3_tp):
                        logger.debug(f"{prefix}📊 T3 测试: 行情数据缺失, 本轮放弃")
                        break

                    # 🌟 T3 利润验证用 VWAP (三腿都是 Taker 必穿档, 单档价会乐观高估)
                    # 锚定腿: 使用 _t3_np (对手价)作为基准, 但如果我们的量 > 首档深度,
                    #         实际成交会穿档, 所以用 _calculate_adaptive_price 估算 VWAP
                    # 另一条期权腿: 同样是 Taker, 用 VWAP 估算
                    _t3_anchor_vwap = await self._calculate_adaptive_price(
                        anchor_symbol, anchor_side, o_amount)
                    _t3_taker_opt_vwap = await self._calculate_adaptive_price(
                        taker_symbol, taker_side, o_amount)

                    # VWAP 获取失败则降级到首档价 (ticker)
                    if _t3_anchor_vwap is None or _t3_anchor_vwap <= 0:
                        _t3_anchor_vwap = _t3_np  # 降级: 用对手价单档
                    if _t3_taker_opt_vwap is None or _t3_taker_opt_vwap <= 0:
                        # 降级: 用 Taker 腿的对手方单档价
                        if taker_symbol == call_symbol:
                            _t3_taker_opt_vwap = _t3_tc.ask if c_side == 'buy' else _t3_tc.bid
                        else:
                            _t3_taker_opt_vwap = _t3_tp.ask if p_side == 'buy' else _t3_tp.bid

                    # 构造全 Taker 三腿模拟 (VWAP 估算)
                    _t3_cpr = _t3_anchor_vwap if anchor_type == 'call' else _t3_taker_opt_vwap
                    _t3_ppr = _t3_anchor_vwap if anchor_type == 'put' else _t3_taker_opt_vwap
                    _t3_gross = ((_t3_f_price - ((_t3_cpr - _t3_ppr) * _t3_f_price + strike)) * o_amount
                                 if strategy_type == 'sell_future_buy_synthetic'
                                 else (((_t3_cpr - _t3_ppr) * _t3_f_price + strike) - _t3_f_price) * o_amount)
                    _t3_fee = _estimate_open_fee_usd(_t3_f_price, _t3_cpr, _t3_ppr, anchor_as_taker=True)
                    _t3_settle = _estimate_settle_fee_usd(_t3_f_price, _t3_cpr, _t3_ppr)
                    _t3_profit = _t3_gross - _t3_fee - _t3_settle - funding_deduction_usd

                    if _t3_profit >= min_profit_threshold:
                        # 达标 → 立即改单为对手价 (Taker, 应立即成交)
                        _t3_edit_ok = await self.client.edit_order(
                            anchor_order_id, amount=o_amount, price=_t3_np, log_prefix=log_prefix)
                        if _t3_edit_ok:
                            current_anchor_price = _t3_np
                            anchor_price = _t3_np
                            logger.info(
                                f"{prefix}🚀 T3-对手价达标! 改为 {_t3_np} | "
                                f"VWAP预估利润 {_t3_profit:.2f} USD (Taker 执行)")
                            # 继续循环等待成交确认 (Taker 通常瞬时成交)
                        else:
                            logger.warning(f"{prefix}⚠️ T3 改价失败, 放弃本轮")
                            break
                    else:
                        # 不达标 → 本轮放弃, 撤单返回, 等下轮扫描
                        logger.info(
                            f"{prefix}📊 T3 测试: 对手价 {_t3_np} VWAP预估利润 {_t3_profit:.2f} < 门槛 {min_profit_threshold:.2f} USD, 本轮放弃")
                        _cancel_reason = 'profit'
                        break
                # ================= 三档递进结束 =================

                # ================= 🌟 升级：基于实时利润底线的动态盘口咬合 (Smart Pennying) =================
                if status == 'open':
                    ticker = self.client.tickers.get(anchor_symbol)
                    if ticker and ticker.bid > 0 and ticker.ask > 0:
                        new_price, need_edit = current_anchor_price, False

                        # ================= 🌟 修复：阶梯精度陷阱 (Tiered Tick Size) 动态判断 =================
                        cur_price_dec = Decimal(str(current_anchor_price))
                        dyn_tick = self.client._get_dynamic_tick(cur_price_dec, a_info)
                        # =====================================================================================

                        # 防内卷排队悖论：对方挂单量超阈值才视为有效阻挡并插队
                        # 阈值随机化 15-25%，防止固定门槛被 HFT 学习/探测
                        _sp_ratio = Decimal(str(round(random.uniform(0.15, 0.25), 3)))
                        if anchor_side == 'buy' and current_anchor_price < ticker.bid:
                            if ticker.bid_size > o_amount * _sp_ratio:
                                raw_price = Decimal(str(ticker.bid)) + dyn_tick
                                new_price = Decimal(str(int(round(raw_price / dyn_tick)))) * dyn_tick
                                new_tick = self.client._get_dynamic_tick(new_price, a_info)
                                if new_tick != dyn_tick:
                                    new_price = self.client._adjust_to_tick_size(new_price, new_tick)
                                need_edit = True
                        elif anchor_side == 'sell' and current_anchor_price > ticker.ask:
                            if ticker.ask_size > o_amount * _sp_ratio:
                                raw_price = Decimal(str(ticker.ask)) - dyn_tick
                                new_price = Decimal(str(int(round(raw_price / dyn_tick)))) * dyn_tick
                                new_tick = self.client._get_dynamic_tick(new_price, a_info)
                                if new_tick != dyn_tick:
                                    new_price = self.client._adjust_to_tick_size(new_price, new_tick)
                                need_edit = True

                        if need_edit:
                            can_chase = False
                            # 跨所模式：追价利润验证仅使用 Binance 有效盘口
                            _chase_f_price = self._get_binance_exec_price(
                                binance_symbol, strategy_type, max_age_sec=5.0) if binance_symbol else None
                            if _chase_f_price is None and not binance_symbol:
                                latest_f_ticker = self.client.tickers.get(future_symbol)
                                if latest_f_ticker:
                                    _chase_f_price = latest_f_ticker.bid if strategy_type == 'sell_future_buy_synthetic' else latest_f_ticker.ask
                            elif binance_symbol and _chase_f_price is None:
                                logger.debug(f"{prefix}Binance 盘口不可用/过期，跳过本轮追价")
                            latest_c_ticker = self.client.tickers.get(call_symbol)
                            latest_p_ticker = self.client.tickers.get(put_symbol)

                            if _chase_f_price and latest_c_ticker and latest_p_ticker:
                                curr_f_price = _chase_f_price

                                sim_c_price = new_price if anchor_type == 'call' else (
                                    latest_c_ticker.ask if c_side == 'buy' else latest_c_ticker.bid)
                                sim_p_price = new_price if anchor_type == 'put' else (
                                    latest_p_ticker.ask if p_side == 'buy' else latest_p_ticker.bid)

                                curr_premium = sim_c_price - sim_p_price
                                if strategy_type == 'sell_future_buy_synthetic':
                                    sim_syn_price = (curr_premium * curr_f_price) + strike
                                    sim_gross_usd = (curr_f_price - sim_syn_price) * o_amount
                                else:
                                    sim_syn_price = (curr_premium * curr_f_price) + strike
                                    sim_gross_usd = (sim_syn_price - curr_f_price) * o_amount
                                # 从毛利中扣除开仓费+结算费，与利润守卫/三档递进保持一致
                                _chase_fee_est = _estimate_open_fee_usd(curr_f_price, sim_c_price, sim_p_price)
                                _chase_settle_est = _estimate_settle_fee_usd(curr_f_price, sim_c_price, sim_p_price)
                                sim_profit_usd = sim_gross_usd - _chase_fee_est - _chase_settle_est - funding_deduction_usd

                                if sim_profit_usd >= min_profit_threshold:
                                    can_chase = True
                                    logger.info(
                                        f"{prefix}🏃 盘口被抢位! 测算追击后净利 {sim_profit_usd:.2f} USD (毛利 {sim_gross_usd:.2f}), 自动抢夺排位 -> {new_price}")
                                else:
                                    # ================= 🌟 新增：透明化拒绝追击日志 =================
                                    # 防刷屏设计：只有当对手机器人挂出新的内卷价格时，才打印一次账本
                                    last_refused = getattr(self, f"_last_refused_{anchor_symbol}", None)
                                    if last_refused != new_price:
                                        _ref_side = "ask" if anchor_side == 'sell' else "bid"
                                        _ref_price = ticker.ask if anchor_side == 'sell' else ticker.bid
                                        logger.info(
                                            f"{prefix}🛑 拒绝追价: 当前盘口 {_ref_side}={_ref_price}, "
                                            f"拟追价={new_price}, 追价后净利={sim_profit_usd:.2f} "
                                            f"< 门槛 {min_profit_threshold:.2f} USD, "
                                            f"维持当前挂单={current_anchor_price}")
                                        setattr(self, f"_last_refused_{anchor_symbol}", new_price)

                            # 2. 携带原生 amount 执行追击
                            if can_chase:
                                edit_ok = await self.client.edit_order(
                                    anchor_order_id,
                                    amount=o_amount,
                                    price=new_price,
                                    log_prefix=log_prefix
                                )
                                if edit_ok:
                                    current_anchor_price = new_price
                                    anchor_price = new_price
                # =========================================================================================

                # ================= 🌟 升级注入 2：动态利润守卫 (防等待期间行情暴跌) =================
                # 平仓时跳过利润守卫：平仓策略方向与开仓相反，利润守卫会把反向价差误判为"行情恶化"
                # 平仓的安全性已由上游的官方PnL门槛 + VWAP穿透测试保障
                if not is_exit:
                    # 跨所模式：利润守卫必须基于 Binance 盘口；不可回退 Deribit 远期价
                    _guard_f_price = self._get_binance_exec_price(
                        binance_symbol, strategy_type, max_age_sec=5.0) if binance_symbol else None
                    if binance_symbol and _guard_f_price is None:
                        logger.info(f"{prefix}🚨 Binance 盘口不可用/过期，无法继续利润守卫，放弃等待执行撤单！")
                        break
                    if _guard_f_price is None:
                        latest_f_ticker = self.client.tickers.get(future_symbol)
                        if latest_f_ticker:
                            _guard_f_price = latest_f_ticker.bid if strategy_type == 'sell_future_buy_synthetic' else latest_f_ticker.ask

                    latest_c_ticker = self.client.tickers.get(call_symbol)
                    latest_p_ticker = self.client.tickers.get(put_symbol)

                    if _guard_f_price and latest_c_ticker and latest_p_ticker:
                        current_f_price = _guard_f_price
                        curr_c_price = anchor_price if anchor_type == 'call' else (
                            latest_c_ticker.ask if c_side == 'buy' else latest_c_ticker.bid)
                        curr_p_price = anchor_price if anchor_type == 'put' else (
                            latest_p_ticker.ask if p_side == 'buy' else latest_p_ticker.bid)

                        curr_premium = curr_c_price - curr_p_price
                        if strategy_type == 'sell_future_buy_synthetic':
                            curr_syn_price = (curr_premium * current_f_price) + strike
                            _guard_gross = (current_f_price - curr_syn_price) * o_amount
                        else:
                            curr_syn_price = (curr_premium * current_f_price) + strike
                            _guard_gross = (curr_syn_price - current_f_price) * o_amount
                        # 从毛利中扣除开仓费+结算费
                        _guard_fee = _estimate_open_fee_usd(current_f_price, curr_c_price, curr_p_price)
                        _guard_settle = _estimate_settle_fee_usd(current_f_price, curr_c_price, curr_p_price)
                        current_profit_usd = _guard_gross - _guard_fee - _guard_settle - funding_deduction_usd

                        # 利润守卫：净利润低于门槛则撤单
                        _guard_threshold = min_profit_threshold * _tier_profit_factors[_current_tier]
                        if current_profit_usd < _guard_threshold:
                            logger.info(f"{prefix}🚨 行情恶化！预估利润 {current_profit_usd:.2f} < 门槛 {_guard_threshold:.2f} USD ({_tier_names[_current_tier]})，放弃等待执行撤单！")
                            _cancel_reason = 'profit'
                            break
                # =========================================================================================
                # ================= 🌟 机构级升级：事件驱动替换无脑轮询 =================
                # 挂起当前协程，直到 WS 收到任何数据摇响铃铛时瞬间唤醒。
                # 设置 0.5 秒超时是为了防止盘口彻底死寂时的安全兜底。
                try:
                    async with self.client.state_condition:
                        await asyncio.wait_for(self.client.state_condition.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass  # 正常超时，直接进入下一轮循环继续监控
                # =======================================================================

            # ================= 📈 三档递进执行统计 =================
            self.tier_stats['total'] += 1
            _elapsed = time.time() - start_time
            if anchor_filled:
                _fill_key = ['T1_fill', 'T2_fill', 'T3_fill'][_current_tier]
                self.tier_stats[_fill_key] += 1
                _s = self.tier_stats
                _fills = _s['T1_fill'] + _s['T2_fill'] + _s['T3_fill']
                _cancels = _s['cancel_profit'] + _s['cancel_timeout']
                logger.info(
                    f"{prefix}📈 成交于 {_tier_names[_current_tier]} | "
                    f"Anchor={anchor_type} spread=${float(c_spread if anchor_type == 'call' else p_spread):.0f} | "
                    f"耗时 {_elapsed:.1f}s | "
                    f"累计 T1={_s['T1_fill']} T2={_s['T2_fill']} T3={_s['T3_fill']} "
                    f"取消={_cancels} | 成交率={_fills}/{_s['total']}={_fills/_s['total']*100:.0f}%")
            else:
                self.tier_stats[f'cancel_{_cancel_reason}'] += 1
                _s = self.tier_stats
                _fills = _s['T1_fill'] + _s['T2_fill'] + _s['T3_fill']
                _cancels = _s['cancel_profit'] + _s['cancel_timeout']
                logger.info(
                    f"{prefix}📈 取消({_cancel_reason}) 在 {_tier_names[_current_tier]} | "
                    f"Anchor={anchor_type} spread=${float(c_spread if anchor_type == 'call' else p_spread):.0f} | "
                    f"耗时 {_elapsed:.1f}s | "
                    f"累计 T1={_s['T1_fill']} T2={_s['T2_fill']} T3={_s['T3_fill']} "
                    f"取消={_cancels} | 成交率={_fills}/{_s['total']}={_fills/_s['total']*100:.0f}%")
            # =========================================================

            # ================= 🌟 升级注入 3：幽灵地雷 1 (撤单同步静默期，防单边裸奔) =================
            if not anchor_filled:
                # ================= 🌟 修复：WS 断连时跳过网络操作，避免错误雪崩 =================
                # WS 已死时撤单/查单注定失败，跳过这些操作。重连后 initialize() 会同步真实状态。
                if not self.client.is_connected:
                    logger.warning(f"{prefix}⚠️ WS 已断开，跳过撤单/查单，等待重连后同步状态")
                    return {'success': False, 'error': 'WS disconnected, skip cleanup'}
                # =================================================================

                logger.info(f"{prefix}等待超时或利润不达标，撤销挂单并回滚。")

                # 第一步：先强行发撤单指令
                await self.client.cancel_order(anchor_order_id, log_prefix=log_prefix)

                # 🚨 必须休眠 0.5 秒让子弹飞一会儿！等待交易所结算最后瞬间的成交！
                await asyncio.sleep(0.5)

                # ================= 🌟 修复：主动 REST 查单防"幽灵成交" =================
                # filled_qty = Decimal('0')
                try:
                    msg = {"jsonrpc": "2.0", "id": self.client._get_next_request_id(),
                           "method": "private/get_order_state", "params": {"order_id": anchor_order_id}}
                    resp = await self.client.send_request(msg, is_private=True, timeout=2.0)
                    if 'result' in resp:
                        filled_qty = Decimal(str(resp['result'].get('filled_amount', 0)))
                    else:
                        raise ValueError("API没返回结果")
                except Exception as e:
                    logger.warning(f"{prefix}⚠️ REST查单超时，降级使用WS内存状态: {e}")
                    latest_order = self.client.get_order_by_id(anchor_order_id) or anchor_order
                    filled_qty = latest_order.filled_amount
                # ==========================================================================

                if filled_qty > 0:
                    logger.info(f"{prefix}🚑 执行锚定腿回滚，真实残余平仓数量: {filled_qty}")
                    await _rollback_anchor_with_guard(
                        filled_qty,
                        "anchor_timeout_or_profit_cancel",
                        "cl",
                        current_anchor_price)
                return {'success': False, 'error': 'Anchor timeout or dropped'}
            # ================= 核心修复：用 Anchor 真实成交价重新验证利润 =================
            # 平仓时跳过此验证：平仓策略方向与开仓相反，此公式会算出巨大负值导致永远回滚
            # 平仓的安全性已由上游的官方PnL门槛 + VWAP穿透测试保障
            latest_anchor = self.client.get_order_by_id(anchor_order_id) or anchor_order
            if _fallback_used and _orig_partial_qty > 0:
                _fb_avg = latest_anchor.average_price if latest_anchor.average_price > 0 else current_anchor_price
                _fb_qty = latest_anchor.filled_amount if latest_anchor.filled_amount > 0 else (o_amount - _orig_partial_qty)
                _total_qty = _orig_partial_qty + _fb_qty
                actual_anchor_avg = ((_orig_partial_qty * _orig_partial_avg + _fb_qty * _fb_avg) / _total_qty
                                     if _total_qty > 0 else current_anchor_price)
                _anchor_weighted_avg = actual_anchor_avg
                _anchor_filled_qty = _total_qty
                logger.info(f"{prefix}[Fallback聚合] 原单 {_orig_partial_qty}@{_orig_partial_avg} + "
                            f"补单 {_fb_qty}@{_fb_avg} = 加权均价 {actual_anchor_avg:.6f}")
            else:
                actual_anchor_avg = latest_anchor.average_price if latest_anchor.average_price > 0 else current_anchor_price
                _anchor_weighted_avg = actual_anchor_avg
                _anchor_filled_qty = latest_anchor.filled_amount if latest_anchor.filled_amount > 0 else o_amount
            if actual_anchor_avg <= 0:
                actual_anchor_avg = anchor_price
            try:
                _post_anchor_threshold = Decimal(str(post_anchor_min_profit_usd))
            except Exception:
                _post_anchor_threshold = Decimal('12')
            if _post_anchor_threshold < 0:
                _post_anchor_threshold = Decimal('0')
            # 成交后门槛不应比开仓前更严格；锚定腿已成交时重点是避免裸腿回滚风险。
            _post_anchor_threshold = min(_post_anchor_threshold, min_profit_threshold)

            if not is_exit:
                # 跨所模式：Anchor 成交后的净利验证必须使用 Binance 有效盘口
                _verify_f_price = self._get_binance_exec_price(
                    binance_symbol, strategy_type, max_age_sec=5.0) if binance_symbol else None
                if binance_symbol and _verify_f_price is None:
                    logger.warning(f"{prefix}🚨 Anchor 已成交但 Binance 盘口不可用/过期，执行回滚避免错误开仓")
                    actual_filled = o_amount if _fallback_used else (latest_anchor.filled_amount if latest_anchor.filled_amount > 0 else o_amount)
                    await _rollback_anchor_with_guard(
                        actual_filled, "binance_quote_unavailable", "vrf", actual_anchor_avg)
                    return {'success': False, 'error': 'Binance quote unavailable after anchor fill'}
                if _verify_f_price is None:
                    latest_f_ticker = self.client.tickers.get(future_symbol)
                    if latest_f_ticker:
                        _verify_f_price = latest_f_ticker.bid if strategy_type == 'sell_future_buy_synthetic' else latest_f_ticker.ask
                latest_c_ticker = self.client.tickers.get(call_symbol)
                latest_p_ticker = self.client.tickers.get(put_symbol)

                if _verify_f_price and latest_c_ticker and latest_p_ticker:
                    verify_f_price = _verify_f_price
                    # 用真实成交均价替换 anchor 预期价，其余腿用当前最新 Taker 价
                    if anchor_type == 'call':
                        verify_c = actual_anchor_avg
                        verify_p = latest_p_ticker.ask if p_side == 'buy' else latest_p_ticker.bid
                    else:
                        verify_p = actual_anchor_avg
                        verify_c = latest_c_ticker.ask if c_side == 'buy' else latest_c_ticker.bid

                    verify_premium = verify_c - verify_p
                    if strategy_type == 'sell_future_buy_synthetic':
                        verify_syn = (verify_premium * verify_f_price) + strike
                        verify_gross = (verify_f_price - verify_syn) * o_amount
                    else:
                        verify_syn = (verify_premium * verify_f_price) + strike
                        verify_gross = (verify_syn - verify_f_price) * o_amount
                    # 从毛利中扣除开仓费+结算费，与利润守卫/三档递进保持一致
                    _vrf_fee_est = _estimate_open_fee_usd(verify_f_price, verify_c, verify_p)
                    _vrf_settle_est = _estimate_settle_fee_usd(verify_f_price, verify_c, verify_p)
                    verify_profit = verify_gross - _vrf_fee_est - _vrf_settle_est - funding_deduction_usd

                    if verify_profit < _post_anchor_threshold:
                        logger.info(
                            f"{prefix}🚨 Anchor 真实成交价 {actual_anchor_avg} 导致净利润缩水至 "
                            f"{verify_profit:.2f} USD (毛利 {verify_gross:.2f}, "
                            f"费用 {_vrf_fee_est + _vrf_settle_est:.2f}, "
                            f"成交后门槛 {_post_anchor_threshold:.2f}, 开仓门槛 {min_profit_threshold:.2f})，执行回滚！")
                        actual_filled = o_amount if _fallback_used else (latest_anchor.filled_amount if latest_anchor.filled_amount > 0 else o_amount)
                        await _rollback_anchor_with_guard(
                            actual_filled, "post_anchor_profit_below_threshold", "vrf", actual_anchor_avg)
                        self.tier_stats['rollback_verify'] += 1
                        return {
                            'success': False,
                            'error': (
                                f'Anchor real price {actual_anchor_avg} made profit drop to '
                                f'{verify_profit:.2f} below post-anchor threshold {_post_anchor_threshold:.2f}')
                        }
                    else:
                        if verify_profit < min_profit_threshold:
                            logger.warning(
                                f"{prefix}⚠️ Anchor 真实成交价 {actual_anchor_avg} 后净利润 "
                                f"{verify_profit:.2f} USD 低于开仓门槛 {min_profit_threshold:.2f}，"
                                f"但高于成交后门槛 {_post_anchor_threshold:.2f}，继续发射 Taker 以避免裸腿回滚风险")
                        else:
                            logger.info(
                                f"{prefix}Anchor 真实成交价 {actual_anchor_avg}，验证后净利润 "
                                f"{verify_profit:.2f} USD (毛利 {verify_gross:.2f}, "
                                f"费用 {_vrf_fee_est + _vrf_settle_est:.2f})，继续发射 Taker")
            else:
                logger.info(f"{prefix}[平仓模式] 跳过 Anchor 利润验证（平仓安全性由上游 VWAP+PnL 保障），继续发射 Taker")
            # =========================================================================

            # 🚨 紧急停止检查：Maker 成交后、Taker 发射前的最后关卡
            if self.emergency_stop:
                logger.warning(f"{prefix}🚨 紧急停止信号！Maker已成交但放弃发射Taker，回滚Maker仓位")
                actual_filled = o_amount if _fallback_used else (latest_anchor.filled_amount if latest_anchor.filled_amount > 0 else o_amount)
                await _rollback_anchor_with_guard(
                    actual_filled, "emergency_stop_after_anchor_fill", "es", actual_anchor_avg)
                return {'success': False, 'error': 'Emergency stop: anchor filled but taker aborted'}

            # 🌟 重连安全: Maker 已成交但 WS 断连 → Taker/回滚注定全部失败，快速返回
            # 重连后 initialize() 会同步持仓，幽灵检测器会在 30s 内自动清理裸腿
            if not self.client.is_connected:
                logger.error(f"{prefix}🚨 Maker 已成交但 WS 已断连！裸腿 {anchor_symbol} 等待重连后幽灵检测处理")
                try:
                    engine = getattr(self, 'engine', None) or getattr(tg_notifier, 'engine', None)
                    if engine:
                        pending = getattr(engine, '_anchor_ws_disconnect_pending_orders', set())
                        pending.add(str(anchor_order_id))
                        engine._anchor_ws_disconnect_pending_orders = pending
                        engine._add_pause("锚定腿WS断连待核查")
                        engine._add_pause("锚定腿回滚失败")
                except Exception as pause_err:
                    logger.warning(f"{prefix}锚定腿 WS 断线强暂停标记失败: {pause_err}")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 [裸腿风险] {anchor_symbol} Maker 已成交但 WS 断连！\n"
                    f"Taker 无法发射，系统已暂停新开仓；重连后将先撤单/查成交并等待幽灵清理。",
	                    "naked_leg_ws_disconnect"))
                return {'success': False, 'error': 'WS disconnected after anchor fill, naked leg pending ghost cleanup'}

            if self._deribit_core_settlement_active():
                self._mark_core_settlement_deferred(
                    "core_settlement_after_anchor_fill",
                    pause_reason="锚定腿回滚失败",
                    anchor_order_id=anchor_order_id)
                asyncio.create_task(tg_notifier.send_error_async(
                    f"⏸️ [结算核心窗口] {anchor_symbol} Maker 已成交，但 Deribit core window 已开始。\n"
                    f"按规则不发射 Taker/回滚单，系统暂停新开仓，窗口结束后重新对账处理。",
                    "core_settlement_after_anchor_fill"))
                return {'success': False, 'error': 'core_settlement_after_anchor_fill'}

            logger.info(f"{prefix}✅ 锚定腿确认成交！瞬间发射 侵略性限价单 收网对冲...")

            t_opt_ticker = self.client.tickers.get(taker_symbol)
            f_ticker = self.client.tickers.get(future_symbol)

            t_opt_price, f_price = None, None

            # ================= 🌟 修复：提前赋予默认 Tick，防止作用域报错 =================
            t_dyn_tick = Decimal('0.0001')
            # ================= 🌟 修复：坚决不猜期货 Tick，从 API 获取真相 =================
            f_tick = Decimal('0.5')  # 默认占位符
            f_info = await self.client.get_instrument_info(future_symbol)
            if f_info:
                # 真实报文显示交割期货的 tick 可能是 2.5！
                f_tick = Decimal(str(f_info.get('tick_size', '0.5')))
            # 获取 Taker 期权合约信息（用于分档 tick_size_steps 计算）
            t_info = await self.client.get_instrument_info(taker_symbol)
            # =========================================================================

            # --- Taker 期权腿动态精度与网格吸附 ---
            if t_opt_ticker and t_opt_ticker.bid > 0 and t_opt_ticker.ask > 0:
                # 如果我们要买，就拿当前的卖一价作为基准；如果要卖，就拿买一价
                base_t_price = t_opt_ticker.ask if taker_side == 'buy' else t_opt_ticker.bid
                cur_t_price_dec = Decimal(str(base_t_price))

                t_dyn_tick = self.client._get_dynamic_tick(cur_t_price_dec, t_info)

                # 动态滑点保护：基于利润门槛计算最大可容忍的期权价格偏移
                # max_opt_slippage_btc = 利润门槛的 30% 转换为 BTC 单价偏移
                # 🌟 跨所修复: 优先用 Binance 价格做 USD→BTC 转换
                _slippage_ref_price = None
                if binance_symbol and self.binance_ws:
                    _bn_ob_s = self.binance_ws.order_books.get(binance_symbol)
                    if _bn_ob_s and _bn_ob_s.mid_price is not None and _bn_ob_s.mid_price > 0:
                        _slippage_ref_price = _bn_ob_s.mid_price
                if _slippage_ref_price is None:
                    _latest_f_ticker = self.client.tickers.get(future_symbol)
                    if _latest_f_ticker:
                        _slippage_ref_price = _latest_f_ticker.mid_price
                if _slippage_ref_price and _slippage_ref_price > 0 and o_amount > 0:
                    max_opt_slippage_btc = (min_profit_threshold * Decimal('0.3')) / (_slippage_ref_price * o_amount)
                else:
                    max_opt_slippage_btc = t_dyn_tick * 20  # 兜底

                # 限制在 5~50 tick 范围内，防止极端值
                opt_slippage_ticks = max(Decimal('5'), min(Decimal('50'), max_opt_slippage_btc / t_dyn_tick))
                opt_slippage_ticks = opt_slippage_ticks.quantize(Decimal('1'))

                if taker_side == 'buy':
                    raw_t_price = cur_t_price_dec + (t_dyn_tick * opt_slippage_ticks)
                    t_opt_price = Decimal(str(int(round(raw_t_price / t_dyn_tick)))) * t_dyn_tick
                else:
                    raw_t_price = max(cur_t_price_dec - (t_dyn_tick * opt_slippage_ticks), t_dyn_tick)
                    t_opt_price = Decimal(str(int(round(raw_t_price / t_dyn_tick)))) * t_dyn_tick

                # 🌟 修复 #4: Taker 期权 IOC 价格钳位到交易所 price band，防被拒导致裸锚
                if t_opt_ticker.min_price > 0 and t_opt_price < t_opt_ticker.min_price:
                    t_opt_price = t_opt_ticker.min_price
                if t_opt_ticker.max_price > 0 and t_opt_price > t_opt_ticker.max_price:
                    t_opt_price = t_opt_ticker.max_price

            if f_ticker and f_ticker.bid > 0 and f_ticker.ask > 0:
                f_info = await self.client.get_instrument_info(future_symbol)
                if f_info:
                    f_tick = Decimal(str(f_info.get('tick_size', '0.5')))

                # 修复：使用最新期货盘口价
                base_f_price = f_ticker.ask if f_side == 'buy' else f_ticker.bid
                # 期货 IOC 滑点保护：固定 20 ticks
                # BTC 永续/季度 tick=0.5 → 20 ticks = 10 USD (期货深度极好，足够覆盖)
                # 交割期货 tick=2.5 → 20 ticks = 50 USD (更宽，适应低流动性)
                dynamic_slippage = Decimal('20')
                if f_side == 'buy':
                    raw_f_price = base_f_price + (f_tick * dynamic_slippage)
                    f_price = Decimal(str(int(round(raw_f_price / f_tick)))) * f_tick
                else:
                    raw_f_price = max(base_f_price - (f_tick * dynamic_slippage), f_tick)
                    f_price = Decimal(str(int(round(raw_f_price / f_tick)))) * f_tick

                # 🌟 修复 #4: 期货 IOC 价格也钳位到交易所 price band
                if f_ticker.min_price > 0 and f_price < f_ticker.min_price:
                    f_price = f_ticker.min_price
                if f_ticker.max_price > 0 and f_price > f_ticker.max_price:
                    f_price = f_ticker.max_price

            # ================= 安全防线：Taker 必须有价格保护，禁止裸 Market 单 =================
            # Maker 已成交，此时 Taker 必须发射，但绝对不能用无保护的 Market 单
            if not t_opt_price:
                # Ticker 缺失时，用传入的预期价格加大滑点作为保底限价
                fallback_base = Decimal(str(c_price if anchor_type == 'put' else p_price))
                if taker_side == 'buy':
                    t_opt_price = fallback_base + (t_dyn_tick * 50)
                else:
                    t_opt_price = max(fallback_base - (t_dyn_tick * 50), t_dyn_tick)
                t_opt_price = Decimal(str(int(round(t_opt_price / t_dyn_tick)))) * t_dyn_tick
                logger.warning(f"{prefix}⚠️ Taker期权盘口缺失，使用预期价±50tick保底: {t_opt_price}")

            if not f_price:
                fallback_f_base = Decimal(str(future_price)) if future_price else Decimal('50000')
                if f_side == 'buy':
                    f_price = fallback_f_base + (f_tick * 20)
                else:
                    f_price = max(fallback_f_base - (f_tick * 20), f_tick)
                f_price = Decimal(str(int(round(f_price / f_tick)))) * f_tick
                logger.warning(f"{prefix}⚠️ Taker期货盘口缺失，使用预期价±20tick保底: {f_price}")

            t_order_type = 'limit'
            f_order_type = 'limit'

            # 🌟 跨所模式: Deribit 期货腿完全跳过 (期货对冲在 Binance)
            # 开仓: 仅 Call+Put 在 Deribit，期货由 Binance hedge_order 处理
            # 平仓: 仅 Call+Put 在 Deribit，期货由 _close_binance_hedge 处理
            _skip_deribit_future = bool(binance_symbol)

            if _skip_deribit_future:
                logger.info(
                    f"{prefix}=== Anchor Filled -> Taker IOC 发射 (跨所: 仅期权) ===\n"
                    f"  Anchor(Maker): {anchor_symbol} {anchor_side} {o_amount} @ {current_anchor_price} (成交)\n"
                    f"  Taker期权: {taker_symbol} {taker_side} {o_amount} @ {t_opt_price} ({t_order_type}+IOC)\n"
                    f"  Deribit期货: 跳过 (由 Binance 对冲替代)"
                )
            else:
                logger.info(
                    f"{prefix}=== Anchor Filled -> Taker IOC 发射 ===\n"
                    f"  Anchor(Maker): {anchor_symbol} {anchor_side} {o_amount} @ {current_anchor_price} (成交)\n"
                    f"  Taker期权: {taker_symbol} {taker_side} {o_amount} @ {t_opt_price} ({t_order_type}+IOC)\n"
                    f"  Taker期货: {future_symbol} {f_side} {f_amount}USD @ {f_price} ({f_order_type}+IOC)"
                )
            # ================= IOC 基因 (立刻成交或取消，防断腿) =================
            if self._deribit_core_settlement_active():
                self._mark_core_settlement_deferred(
                    "core_settlement_before_taker_order",
                    pause_reason="锚定腿回滚失败",
                    anchor_order_id=anchor_order_id)
                return {'success': False, 'error': 'core_settlement_before_taker_order'}

            taker_opt_task = self.client.place_order(taker_symbol, o_amount, taker_side, t_order_type,
                                                     price=t_opt_price, label=label, log_prefix=log_prefix,
                                                     reduce_only=is_exit, time_in_force="immediate_or_cancel")

            if _skip_deribit_future:
                t_opt_order = await taker_opt_task
                f_order = None
            else:
                # 🌟 H1 修复: 期货平仓先尝试 reduce_only=True 防止翻转净头寸
                _fut_reduce = is_exit  # 开仓时 False，平仓时 True
                taker_fut_task = self.client.place_order(future_symbol, f_amount, f_side, f_order_type, price=f_price,
                                                         label=label, log_prefix=log_prefix, reduce_only=_fut_reduce,
                                                         time_in_force="immediate_or_cancel")
                _opt_res, _fut_res = await asyncio.gather(
                    taker_opt_task, taker_fut_task, return_exceptions=True)
                if isinstance(_opt_res, Exception):
                    logger.error(f"{prefix}Taker期权腿下单异常: {_opt_res}")
                    t_opt_order = None
                else:
                    t_opt_order = _opt_res
                if isinstance(_fut_res, Exception):
                    logger.error(f"{prefix}Taker期货腿下单异常: {_fut_res}")
                    f_order = None
                else:
                    f_order = _fut_res

                # 🌟 H1: 期货 reduce_only 被拒时降级重试
                if is_exit and (not f_order or (hasattr(f_order, 'filled_amount') and f_order.filled_amount == 0)):
                    if self._deribit_core_settlement_active():
                        self._mark_core_settlement_deferred(
                            "core_settlement_reduce_only_fallback_deferred",
                            pause_reason="Taker IOC对冲失败",
                            anchor_order_id=anchor_order_id)
                        return {'success': False, 'error': 'core_settlement_reduce_only_fallback_deferred'}
                    logger.warning(f"{prefix}⚠️ 期货 reduce_only=True 被拒或未成交，降级为 reduce_only=False 重试")
                    f_order = await self.client.place_order(future_symbol, f_amount, f_side, f_order_type, price=f_price,
                                                            label=f"ro_fallback_{label}", log_prefix=log_prefix,
                                                            reduce_only=False, time_in_force="immediate_or_cancel")

            # ================= 🌟 终极防漏网：判定 IOC 实际成交量，防部分成交裸奔 =================
            # 容差：如果差额 ≤ 最小交易单位，视为成功（Deribit 取整导致的微小偏差）
            f_min_trade = Decimal(str(f_info.get('min_trade_amount', 10))) if f_info else Decimal('10')
            o_min_trade = Decimal('0.1')  # 期权最小交易量

            t_opt_filled = t_opt_order.filled_amount if t_opt_order else Decimal('0')
            f_filled = f_order.filled_amount if f_order else Decimal('0')
            t_opt_shortfall = o_amount - t_opt_filled
            f_shortfall = f_amount - f_filled

            t_opt_failed = not t_opt_order or t_opt_shortfall >= o_min_trade
            # 跨所平仓时 Deribit 期货腿已跳过，不视为失败
            f_failed = False if _skip_deribit_future else (not f_order or f_shortfall >= f_min_trade)

            # 微小差额警告但不回滚（shortfall > 0 但 < min_trade，属于取整误差）
            if Decimal('0') < t_opt_shortfall < o_min_trade:
                logger.warning(f"{prefix}⚠️ 期权Taker腿微小欠额 {t_opt_shortfall}（≤最小单位{o_min_trade}），视为成功")
            if not _skip_deribit_future and Decimal('0') < f_shortfall < f_min_trade:
                logger.warning(f"{prefix}⚠️ 期货Taker腿微小欠额 {f_shortfall} USD（≤最小单位{f_min_trade}），视为成功")

            if t_opt_failed or f_failed:
                # 通过全局通知器的引擎引用来暂停交易 (TradeExecutor 自身没有 trading_paused)
                if tg_notifier.engine:
                    tg_notifier.engine._add_pause("Taker IOC对冲失败")
                logger.error(
                    f"{prefix}🚨 致命异常：Taker 腿 IOC 对冲请求未能满额成交 (滑点打穿或深度不足)！触发全面 Delta 中和！")

                # 1. 杀活单 (针对极端延迟下的残余挂单)
                if t_opt_order and t_opt_order.status == 'open': await self.client.cancel_order(t_opt_order.order_id, log_prefix=log_prefix)
                if f_order and f_order.status == 'open': await self.client.cancel_order(f_order.order_id, log_prefix=log_prefix)

                await asyncio.sleep(0.5)

                if self._deribit_core_settlement_active():
                    engine = getattr(self, 'engine', None) or getattr(tg_notifier, 'engine', None)
                    if engine and hasattr(engine, '_remove_pause'):
                        engine._remove_pause("Taker IOC对冲失败")
                    self._mark_core_settlement_deferred(
                        "core_settlement_taker_rollback_deferred",
                        pause_reason="锚定腿回滚失败",
                        anchor_order_id=anchor_order_id)
                    return {'success': False, 'error': 'core_settlement_taker_rollback_deferred'}

                # ================= 机构级修复：回滚单发射 + REST 验证成交 =================
                rollback_failures = []  # 收集回滚失败的腿

                # 2. 强平锚定腿 (IOC + 加大滑点重试)
                latest_anchor = self.client.get_order_by_id(anchor_order_id) or anchor_order
                actual_filled = o_amount if _fallback_used else latest_anchor.filled_amount
                if actual_filled > 0:
                    close_side = 'sell' if anchor_side == 'buy' else 'buy'
                    _em_a_ticker = self.client.tickers.get(anchor_symbol)
                    # 动态 tick：优先用已计算的 a_tick，fallback 用实时盘口价格算
                    if a_tick > 0:
                        em_tick = a_tick
                    elif _em_a_ticker and (_em_a_ticker.bid > 0 or _em_a_ticker.ask > 0):
                        _em_ref = _em_a_ticker.ask if close_side == 'buy' else _em_a_ticker.bid
                        em_tick = self.client._get_dynamic_tick(Decimal(str(_em_ref))) if _em_ref > 0 else Decimal('0.0005')
                    else:
                        em_tick = Decimal('0.0005')
                    if _em_a_ticker and ((_em_a_ticker.ask > 0 and close_side == 'buy') or (_em_a_ticker.bid > 0 and close_side == 'sell')):
                        _em_a_raw = _em_a_ticker.ask if close_side == 'buy' else _em_a_ticker.bid
                        dump_price = Decimal(str(int(round(Decimal(str(_em_a_raw)) / em_tick)))) * em_tick
                    else:
                        # 🌟 防呆: current_anchor_price 若异常为 0, fallback 会产生 0 ± offset 的怪价
                        # 退化到 ticker mid 或最小合理价格防止零价甩卖
                        _ref_price = Decimal(str(current_anchor_price)) if current_anchor_price and current_anchor_price > 0 else Decimal('0')
                        if _ref_price <= 0 and _em_a_ticker:
                            _ref_price = (_em_a_ticker.bid + _em_a_ticker.ask) / 2 if (_em_a_ticker.bid > 0 and _em_a_ticker.ask > 0) else max(
                                _em_a_ticker.bid, _em_a_ticker.ask, _em_a_ticker.mark_price if hasattr(_em_a_ticker, 'mark_price') else Decimal('0'))
                        if _ref_price <= 0:
                            # 最后一道防线: 参考价完全失效时放弃自动强平, 告警人工介入
                            logger.error(f"{prefix}🚨 强平参考价失效 (current_anchor={current_anchor_price}, ticker 全 0), 跳过自动强平")
                            rollback_failures.append(f"锚定腿 {anchor_symbol}: 参考价失效, 无法计算强平价")
                            raise RuntimeError(f"emergency dump price reference missing for {anchor_symbol}")
                        raw_dump = _ref_price + (em_tick * 1000) if close_side == 'buy' else max(
                            _ref_price - (em_tick * 1000), em_tick)
                        dump_price = Decimal(str(int(round(raw_dump / em_tick)))) * em_tick

                    # 🌟 price band 保护: 防止 1000-tick 偏移超出 Deribit 允许范围
                    if _em_a_ticker:
                        if _em_a_ticker.min_price > 0 and dump_price < _em_a_ticker.min_price:
                            dump_price = _em_a_ticker.min_price
                        if _em_a_ticker.max_price > 0 and dump_price > _em_a_ticker.max_price:
                            dump_price = _em_a_ticker.max_price
                    logger.warning(f"{prefix}🚑 正在强平已落袋的锚定腿 {anchor_symbol} (数量: {actual_filled})...")
                    rb_anchor = await self.client.place_order(anchor_symbol, actual_filled, close_side, 'limit', price=dump_price,
                                                  label=f"em_{label}", log_prefix=log_prefix, reduce_only=not is_exit,
                                                  time_in_force="immediate_or_cancel")
                    remaining = actual_filled - (rb_anchor.filled_amount if rb_anchor else Decimal('0'))
                    if remaining > 0:
                        # 重试仍用盘口价（IOC若未成交说明盘口已移动，重新取最新价）
                        _em_a_ticker2 = self.client.tickers.get(anchor_symbol)
                        if _em_a_ticker2 and ((_em_a_ticker2.ask > 0 and close_side == 'buy') or (_em_a_ticker2.bid > 0 and close_side == 'sell')):
                            _em_a_raw2 = _em_a_ticker2.ask if close_side == 'buy' else _em_a_ticker2.bid
                            retry_price = Decimal(str(int(round(Decimal(str(_em_a_raw2)) / em_tick)))) * em_tick
                        else:
                            raw_retry = Decimal(str(current_anchor_price)) + (em_tick * 1000) if close_side == 'buy' else max(
                                Decimal(str(current_anchor_price)) - (em_tick * 1000), em_tick)
                            retry_price = Decimal(str(int(round(raw_retry / em_tick)))) * em_tick
                        # 🌟 price band 保护
                        if _em_a_ticker2:
                            if _em_a_ticker2.min_price > 0 and retry_price < _em_a_ticker2.min_price:
                                retry_price = _em_a_ticker2.min_price
                            if _em_a_ticker2.max_price > 0 and retry_price > _em_a_ticker2.max_price:
                                retry_price = _em_a_ticker2.max_price
                        logger.warning(f"{prefix}🚑 锚定腿首次IOC未满额(残{remaining})，加大滑点重试...")
                        rb_anchor2 = await self.client.place_order(anchor_symbol, remaining, close_side, 'limit', price=retry_price,
                                                      label=f"em2_{label}", log_prefix=log_prefix, reduce_only=not is_exit,
                                                      time_in_force="immediate_or_cancel")
                        remaining -= (rb_anchor2.filled_amount if rb_anchor2 else Decimal('0'))
                    if remaining > 0:
                        rollback_failures.append(f"锚定腿 {anchor_symbol}: 需平{actual_filled}, 残余{remaining}")
                        # ===== Layer 2: IOC全部失败后挂普通限价单兜底 (await追踪) =====
                        _l2_ticker = self.client.tickers.get(anchor_symbol)
                        if _l2_ticker:
                            _l2_price = _l2_ticker.ask if close_side == 'buy' else _l2_ticker.bid
                            if _l2_price > 0:
                                _l2_p = Decimal(str(int(round(Decimal(str(_l2_price)) / em_tick)))) * em_tick
                                try:
                                    _l2_order = await asyncio.wait_for(
                                        self.client.place_order(
                                            anchor_symbol, remaining, close_side, 'limit', price=_l2_p,
                                            label=f"l2a_{label}", log_prefix=log_prefix, reduce_only=not is_exit),
                                        timeout=5.0)
                                    if _l2_order and _l2_order.order_id:
                                        logger.warning(f"{prefix}🔄 [L2-兜底挂单] 锚定腿 {anchor_symbol} 已挂限价回滚 @ {_l2_p} (id={_l2_order.order_id})")
                                        _l2_left = await _watch_l2_rollback_order(
                                            _l2_order, anchor_symbol, remaining, "锚定腿")
                                        if _l2_left > Decimal('0.0001'):
                                            rollback_failures.append(f"锚定腿 L2超时未成已撤单: {anchor_symbol} 残余{_l2_left}")
                                            logger.error(
                                                f"{prefix}🚨 [L2-超时撤单] 锚定腿 {anchor_symbol} "
                                                f"残余={_l2_left}，已撤单并上报人工处理")
                                    else:
                                        rollback_failures.append(f"锚定腿 L2 下单失败: {anchor_symbol} @ {_l2_p}")
                                except Exception as _l2_err:
                                    logger.error(f"{prefix}🚨 [L2-兜底失败] 锚定腿 {anchor_symbol}: {_l2_err}")
                                    rollback_failures.append(f"锚定腿 L2 异常: {anchor_symbol} - {_l2_err}")
                        # ==================================================

                # 3. 强平期权 Taker 腿 (IOC + 加大滑点重试)
                t_opt_final = self.client.get_order_by_id(t_opt_order.order_id) if t_opt_order else None
                if t_opt_final and t_opt_final.filled_amount > 0:
                    t_close_side = 'sell' if taker_side == 'buy' else 'buy'
                    base_t_price = Decimal(str(t_opt_price)) if t_opt_price else Decimal(
                        str(t_opt_final.average_price or 0.05))
                    _em_t_ticker = self.client.tickers.get(taker_symbol)
                    if _em_t_ticker and ((_em_t_ticker.ask > 0 and t_close_side == 'buy') or (_em_t_ticker.bid > 0 and t_close_side == 'sell')):
                        _em_t_raw = _em_t_ticker.ask if t_close_side == 'buy' else _em_t_ticker.bid
                        t_dump_price = Decimal(str(int(round(Decimal(str(_em_t_raw)) / t_dyn_tick)))) * t_dyn_tick
                    else:
                        raw_t_dump = base_t_price + (t_dyn_tick * 1000) if t_close_side == 'buy' else max(
                            base_t_price - (t_dyn_tick * 1000), t_dyn_tick)
                        t_dump_price = Decimal(str(int(round(raw_t_dump / t_dyn_tick)))) * t_dyn_tick

                    # 🌟 price band 保护: 防止 1000-tick 偏移超出 Deribit 允许范围
                    if _em_t_ticker:
                        if _em_t_ticker.min_price > 0 and t_dump_price < _em_t_ticker.min_price:
                            t_dump_price = _em_t_ticker.min_price
                        if _em_t_ticker.max_price > 0 and t_dump_price > _em_t_ticker.max_price:
                            t_dump_price = _em_t_ticker.max_price
                    logger.warning(
                        f"{prefix}🚑 正在强平期权 Taker 腿残骸 {taker_symbol} (数量: {t_opt_final.filled_amount})...")
                    rb_topt = await self.client.place_order(taker_symbol, t_opt_final.filled_amount, t_close_side, 'limit',
                                                  price=t_dump_price, label=f"em_t_{label}", log_prefix=log_prefix,
                                                  reduce_only=not is_exit, time_in_force="immediate_or_cancel")
                    t_remaining = t_opt_final.filled_amount - (rb_topt.filled_amount if rb_topt else Decimal('0'))
                    if t_remaining > 0:
                        # 重试取最新盘口价
                        _em_t_ticker2 = self.client.tickers.get(taker_symbol)
                        if _em_t_ticker2 and ((_em_t_ticker2.ask > 0 and t_close_side == 'buy') or (_em_t_ticker2.bid > 0 and t_close_side == 'sell')):
                            _em_t_raw2 = _em_t_ticker2.ask if t_close_side == 'buy' else _em_t_ticker2.bid
                            t_retry_price = Decimal(str(int(round(Decimal(str(_em_t_raw2)) / t_dyn_tick)))) * t_dyn_tick
                        else:
                            raw_t_retry = base_t_price + (t_dyn_tick * 1000) if t_close_side == 'buy' else max(
                                base_t_price - (t_dyn_tick * 1000), t_dyn_tick)
                            t_retry_price = Decimal(str(int(round(raw_t_retry / t_dyn_tick)))) * t_dyn_tick
                        # 🌟 price band 保护
                        if _em_t_ticker2:
                            if _em_t_ticker2.min_price > 0 and t_retry_price < _em_t_ticker2.min_price:
                                t_retry_price = _em_t_ticker2.min_price
                            if _em_t_ticker2.max_price > 0 and t_retry_price > _em_t_ticker2.max_price:
                                t_retry_price = _em_t_ticker2.max_price
                        logger.warning(f"{prefix}🚑 期权Taker腿首次IOC未满额(残{t_remaining})，加大滑点重试...")
                        rb_topt2 = await self.client.place_order(taker_symbol, t_remaining, t_close_side, 'limit',
                                                      price=t_retry_price, label=f"em2_t_{label}", log_prefix=log_prefix,
                                                      reduce_only=not is_exit, time_in_force="immediate_or_cancel")
                        t_remaining -= (rb_topt2.filled_amount if rb_topt2 else Decimal('0'))
                    if t_remaining > 0:
                        rollback_failures.append(f"期权Taker腿 {taker_symbol}: 需平{t_opt_final.filled_amount}, 残余{t_remaining}")
                        # ===== Layer 2: IOC全部失败后挂普通限价单兜底 (await追踪) =====
                        _l2t_ticker = self.client.tickers.get(taker_symbol)
                        if _l2t_ticker:
                            _l2t_price = _l2t_ticker.ask if t_close_side == 'buy' else _l2t_ticker.bid
                            if _l2t_price > 0:
                                _l2t_p = Decimal(str(int(round(Decimal(str(_l2t_price)) / t_dyn_tick)))) * t_dyn_tick
                                try:
                                    _l2t_order = await asyncio.wait_for(
                                        self.client.place_order(
                                            taker_symbol, t_remaining, t_close_side, 'limit', price=_l2t_p,
                                            label=f"l2t_{label}", log_prefix=log_prefix, reduce_only=not is_exit),
                                        timeout=5.0)
                                    if _l2t_order and _l2t_order.order_id:
                                        logger.warning(f"{prefix}🔄 [L2-兜底挂单] 期权Taker腿 {taker_symbol} 已挂限价回滚 @ {_l2t_p} (id={_l2t_order.order_id})")
                                        _l2t_left = await _watch_l2_rollback_order(
                                            _l2t_order, taker_symbol, t_remaining, "期权Taker腿")
                                        if _l2t_left > Decimal('0.0001'):
                                            rollback_failures.append(f"期权Taker L2超时未成已撤单: {taker_symbol} 残余{_l2t_left}")
                                            logger.error(
                                                f"{prefix}🚨 [L2-超时撤单] 期权Taker腿 {taker_symbol} "
                                                f"残余={_l2t_left}，已撤单并上报人工处理")
                                    else:
                                        rollback_failures.append(f"期权Taker L2 下单失败: {taker_symbol} @ {_l2t_p}")
                                except Exception as _l2t_err:
                                    logger.error(f"{prefix}🚨 [L2-兜底失败] 期权Taker腿 {taker_symbol}: {_l2t_err}")
                                    rollback_failures.append(f"期权Taker L2 异常: {taker_symbol} - {_l2t_err}")
                        # ==================================================

                # 4. 强平期货 Taker 腿 (IOC + 加大滑点重试)
                f_order_final = self.client.get_order_by_id(f_order.order_id) if f_order else None
                if f_order_final and f_order_final.filled_amount > 0:
                    f_close_side = 'sell' if f_side == 'buy' else 'buy'
                    # 使用当前最新盘口价，避免 price_too_low/high 错误
                    _f_ticker = self.client.tickers.get(future_symbol)
                    if _f_ticker and _f_ticker.bid > 0 and _f_ticker.ask > 0:
                        base_f_price = _f_ticker.ask if f_close_side == 'buy' else _f_ticker.bid
                    else:
                        base_f_price = Decimal(str(f_price)) if f_price else Decimal(str(f_order_final.average_price or 60000))
                    raw_f_dump = base_f_price + (f_tick * 1000) if f_close_side == 'buy' else max(
                        base_f_price - (f_tick * 1000), f_tick)
                    f_dump_price = Decimal(str(int(round(raw_f_dump / f_tick)))) * f_tick

                    logger.warning(f"{prefix}🚑 正在强平期货 Taker 腿残骸 {future_symbol} (数量: {f_order_final.filled_amount})...")
                    rb_fut = await self.client.place_order(future_symbol, f_order_final.filled_amount, f_close_side, 'limit',
                                                  price=f_dump_price, label=f"em_f_{label}", log_prefix=log_prefix,
                                                  reduce_only=False, time_in_force="immediate_or_cancel")
                    f_remaining = f_order_final.filled_amount - (rb_fut.filled_amount if rb_fut else Decimal('0'))
                    if f_remaining > 0:
                        # 加大滑点重试 (1000 ticks)，使用最新盘口价
                        _f_ticker2 = self.client.tickers.get(future_symbol)
                        if _f_ticker2 and _f_ticker2.bid > 0 and _f_ticker2.ask > 0:
                            base_f_price2 = _f_ticker2.ask if f_close_side == 'buy' else _f_ticker2.bid
                        else:
                            base_f_price2 = base_f_price
                        raw_f_retry = base_f_price2 + (f_tick * 1000) if f_close_side == 'buy' else max(
                            base_f_price2 - (f_tick * 1000), f_tick)
                        f_retry_price = Decimal(str(int(round(raw_f_retry / f_tick)))) * f_tick
                        logger.warning(f"{prefix}🚑 期货Taker腿首次IOC未满额(残{f_remaining})，加大滑点重试...")
                        rb_fut2 = await self.client.place_order(future_symbol, f_remaining, f_close_side, 'limit',
                                                      price=f_retry_price, label=f"em2_f_{label}", log_prefix=log_prefix,
                                                      reduce_only=False, time_in_force="immediate_or_cancel")
                        f_remaining -= (rb_fut2.filled_amount if rb_fut2 else Decimal('0'))
                    if f_remaining > 0:
                        rollback_failures.append(f"期货Taker腿 {future_symbol}: 需平{f_order_final.filled_amount}, 残余{f_remaining}")
                        # ===== Layer 2: IOC全部失败后挂普通限价单兜底 (await追踪) =====
                        _l2f_ticker = self.client.tickers.get(future_symbol)
                        if _l2f_ticker:
                            _l2f_price = _l2f_ticker.ask if f_close_side == 'buy' else _l2f_ticker.bid
                            if _l2f_price > 0:
                                _l2f_p = Decimal(str(int(round(Decimal(str(_l2f_price)) / f_tick)))) * f_tick
                                try:
                                    _l2f_order = await asyncio.wait_for(
                                        self.client.place_order(
                                            future_symbol, f_remaining, f_close_side, 'limit', price=_l2f_p,
                                            label=f"l2f_{label}", log_prefix=log_prefix, reduce_only=False),
                                        timeout=5.0)
                                    if _l2f_order and _l2f_order.order_id:
                                        logger.warning(f"{prefix}🔄 [L2-兜底挂单] 期货Taker腿 {future_symbol} 已挂限价回滚 @ {_l2f_p} (id={_l2f_order.order_id})")
                                        _l2f_left = await _watch_l2_rollback_order(
                                            _l2f_order, future_symbol, f_remaining, "期货Taker腿")
                                        if _l2f_left > Decimal('0.0001'):
                                            rollback_failures.append(f"期货Taker L2超时未成已撤单: {future_symbol} 残余{_l2f_left}")
                                            logger.error(
                                                f"{prefix}🚨 [L2-超时撤单] 期货Taker腿 {future_symbol} "
                                                f"残余={_l2f_left}，已撤单并上报人工处理")
                                    else:
                                        rollback_failures.append(f"期货Taker L2 下单失败: {future_symbol} @ {_l2f_p}")
                                except Exception as _l2f_err:
                                    logger.error(f"{prefix}🚨 [L2-兜底失败] 期货Taker腿 {future_symbol}: {_l2f_err}")
                                    rollback_failures.append(f"期货Taker L2 异常: {future_symbol} - {_l2f_err}")
                        # ==================================================

                # ================= 回滚验证：REST 二次确认残余敞口 =================
                if rollback_failures:
                    await asyncio.sleep(1.0)  # 等待交易所结算
                    # REST 查询真实持仓，检测是否还有未平的裸露敞口
                    try:
                        await self.client.get_positions(self.client.target_currency, silent=True)
                    except Exception:
                        pass
                    failure_detail = "\n".join(rollback_failures)
                    logger.error(f"{prefix}🚨🚨 回滚单未完全成交！残余敞口详情:\n{failure_detail}")
                    asyncio.create_task(tg_notifier.send_async(
                        f"🚨🚨【回滚失败·敞口裸露】{prefix}\n"
                        f"回滚单未完全成交，存在残余敞口！\n{failure_detail}\n"
                        f"⚠️ 请立即登录交易所手动处理！"))
                # ==================================================================================

                if rollback_failures:
                    # 回滚有残余 → 保持暂停，需要人工介入
                    asyncio.create_task(tg_notifier.send_async(
                        f"🚨【致命异常】{prefix}Taker 对冲阵型崩溃！\n"
                        f"回滚存在残余敞口，系统进入暂停状态！请立即登录交易所核对！"))
                else:
                    # 回滚全部成功 → 仅移除 Taker 失败原因，其他暂停原因不受影响
                    if tg_notifier.engine:
                        tg_notifier.engine._remove_pause("Taker IOC对冲失败")
                    logger.info(f"{prefix}✅ Taker IOC 失败但回滚全部成功，敞口已清零，系统自动恢复交易")
                    asyncio.create_task(tg_notifier.send_async(
                        f"⚠️【IOC对冲失败·已自愈】{prefix}\n"
                        f"Taker IOC 未满额但回滚强平全部成功，无残余敞口。\n系统已自动恢复交易。"))
                return {'success': False, 'error': 'Taker hedging failed, ALL positions forcefully dumped'}

            call_id = anchor_order_id if anchor_type == 'call' else t_opt_order.order_id
            put_id = anchor_order_id if anchor_type == 'put' else t_opt_order.order_id
            future_id = f_order.order_id if f_order else ''
            return {'success': True, 'orders': [call_id, put_id, future_id], 'label': label,
                    'anchor_leg': anchor_symbol, 'anchor_type': anchor_type,
                    'anchor_order_ids': _anchor_order_ids,
                    'anchor_weighted_avg': str(_anchor_weighted_avg),
                    'anchor_filled_qty': str(_anchor_filled_qty),
                    'fill_tier': _tier_names[_current_tier]}
        except Exception as e:
            logger.error(f"{prefix}策略执行内层捕获异常: {e}")
            return {'success': False, 'error': str(e)}

            # --- 修复：在方法最后添加 finally 强制同步 ---
        finally:
            # 核心：无论代码是正常执行完毕、遇到超时异常、还是中途 return
            # 只要这个方法结束，就强制执行一次真实的持仓同步！
            # 🌟 修复：WS 断连时跳过，避免注定失败的 API 调用刷屏
            # ⚠️ 注意：不能在 finally 中使用 return，否则会覆盖 try/except 的正常返回值！
            if not self.client.is_connected:
                logger.warning(f"{prefix}⚠️ WS 已断开，跳过 finally 持仓同步")
                try:
                    anchor_id = locals().get('anchor_order_id')
                    if anchor_id and not locals().get('is_exit', False) and not locals().get('anchor_filled', False):
                        latest_anchor = self.client.get_order_by_id(anchor_id)
                        latest_status = str(getattr(latest_anchor, 'status', '') or '').lower() if latest_anchor else ''
                        latest_filled = Decimal(str(getattr(latest_anchor, 'filled_amount', 0) or 0)) if latest_anchor else Decimal('0')
                        latest_amount = Decimal(str(getattr(latest_anchor, 'amount', 0) or 0)) if latest_anchor else Decimal('0')
                        if latest_status not in ('filled', 'cancelled', 'rejected') and (latest_amount <= 0 or latest_filled < latest_amount):
                            engine = getattr(self, 'engine', None) or getattr(tg_notifier, 'engine', None)
                            if engine:
                                pending = getattr(engine, '_anchor_ws_disconnect_pending_orders', set())
                                pending.add(str(anchor_id))
                                engine._anchor_ws_disconnect_pending_orders = pending
                                engine._add_pause("锚定腿WS断连待核查")
                                now = time.time()
                                if now - getattr(engine, '_anchor_ws_disconnect_log_ts', 0.0) >= max(float(getattr(engine, 'risk_alert_throttle_seconds', 300.0)), 30.0):
                                    engine._anchor_ws_disconnect_log_ts = now
                                    logger.warning(
                                        f"{prefix}🛑 WS断线时存在未确认锚定单 {anchor_id}，"
                                        f"暂停新扫描，等待重连后撤单+查成交")
                except Exception as pause_err:
                    logger.warning(f"{prefix}锚定腿断线暂停标记失败: {pause_err}")
            else:
                try:
                    # ====== 加上 silent=True 防止疯狂刷屏 ======
                    await self.client.get_positions(self.client.target_currency, silent=True)
                    logger.info(f"{prefix}🔄 状态机护栏：已通过 API 强制刷新账户真实持仓。")
                except Exception as sync_e:
                    logger.error(f"{prefix}状态机护栏同步失败: {sync_e}")

    def _get_spread(self, instrument: str) -> Decimal:
        """获取买卖价差"""
        ticker = self.client.tickers.get(instrument)
        if ticker and ticker.ask > Decimal('0') and ticker.bid > Decimal('0'):
            return ticker.ask - ticker.bid
        return Decimal('999999')
