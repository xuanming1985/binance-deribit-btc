"""engine/redis_mixin.py — Redis 状态存储/恢复/重建"""
from __future__ import annotations
import asyncio
from utils import FastJSON
json = FastJSON()
import logging
import os
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set, Any
from collections import defaultdict

if TYPE_CHECKING:
    pass

from telegram_handler import tg_notifier
from engine.models import ArbitrageState

logger = logging.getLogger(__name__)


class RedisMixin:
    """Mixin: Redis 状态存储 + 恢复 + 降级重建"""

    async def _save_state_to_redis(self, state: ArbitrageState):
        """将状态机快照写入 Redis 内存库"""
        try:
            _bn_qty_to_save = state.binance_filled_qty
            _bn_open_qty_to_save = getattr(state, 'binance_open_qty', Decimal('0'))
            _entry_amt = state.entry_amount if getattr(state, 'entry_amount', Decimal('0')) > 0 else Decimal('0')
            if _entry_amt > 0 and (_bn_qty_to_save - _entry_amt) > Decimal('0.001'):
                logger.warning(
                    f"[Redis保存护栏] [{state.expiry_strike[0]}-{state.expiry_strike[1]}] "
                    f"binance_filled_qty={_bn_qty_to_save} > entry_amount={_entry_amt}，已夹紧")
                _bn_qty_to_save = _entry_amt
                state.binance_filled_qty = _bn_qty_to_save
            if _entry_amt > 0 and (_bn_open_qty_to_save - _entry_amt) > Decimal('0.001'):
                logger.warning(
                    f"[Redis保存护栏] [{state.expiry_strike[0]}-{state.expiry_strike[1]}] "
                    f"binance_open_qty={_bn_open_qty_to_save} > entry_amount={_entry_amt}，已夹紧")
                _bn_open_qty_to_save = _entry_amt
                state.binance_open_qty = _bn_open_qty_to_save
            if _bn_open_qty_to_save < 0:
                _bn_open_qty_to_save = Decimal('0')
                state.binance_open_qty = Decimal('0')

            key = f"arb_state:{self.target_currency}:{state.expiry_strike[0]}_{state.expiry_strike[1]}"
            data = {
                'expiry': state.expiry_strike[0],
                'strike': str(state.expiry_strike[1]),
                'state': state.state,
                'strategy_type': state.strategy_type,
                'entry_amount': str(state.entry_amount),
                'future_size_usd': str(state.future_size_usd),
                'entry_prices': {k: str(v) for k, v in state.entry_prices.items()},
                'start_time': state.start_time,
                'last_update': state.last_update,
                'prices_confirmed': state.prices_confirmed,
                # peak_pnl_usd: 已废弃，保留字段兼容旧 Redis 数据反序列化
                'combo_id': state.combo_id,
                'binance_future_symbol': state.binance_future_symbol,
                'binance_future_type': state.binance_future_type,
                'binance_position_side': state.binance_position_side,
                'binance_order_id': state.binance_order_id,
                'binance_close_order_id': getattr(state, 'binance_close_order_id', ''),
                'binance_entry_price': str(state.binance_entry_price),
                'binance_open_qty': str(_bn_open_qty_to_save),
                'binance_filled_qty': str(_bn_qty_to_save),
                # 🌟 审计关键: Deribit 期权订单 ID 持久化 (P1-17 回归修复)
                'call_order_id': getattr(state, 'call_order_id', ''),
                'put_order_id': getattr(state, 'put_order_id', ''),
                'accumulated_funding': str(state.accumulated_funding),
                'delivery_csv_written': bool(getattr(state, '_delivery_csv_written', False)),
                'settlement_twap_started': bool(getattr(state, '_settlement_twap_started', False)),
                'settlement_twap_qty_snapshot': str(getattr(state, '_settlement_twap_qty_snapshot', Decimal('0'))),
                'settlement_twap_accumulated': getattr(state, '_settlement_twap_accumulated', None),
                'settlement_twap_retry_count': int(getattr(state, '_settlement_twap_retry_count', 0)),
                'entry_basis_usd': getattr(state, 'entry_basis_usd', None),
                # 🌟 2026-04-24: Binance 对冲关闭时刻 (TWAP 提前平仓后 ≈ 2h 才写记录)
                # 持久化防止 TWAP 完成后、交割写入前的重启导致时间丢失
                'hedge_close_completed_ts': float(getattr(state, '_hedge_close_completed_ts', 0.0)),
                'settle_retries': int(getattr(state, '_settle_retries', 0)),
                'settle_last_attempt_ts': float(getattr(state, '_settle_last_attempt_ts', 0.0)),
            }
            # 保存合约名称，重启后无需依赖 arbitrage_combinations 重建
            combination = self.arbitrage_combinations.get(state.expiry_strike)
            if combination:
                data['instruments'] = {
                    'call': combination['call'],
                    'put': combination['put'],
                    'future': combination['future']
                }
            # 序列化为 JSON 存入 Redis
            await self.redis.set(key, json.dumps(data))
        except Exception as e:
            logger.error(f"Redis 保存状态失败: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"Redis 状态持久化失败: {e}", "redis_save_failed"))

    async def _save_daily_pnl_to_redis(self):
        """持久化日损熔断状态，防止重启后丢失已实现亏损"""
        try:
            key = f"daily_pnl:{self.target_currency}"
            data = {
                'date': self._daily_loss_date,
                'realized_pnl': self._daily_realized_pnl,
                'triggered': self._daily_loss_triggered,
            }
            await self.redis.set(key, json.dumps(data))
        except Exception as e:
            logger.info(f"日损状态持久化失败: {e}")

    async def _recover_daily_pnl_from_redis(self):
        """启动时恢复日损熔断状态"""
        try:
            from datetime import datetime, timezone
            _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            key = f"daily_pnl:{self.target_currency}"
            data_str = await self.redis.get(key)
            if not data_str:
                return
            data = json.loads(data_str)
            if data.get('date') != _today:
                return
            self._daily_loss_date = _today
            self._daily_realized_pnl = float(data.get('realized_pnl', 0.0))
            self._daily_loss_triggered = bool(data.get('triggered', False))
            if self._daily_loss_triggered:
                self._add_pause("日损熔断")
            if self._daily_realized_pnl != 0.0 or self._daily_loss_triggered:
                logger.info(f"📅 [日损恢复] 从 Redis 恢复今日已实现净盈亏: ${self._daily_realized_pnl:+.2f}"
                           f"{' (熔断已触发, 已恢复暂停)' if self._daily_loss_triggered else ''}")
        except Exception as e:
            logger.info(f"日损状态恢复失败: {e}")

    async def _calibrate_daily_pnl_from_sqlite(self):
        """SQLite 校准日损账本: 用终态记录的已实现 PnL 替代 Redis 缓存值。
        必须在 _trade_store.init() 之后调用。Redis 仅保留 triggered 标志。"""
        try:
            from datetime import datetime, timezone
            _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            _today_utc_midnight = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0)
            _summary = await self._trade_store.realized_summary_since(
                _today_utc_midnight.timestamp())
            if not _summary.get('available'):
                return
            _sqlite_pnl = float(_summary.get('realized_pnl_usd', 0.0))
            _redis_pnl = self._daily_realized_pnl
            if abs(_sqlite_pnl - _redis_pnl) > 0.01:
                self._daily_loss_date = _today
                self._daily_realized_pnl = min(_sqlite_pnl, _redis_pnl)
                logger.info(
                    f"📅 [日损校准] SQLite=${_sqlite_pnl:+.2f} vs Redis=${_redis_pnl:+.2f} → "
                    f"采用 ${self._daily_realized_pnl:+.2f} (取更保守值)")
        except Exception as e:
            logger.info(f"日损 SQLite 校准失败 (非致命): {e}")

    async def _delete_state_from_redis(self, expiry: str, strike: Decimal):
        """从 Redis 清理已平仓/退出的状态"""
        try:
            key = f"arb_state:{self.target_currency}:{expiry}_{strike}"
            await self.redis.delete(key)
        except Exception as e:
            pass

    async def _sanitize_combo_binance_qty(self, source: str = "") -> int:
        """护栏：单组合 Binance 对冲数量不应超过该组合名义数量(entry_amount)"""
        fixed = 0
        try:
            _tol = Decimal('0.001')
            for _es, _st in list(self.arbitrage_states.items()):
                if _st.state not in ('position_open', 'executing', 'exiting'):
                    continue
                if not _st.binance_future_symbol or _st.binance_filled_qty <= 0:
                    continue
                _entry = _st.entry_amount if getattr(_st, 'entry_amount', Decimal('0')) > 0 else Decimal('0')
                if _entry <= 0:
                    continue
                _changed = False
                if (_st.binance_filled_qty - _entry) > _tol:
                    _old = _st.binance_filled_qty
                    _st.binance_filled_qty = _entry
                    _changed = True
                    fixed += 1
                    logger.warning(
                        f"[Binance分腿对账-护栏/{source or 'runtime'}] "
                        f"[{_es[0]}-{_es[1]}] 状态机对冲数量异常: {_old} > 组合名义 {_entry}，已夹紧")
                if getattr(_st, 'binance_open_qty', Decimal('0')) > _entry:
                    _old_open = _st.binance_open_qty
                    _st.binance_open_qty = _entry
                    _changed = True
                    logger.warning(
                        f"[Binance分腿对账-护栏/{source or 'runtime'}] "
                        f"[{_es[0]}-{_es[1]}] 开仓数量快照异常: {_old_open} > 组合名义 {_entry}，已夹紧")
                if _changed:
                    _st.last_update = time.time()
                    await self._save_state_to_redis(_st)
        except Exception as e:
            logger.error(f"组合Binance数量护栏异常: {e}")
        return fixed

    async def _recover_states_from_redis(self):
        """启动时从 Redis 恢复精确成本，并与交易所真实持仓交叉验证 (防呆机制)"""
        await self._recover_daily_pnl_from_redis()
        recovered_count = 0
        try:
            pattern = f"arb_state:{self.target_currency}:*"
            keys = await self.redis.keys(pattern)
            for key in keys:
              try:
                data_str = await self.redis.get(key)
                if not data_str: continue
                data = json.loads(data_str)

                expiry = data['expiry']
                strike = Decimal(data['strike'])

                # 优先从 Redis 中恢复合约名称，避免因初始化过滤条件变更导致找不到组合
                instruments = data.get('instruments')
                combination = self.arbitrage_combinations.get((expiry, strike))

                if not combination and instruments:
                    # 初始化时未重建该组合（如 moneyness 过滤），从 Redis 中恢复
                    combination = {
                        'call': instruments['call'],
                        'put': instruments['put'],
                        'future': instruments['future']
                    }
                    self.arbitrage_combinations[(expiry, strike)] = combination
                    logger.info(f"从 Redis 补全套利组合: [{expiry}-{strike}] (初始化未重建)")

                    # 补充订阅该组合的行情频道
                    channels = []
                    for inst in [combination['call'], combination['put'], combination['future']]:
                        channels.append(f"ticker.{inst}.100ms")
                        channels.append(f"book.{inst}.raw")
                    try:
                        await self.client.send_request({
                            "jsonrpc": "2.0",
                            "id": self.client._get_next_request_id(),
                            "method": "public/subscribe",
                            "params": {"channels": channels}
                        })
                    except Exception as sub_e:
                        logger.warning(f"补充订阅 [{expiry}-{strike}] 行情失败: {sub_e}")

                if not combination:
                    continue

                # 交叉验证：向 Deribit 查验。万一你人工在网页端平仓了，就不该恢复这个状态！
                c_pos = self.client.positions.get(combination['call'])
                p_pos = self.client.positions.get(combination['put'])
                c_alive = c_pos and c_pos.size != 0
                p_alive = p_pos and p_pos.size != 0

                if not c_alive and not p_alive:
                    # 两条期权腿都无持仓
                    _bn_qty = Decimal(data.get('binance_filled_qty', '0'))
                    _entry_amt = Decimal(data.get('entry_amount', '0'))
                    if _entry_amt > 0 and _bn_qty > _entry_amt:
                        logger.info(
                            f"[{expiry}-{strike}] Redis恢复: Binance数量异常({_bn_qty})>组合名义({_entry_amt})，已夹紧到名义值")
                        _bn_qty = _entry_amt
                    _bn_open_qty = Decimal(data.get('binance_open_qty', str(_bn_qty)))
                    if _entry_amt > 0 and _bn_open_qty > _entry_amt:
                        _bn_open_qty = _entry_amt
                    if _bn_open_qty < 0:
                        _bn_open_qty = Decimal('0')
                    _delivery_ready = False
                    try:
                        from datetime import datetime, timezone, timedelta
                        _raw_exp = datetime.strptime(expiry, "%d%b%y")
                        _exp_dt = _raw_exp.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                        _delivery_ready = datetime.now(timezone.utc) >= (_exp_dt - timedelta(minutes=5))
                    except Exception:
                        _delivery_ready = False
                    if _bn_qty > 0:
                        if not _delivery_ready:
                            # 未到交割时窗：恢复到状态机持续跟踪，避免"有仓位但无状态机"的盲区
                            _hold_state = ArbitrageState(
                                expiry_strike=(expiry, strike),
                                state='position_open',
                                strategy_type=data['strategy_type'],
                                entry_amount=Decimal(data['entry_amount']),
                                future_size_usd=Decimal(data['future_size_usd']),
                                entry_prices={k: Decimal(v) for k, v in data['entry_prices'].items()},
                                start_time=data.get('start_time', data['last_update']),
                                last_update=data['last_update'],
                                prices_confirmed=data.get('prices_confirmed', False),
                                combo_id=data.get('combo_id', ''),
                                binance_future_symbol=data.get('binance_future_symbol', ''),
                                binance_future_type=data.get('binance_future_type', ''),
                                binance_position_side=data.get('binance_position_side', ''),
                                binance_order_id=data.get('binance_order_id', ''),
                                binance_close_order_id=data.get('binance_close_order_id', ''),
                                call_order_id=data.get('call_order_id', ''),
                                put_order_id=data.get('put_order_id', ''),
                                binance_entry_price=Decimal(data.get('binance_entry_price', '0')),
                                binance_open_qty=_bn_open_qty,
                                binance_filled_qty=_bn_qty,
                                accumulated_funding=Decimal(data.get('accumulated_funding', '0')),
                                _delivery_csv_written=bool(data.get('delivery_csv_written', False)),
                            )
                            if data.get('settlement_twap_started'):
                                _hold_state._settlement_twap_started = True
                                _hold_state._settlement_twap_qty_snapshot = Decimal(data.get('settlement_twap_qty_snapshot', '0'))
                                _hold_acc = data.get('settlement_twap_accumulated')
                                if isinstance(_hold_acc, dict):
                                    _hold_state._settlement_twap_accumulated = _hold_acc
                                _hold_state._settlement_twap_retry_count = int(data.get('settlement_twap_retry_count', 0))
                            if data.get('entry_basis_usd') is not None:
                                _hold_state.entry_basis_usd = float(data['entry_basis_usd'])
                            _hcct = float(data.get('hedge_close_completed_ts', 0.0) or 0.0)
                            if _hcct > 0:
                                _hold_state._hedge_close_completed_ts = _hcct
                            _sr_h = int(data.get('settle_retries', 0) or 0)
                            if _sr_h > 0:
                                _hold_state._settle_retries = _sr_h
                                _hold_state._settle_last_attempt_ts = float(data.get('settle_last_attempt_ts', 0.0) or 0.0)
                            self.arbitrage_states[(expiry, strike)] = _hold_state
                            self.position_locks.add((expiry, strike))
                            recovered_count += 1
                            logger.warning(
                                f"[{expiry}-{strike}] 期权腿缺失且 Binance 有残仓 qty={_bn_qty}，"
                                f"但未到交割时窗，已恢复状态机跟踪并等待后续结算/重试")
                            continue
                        # 期权已到期/结算，但 Binance 对冲腿仍有持仓 → 触发交割结算流程
                        logger.warning(
                            f"[{expiry}-{strike}] 期权已到期但 Binance 仍有持仓 qty={_bn_qty}，"
                            f"恢复状态并触发交割结算...")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"[启动恢复] {expiry}-{strike} 期权已到期\n"
                            f"Binance 残留: {data.get('binance_future_symbol','')} qty={_bn_qty}\n"
                            f"正在触发交割结算...",
                            f"startup_delivery_{expiry}"))
                        # 构建临时 state 用于交割结算
                        _tmp_state = ArbitrageState(
                            expiry_strike=(expiry, strike),
                            state='position_open',
                            strategy_type=data['strategy_type'],
                            entry_amount=Decimal(data['entry_amount']),
                            future_size_usd=Decimal(data['future_size_usd']),
                            entry_prices={k: Decimal(v) for k, v in data['entry_prices'].items()},
                            start_time=data.get('start_time', data['last_update']),
                            last_update=data['last_update'],
                            prices_confirmed=data.get('prices_confirmed', False),
                            combo_id=data.get('combo_id', ''),
                            binance_future_symbol=data.get('binance_future_symbol', ''),
                            binance_future_type=data.get('binance_future_type', ''),
                            binance_position_side=data.get('binance_position_side', ''),
                            binance_order_id=data.get('binance_order_id', ''),
                            binance_close_order_id=data.get('binance_close_order_id', ''),
                            call_order_id=data.get('call_order_id', ''),
                            put_order_id=data.get('put_order_id', ''),
                            binance_entry_price=Decimal(data.get('binance_entry_price', '0')),
                            binance_open_qty=_bn_open_qty,
                            binance_filled_qty=_bn_qty,
                            accumulated_funding=Decimal(data.get('accumulated_funding', '0')),
                            _delivery_csv_written=bool(data.get('delivery_csv_written', False)),
                        )
                        if data.get('settlement_twap_started'):
                            _tmp_state._settlement_twap_started = True
                            _tmp_state._settlement_twap_qty_snapshot = Decimal(data.get('settlement_twap_qty_snapshot', '0'))
                            _tmp_acc = data.get('settlement_twap_accumulated')
                            if isinstance(_tmp_acc, dict):
                                _tmp_state._settlement_twap_accumulated = _tmp_acc
                            _tmp_state._settlement_twap_retry_count = int(data.get('settlement_twap_retry_count', 0))
                        if data.get('entry_basis_usd') is not None:
                            _tmp_state.entry_basis_usd = float(data['entry_basis_usd'])
                        _hcct_tmp = float(data.get('hedge_close_completed_ts', 0.0) or 0.0)
                        if _hcct_tmp > 0:
                            _tmp_state._hedge_close_completed_ts = _hcct_tmp
                        _sr_t = int(data.get('settle_retries', 0) or 0)
                        if _sr_t > 0:
                            _tmp_state._settle_retries = _sr_t
                            _tmp_state._settle_last_attempt_ts = float(data.get('settle_last_attempt_ts', 0.0) or 0.0)
                        # 🌟 P1-B 修复: 将 _tmp_state 注入 arbitrage_states 和 position_locks
                        # 旧逻辑: _tmp_state 只在异步任务中使用，不注入主状态机 →
                        #   结算首次失败后 monitor_positions 不会重试，且幽灵检测可能误平
                        # 新逻辑: 注入主状态机，让 monitor_positions 的三腿归零检测和
                        #   永续超时安全网都能兜底重试
                        _es = (expiry, strike)
                        self.arbitrage_states[_es] = _tmp_state
                        self.position_locks.add(_es)
                        logger.info(f"[{expiry}-{strike}] 已将延迟交割状态注入 arbitrage_states，monitor 可兜底重试")

                        # 延迟执行交割结算（等待 Binance 连接就绪）
                        async def _delayed_delivery(s, c, k, delay=8):
                            await asyncio.sleep(delay)
                            try:
                                await self._handle_delivery_settlement(s, c)
                            except Exception as de:
                                logger.error(f"延迟交割结算失败: {de}")
                            finally:
                                # 仅在结算成功（或残仓已清）时清理；失败保留让 monitor 继续重试
                                _bn_left = getattr(s, 'binance_filled_qty', Decimal('0'))
                                if s.state == 'exited' or _bn_left <= Decimal('0.0001'):
                                    await self.redis.delete(k)
                                    # state 已被 _handle_delivery_settlement 标记为 exited，
                                    # 5分钟后 monitor_positions 的终态清理会自动移除
                                else:
                                    s.state = 'position_open'
                                    s.last_update = time.time()
                                    await self._save_state_to_redis(s)
                                    logger.warning(
                                        f"[{s.expiry_strike[0]}-{s.expiry_strike[1]}] 启动恢复交割未完成，"
                                        f"保留状态由 monitor_positions 继续重试 (Binance 剩余={_bn_left})")
                        asyncio.create_task(_delayed_delivery(_tmp_state, combination, key))
                        continue
                    # 无 Binance 持仓 → 真正的脏状态，安全清理
                    logger.warning(f"发现脏状态 [{expiry}-{strike}]，交易所 Call+Put 均无持仓，正在清理 Redis...")
                    await self.redis.delete(key)
                    continue

                if not c_alive or not p_alive:
                    # 部分腿缺失 → 破损组合，触发孤立腿强平
                    missing = combination['call'] if not c_alive else combination['put']
                    surviving = combination['put'] if not c_alive else combination['call']
                    surviving_pos = p_pos if not c_alive else c_pos
                    logger.error(
                        f"🚨 [启动恢复] 发现破损组合 [{expiry}-{strike}]："
                        f"缺失 {missing}，残留 {surviving} (size={surviving_pos.size})，"
                        f"将触发孤立腿强平...")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 [启动恢复-破损组合] {expiry}-{strike}\n"
                        f"缺失: {missing}\n"
                        f"残留: {surviving} size={surviving_pos.size}\n"
                        f"正在触发孤立腿强平...",
                        f"startup_broken_{expiry}"))
                    # 延迟强平：等待行情订阅推送到位后再执行（启动阶段行情可能尚未到达）
                    async def _delayed_ghost_close(inst_name, _pos_obj, delay=5):
                        await asyncio.sleep(delay)
                        # 强平前重新获取最新持仓，确认仍需处理
                        await self.client.get_positions(self.target_currency, silent=True)
                        fresh_pos = self.client.positions.get(inst_name)
                        if fresh_pos and fresh_pos.size != 0:
                            await self._auto_close_ghost_position(inst_name, fresh_pos)
                        else:
                            logger.info(f"[启动恢复] {inst_name} 已无持仓，跳过强平")
                            self._ghost_closing.discard(inst_name)

                    self._ghost_first_seen[surviving] = 0  # 设为 0 使宽限期立即过期
                    self._ghost_closing.add(surviving)
                    asyncio.create_task(_delayed_ghost_close(surviving, surviving_pos))
                    # 同时检查是否有残留的期货腿需要处理
                    f_pos = self.client.positions.get(combination['future'])
                    if f_pos and f_pos.size != 0:
                        logger.error(f"🚨 [启动恢复] 期货腿 {combination['future']} 也有残留 size={f_pos.size}，一并强平")
                        self._ghost_first_seen[combination['future']] = 0
                        self._ghost_closing.add(combination['future'])
                        asyncio.create_task(_delayed_ghost_close(combination['future'], f_pos))
                    await self.redis.delete(key)
                    continue

                # 完美复原上一秒的精确成本，不需要再用均价去"瞎猜"了
                _entry_amt = Decimal(data['entry_amount'])
                _bn_qty = Decimal(data.get('binance_filled_qty', '0'))
                if _entry_amt > 0 and _bn_qty > _entry_amt:
                    logger.info(
                        f"[{expiry}-{strike}] Redis恢复: Binance数量异常({_bn_qty})>组合名义({_entry_amt})，已夹紧到名义值")
                    _bn_qty = _entry_amt
                _bn_open_qty = Decimal(data.get('binance_open_qty', str(_bn_qty)))
                if _entry_amt > 0 and _bn_open_qty > _entry_amt:
                    _bn_open_qty = _entry_amt
                if _bn_open_qty < 0:
                    _bn_open_qty = Decimal('0')

                state = ArbitrageState(
                    expiry_strike=(expiry, strike),
                    state=data['state'],
                    strategy_type=data['strategy_type'],
                    entry_amount=_entry_amt,
                    future_size_usd=Decimal(data['future_size_usd']),
                    entry_prices={k: Decimal(v) for k, v in data['entry_prices'].items()},
                    start_time=data.get('start_time', data['last_update']),  # 兜底: 旧数据无 start_time 时用 last_update
                    last_update=data['last_update'],
                    prices_confirmed=data.get('prices_confirmed', False),
                    # peak_pnl_usd: 已废弃，字段保留兼容旧数据，不再主动恢复
                    combo_id=data.get('combo_id', ''),
                    binance_future_symbol=data.get('binance_future_symbol', ''),
                    binance_future_type=data.get('binance_future_type', ''),
                    binance_position_side=data.get('binance_position_side', ''),
                    binance_order_id=data.get('binance_order_id', ''),
                    binance_close_order_id=data.get('binance_close_order_id', ''),
                    call_order_id=data.get('call_order_id', ''),
                    put_order_id=data.get('put_order_id', ''),
                    binance_entry_price=Decimal(data.get('binance_entry_price', '0')),
                    binance_open_qty=_bn_open_qty,
                    binance_filled_qty=_bn_qty,
                    accumulated_funding=Decimal(data.get('accumulated_funding', '0')),
                    _delivery_csv_written=bool(data.get('delivery_csv_written', False)),
                )
                if data.get('settlement_twap_started'):
                    state._settlement_twap_started = True
                    state._settlement_twap_qty_snapshot = Decimal(data.get('settlement_twap_qty_snapshot', '0'))
                    _rest_acc = data.get('settlement_twap_accumulated')
                    if isinstance(_rest_acc, dict):
                        state._settlement_twap_accumulated = _rest_acc
                    state._settlement_twap_retry_count = int(data.get('settlement_twap_retry_count', 0))
                if data.get('entry_basis_usd') is not None:
                    state.entry_basis_usd = float(data['entry_basis_usd'])
                _hcct_st = float(data.get('hedge_close_completed_ts', 0.0) or 0.0)
                if _hcct_st > 0:
                    state._hedge_close_completed_ts = _hcct_st
                _sr = int(data.get('settle_retries', 0) or 0)
                if _sr > 0:
                    state._settle_retries = _sr
                    state._settle_last_attempt_ts = float(data.get('settle_last_attempt_ts', 0.0) or 0.0)
                self.arbitrage_states[(expiry, strike)] = state
                self.position_locks.add((expiry, strike))
                recovered_count += 1
                _bn_info = ""
                if state.binance_future_symbol:
                    _bn_info = f", Binance={state.binance_future_symbol} qty={state.binance_filled_qty}"
                logger.info(f"从 Redis 精确接管状态: [{expiry}-{strike}], 策略: {state.strategy_type}, "
                           f"数量: {state.entry_amount}, 期货面值: {state.future_size_usd} USD, "
                           f"成本: F={state.entry_prices.get('future','?')} C={state.entry_prices.get('call','?')} P={state.entry_prices.get('put','?')}, "
                           f"价格已确认: {state.prices_confirmed}{_bn_info}")
                await self._ensure_open_record_for_recovered_state(state)

              except Exception as per_key_err:
                _key_label = key if isinstance(key, str) else key.decode('utf-8', errors='replace')
                logger.error(f"Redis 恢复单条记录失败 (key={_key_label}): {per_key_err}")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"⚠️ Redis 恢复跳过脏记录\nkey: {_key_label}\n错误: {per_key_err}",
                    "redis_restore_partial"))
                continue

        except Exception as e:
            logger.error(f"从 Redis 恢复状态机失败 (连接级): {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"Redis 状态恢复失败: {e}", "redis_restore_failed"))

        if recovered_count > 0:
            logger.info(f"共从 Redis 无缝接管了 {recovered_count} 个套利组合的精准状态！")
            _fixed = await self._sanitize_combo_binance_qty("redis_recover")
            if _fixed > 0:
                logger.warning(f"[Binance分腿对账-护栏/redis_recover] 已修复 {_fixed} 个组合的异常对冲数量")
            # 启动恢复后强制做一次 Binance 实仓交叉验证（即使当前未暂停）
            if self.binance_ws:
                await self._refresh_binance_residual_pause(force_check=True)

        # =========================================================================
        # 降级方案：如果 Redis 恢复不到任何状态，从 Deribit 真实持仓自动重建
        # 适用场景：换电脑、Redis 数据丢失、Redis 未安装
        # =========================================================================
        await self._rebuild_states_from_positions()

    async def _ensure_open_record_for_recovered_state(self, state: ArbitrageState):
        """恢复持仓时校验开仓记录；缺失则补写一条"恢复接管"记录，避免监控断档"""
        try:
            combo_id = str(getattr(state, 'combo_id', '') or '').strip()
            if not combo_id:
                return

            _open_exists = False
            try:
                from db_store import _open_conn
                conn = _open_conn(self._db_path)
                try:
                    _row = conn.execute(
                        "SELECT 1 FROM trades WHERE order_id = ? AND trade_type = '开仓' LIMIT 1",
                        (combo_id,)
                    ).fetchone()
                    _open_exists = _row is not None
                finally:
                    conn.close()
            except Exception:
                _open_exists = False

            if _open_exists:
                return

            expiry, strike = state.expiry_strike
            _entry_f = state.entry_prices.get('future', Decimal('0'))
            _entry_c = state.entry_prices.get('call', Decimal('0'))
            _entry_p = state.entry_prices.get('put', Decimal('0'))
            _open_record = {
                '订单ID': combo_id,
                '成交时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(state.start_time or time.time())),
                '策略方向': state.strategy_type or '',
                '到期日': expiry,
                '行权价': float(strike),
                '标的': self.target_currency,
                '期权数量': float(state.entry_amount or Decimal('0')),
                '期货面值(USD)': float(state.future_size_usd or Decimal('0')),
                '模拟_Future价格': 0,
                '实际_Future均价': float(_entry_f),
                '模拟_Call价格': 0,
                '实际_Call均价': float(_entry_c),
                '模拟_Put价格': 0,
                '实际_Put均价': float(_entry_p),
                '模拟_手续费(USD)': 0,
                '实际_手续费(USD)': 0,
                '开仓手续费(USD)': 0,
                '预估结算手续费(USD)': 0,
                '已实现funding(USD)': float(state.accumulated_funding or Decimal('0')),
                '模拟_净利润(USD)': 0,
                '实际_净利润(USD)': 0,
                '滑点与偏差损失(USD)': 0,
                # 🌟 P2 修复: 恢复补录使用 Redis 已持久化的 Call/Put 订单 ID (P1-17 字段持久化前提)
                # 之前写空字符串 → 审计链断裂, 现在只有真正空才写 'RECOVERED' 哨兵 (区分于主路径的 UNCONFIRMED)
                'Call_ID': getattr(state, 'call_order_id', '') or 'RECOVERED',
                'Put_ID': getattr(state, 'put_order_id', '') or 'RECOVERED',
                'Future_ID': self._format_future_id(state),
                '交易类型': '开仓',
                '平仓原因': '恢复接管补录',
            }
            await self._enqueue_trade_record(_open_record)
            logger.warning(
                f"[{expiry}-{int(strike)}] ⚠️ 恢复持仓缺少开仓记录，已补录 (订单ID={combo_id})")
        except Exception as e:
            logger.error(f"恢复持仓补录失败: {e}")

    async def _rebuild_states_from_positions(self):
        """从 Deribit 真实持仓重建 arbitrage_states (Redis 降级方案)"""
        try:
            # 扫描所有持仓中的期权，按 (expiry, strike) 分组
            option_positions = {}  # {(expiry, strike): {'call': pos, 'put': pos}}
            for inst, pos in self.client.positions.items():
                if pos.size == 0:
                    continue
                parsed = self._parse_instrument_name(inst)
                if not parsed or not parsed[2]:  # 跳过期货
                    continue
                currency, expiry, strike, opt_type = parsed
                key = (expiry, strike)
                if key not in option_positions:
                    option_positions[key] = {}
                if opt_type == 'call':
                    option_positions[key]['call'] = pos
                elif opt_type == 'put':
                    option_positions[key]['put'] = pos

            rebuild_count = 0
            # 降级重建时，Binance 返回的是 symbol(+side) 总仓位；
            # 同分腿可能被多个组合共享，必须按"可分配剩余量"分配到组合，避免总仓重复写入每个组合。
            _rb_bn_snapshot_cache: Dict[Tuple[str, str], Tuple[Decimal, str, Decimal, bool]] = {}
            _rb_bn_remaining_by_side: Dict[Tuple[str, str], Decimal] = {}
            for (expiry, strike), opts in option_positions.items():
                # 需要 call + put 都有持仓才算套利组合
                if 'call' not in opts or 'put' not in opts:
                    continue

                # 已经从 Redis 恢复过的跳过
                if (expiry, strike) in self.arbitrage_states:
                    continue

                # 查找或构建 combination
                combination = self.arbitrage_combinations.get((expiry, strike))
                if not combination:
                    # 从持仓的合约名推导 combination
                    c_name = opts['call'].instrument_name
                    p_name = opts['put'].instrument_name
                    # 期货名: BTC-24APR26 (去掉 strike 和 option_type)
                    f_name = f"{self.target_currency}-{expiry}"
                    combination = {'call': c_name, 'put': p_name, 'future': f_name}
                    self.arbitrage_combinations[(expiry, strike)] = combination
                    logger.info(f"从持仓推导套利组合: [{expiry}-{strike}]")

                    # 补充订阅行情
                    channels = []
                    for inst in [c_name, p_name, f_name]:
                        channels.append(f"ticker.{inst}.100ms")
                        channels.append(f"book.{inst}.raw")
                    try:
                        await self.client.send_request({
                            "jsonrpc": "2.0",
                            "id": self.client._get_next_request_id(),
                            "method": "public/subscribe",
                            "params": {"channels": channels}
                        })
                    except Exception:
                        pass

                c_pos = opts['call']
                p_pos = opts['put']
                f_pos = self.client.positions.get(combination['future'])

                # 推断策略方向：
                #   sell_future_buy_synthetic: 空期货 + 多Call + 空Put
                #   buy_future_sell_synthetic: 多期货 + 空Call + 多Put
                if c_pos.size > 0 and p_pos.size < 0:
                    strategy_type = 'sell_future_buy_synthetic'
                elif c_pos.size < 0 and p_pos.size > 0:
                    strategy_type = 'buy_future_sell_synthetic'
                else:
                    logger.warning(f"[{expiry}-{strike}] 期权持仓方向异常 (C={c_pos.size}, P={p_pos.size})，跳过重建")
                    continue

                opt_amount = abs(c_pos.size)
                # 🌟 P1-5.1 警告: average_price 是 Deribit 返回的"所有交易加权均价"
                # 如果该合约曾多次进出, average_price 不等于最近一次开仓价
                # → 用它做硬止损基准可能误判
                # → 所以下方 prices_confirmed=False, 禁止平仓决策依赖此值
                # → 策略只能等到期结算, 或人工平仓
                f_avg = f_pos.average_price if f_pos else Decimal('0')
                c_avg = c_pos.average_price if c_pos else Decimal('0')
                p_avg = p_pos.average_price if p_pos else Decimal('0')

                # 推算期货面值
                default_cs = Decimal('1') if self.target_currency == 'ETH' else Decimal('10')
                contract_size = self.contract_sizes.get(combination['future'], default_cs)
                if f_avg > 0 and contract_size > 0:
                    future_size_usd = (opt_amount * f_avg / contract_size).quantize(
                        Decimal('1'), rounding='ROUND_HALF_UP') * contract_size
                else:
                    future_size_usd = Decimal('0')

                # 如果有任何一个 average_price 为 0，说明数据不完整，禁止基于此做平仓决策
                has_valid_prices = f_avg > 0 and c_avg > 0 and p_avg > 0

                # 跨所模式: 如果 Deribit 期货腿为零但引擎启用了 Binance，尝试匹配 Binance 合约
                _rb_bn_symbol = ''
                _rb_bn_type = ''
                _rb_bn_qty = Decimal('0')
                _rb_bn_entry = Decimal('0')
                _rb_bn_pos_side = ''
                if (not f_pos or f_pos.size == 0) and self.binance_auth is not None and self.binance_matcher is not None:
                    _rb_bn_symbol, _rb_bn_type = self.binance_matcher.match(expiry)
                    if _rb_bn_symbol:
                        _expected_ps = 'LONG' if strategy_type == 'buy_future_sell_synthetic' else 'SHORT'
                        _query_ps = _expected_ps if self.binance_dual_side_mode else ''
                        _snap_key = (_rb_bn_symbol, _query_ps)
                        if _snap_key in _rb_bn_snapshot_cache:
                            _rb_bn_qty_q, _rb_bn_side_q, _rb_bn_entry_q, _rb_known_q = _rb_bn_snapshot_cache[_snap_key]
                        else:
                            _rb_bn_qty_q, _rb_bn_side_q, _rb_bn_entry_q, _rb_known_q = await self._get_binance_actual_position(
                                _rb_bn_symbol, _query_ps)
                            _rb_bn_snapshot_cache[_snap_key] = (_rb_bn_qty_q, _rb_bn_side_q, _rb_bn_entry_q, _rb_known_q)
                        if _rb_known_q:
                            if _rb_bn_qty_q > Decimal('0.0001'):
                                # One-way 模式下，若交易所净仓方向与组合预期方向相反，不应把该净仓分配给本组合
                                if (not self.binance_dual_side_mode) and _rb_bn_side_q in ('LONG', 'SHORT') and _rb_bn_side_q != _expected_ps:
                                    logger.warning(
                                        f"[{expiry}-{strike}] 降级重建: Binance {_rb_bn_symbol} 净仓方向={_rb_bn_side_q} "
                                        f"与组合预期={_expected_ps} 不一致，跳过该方向分配")
                                else:
                                    _alloc_side = _expected_ps if self.binance_dual_side_mode else (_rb_bn_side_q or _expected_ps)
                                    _alloc_key = (_rb_bn_symbol, _alloc_side)
                                    _remain = _rb_bn_remaining_by_side.get(_alloc_key, _rb_bn_qty_q)
                                    _rb_bn_qty = min(opt_amount, max(_remain, Decimal('0')))
                                    _rb_bn_remaining_by_side[_alloc_key] = max(_remain - _rb_bn_qty, Decimal('0'))
                                    if _rb_bn_qty > Decimal('0.0001'):
                                        _rb_bn_entry = _rb_bn_entry_q
                                    if self.binance_dual_side_mode:
                                        _rb_bn_pos_side = _expected_ps
                                    elif _rb_bn_side_q in ('LONG', 'SHORT'):
                                        _rb_bn_pos_side = _rb_bn_side_q
                                    if _rb_bn_qty < opt_amount:
                                        logger.warning(
                                            f"[{expiry}-{strike}] 降级重建: Binance {_rb_bn_symbol} {_alloc_side} "
                                            f"可分配仓位不足，组合名义={opt_amount}，分配={_rb_bn_qty}，剩余={_rb_bn_remaining_by_side[_alloc_key]}")
                            else:
                                logger.warning(
                                    f"[{expiry}-{strike}] 降级重建: Deribit 无期货腿且 Binance {_rb_bn_symbol} 未检测到对冲仓位，"
                                    f"将继续监控并等待后续巡检确认")
                        else:
                            # 查询未知时保守处理：先按组合名义数量挂跟踪，防止把真实仓位误判为无人跟踪裸腿
                            _rb_bn_qty = opt_amount
                            _rb_bn_entry = f_avg if f_avg > 0 else Decimal('0')
                            _rb_bn_pos_side = _expected_ps if self.binance_dual_side_mode else ''
                            has_valid_prices = False
                            logger.warning(
                                f"[{expiry}-{strike}] 降级重建: Binance {_rb_bn_symbol} 仓位状态未知，"
                                f"先按名义数量 {opt_amount} 挂跟踪，后续自动对账修正")

                _entry_future_price = f_avg
                if _rb_bn_qty > Decimal('0.0001'):
                    if _rb_bn_entry > 0:
                        if future_size_usd <= 0:
                            future_size_usd = (_rb_bn_qty * _rb_bn_entry).quantize(Decimal('0.01'))
                        if _entry_future_price <= 0:
                            _entry_future_price = _rb_bn_entry
                    if _entry_future_price <= 0:
                        has_valid_prices = False

                state = ArbitrageState(
                    expiry_strike=(expiry, strike),
                    state='position_open',
                    strategy_type=strategy_type,
                    entry_amount=opt_amount,
                    future_size_usd=future_size_usd,
                    combo_id=f"{expiry}-{strike}-rebuild-{int(time.time())}",
                    entry_prices={
                        'future': _entry_future_price,
                        'call': c_avg,
                        'put': p_avg
                    },
                    last_update=time.time() - 120,  # 允许立即开始监控
                    # 🌟 P1-5.1: 从 average_price 重建的 entry 不精确, 强制 prices_confirmed=False
                    # 这会阻止 _check_exit_opportunity 用错误基准触发硬止损
                    # 副作用: 必须等到期结算 (这是降级路径, 符合"安全优先"原则)
                    prices_confirmed=False,
                    binance_future_symbol=_rb_bn_symbol,
                    binance_future_type=_rb_bn_type,
                    binance_position_side=_rb_bn_pos_side,
                    binance_entry_price=_rb_bn_entry,
                    binance_open_qty=_rb_bn_qty,
                    binance_filled_qty=_rb_bn_qty,
                )
                self.arbitrage_states[(expiry, strike)] = state
                self.position_locks.add((expiry, strike))
                rebuild_count += 1

                # 同时保存到 Redis 防止下次又丢
                await self._save_state_to_redis(state)
                # 补录开仓记录：重启重建时自动补齐当前持仓
                await self._ensure_open_record_for_recovered_state(state)

                logger.info(
                    f"⚠️ 从持仓降级重建: [{expiry}-{strike}] | 策略: {strategy_type} | "
                    f"数量: {opt_amount} | 期货面值: {future_size_usd} USD | "
                    f"成本(均价): F={_entry_future_price} C={c_avg} P={p_avg}"
                    f"{f' | Binance={_rb_bn_symbol} {_rb_bn_pos_side} qty={_rb_bn_qty} entry={_rb_bn_entry}' if _rb_bn_symbol else ''}")

            if rebuild_count > 0:
                logger.warning(f"⚠️ Redis 无数据，从 Deribit 持仓降级重建了 {rebuild_count} 个套利组合 (使用均价作为成本)")
                asyncio.create_task(tg_notifier.send_async(
                    f"⚠️ Redis 状态丢失，已从 Deribit 持仓自动重建 {rebuild_count} 个套利组合。\n"
                    f"注意：成本价使用持仓均价，可能与实际成交价有微小偏差。"))
                # 🛡️ 修复 B: 降级重建路径主动 pause "Binance残余仓位"，配合修复 A 屏蔽自动减损
                # 原因: 跨所对冲信息无法从 Deribit 推断，必须等 Binance WS 数据稳定后再确认。
                # 解除路径: monitor_positions 内 _refresh_binance_residual_pause 每 5s 巡检，
                #          对账清白后会自动 _remove_pause("Binance残余仓位") 并恢复交易。
                self._add_pause("Binance残余仓位")
                logger.warning("⏸️ [启动保护] 降级重建已触发，自动 pause(Binance残余仓位)，等待对账清白后自动解除")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 Redis 丢失 + 降级重建路径，开仓和自动减损已暂停\n"
                    f"重建组合数: {rebuild_count}\n"
                    f"系统将在 Binance 实仓加载稳定后自动验证和解除暂停。\n"
                    f"如长时间未解除，请人工核对状态机 vs 交易所实仓。",
                    "rebuild_pause"))

        except Exception as e:
            logger.error(f"从持仓重建状态机失败: {e}")
