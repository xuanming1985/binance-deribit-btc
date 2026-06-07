"""engine/ghost_mixin.py — 幽灵仓位检测 + 破损组合处理"""
from __future__ import annotations
import logging
import time
import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set

if TYPE_CHECKING:
    pass

from telegram_handler import tg_notifier
from engine.twap_state import is_settlement_twap_active_or_pending_delivery

logger = logging.getLogger(__name__)


class GhostMixin:
    """Mixin: 幽灵仓位检测 + 自动清理 + 破损组合处理"""

    def _build_tracked_deribit_instruments(self) -> Set[str]:
        tracked_instruments = set()
        for es, st in self.arbitrage_states.items():
            if st.state not in ('position_open', 'executing', 'exiting'):
                continue
            combo = self.arbitrage_combinations.get(es)
            if combo:
                tracked_instruments.add(combo['future'])
                tracked_instruments.add(combo['call'])
                tracked_instruments.add(combo['put'])
        return tracked_instruments

    def _has_untracked_deribit_position(self, dust: Decimal = Decimal('0.0001'),
                                        ignore_instruments: Optional[Set[str]] = None) -> bool:
        tracked_instruments = self._build_tracked_deribit_instruments()
        ignore_instruments = ignore_instruments or set()
        for inst, pos in self.client.positions.items():
            if inst in ignore_instruments:
                continue
            try:
                size = abs(Decimal(str(getattr(pos, 'size', 0))))
            except Exception:
                continue
            if size > dust and inst not in tracked_instruments:
                return True
        return False

    def _has_active_deribit_cleanup_order(self, dust: Decimal = Decimal('0.0001')) -> bool:
        cleanup_prefixes = ('l2a_', 'ghost_')
        terminal_status = {'filled', 'cancelled', 'rejected'}
        for order in self.client.active_orders.values():
            label = getattr(order, 'label', '') or ''
            if not label.startswith(cleanup_prefixes):
                continue
            status = str(getattr(order, 'status', '') or '').lower()
            if status in terminal_status:
                continue
            try:
                amount = Decimal(str(getattr(order, 'amount', 0) or 0))
                filled = Decimal(str(getattr(order, 'filled_amount', 0) or 0))
                if amount > 0 and max(amount - filled, Decimal('0')) <= dust:
                    continue
            except Exception:
                pass
            return True
        return False

    async def _maybe_clear_anchor_rollback_pause(self, source: str,
                                                cleared_instrument: Optional[str] = None):
        """Only clear the global pause after all untracked Deribit risk is gone."""
        cleared_set = getattr(self, '_anchor_rollback_cleared_instruments', None)
        if cleared_set is None:
            self._anchor_rollback_cleared_instruments = set()
            cleared_set = self._anchor_rollback_cleared_instruments
        if not hasattr(self, '_has_pause') or not self._has_pause("锚定腿回滚失败"):
            cleared_set.clear()
            return
        if cleared_instrument:
            cleared_set.add(cleared_instrument)
        try:
            await self.client.get_positions(self.target_currency, silent=True)
        except Exception as exc:
            logger.info(f"[锚定腿回滚] 刷新持仓失败，保持暂停: {exc}")
            return
        ignore_instruments = set(cleared_set)
        if self._has_untracked_deribit_position(ignore_instruments=ignore_instruments):
            logger.info(f"[锚定腿回滚] {source}: 仍有未跟踪 Deribit 仓位，保持暂停")
            return
        if self._has_active_deribit_cleanup_order():
            logger.info(f"[锚定腿回滚] {source}: 仍有 L2/ghost 清理挂单，保持暂停")
            return
        self._remove_pause("锚定腿回滚失败")
        cleared_set.clear()
        logger.info(f"✅ [锚定腿回滚] {source}: 残腿风险已清除，解除暂停")

    async def _ghost_and_integrity_check(self):
        """幽灵仓位检测 + 组合完整性校验 + L2 残留清理 (每轮扫描周期调用)"""
        try:
            if self._is_deribit_settlement_core_window():
                _now_core = time.time()
                # Core settlement window is exchange-locked; do not carry stale
                # first-seen timers across it, or a legal delivery transition can
                # become an immediate broken-combo action after the window.
                self._ghost_first_seen.clear()
                self._bn_ghost_first_seen.clear()
                self._broken_combo_first_seen.clear()
                if _now_core - getattr(self, '_ghost_settlement_core_skip_log_ts', 0.0) >= max(
                        float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0):
                    logger.info("⏸️ [结算核心窗口] 跳过幽灵/组合完整性自动处置，窗口结束后重新评估")
                    self._ghost_settlement_core_skip_log_ts = _now_core
                return

            # 第一步: 收集所有状态机正在跟踪的合约
            tracked_instruments = self._build_tracked_deribit_instruments()
            # 第二步: 收集当前有挂单的合约
            active_order_instruments = set(
                o.instrument_name for o in self.client.active_orders.values()
            )
            # 第三步: 检查所有持仓
            _now = time.time()
            _ghost_grace = 30
            for inst, pos in self.client.positions.items():
                if pos.size == 0 or inst in tracked_instruments:
                    self._ghost_first_seen.pop(inst, None)
                    continue
                if inst in active_order_instruments:
                    self._ghost_first_seen.pop(inst, None)
                    continue
                # 无状态、无挂单 → 启动/检查宽限期
                if inst not in self._ghost_first_seen:
                    self._ghost_first_seen[inst] = _now
                    logger.warning(f"⚠️ [幽灵检测] 首次发现未跟踪仓位 {inst}，启动 {_ghost_grace}s 宽限期")
                    continue
                if _now - self._ghost_first_seen[inst] < _ghost_grace:
                    continue
                # 确认幽灵仓位
                if inst in self._ghost_closing:
                    continue
                logger.error(f"🚨 [持仓对账] 发现未跟踪仓位: {inst} | 方向: {'多' if pos.size > 0 else '空'} | 数量: {abs(pos.size)}")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 发现幽灵仓位: {inst} {'多' if pos.size > 0 else '空'} {abs(pos.size)}，状态机无记录！请立即检查！",
                    "ghost_position"))
                self._ghost_closing.add(inst)
                asyncio.create_task(self._auto_close_ghost_position(inst, pos))
            # 清理已消失仓位的宽限期记录
            for inst in list(self._ghost_first_seen.keys()):
                if inst not in self.client.positions or self.client.positions[inst].size == 0:
                    self._ghost_first_seen.pop(inst, None)

            # Binance 端"孤立仓位"巡检（补齐幽灵检测覆盖面）
            if self.binance_ws:
                _bn_dust = Decimal('0.001')
                _bn_ghost_grace = 20
                if self.binance_dual_side_mode and getattr(self.binance_ws, "positions_by_side", None):
                    _tracked_side = self._build_tracked_binance_by_side()
                    _active_bn_keys = set()
                    for (_bn_sym, _ps), _bn_pos in self.binance_ws.positions_by_side.items():
                        if _bn_pos.quantity <= _bn_dust:
                            continue
                        _actual = _bn_pos.quantity
                        _tracked = _tracked_side.get((_bn_sym, _ps), Decimal('0'))
                        _extra = _actual - _tracked
                        _k = f"{_bn_sym}:{_ps}"
                        _active_bn_keys.add(_k)
                        if _extra <= _bn_dust:
                            self._bn_ghost_first_seen.pop(_k, None)
                            self._bn_ghost_handling.discard(_k)
                            continue
                        if _k not in self._bn_ghost_first_seen:
                            self._bn_ghost_first_seen[_k] = _now
                            logger.warning(
                                f"⚠️ [Binance幽灵检测] 首次发现未跟踪实仓 {_bn_sym} {_ps} "
                                f"(实际={_actual}, 跟踪={_tracked})，启动 {_bn_ghost_grace}s 宽限期")
                            continue
                        if _now - self._bn_ghost_first_seen[_k] < _bn_ghost_grace:
                            continue
                        if _k in self._bn_ghost_handling:
                            continue
                        self._bn_ghost_handling.add(_k)
                        logger.error(
                            f"🚨 [Binance幽灵检测] 未跟踪实仓确认: {_bn_sym} {_ps} "
                            f"(实际={_actual}, 跟踪={_tracked}, 差额={_extra})")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 检测到 Binance 未跟踪实仓\n"
                            f"{_bn_sym} {_ps}: 实际={_actual} | 状态机={_tracked} | 差额={_extra}\n"
                            f"系统将自动减损并持续重试。", "binance_ghost_position"))

                        async def _bn_ghost_auto_close(_key: str):
                            try:
                                await self._auto_close_naked_legs()
                            finally:
                                self._bn_ghost_handling.discard(_key)

                        asyncio.create_task(_bn_ghost_auto_close(_k))

                    # 清理已消失 key
                    for _k in list(self._bn_ghost_first_seen.keys()):
                        if _k not in _active_bn_keys:
                            self._bn_ghost_first_seen.pop(_k, None)
                            self._bn_ghost_handling.discard(_k)
                else:
                    _tracked_signed = self._build_tracked_binance_signed()
                    _active_bn_keys = set()
                    for _bn_sym, _bn_pos in self.binance_ws.positions.items():
                        if _bn_pos.quantity <= _bn_dust:
                            continue
                        _actual_signed = _bn_pos.quantity if _bn_pos.side == "LONG" else -_bn_pos.quantity
                        _tracked_signed_qty = _tracked_signed.get(_bn_sym, Decimal('0'))
                        _extra_signed = _actual_signed - _tracked_signed_qty
                        _k = f"{_bn_sym}:NET"
                        _active_bn_keys.add(_k)
                        if abs(_extra_signed) <= _bn_dust:
                            self._bn_ghost_first_seen.pop(_k, None)
                            self._bn_ghost_handling.discard(_k)
                            continue
                        if _k not in self._bn_ghost_first_seen:
                            self._bn_ghost_first_seen[_k] = _now
                            logger.warning(
                                f"⚠️ [Binance幽灵检测] 首次发现未跟踪净仓 {_bn_sym} "
                                f"(实际={_actual_signed:+f}, 跟踪={_tracked_signed_qty:+f})，启动 {_bn_ghost_grace}s 宽限期")
                            continue
                        if _now - self._bn_ghost_first_seen[_k] < _bn_ghost_grace:
                            continue
                        if _k in self._bn_ghost_handling:
                            continue
                        self._bn_ghost_handling.add(_k)
                        logger.error(
                            f"🚨 [Binance幽灵检测] 未跟踪净仓确认: {_bn_sym} "
                            f"(实际={_actual_signed:+f}, 跟踪={_tracked_signed_qty:+f}, 差额={_extra_signed:+f})")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 检测到 Binance 未跟踪净仓\n"
                            f"{_bn_sym}: 实际={_actual_signed:+f} | 状态机={_tracked_signed_qty:+f} | 差额={_extra_signed:+f}\n"
                            f"系统将自动减损并持续重试。", "binance_ghost_position"))

                        async def _bn_ghost_auto_close(_key: str):
                            try:
                                await self._auto_close_naked_legs()
                            finally:
                                self._bn_ghost_handling.discard(_key)

                        asyncio.create_task(_bn_ghost_auto_close(_k))

                    for _k in list(self._bn_ghost_first_seen.keys()):
                        if _k not in _active_bn_keys:
                            self._bn_ghost_first_seen.pop(_k, None)
                            self._bn_ghost_handling.discard(_k)

            # 组合完整性校验
            for es, st in self.arbitrage_states.items():
                if st.state != 'position_open':
                    self._broken_combo_first_seen.pop(es, None)
                    getattr(self, '_broken_combo_retry_after', {}).pop(es, None)
                    continue
                if is_settlement_twap_active_or_pending_delivery(st):
                    # 结算 TWAP 阶段会主动关闭 Binance 对冲腿，Deribit future 为 0 是预期状态；
                    # 等待到期交割时不得由 broken-combo 完整性检查接管。
                    self._broken_combo_first_seen.pop(es, None)
                    self._broken_combos_alerted.discard(es)
                    getattr(self, '_broken_combo_retry_after', {}).pop(es, None)
                    continue
                combo = self.arbitrage_combinations.get(es)
                if not combo:
                    self._broken_combo_first_seen.pop(es, None)
                    getattr(self, '_broken_combo_retry_after', {}).pop(es, None)
                    continue
                # 仅当该组合真实存在 Binance 对冲仓位时，Deribit future 腿缺失才是正常状态
                _combo_has_bn_hedge = False
                if st.binance_future_symbol:
                    if st.binance_filled_qty > 0:
                        _combo_has_bn_hedge = True
                    elif self.binance_ws:
                        _bn_pos = None
                        _ps = (st.binance_position_side or "").upper()
                        if _ps in ("LONG", "SHORT"):
                            _bn_pos = self.binance_ws.positions_by_side.get((st.binance_future_symbol, _ps))
                        if _bn_pos is None:
                            _bn_pos = self.binance_ws.positions.get(st.binance_future_symbol)
                        if _bn_pos and _bn_pos.quantity > 0:
                            _combo_has_bn_hedge = True
                missing_legs = []
                for leg_key in ('future', 'call', 'put'):
                    leg_inst = combo.get(leg_key)
                    if not leg_inst:
                        continue
                    # 跨所模式: 期货腿由 Binance 对冲时，Deribit 期货为零是正常状态
                    if leg_key == 'future' and _combo_has_bn_hedge:
                        continue
                    leg_pos = self.client.positions.get(leg_inst)
                    if leg_pos is None or leg_pos.size == 0:
                        missing_legs.append(leg_inst)
                if missing_legs:
                    combo_label = f"{es[0]}-{es[1]}"
                    _broken_grace = 20
                    _first_seen = self._broken_combo_first_seen.get(es)
                    if _first_seen is None:
                        self._broken_combo_first_seen[es] = _now
                        logger.info(
                            f"⚠️ [组合完整性] {combo_label} 发现缺腿 {missing_legs}，"
                            f"进入 {_broken_grace}s 宽限观察（防止WS瞬时不同步）")
                        continue
                    if _now - _first_seen < _broken_grace:
                        continue
                    if es in self._broken_combo_handling:
                        continue
                    if _now < getattr(self, '_broken_combo_retry_after', {}).get(es, 0.0):
                        continue
                    self._broken_combo_handling.add(es)
                    if es not in self._broken_combos_alerted:
                        self._broken_combos_alerted.add(es)
                        logger.error(
                            f"🚨 [组合完整性] {combo_label} 状态机记录为 position_open，"
                            f"但以下腿在交易所实际为零: {missing_legs}")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 [破损组合] {combo_label} 状态=position_open 但腿缺失: "
                            f"{missing_legs}\n"
                            f"系统将立即执行紧急处置（关闭剩余腿并重试，必要时暂停）。",
                            f"broken_combo_{es[0]}"))
                    asyncio.create_task(self._handle_broken_combo(es, missing_legs))
                else:
                    self._broken_combo_first_seen.pop(es, None)
                    self._broken_combos_alerted.discard(es)
                    getattr(self, '_broken_combo_retry_after', {}).pop(es, None)

            # L2 兜底挂单清理
            for oid, order in list(self.client.active_orders.items()):
                if order.label and order.label.startswith(('l2a_', 'l2t_', 'l2f_')):
                    pos = self.client.positions.get(order.instrument_name)
                    if pos is None or pos.size == 0:
                        logger.warning(f"🗑️ [L2清理] 取消已无仓位的L2挂单: {order.instrument_name} {order.label} #{oid}")
                        asyncio.create_task(self.client.cancel_order(oid, log_prefix="[L2清理]"))
            await self._maybe_clear_anchor_rollback_pause("巡检确认")
        except Exception as e:
            logger.error(f"幽灵/完整性检测异常: {e}")

    async def _auto_close_ghost_position(self, inst: str, pos):
        """Layer 3: 幽灵仓位自动强平 - 先用1000 ticks IOC，失败则降级为盘口挂单"""
        try:
            if self._is_deribit_settlement_core_window():
                self._add_pause("结算窗口")
                logger.warning(f"[幽灵强平] ⏸️ Deribit core settlement window active，跳过 {inst} 自动强平")
                return

            close_side = 'sell' if pos.size > 0 else 'buy'
            amount = abs(pos.size)
            ticker = self.client.tickers.get(inst)
            if not ticker:
                logger.error(f"[幽灵强平] 无法获取 {inst} 行情，跳过")
                asyncio.create_task(tg_notifier.send_error_async(f"幽灵强平失败 {inst}: 无法获取行情", "ghost_liquidation_failed"))
                return
            # 区分期权 (BTC-26JUN26-78000-P) 与期货 (BTC-26JUN26)
            parts = inst.split('-')
            is_option = len(parts) >= 4 and parts[-1] in ('C', 'P')
            # 动态获取 tick_size（走 instrument_cache，避免重复请求）
            inst_info = await self.client.get_instrument_info(inst)
            base_price = ticker.bid if close_side == 'sell' else ticker.ask
            if base_price <= 0:
                logger.error(f"[幽灵强平] {inst} 盘口为空，跳过")
                asyncio.create_task(tg_notifier.send_error_async(f"幽灵强平失败 {inst}: 盘口为空", "ghost_liquidation_failed"))
                return
            if is_option:
                tick = self._get_dynamic_tick(Decimal(str(base_price)), inst_info)
            else:
                tick = Decimal(str(inst_info.get('tick_size', '0.5'))) if inst_info else Decimal('0.5')
            # 1000 ticks 激进 IOC（期权 buy 方向直接用 ask，避免触发 price_too_high）
            if is_option and close_side == 'buy':
                ioc_price = Decimal(str(int(round(Decimal(str(base_price)) / tick)))) * tick
            else:
                raw_p = Decimal(str(base_price)) - (tick * 1000) if close_side == 'sell' else Decimal(str(base_price)) + (tick * 1000)
                ioc_price = max(Decimal(str(int(round(raw_p / tick)))) * tick, tick)
            # 🌟 price band 保护: 防止激进 IOC 价格超出 Deribit 允许范围
            if ticker.min_price > 0 and ioc_price < ticker.min_price:
                ioc_price = ticker.min_price
            if ticker.max_price > 0 and ioc_price > ticker.max_price:
                ioc_price = ticker.max_price
            logger.warning(f"[幽灵强平] 强平 {inst} {close_side} {amount} @ {ioc_price} (IOC 1000 ticks)")
            result = await self.client.place_order(
                inst, amount, close_side, 'limit', price=ioc_price,
                label="ghost_ioc", log_prefix="[幽灵强平]",
                reduce_only=True, time_in_force="immediate_or_cancel")
            filled = result.filled_amount if result else Decimal('0')
            if filled >= amount:
                logger.info(f"[幽灵强平] ✅ {inst} IOC强平成功")
                asyncio.create_task(tg_notifier.send_async(
                    f"✅ [幽灵强平成功] {inst} {close_side} {amount}，已清零"))
                self._ghost_first_seen.pop(inst, None)
                await self._maybe_clear_anchor_rollback_pause("ghost_ioc 全成", cleared_instrument=inst)
                return
            # IOC 未完全成交，降级为盘口挂单
            rest_amt = amount - filled
            rest_raw = ticker.bid if close_side == 'sell' else ticker.ask
            if rest_raw <= 0:
                logger.error(f"[幽灵强平] {inst} 降级挂单盘口为空，放弃")
                asyncio.create_task(tg_notifier.send_error_async(f"幽灵强平失败 {inst}: 降级挂单盘口为空", "ghost_liquidation_failed"))
                return
            rest_price = Decimal(str(int(round(Decimal(str(rest_raw)) / tick)))) * tick
            logger.warning(f"[幽灵强平] IOC成交{filled}/{amount}，降级挂单 {inst} {close_side} {rest_amt} @ {rest_price}")
            _ghost_rest_order = await self.client.place_order(
                inst, rest_amt, close_side, 'limit', price=rest_price,
                label="ghost_rest", log_prefix="[幽灵挂单]", reduce_only=True)
            if _ghost_rest_order and _ghost_rest_order.order_id:
                asyncio.create_task(self._watch_ghost_rest_order(
                    inst, close_side, rest_amt, rest_price, _ghost_rest_order.order_id))
            asyncio.create_task(tg_notifier.send_async(
                f"⚠️ [幽灵强平-挂单兜底] {inst} {close_side} {rest_amt} @ {rest_price}，等待成交"))
        except Exception as e:
            logger.error(f"[幽灵强平] {inst} 异常: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"幽灵强平失败 {inst}: {e}", "ghost_liquidation_failed"))
        finally:
            self._ghost_closing.discard(inst)  # 无论成败都释放锁

    async def _watch_ghost_rest_order(self, inst: str, side: str, target_amount: Decimal,
                                      price: Decimal, order_id: str):
        """监控 ghost_rest 兜底挂单，超时未成交则撤单并触发下一轮强平重试"""
        if not order_id:
            return
        _timeout = max(float(getattr(self, 'ghost_rest_watch_seconds', 6.0)), 2.0)
        _dust = Decimal('0.0001')
        try:
            await asyncio.sleep(_timeout)
            _ord = self.client.get_order_by_id(order_id)
            _filled = Decimal(str(getattr(_ord, 'filled_amount', 0))) if _ord else Decimal('0')
            _left = max(target_amount - _filled, Decimal('0'))
            if _left <= _dust:
                self._ghost_first_seen.pop(inst, None)
                await self._maybe_clear_anchor_rollback_pause("ghost_rest 全成", cleared_instrument=inst)
                return

            try:
                await self.client.cancel_order(order_id, log_prefix="[幽灵挂单超时撤单]")
            except Exception as _ce:
                logger.warning(f"[幽灵挂单超时撤单] 取消失败 {inst} #{order_id}: {_ce}")

            logger.error(
                f"[幽灵挂单超时] {inst} {side} @ {price} 挂单 {int(_timeout)}s 未完成，"
                f"剩余 {_left}，已撤单并触发重试")
            asyncio.create_task(tg_notifier.send_error_async(
                f"🚨 幽灵挂单超时未完成\n"
                f"{inst} {side} @ {price}\n"
                f"剩余: {_left}\n"
                f"系统已撤单并重试强平。", "ghost_rest_timeout"))

            _pos = self.client.positions.get(inst)
            if _pos and _pos.size != 0:
                self._ghost_first_seen[inst] = 0  # 立即触发，无宽限
                if inst not in self._ghost_closing:
                    if self._is_deribit_settlement_core_window():
                        logger.warning(
                            f"[幽灵挂单超时] ⏸️ Deribit core settlement window active，"
                            f"暂不重启 {inst} 幽灵强平")
                    else:
                        self._ghost_closing.add(inst)
                        asyncio.create_task(self._auto_close_ghost_position(inst, _pos))
        except Exception as e:
            logger.info(f"[幽灵挂单监控] {inst} 监控异常: {e}")

    async def _handle_broken_combo(self, expiry_strike: Tuple[str, Decimal], missing_legs: List[str]):
        """破损组合自动处置：触发紧急强平并在失败时保留状态以便重试"""
        expiry, strike = expiry_strike
        log_prefix = f"[{expiry}-{strike}-破损组合]"
        _suppress_first_seen_refresh = False
        try:
            if self._is_deribit_settlement_core_window():
                self._add_pause("结算窗口")
                logger.warning(f"{log_prefix} ⏸️ Deribit core settlement window active，跳过自动处置")
                return

            state = self.arbitrage_states.get(expiry_strike)
            combo = self.arbitrage_combinations.get(expiry_strike)
            if not state or state.state != 'position_open' or not combo:
                return
            if is_settlement_twap_active_or_pending_delivery(state):
                logger.info(f"{log_prefix} 结算TWAP进行中/已完成等待交割，跳过破损组合自动处置")
                _suppress_first_seen_refresh = True
                self._broken_combo_first_seen.pop(expiry_strike, None)
                self._broken_combos_alerted.discard(expiry_strike)
                self._broken_combo_retry_after.pop(expiry_strike, None)
                return
            f_pos = self.client.positions.get(combo['future'])
            c_pos = self.client.positions.get(combo['call'])
            p_pos = self.client.positions.get(combo['put'])

            if self._should_emit_throttled(f"broken_combo_start:{expiry_strike}:{tuple(missing_legs)}"):
                logger.error(
                    f"{log_prefix} 启动自动处置: 缺失腿={missing_legs}，"
                    f"剩余仓位将执行紧急强平并持续重试")
            await self._emergency_dump_all(state, combo, f_pos, c_pos, p_pos)

            if state.state != 'exited':
                # 处置未完成时保持跟踪并暂停开仓，避免风险扩大
                self._add_pause("破损组合")
                self._broken_combo_retry_after[expiry_strike] = time.time() + 60
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 {log_prefix} 自动处置未完成，状态保持 {state.state} 并将持续重试。\n"
                    f"缺失腿: {missing_legs}\n"
                    f"系统已自动暂停开仓，请人工关注。", "broken_combo_retry"))
            else:
                # 处置完成则允许后续再次检测同组合
                self._broken_combos_alerted.discard(expiry_strike)
                self._broken_combo_retry_after.pop(expiry_strike, None)
                await self._refresh_broken_combo_pause()
        except Exception as e:
            logger.error(f"{log_prefix} 自动处置异常: {e}")
            asyncio.create_task(tg_notifier.send_error_async(
                f"{log_prefix} 自动处置异常: {e}", "broken_combo_handler_error"))
        finally:
            _state = self.arbitrage_states.get(expiry_strike)
            if _suppress_first_seen_refresh:
                self._broken_combo_first_seen.pop(expiry_strike, None)
            elif _state and _state.state != 'exited':
                self._broken_combo_first_seen[expiry_strike] = time.time()
            else:
                self._broken_combo_first_seen.pop(expiry_strike, None)
            self._broken_combo_handling.discard(expiry_strike)
            await self._refresh_broken_combo_pause()

    def _has_overlapping_binance_combo(self, symbol: str, current_state,
                                         window_start_ts: float = 0.0) -> bool:
        """检查是否存在同 symbol 的其他活跃组合或窗口内重叠记录。

        当同一 Binance symbol 上有多组合并行时，按 symbol+时间窗聚合的 income(COMMISSION/FUNDING)
        会把其他组合成本混入当前结算，导致单笔净利被污染。

        🌟 Plan Bug #4 修复: 不仅检查当前活跃组合, 还要检查在查询时间窗内已关闭的其他组合
        (例如 A 10:00 开仓, B 10:30 开+11:00 平, A 14:00 结算 → income 10:00-14:00 含 B 的部分)
        """
        if not symbol:
            return False
        _dust = Decimal('0.0001')
        _combo_id_current = getattr(current_state, 'combo_id', '')
        for _st in self.arbitrage_states.values():
            if _st is current_state:
                continue
            # 1. 当前活跃组合检查
            if _st.state in ('position_open', 'executing', 'exiting'):
                if (_st.binance_future_symbol == symbol and
                        _st.binance_filled_qty > _dust):
                    return True
            # 2. 窗口内已关闭组合检查 (包括 state='failed'/'closed' 等)
            # 若其他组合曾在 window_start 之后使用过同 symbol, 它们的 close 产生的 income 会落在我们的窗口内
            if window_start_ts > 0:
                _other_start = getattr(_st, 'start_time', 0)
                _other_last = getattr(_st, 'last_update', 0)
                if (_st.binance_future_symbol == symbol and
                        _other_start > 0 and _other_last >= window_start_ts):
                    return True
        return False
