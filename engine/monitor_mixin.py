"""engine/monitor_mixin.py — 持仓监控 + 平仓判断 + 启动审核"""
from __future__ import annotations
import logging
import time
import asyncio
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set

if TYPE_CHECKING:
    pass

from telegram_handler import tg_notifier
from engine.models import ArbitrageState
from engine.twap_state import is_settlement_twap_active_or_pending_delivery

logger = logging.getLogger(__name__)


class MonitorMixin:
    """Mixin: 持仓监控 + 退出机会检测 + 启动稳定审核 + 暂停刷新"""

    @staticmethod
    def _is_twap_hedge_closed(state) -> bool:
        """判断组合的 Binance 对冲是否已被结算 TWAP 主动平掉（等待 Deribit 到期交割）。
        仅使用 Redis 可持久化字段，重启后仍然有效。"""
        if not getattr(state, '_settlement_twap_started', False):
            return False
        if state.binance_filled_qty > Decimal('0.0001'):
            return False
        _acc = getattr(state, '_settlement_twap_accumulated', None)
        _snap = getattr(state, '_settlement_twap_qty_snapshot', Decimal('0'))
        if _acc and _snap > 0:
            _filled = Decimal(str(_acc.get('filled', 0)))
            if _filled >= _snap - Decimal('0.0001'):
                return True
        return False

    async def _initial_post_startup_audit(self):
        """🛡️ 修复 E+F (重构): 启动稳定窗口后的一次性完整审核

        触发条件: monitor_positions 循环内, _initial_audit_done=False 且 uptime >= 60s。
        作用:
          1) 调用 _refresh_binance_residual_pause(force_check=True) — 完整对账 + 自愈 + 必要时 add_pause
          2) 按 (symbol, side) 方向粒度做"期望对冲总量 vs 实际持仓总量"诊断
          3) 输出完整的裸腿缺口（不被自愈分摊掩盖）+ Telegram 告警

        设计决策:
          - 不在 initialize() 末尾调用: 启动期 WS 加载/dual_side 探测/状态机恢复异步, 立即对账易误判
          - 60s 是稳定窗口 (与 _auto_close_naked_legs 启动冷启动窗一致): 给所有数据源充分加载时间
          - 一次性: _initial_audit_done 标志防止重复触发; 后续巡检由 monitor_positions 常规循环负责
        """
        try:
            if not (self.binance_ws and self.binance_connected):
                logger.warning("⏸️ [启动稳定审核] Binance 未连接，跳过本次审核（将在恢复后由常规巡检接管）")
                return

            logger.info("🔍 [启动稳定审核] 开始完整对账 + 裸腿诊断...")

            # === 步骤 1: 完整对账 (含自愈 + expect_hedge_missing 检测) ===
            try:
                await self._refresh_binance_residual_pause(force_check=True)
            except Exception as _re:
                logger.warning(f"[启动稳定审核] 强制对账异常 (非致命): {_re}")

            # === 步骤 2: 方向粒度真实裸腿诊断（不被自愈干扰）===
            # 仅 Hedge Mode (dual_side_mode) 才有 positions_by_side 按 LONG/SHORT 拆分;
            # One-way 模式无方向概念, 步骤 1 的净仓对账已覆盖, 跳过方向诊断避免误报。
            try:
                if not self.binance_dual_side_mode:
                    logger.info("✅ [启动稳定审核] One-way 模式，跳过方向粒度诊断 (步骤 1 已对账)")
                else:
                    _unhedged_lines = []
                    _expected_by_side: Dict[Tuple[str, str], Tuple[Decimal, int]] = {}
                    for _es, _st in self.arbitrage_states.items():
                        if _st.state not in ('position_open', 'executing', 'exiting'):
                            continue
                        if not _st.binance_future_symbol:
                            continue
                        _entry_amt = getattr(_st, 'entry_amount', Decimal('0')) or Decimal('0')
                        if _entry_amt <= 0:
                            continue
                        if self._is_twap_hedge_closed(_st):
                            continue
                        _ps_f = (_st.binance_position_side or "").upper()
                        if _ps_f not in ("LONG", "SHORT"):
                            _ps_f = "LONG" if _st.strategy_type == 'buy_future_sell_synthetic' else "SHORT"
                        _key_f = (_st.binance_future_symbol, _ps_f)
                        _prev_amt, _prev_cnt = _expected_by_side.get(_key_f, (Decimal('0'), 0))
                        _expected_by_side[_key_f] = (_prev_amt + _entry_amt, _prev_cnt + 1)

                    _actual_by_side: Dict[Tuple[str, str], Decimal] = {}
                    if getattr(self.binance_ws, "positions_by_side", None):
                        for (_sym_f, _ps_f), _bn_pos_f in self.binance_ws.positions_by_side.items():
                            if _bn_pos_f.quantity > 0:
                                _actual_by_side[(_sym_f, _ps_f)] = Decimal(str(_bn_pos_f.quantity))

                    _diag_tol = Decimal('0.001')
                    _total_gap = Decimal('0')
                    for _key_f, (_exp_amt, _cnt) in _expected_by_side.items():
                        _act_amt = _actual_by_side.get(_key_f, Decimal('0'))
                        _gap = _exp_amt - _act_amt
                        if _gap > _diag_tol:
                            _sym_f, _ps_f = _key_f
                            _unhedged_lines.append(
                                f"{_sym_f} {_ps_f}: 期望={_exp_amt} 实际={_act_amt} 缺口={_gap} (影响 {_cnt} 个组合)")
                            _total_gap += _gap

                    if _unhedged_lines:
                        _diag_msg = (
                            f"⚠️ [启动稳定审核] 检测到 Binance 对冲缺口 (按方向粒度，与自愈无关)\n"
                            + "\n".join(_unhedged_lines)
                            + f"\n累计缺口: {_total_gap} BTC"
                        )
                        logger.warning(_diag_msg)
                        if not self._has_pause("Binance残余仓位"):
                            self._add_pause("Binance残余仓位")
                            logger.warning("⏸️ [启动稳定审核] 检测到对冲缺口但步骤 1 未 pause，已补充 add_pause")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"⚠️ 启动稳定审核：Binance 对冲缺口 {_total_gap} BTC\n"
                            + "\n".join(_unhedged_lines)
                            + f"\n\n系统已暂停开仓和自动减损。\n"
                            f"建议: 用 /pos 查看当前持仓，必要时手动补对冲或人工平 Deribit 期权。",
                            "startup_unhedged_diag"))
                    else:
                        logger.info("✅ [启动稳定审核] 所有跨所组合对冲完整，无方向缺口")
            except Exception as _diag_err:
                logger.warning(f"[启动稳定审核] 方向粒度诊断异常 (非致命): {_diag_err}")
        except Exception as e:
            logger.error(f"[启动稳定审核] 顶层异常: {e}")

    async def _refresh_binance_residual_pause(self, force_check: bool = False):
        """自动解除 Binance残余仓位 暂停：仅在残余风险确认消失后移除

        force_check=True 时，即使当前未暂停也执行一次完整对账（用于启动恢复后的交叉校验）。
        """
        try:
            if (not force_check) and (not self._has_pause("Binance残余仓位")):
                return

            _now = time.time()
            _interval = max(float(getattr(self, '_bn_residual_recheck_interval', 5.0)), 1.0)
            if _now - getattr(self, '_bn_residual_recheck_last_ts', 0.0) < _interval:
                return
            self._bn_residual_recheck_last_ts = _now

            _dust = Decimal('0.0001')
            _issues = []

            _active_states = [
                (es, st) for es, st in self.arbitrage_states.items()
                if st.state in ('position_open', 'executing', 'exiting') and st.binance_future_symbol
            ]

            # 1) 逐组合校验：状态机跟踪 qty 与 Binance 实仓是否一致；是否存在"应有对冲但对冲为0"
            _groups = defaultdict(list)  # (symbol, positionSideOrBoth) -> [(expiry_strike, state)]
            for _es, _st in _active_states:
                _ps = (_st.binance_position_side or "").upper()
                if self.binance_dual_side_mode:
                    if _ps not in ("LONG", "SHORT"):
                        _ps = "LONG" if _st.strategy_type == 'buy_future_sell_synthetic' else "SHORT"
                else:
                    _ps = ""
                _groups[(_st.binance_future_symbol, _ps)].append((_es, _st))

            for (_sym, _ps), _items in _groups.items():
                _actual_qty, _actual_side, _, _known = await self._get_binance_actual_position(_sym, _ps)
                if not _known:
                    _issues.append(f"{_sym} {_ps or 'BOTH'}=unknown")
                    continue

                _tracked_total = sum((_st.binance_filled_qty for _, _st in _items), Decimal('0'))
                if abs(_tracked_total - _actual_qty) > _dust:
                    # 仅在单组合时可直接覆写；多组合共享同分腿时按权重重标定，避免"总仓位重复写入每个组合"
                    if len(_items) == 1:
                        _es, _st = _items[0]
                        logger.warning(
                            f"[{_es[0]}-{_es[1]}] Binance残余巡检: 同步状态机数量 "
                            f"{_st.binance_filled_qty} -> {_actual_qty}")
                        _st.binance_filled_qty = max(_actual_qty, Decimal('0'))
                        if self.binance_dual_side_mode and _actual_side in ("LONG", "SHORT"):
                            _st.binance_position_side = _actual_side
                        _st.last_update = time.time()
                        await self._save_state_to_redis(_st)
                    else:
                        _weights = []
                        _w_sum = Decimal('0')
                        for _es, _st in _items:
                            _w = _st.entry_amount if getattr(_st, 'entry_amount', Decimal('0')) > 0 else Decimal('1')
                            _weights.append((_es, _st, _w))
                            _w_sum += _w
                        if _w_sum <= 0:
                            _w_sum = Decimal(str(len(_items)))

                        _allocated = Decimal('0')
                        _changed = 0
                        for _idx, (_es, _st, _w) in enumerate(_weights):
                            if _idx < len(_weights) - 1:
                                _new_qty = (_actual_qty * _w / _w_sum).quantize(Decimal('0.0001'))
                                _allocated += _new_qty
                            else:
                                _new_qty = max(_actual_qty - _allocated, Decimal('0'))
                            if abs(_st.binance_filled_qty - _new_qty) > _dust:
                                _st.binance_filled_qty = _new_qty
                                _st.last_update = time.time()
                                await self._save_state_to_redis(_st)
                                _changed += 1
                        if _changed > 0:
                            logger.warning(
                                f"[Binance残余巡检-自愈] {_sym} {_ps or 'BOTH'}: "
                                f"tracked_total={_tracked_total} -> actual_total={_actual_qty} | 组合数={len(_items)}")

                for _es, _st in _items:
                    _combo = self.arbitrage_combinations.get(_es)
                    if not _combo:
                        continue
                    _f_pos = self.client.positions.get(_combo['future'])
                    _c_pos = self.client.positions.get(_combo['call'])
                    _p_pos = self.client.positions.get(_combo['put'])
                    _has_option_pair = bool(_c_pos and _p_pos and _c_pos.size != 0 and _p_pos.size != 0)
                    _has_deribit_future = bool(_f_pos and _f_pos.size != 0)
                    _expect_hedge = bool(_st.binance_future_symbol and _has_option_pair and not _has_deribit_future)
                    if _expect_hedge and _actual_qty <= _dust and not self._is_twap_hedge_closed(_st):
                        _issues.append(f"{_es[0]}-{_es[1]}:expect_hedge_missing")

            # 2) 额外校验：是否存在 Binance 未跟踪实仓（状态机之外）
            if self.binance_ws and self.binance_dual_side_mode and getattr(self.binance_ws, "positions_by_side", None):
                _tracked_side = self._build_tracked_binance_by_side()
                for (_bn_sym, _ps), _bn_pos in self.binance_ws.positions_by_side.items():
                    if _bn_pos.quantity <= _dust:
                        continue
                    _tracked_qty = _tracked_side.get((_bn_sym, _ps), Decimal('0'))
                    if (_bn_pos.quantity - _tracked_qty) > _dust:
                        _issues.append(f"{_bn_sym} {_ps}:untracked={_bn_pos.quantity - _tracked_qty}")
            elif self.binance_ws and getattr(self.binance_ws, "positions", None):
                _tracked_signed = self._build_tracked_binance_signed()
                for _bn_sym, _bn_pos in self.binance_ws.positions.items():
                    if _bn_pos.quantity <= _dust:
                        continue
                    _actual_signed = _bn_pos.quantity if _bn_pos.side == "LONG" else -_bn_pos.quantity
                    _tracked_signed_qty = _tracked_signed.get(_bn_sym, Decimal('0'))
                    if abs(_actual_signed - _tracked_signed_qty) > _dust:
                        _issues.append(f"{_bn_sym}:actual={_actual_signed},tracked={_tracked_signed_qty}")

            if _issues:
                if force_check and not self._has_pause("Binance残余仓位"):
                    self._add_pause("Binance残余仓位")
                    logger.error(
                        f"🚨 [启动对账] 检测到 Binance 残余/错配风险 {len(_issues)} 条，"
                        f"已自动暂停开仓。示例: {_issues[0]}")
                    if _now - getattr(self, '_bn_residual_pause_tg_ts', 0.0) >= 120:
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 启动恢复后 Binance 对账发现残余/错配风险（{len(_issues)} 条）\n"
                            f"示例: {_issues[0]}\n"
                            f"系统已自动暂停开仓，并将持续自愈/重试。",
                            "binance_residual_startup"))
                        self._bn_residual_pause_tg_ts = _now
                if _now - getattr(self, '_bn_residual_pause_log_ts', 0.0) >= 60:
                    logger.warning(
                        f"⏸️ Binance残余仓位暂停保持中: 未解除条件 {len(_issues)} 条，示例: {_issues[0]}")
                    self._bn_residual_pause_log_ts = _now
                return

            # 风险条件已消失，自动解除该暂停原因
            self._remove_pause("Binance残余仓位")
            if self.trading_paused:
                logger.info(f"✅ 已自动解除 Binance残余仓位 暂停，但仍有其他暂停原因: {self._pause_reason}")
            else:
                logger.info("✅ Binance残余仓位 风险已解除，系统自动恢复开仓。")
        except Exception as e:
            logger.error(f"Binance残余仓位自动解锁巡检异常: {e}")

    async def _refresh_broken_combo_pause(self):
        """自动解除 破损组合 暂停：仅在缺腿风险消失后移除"""
        try:
            if not self._has_pause("破损组合"):
                return
            # Deribit 未连接时不做解除判断，避免误判
            if not self.client.is_connected:
                return

            for es, st in self.arbitrage_states.items():
                if st.state not in ('position_open', 'executing', 'exiting'):
                    continue
                if is_settlement_twap_active_or_pending_delivery(st):
                    continue
                combo = self.arbitrage_combinations.get(es)
                if not combo:
                    continue

                _combo_has_bn_hedge = False
                if st.binance_future_symbol:
                    if st.binance_filled_qty > Decimal('0.0001'):
                        _combo_has_bn_hedge = True
                    elif self.binance_ws:
                        _ps = (st.binance_position_side or "").upper()
                        _bn_pos = None
                        if _ps in ("LONG", "SHORT"):
                            _bn_pos = self.binance_ws.positions_by_side.get((st.binance_future_symbol, _ps))
                        if _bn_pos is None:
                            _bn_pos = self.binance_ws.positions.get(st.binance_future_symbol)
                        if _bn_pos and _bn_pos.quantity > Decimal('0.0001'):
                            _combo_has_bn_hedge = True

                _missing = False
                for _leg in ('future', 'call', 'put'):
                    if _leg == 'future' and _combo_has_bn_hedge:
                        continue
                    _inst = combo.get(_leg)
                    if not _inst:
                        continue
                    _pos = self.client.positions.get(_inst)
                    if _pos is None or _pos.size == 0:
                        _missing = True
                        break
                if _missing:
                    return

            self._remove_pause("破损组合")
            if self.trading_paused:
                logger.info(f"✅ 已自动解除 破损组合 暂停，但仍有其他暂停原因: {self._pause_reason}")
            else:
                logger.info("✅ 破损组合风险已解除，系统自动恢复开仓。")
                asyncio.create_task(tg_notifier.send_async("✅ 破损组合风险已解除，系统自动恢复开仓。"))
        except Exception as e:
            logger.error(f"破损组合自动解锁巡检异常: {e}")

    async def monitor_positions(self):
        """监控持仓"""
        while True:
            try:
                if self._fatal_shutdown:
                    logger.info("🛑 monitor_positions: 致命关机信号，退出巡检")
                    break
                if not self.running:
                    if self._has_pause("紧急清仓"):
                        _now = time.time()
                        if _now - getattr(self, '_monitor_manual_stop_all_log_ts', 0.0) >= 60:
                            logger.info("⏸️ /stop_all 完全暂停中，monitor_positions 不执行自动风控/自动平仓。")
                            self._monitor_manual_stop_all_log_ts = _now
                        await asyncio.sleep(2)
                        continue
                    # running=False 通常表示停止扫描；但若仍有残仓，风控监控必须继续运行
                    _has_open_combo = any(
                        s.state in ('position_open', 'executing', 'exiting')
                        for s in self.arbitrage_states.values()
                    )
                    _has_deribit_pos = any(
                        getattr(_p, 'size', Decimal('0')) != 0
                        for _p in self.client.positions.values()
                    )
                    _has_binance_pos = False
                    if self.binance_ws:
                        if self.binance_dual_side_mode and getattr(self.binance_ws, "positions_by_side", None):
                            _has_binance_pos = any(
                                _bp.quantity > 0 for _bp in self.binance_ws.positions_by_side.values()
                            )
                        else:
                            _has_binance_pos = any(
                                _bp.quantity > 0 for _bp in self.binance_ws.positions.values()
                            )

                    if not (_has_open_combo or _has_deribit_pos or _has_binance_pos):
                        await asyncio.sleep(2)
                        continue

                    _now = time.time()
                    if _now - getattr(self, '_monitor_risk_on_stopped_log_ts', 0.0) >= 30:
                        logger.warning("⚠️ running=False 但检测到残仓/持仓，monitor_positions 继续执行风控巡检。")
                        self._monitor_risk_on_stopped_log_ts = _now
                if getattr(self.client, 'maintenance_sleep_active', False) or self._has_pause("Deribit维护"):
                    await asyncio.sleep(2)
                    continue
                # 🛡️ 修复 E+F (重构) + 修复 H: 启动稳定窗口后做一次性完整审核
                # "系统稳定" = uptime >= 60s **且** Binance + Deribit 都已连接
                # 任一条件不满足时等下次循环再检查（避免在未就绪状态置位导致永久跳过）
                # 一次性触发 (_initial_audit_done 标志), 后续巡检由本循环的常规步骤负责
                if not getattr(self, '_initial_audit_done', False):
                    _e_start = getattr(self, '_engine_start_ts', 0.0)
                    if _e_start > 0 and (time.time() - _e_start) >= 60.0:
                        # 修复 H: 检查"系统稳定"的两个条件 (两端都连)
                        _audit_ready = (
                            self.binance_ws is not None and self.binance_connected
                            and self.client.is_connected
                        )
                        if not _audit_ready:
                            # 节流日志, 避免长期未连时刷屏 (60s 间隔)
                            _now_w = time.time()
                            if _now_w - getattr(self, '_audit_wait_log_ts', 0.0) >= 60:
                                _missing = []
                                if not (self.binance_ws is not None and self.binance_connected):
                                    _missing.append("Binance")
                                if not self.client.is_connected:
                                    _missing.append("Deribit")
                                logger.info(f"[启动稳定审核] 等待 {'+'.join(_missing)} 连接就绪后再触发...")
                                self._audit_wait_log_ts = _now_w
                        else:
                            try:
                                await self._initial_post_startup_audit()
                            except Exception as _audit_err:
                                logger.error(f"[启动稳定审核] 触发异常 (非致命，置位避免反复): {_audit_err}")
                            finally:
                                # 仅在 _audit_ready 路径才置位 — 未就绪时不置位, 下次循环重试
                                self._initial_audit_done = True
                # Binance残余仓位暂停原因自动解锁巡检（仅解除该原因，不影响其他风控暂停）
                await self._refresh_binance_residual_pause()
                # 破损组合暂停原因自动解锁巡检（仅解除该原因，不影响其他暂停）
                await self._refresh_broken_combo_pause()
                # ================= 🌟 插入代码：每秒进行一次全局扫描 =================
                await self._check_global_risk()
                # =================================================================
                # 🌟 重连安全: Deribit WS 断连或引擎重初始化期间，暂停 Deribit 依赖的组合级平仓判断
                # 但全局风控(_check_global_risk)已在上方继续运行，避免 Binance 风险盲区
                if not self.client.is_connected or not self.initialized:
                    _now = time.time()
                    _log_gap = max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0)
                    if _now - getattr(self, '_monitor_deribit_disconnected_log_ts', 0.0) >= _log_gap:
                        logger.warning("⚠️ Deribit 未连接/未初始化，组合级平仓巡检暂缓；全局风控仍持续运行。")
                        self._monitor_deribit_disconnected_log_ts = _now
                    await asyncio.sleep(1)
                    continue
                _cycle_rest_positions = None
                for expiry_strike, state in list(self.arbitrage_states.items()):
                    if state.state == 'executing':
                        # executing 长时间未落地通常意味着开仓流程中断（网络抖动/进程重启）。
                        # 超时后回退为 position_open，交给正常监控链路接管，避免状态长期悬挂。
                        _exec_timeout = float(getattr(self, 'executing_state_timeout_sec', 120.0))
                        if time.time() - state.last_update > _exec_timeout:
                            logger.warning(
                                f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] executing 状态超时(>{int(_exec_timeout)}s)，"
                                f"回退为 position_open 进入常规巡检")
                            state.state = 'position_open'
                            state.last_update = time.time()
                            await self._save_state_to_redis(state)
                        continue
                    if state.state == 'position_open':
                        # 检测交割后持仓归零：Deribit 到期自动结算，3条腿持仓全部变为 0
                        combination = self.arbitrage_combinations.get(expiry_strike)
                        if combination:
                            f_pos = self.client.positions.get(combination['future'])
                            c_pos = self.client.positions.get(combination['call'])
                            p_pos = self.client.positions.get(combination['put'])
                            all_flat = (
                                (not f_pos or f_pos.size == 0) and
                                (not c_pos or c_pos.size == 0) and
                                (not p_pos or p_pos.size == 0)
                            )
                            if all_flat and time.time() - state.last_update > 15:
                                expiry, strike = expiry_strike
                                # REST 二次确认：防止 WS 重连期间缓存为空导致误触发
                                _rest_confirmed = False
                                # 到期闸门：仅在到期时窗内允许进入交割结算，避免行情异常导致"提前结算"
                                _delivery_ready = False
                                try:
                                    from datetime import datetime, timezone, timedelta
                                    _raw_exp = datetime.strptime(expiry_strike[0], "%d%b%y")
                                    _exp_dt = _raw_exp.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                                    _delivery_ready = datetime.now(timezone.utc) >= (_exp_dt - timedelta(minutes=5))
                                except Exception:
                                    _delivery_ready = False
                                try:
                                    if _cycle_rest_positions is None:
                                        _rest_resp = await self.client.send_request({
                                            "jsonrpc": "2.0",
                                            "id": self.client._get_next_request_id(),
                                            "method": "private/get_positions",
                                            "params": {"currency": self.target_currency}
                                        }, is_private=True)
                                        if isinstance(_rest_resp, dict) and 'error' not in _rest_resp and isinstance(_rest_resp.get('result'), list):
                                            _cycle_rest_positions = _rest_resp.get('result', [])
                                        else:
                                            logger.info(f"[{expiry}-{strike}] 交割检测REST响应无效，延迟重试: {_rest_resp}")
                                    if _cycle_rest_positions is not None:
                                        _rest_confirmed = True
                                        _rest_c = None
                                        _rest_p = None
                                        for _rp in _cycle_rest_positions:
                                            if not isinstance(_rp, dict):
                                                continue
                                            _inst = _rp.get('instrument_name', '')
                                            _sz = Decimal(str(_rp.get('size', 0)))
                                            if _sz == 0:
                                                continue
                                            if _inst == combination['call']:
                                                _rest_c = _rp
                                            elif _inst == combination['put']:
                                                _rest_p = _rp
                                        if _rest_c or _rest_p:
                                            logger.warning(f"[{expiry}-{strike}] 交割检测: WS显示持仓归零但REST确认仍有持仓，跳过(可能是WS重连)")
                                            state.last_update = time.time()
                                            continue
                                except Exception as _rest_e:
                                    logger.info(f"[{expiry}-{strike}] 交割检测REST确认失败，延迟重试: {_rest_e}")
                                if not _rest_confirmed:
                                    _now_ts = time.time()
                                    _last_rest_warn = getattr(state, '_settle_rest_unknown_warn_ts', 0.0)
                                    if _now_ts - _last_rest_warn >= 60:
                                        logger.warning(
                                            f"[{expiry}-{strike}] 交割检测: REST 状态未知，暂不按三腿归零处理，等待下一轮确认")
                                        state._settle_rest_unknown_warn_ts = _now_ts
                                    continue
                                if not _delivery_ready:
                                    _now_ts = time.time()
                                    _last_warn = getattr(state, '_premature_settle_warn_ts', 0.0)
                                    _flat_cnt = getattr(state, '_premature_all_flat_count', 0)
                                    if _rest_confirmed:
                                        _flat_cnt += 1
                                        state._premature_all_flat_count = _flat_cnt
                                    if _now_ts - _last_warn >= 60:
                                        logger.warning(
                                            f"[{expiry}-{strike}] 检测到三腿归零，但未到交割时窗，"
                                            f"跳过交割处理并等待下一轮确认"
                                            f"{f' (REST连续确认 {_flat_cnt} 次)' if _rest_confirmed else ''}")
                                        state._premature_settle_warn_ts = _now_ts
                                    # 高风险兜底：若未到期但 REST 连续确认期权腿已归零，视为异常归零，优先处理 Binance 残余腿
                                    if not _rest_confirmed or _flat_cnt < 3:
                                        continue
                                    _dust = Decimal('0.0001')
                                    _bn_qty = state.binance_filled_qty
                                    _bn_known = True
                                    if state.binance_future_symbol:
                                        _bn_qty, _, _, _bn_known = await self._get_binance_actual_position(
                                            state.binance_future_symbol, state.binance_position_side)
                                    if state.binance_future_symbol and (not _bn_known):
                                        logger.error(
                                            f"[{expiry}-{strike}] 未到交割时窗但期权腿异常归零，"
                                            f"Binance 持仓状态未知，保持跟踪并重试")
                                        self._add_pause("Binance残余仓位")
                                        state.last_update = time.time()
                                        await self._save_state_to_redis(state)
                                        continue
                                    if state.binance_future_symbol and (_bn_qty > _dust or state.binance_filled_qty > _dust):
                                        logger.error(
                                            f"[{expiry}-{strike}] 未到交割时窗但期权腿异常归零，"
                                            f"触发 Binance 残余腿兜底平仓 qty={max(_bn_qty, state.binance_filled_qty)}")
                                        _bn_res = await self._close_binance_hedge(state, emergency=True)
                                        # 🌟 P2 回归修复: 残余兜底成功也要保存 close IDs
                                        if _bn_res:
                                            self._capture_binance_close_ids(state, _bn_res)
                                        if (not _bn_res) or state.binance_filled_qty > _dust:
                                            self._add_pause("Binance残余仓位")
                                            state.last_update = time.time()
                                            await self._save_state_to_redis(state)
                                            asyncio.create_task(tg_notifier.send_error_async(
                                                f"🚨 [{expiry}-{strike}] 期权腿异常归零，Binance 残余腿兜底平仓失败\n"
                                                f"剩余: {state.binance_filled_qty}，系统将持续重试，请人工关注。",
                                                "premature_flat_binance_residual"))
                                            continue
                                    logger.warning(
                                        f"[{expiry}-{strike}] 未到交割时窗但期权腿已连续确认归零，"
                                        f"且 Binance 残余已清，标记组合退出")
                                    state.state = 'exited'
                                    state.last_update = time.time()
                                    await self._delete_state_from_redis(expiry, strike)
                                    self.position_locks.discard(state.expiry_strike)
                                    self._bn_mark_missing_since.pop(state.expiry_strike, None)
                                    self._bn_mark_degraded_log_ts.pop(state.expiry_strike, None)
                                    self._broken_combo_first_seen.pop(state.expiry_strike, None)
                                    self._broken_combo_handling.discard(state.expiry_strike)
                                    self._combo_closing_locks.pop(state.expiry_strike, None)
                                    continue
                                if self._is_deribit_settlement_core_window():
                                    _now_ts = time.time()
                                    if _now_ts - getattr(state, '_delivery_core_window_delay_log_ts', 0.0) >= 30:
                                        logger.warning(
                                            f"[{expiry}-{strike}] ⏸️ Deribit core settlement window active，"
                                            f"交割处理延后到窗口结束后执行")
                                        state._delivery_core_window_delay_log_ts = _now_ts
                                    continue

                                # 进入到期结算路径时重置"未到期归零"计数
                                state._premature_all_flat_count = 0
                                logger.info(f"[{expiry}-{strike}] 检测到持仓已归零 (交割/结算)，执行交割结算流程")
                                await self._handle_delivery_settlement(state, combination)
                                continue

                        # 检查是否可以平仓
                        await self._check_exit_opportunity(state)
                    elif state.state == 'exiting':
                        # 系统崩溃恢复：exiting 状态说明上次平仓中途中断
                        # 回退为 position_open 让 _check_exit_opportunity 重新处理
                        # 🌟 C1 修复: 30s→60s，三档平仓每档会刷新 last_update，正常执行不会超时
                        # 只有真正的崩溃/卡死才会触发60秒超时回滚
                        if time.time() - state.last_update > 60:
                            logger.warning(f"[{state.expiry_strike}] 检测到 exiting 状态超时(>60s)，回退为 position_open 重试平仓")
                            state.state = 'position_open'
                            state.last_update = time.time()
                    elif state.state in ['failed', 'error', 'exited']:
                        # 统一在 5 分钟后静默清理内存，绝不阻塞主循环
                        if time.time() - state.last_update > 300:
                            # 🌟 竞态防护：确认 dict 中当前值仍是遍历快照中的同一对象
                            # 主循环可能已用新状态覆盖了同一 key，此时不能误删新状态
                            current = self.arbitrage_states.get(expiry_strike)
                            if current is state:
                                del self.arbitrage_states[expiry_strike]
                                self.position_locks.discard(expiry_strike)
                                self._bn_mark_missing_since.pop(expiry_strike, None)
                                self._bn_mark_degraded_log_ts.pop(expiry_strike, None)
                                self._broken_combo_first_seen.pop(expiry_strike, None)
                                self._broken_combo_handling.discard(expiry_strike)
                                self._combo_closing_locks.pop(expiry_strike, None)

                # 🌟 Bug#15: 内存增长保护 — 终态条目超过 200 条时，强制清理最老的
                _terminal = ['failed', 'error', 'exited']
                if len(self.arbitrage_states) > 200:
                    _stale = sorted(
                        [(k, s) for k, s in self.arbitrage_states.items() if s.state in _terminal],
                        key=lambda x: x[1].last_update
                    )
                    for _k, _s in _stale[:len(_stale) - 50]:  # 清理到只剩 50 条终态
                        current = self.arbitrage_states.get(_k)
                        if current is _s:
                            del self.arbitrage_states[_k]
                            self.position_locks.discard(_k)
                            self._bn_mark_missing_since.pop(_k, None)
                            self._bn_mark_degraded_log_ts.pop(_k, None)
                            self._broken_combo_first_seen.pop(_k, None)
                            self._broken_combo_handling.discard(_k)
                            self._combo_closing_locks.pop(_k, None)
                    if _stale:
                        logger.info(f"[内存保护] arbitrage_states 清理完成，当前 {len(self.arbitrage_states)} 条")

                await asyncio.sleep(1)  # 每秒检查一次
            except Exception as e:
                logger.error(f"监控持仓异常: {e}")
                asyncio.create_task(tg_notifier.send_error_async(f"监控持仓循环异常: {e}", "monitor_error"))
                await asyncio.sleep(5)

    async def _check_exit_opportunity(self, state: ArbitrageState):
        """检查退出机会 (实盘机构级版：四大护栏 + 官方PnL平仓 + VWAP防滑点)"""
        try:
            # 风控检查保持 1s 级别，避免硬止损/Gamma 被日志节流误伤
            _now_check = time.time()
            if _now_check - getattr(state, 'last_exit_check_ts', 0.0) < 0.9:
                return
            state.last_exit_check_ts = _now_check
            combination = self.arbitrage_combinations.get(state.expiry_strike)
            if not combination:
                return
            expiry, strike = state.expiry_strike

            # 🌟 P1-1 修复: 跨所模式 + Binance 断连 → 跳过硬止损/Gamma/紧急平仓
            # 原因: Binance 价格不可用时，PnL 计算用 Deribit 价格(有基差)会误算
            #       误触发 emergency_dump_all 可能制造裸腿 (配合 P0 修复)
            # 保留: 持仓巡检日志 + 永续超时检测（只告警不执行）
            _is_cross = bool(state.binance_future_symbol and state.binance_filled_qty > Decimal('0.0001'))
            if _is_cross and not self.binance_connected:
                # Binance 断连期间 Gamma 计数无效，强制清零避免恢复后沿用旧计数误触发强平
                state.gamma_exceed_start = 0
                state.gamma_exceed_count = 0
                _last_skip = getattr(state, '_exit_bn_disconnect_skip_ts', 0.0)
                if time.time() - _last_skip >= 60:
                    logger.info(
                        f"[{expiry}-{strike}] 🛡️ Binance 断连中，跳过单组合风控检查 "
                        f"(硬止损/Gamma/紧急平仓)，等待恢复")
                    state._exit_bn_disconnect_skip_ts = time.time()
                return

            # 1. 获取当前盘口
            future_ticker = self.client.tickers.get(combination['future'])
            call_ticker = self.client.tickers.get(combination['call'])
            put_ticker = self.client.tickers.get(combination['put'])
            _risk_data_degraded = False
            _risk_degraded_reasons = []
            for _nm, _tk in [('future', future_ticker), ('call', call_ticker), ('put', put_ticker)]:
                if _tk is None:
                    _risk_data_degraded = True
                    _risk_degraded_reasons.append(f"{_nm}:missing")
                elif _tk.bid <= Decimal('0') or _tk.ask <= Decimal('0'):
                    _risk_data_degraded = True
                    _risk_degraded_reasons.append(f"{_nm}:bidask<=0")

            # 🌟 H4 修复: 平仓路径的行情过期检测 (阈值更宽松，60秒，因为止损仍需执行)
            _now_exit = time.time()
            _stale_exit = 60
            for _te_name, _te_data in [('future', future_ticker), ('call', call_ticker), ('put', put_ticker)]:
                if _te_data and hasattr(_te_data, 'timestamp') and _te_data.timestamp > 0 and (_now_exit - _te_data.timestamp) > _stale_exit:
                    _risk_data_degraded = True
                    _risk_degraded_reasons.append(f"{_te_name}:stale>{_stale_exit}s")

            if _risk_data_degraded:
                _last_log = getattr(self, '_risk_data_degraded_log_ts', {}).get(state.expiry_strike, 0.0)
                if _now_exit - _last_log > 60:
                    if not hasattr(self, '_risk_data_degraded_log_ts'):
                        self._risk_data_degraded_log_ts = {}
                    self._risk_data_degraded_log_ts[state.expiry_strike] = _now_exit
                    _why = ",".join(_risk_degraded_reasons[:4])
                    _is_ws_degrade = (not self.client.is_connected or self._has_pause("Deribit维护"))
                    _prefix = "WS断连/维护中" if _is_ws_degrade else "行情降级"
                    _msg = (
                        f"[{expiry}-{strike}] {'⚠️' if _is_ws_degrade else 'ℹ️'} "
                        f"风控行情降级({_prefix}): {_why} | 仍执行硬止损/Gamma检查"
                    )
                    if _is_ws_degrade:
                        logger.warning(_msg)
                    else:
                        logger.info(_msg)

            # 2. 获取真实持仓 (获取 f_pos, c_pos, p_pos)
            f_pos = self.client.positions.get(combination['future'])
            c_pos = self.client.positions.get(combination['call'])
            p_pos = self.client.positions.get(combination['put'])
            _dust = Decimal('0.0001')
            _has_option_pair = bool(c_pos and p_pos and c_pos.size != 0 and p_pos.size != 0)
            _has_deribit_future = bool(f_pos and f_pos.size != 0)

            # Binance 实仓对账（修正状态机漂移，防止"账面有对冲/实仓无对冲"）
            _bn_actual_qty = Decimal('0')
            _bn_actual_side = ''
            _bn_actual_known = True
            if state.binance_future_symbol:
                _query_ps = state.binance_position_side if self.binance_dual_side_mode else ''
                _bn_actual_qty, _bn_actual_side, _, _bn_actual_known = await self._get_binance_actual_position(
                    state.binance_future_symbol, _query_ps)
                if _bn_actual_known and abs(state.binance_filled_qty - _bn_actual_qty) > _dust:
                    # 关键防护：同 symbol+同 side 可能对应多个组合，Binance 返回的是"分腿总仓位"而非单组合仓位
                    # 仅在该分腿只被单个组合引用时，才允许按实仓覆写，避免把总仓位重复写入每个组合。
                    _share_cnt = 0
                    for _s in self.arbitrage_states.values():
                        if _s.state not in ('position_open', 'executing', 'exiting'):
                            continue
                        if _s.binance_future_symbol != state.binance_future_symbol:
                            continue
                        _ps_s = (_s.binance_position_side or '').upper()
                        if self.binance_dual_side_mode:
                            if _ps_s not in ("LONG", "SHORT"):
                                _ps_s = "LONG" if _s.strategy_type == 'buy_future_sell_synthetic' else "SHORT"
                            if _ps_s != (_query_ps or _ps_s):
                                continue
                        _share_cnt += 1

                    if _share_cnt <= 1:
                        logger.warning(
                            f"[{expiry}-{strike}] Binance 持仓对账修正: 状态机={state.binance_filled_qty} -> 实仓={_bn_actual_qty}")
                        state.binance_filled_qty = _bn_actual_qty
                        if self.binance_dual_side_mode and _bn_actual_side in ("LONG", "SHORT"):
                            state.binance_position_side = _bn_actual_side
                        state.last_update = time.time()
                        await self._save_state_to_redis(state)
                    else:
                        logger.debug(
                            f"[{expiry}-{strike}] Binance 持仓对账仅做观察: "
                            f"同分腿被 {_share_cnt} 个组合共享，跳过逐组合覆写 "
                            f"(state={state.binance_filled_qty}, actual_total={_bn_actual_qty})")

            # 跨所模式: 期货腿在 Binance，Deribit 无 f_pos，只需检查 c_pos/p_pos
            _is_cross_exchange = False
            if state.binance_future_symbol:
                if state.binance_filled_qty > _dust:
                    _is_cross_exchange = True
                elif _bn_actual_known and _bn_actual_qty > _dust:
                    _is_cross_exchange = True
                elif not _bn_actual_known:
                    # 查询未知时按跨所处理，避免误走 Deribit 期货分支
                    _is_cross_exchange = True
            # 结构化兜底：只要该组合记录了 Binance 对冲符号，且 Deribit 期货腿为空、期权腿存在，就按跨所处理
            if not _is_cross_exchange and state.binance_future_symbol and _has_option_pair and not _has_deribit_future:
                _is_cross_exchange = True

            # 提前计算 hours_to_expiry（裸腿豁免需要用到）
            from datetime import datetime, timezone, timedelta
            try:
                _raw_dt = datetime.strptime(expiry, '%d%b%y')
                _expiry_dt_early = _raw_dt.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                _hours_to_expiry_early = (_expiry_dt_early - datetime.now(timezone.utc)).total_seconds() / 3600
            except Exception:
                _hours_to_expiry_early = float('inf')
            _twap_window_hours = max(float(getattr(self, 'settlement_twap_minutes', 30)), 5) / 60.0

            # 高危护栏：Deribit 期权腿存在、Deribit 期货腿为空，但 Binance 对冲实仓为0 => 裸腿
            # 豁免: 结算TWAP 已主动关闭 Binance 对冲腿，这是预期行为而非裸腿
            _expect_binance_hedge = bool(state.binance_future_symbol and _has_option_pair and not _has_deribit_future)
            _twap_task_obj = getattr(state, '_settlement_twap_task', None)
            _twap_intentional_close = (
                getattr(state, '_settlement_twap_started', False)
                and (getattr(state, '_settlement_twap_result', None) is not None
                     or (_twap_task_obj is not None and not _twap_task_obj.done())
                     or (getattr(state, '_settlement_twap_qty_snapshot', Decimal('0')) > _dust
                         and 0 < _hours_to_expiry_early <= _twap_window_hours))
            )
            if _expect_binance_hedge and _bn_actual_known and _bn_actual_qty <= _dust and not _twap_intentional_close:
                _now_hm = time.time()
                _grace = max(float(getattr(self, '_bn_hedge_missing_grace_sec', 8.0)), 2.0)
                _first_seen = getattr(state, '_bn_hedge_missing_since', 0.0)
                if _first_seen <= 0:
                    state._bn_hedge_missing_since = _now_hm
                    return
                if (_now_hm - _first_seen) < _grace:
                    return

                _last_trigger = getattr(state, '_bn_hedge_missing_trigger_ts', 0.0)
                if (_now_hm - _last_trigger) >= 30:
                    state._bn_hedge_missing_trigger_ts = _now_hm
                    self._add_pause("Binance残余仓位")
                    logger.error(
                        f"🚨 [{expiry}-{strike}] 检测到裸腿: Deribit 期权在仓，但 Binance 对冲实仓为0。"
                        f" tracked={state.binance_filled_qty}, actual={_bn_actual_qty}")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 [{expiry}-{strike}] 对冲腿缺失\n"
                        f"Deribit 期权仍在仓，但 Binance {state.binance_future_symbol} 实仓为0\n"
                        f"系统将执行组合级紧急处置（断尾求生）。",
                        "binance_hedge_missing_combo"))
                    await self._emergency_dump_all(state, combination, f_pos, c_pos, p_pos)
                return
            else:
                state._bn_hedge_missing_since = 0.0

            if _is_cross_exchange:
                if not _has_option_pair:
                    return
            else:
                if not (_has_deribit_future and _has_option_pair):
                    return

            # 获取 BTC 标记价格
            # 🌟 跨所修复: 跨所模式必须优先用 Binance 价格
            # Deribit 远期期货有基差溢价(如 71505 vs Binance 69696)，不能作为跨所 P&L 的基准价
            current_mark_price = Decimal('0')
            _bn_price_source = "none"
            _bn_price_age = float('inf')
            _bn_price_fresh_for_basis = False
            _bn_reconnect_grace_ok = True
            if _is_cross_exchange:
                _mon_perp = state.binance_future_symbol or (self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT')
                _bn_mark_ob = self.binance_ws.order_books.get(_mon_perp) if self.binance_ws else None
                _bn_mark_ws = self.binance_ws.mark_prices.get(_mon_perp, Decimal('0')) if self.binance_ws else Decimal('0')
                _bn_last_trade = self.binance_ws.last_prices.get(_mon_perp, Decimal('0')) if self.binance_ws else Decimal('0')
                _now_price = time.time()
                _basis_price_stale_sec = 10.0
                _basis_reconnect_grace_sec = 10.0
                _market_connected_at = float(getattr(self.binance_ws, 'market_connected_at', 0.0) or 0.0) if self.binance_ws else 0.0
                _bn_reconnect_grace_ok = bool(
                    _market_connected_at > 0 and (_now_price - _market_connected_at) >= _basis_reconnect_grace_sec)
                _mark_ts = getattr(self.binance_ws, 'mark_price_update_times', {}).get(_mon_perp, 0.0) if self.binance_ws else 0.0
                _last_ts = getattr(self.binance_ws, 'last_price_update_times', {}).get(_mon_perp, 0.0) if self.binance_ws else 0.0
                _bn_price_candidates = []
                if _bn_mark_ob and _bn_mark_ob.mid_price is not None and _bn_mark_ob.mid_price > 0:
                    _bn_price_candidates.append((_bn_mark_ob.mid_price, "binance_orderbook", getattr(_bn_mark_ob, 'update_time', 0.0)))
                if _bn_mark_ws > 0:
                    _bn_price_candidates.append((_bn_mark_ws, "binance_mark", _mark_ts))
                if _bn_last_trade > 0:
                    _bn_price_candidates.append((_bn_last_trade, "binance_last", _last_ts))

                for _px, _src, _ts in _bn_price_candidates:
                    _age = (_now_price - _ts) if _ts else float('inf')
                    if _age <= _basis_price_stale_sec:
                        current_mark_price = _px
                        _bn_price_source = _src
                        _bn_price_age = _age
                        _bn_price_fresh_for_basis = True
                        break

                if _bn_price_fresh_for_basis:
                    self._bn_mark_missing_since.pop(state.expiry_strike, None)
                    self._bn_mark_degraded_log_ts.pop(state.expiry_strike, None)
                else:
                    _miss_start = self._bn_mark_missing_since.setdefault(state.expiry_strike, time.time())
                    _miss_sec = time.time() - _miss_start
                    _fallback = future_ticker.mid_price if future_ticker and future_ticker.mid_price > 0 else Decimal('0')
                    _last_log = self._bn_mark_degraded_log_ts.get(state.expiry_strike, 0.0)
                    if _fallback > 0 and _miss_sec >= 15:
                        current_mark_price = _fallback
                        _bn_price_source = "deribit_fallback"
                        if time.time() - _last_log > 60:
                            logger.info(
                                f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] Binance 价格缺失 {_miss_sec:.0f}s，"
                                f"降级使用 Deribit 价格进行风控，仅用于止损防线")
                            self._bn_mark_degraded_log_ts[state.expiry_strike] = time.time()
                    else:
                        if time.time() - _last_log > 60:
                            logger.info(
                                f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] Binance 价格缺失 {_miss_sec:.0f}s，"
                                f"本轮暂不更新PnL，继续等待行情恢复")
                            self._bn_mark_degraded_log_ts[state.expiry_strike] = time.time()
                        _ref_px = await self._get_reference_btc_price(_mon_perp)
                        if _ref_px > 0:
                            current_mark_price = _ref_px
                            _bn_price_source = "reference_fallback"
                        else:
                            _entry_ref = state.entry_prices.get('future', Decimal('0')) if state.entry_prices else Decimal('0')
                            current_mark_price = _entry_ref if _entry_ref > 0 else Decimal('0')
                            _bn_price_source = "entry_fallback" if current_mark_price > 0 else "none"
            elif f_pos and f_pos.mark_price > 0:
                current_mark_price = f_pos.mark_price
                _bn_price_source = "deribit_position"
            else:
                current_mark_price = future_ticker.mid_price if future_ticker else Decimal('0')
                _bn_price_source = "deribit_ticker" if current_mark_price > 0 else "none"
            if current_mark_price <= 0:
                _ref_px = await self._get_reference_btc_price(
                    state.binance_future_symbol if _is_cross_exchange else '')
                if _ref_px > 0:
                    current_mark_price = _ref_px
                    _bn_price_source = "reference_fallback"
                else:
                    _entry_ref = state.entry_prices.get('future', Decimal('0')) if state.entry_prices else Decimal('0')
                    current_mark_price = _entry_ref if _entry_ref > 0 else Decimal('1')
                    _bn_price_source = "entry_fallback"
                    logger.warning(
                        f"[{expiry}-{strike}] ⚠️ 无可用标记价，使用入场价兜底执行风控检查: {current_mark_price}")

            # =========================================================================
            # =========================================================================
            # 🚨 第一部分：到期时间判断 + 末日轮 (Gamma) 风险控制逻辑
            # =========================================================================
            from datetime import datetime, timezone, timedelta

            # 预设平仓门槛与风控参数
            max_gamma_limit = getattr(self, 'max_net_gamma', Decimal('0.02'))
            hours_to_expiry = float('inf')  # 默认无穷大（解析失败时当作长期合约处理）

            try:
                # 1. 解析出无时区的日期 (如 "27MAR26" -> 2026-03-27 00:00:00)
                raw_dt = datetime.strptime(expiry, '%d%b%y')

                # 2. 补全时区 (UTC) 和 Deribit 真实的交割时间 (UTC 08:00)
                expiry_dt = raw_dt.replace(tzinfo=timezone.utc) + timedelta(hours=8)

                # 3. 获取带时区感知的当前 UTC 时间
                now_utc = datetime.now(timezone.utc)
                hours_to_expiry = (expiry_dt - now_utc).total_seconds() / 3600

                # 距离到期不足 24 小时：收紧 Gamma 容忍度
                if hours_to_expiry < 24:
                    max_gamma_limit = max_gamma_limit * Decimal('0.5')
            except Exception as e:
                logger.error(f"到期时间判断 + 末日轮 (Gamma) 风险控制逻辑 异常:{e}")
                pass

            # =========================================================================
            # 基差监控 + 条件提前 TWAP (方案C)
            # Binance永续价 vs Deribit指数价的偏差在到期前 1-2h 可能突然拉大 ($200-500+)
            # 监控窗口内每 60 秒记录基差，超阈值时提前启动 TWAP 锁住利润
            # =========================================================================
            _basis_monitor_h = float(getattr(self, 'basis_monitor_hours', 3.0))
            _basis_trigger = float(getattr(self, 'basis_early_trigger_usd', 300.0))
            _basis_deterioration_trigger = float(getattr(self, 'basis_deterioration_trigger_usd', 150.0))
            if (_is_cross_exchange
                    and 0 < hours_to_expiry <= _basis_monitor_h
                    and state.binance_filled_qty > _dust
                    and current_mark_price > 0
                    and self.binance_connected
                    and _bn_price_fresh_for_basis
                    and _bn_reconnect_grace_ok
                    and not getattr(state, '_settlement_twap_started', False)):
                _basis_usd = Decimal('0')
                try:
                    _idx_resp = await self.client.send_request({
                        "jsonrpc": "2.0",
                        "id": self.client._get_next_request_id(),
                        "method": "public/get_index_price",
                        "params": {"index_name": f"{self.target_currency.lower()}_usd"}
                    })
                    _deribit_index = Decimal(str(_idx_resp['result'].get('index_price', 0))) if _idx_resp and 'result' in _idx_resp else Decimal('0')
                    if _deribit_index > 0:
                        _basis_usd = current_mark_price - _deribit_index
                        _entry_basis = getattr(state, 'entry_basis_usd', None)
                        if _entry_basis is not None:
                            if state.strategy_type == 'sell_future_buy_synthetic':
                                _deterioration = float(_basis_usd) - float(_entry_basis)
                            else:
                                _deterioration = float(_entry_basis) - float(_basis_usd)
                        else:
                            _deterioration = None
                        _last_basis_log = getattr(state, '_basis_monitor_log_ts', 0.0)
                        if time.time() - _last_basis_log >= 60:
                            state._basis_monitor_log_ts = time.time()
                            _basis_pct = float(_basis_usd / _deribit_index) * 100
                            _det_str = f" | 恶化{_deterioration:+.1f}" if _deterioration is not None else ""
                            _entry_str = f" | 开仓基差={_entry_basis:+.1f}{_det_str}" if _entry_basis is not None else ""
                            logger.info(
                                f"[{expiry}-{int(strike)}] 📈 基差监控: Binance={current_mark_price:.1f} "
                                f"({_bn_price_source}, age={_bn_price_age:.1f}s) - "
                                f"Deribit指数={_deribit_index:.1f} = {_basis_usd:+.1f} USD ({_basis_pct:+.3f}%)"
                                f"{_entry_str}"
                                f" | 距到期 {hours_to_expiry:.1f}h | 阈值 abs≥{_basis_trigger:.0f}/恶化≥{_basis_deterioration_trigger:.0f}")
                        # 触发条件: 绝对基差超阈值 OR 基差恶化超阈值
                        _abs_triggered = abs(float(_basis_usd)) >= _basis_trigger
                        _det_triggered = _deterioration is not None and _deterioration >= _basis_deterioration_trigger
                        if _abs_triggered or _det_triggered:
                            _trigger_reason = []
                            if _abs_triggered:
                                _trigger_reason.append(f"绝对基差 {abs(float(_basis_usd)):.1f}≥{_basis_trigger:.0f}")
                            if _det_triggered:
                                _trigger_reason.append(f"恶化 {_deterioration:.1f}≥{_basis_deterioration_trigger:.0f}")
                            _reason_str = " + ".join(_trigger_reason)
                            _basis_dir = "多头付费溢价" if _basis_usd > 0 else "空头付费折价"
                            logger.warning(
                                f"[{expiry}-{int(strike)}] ⚡ 基差触发提前TWAP! "
                                f"{_reason_str} ({_basis_dir}) | "
                                f"距到期 {hours_to_expiry:.1f}h，提前启动 TWAP 锁住利润")
                            _tg_entry_line = f"开仓基差: {_entry_basis:+.1f} USD\n" if _entry_basis is not None else ""
                            asyncio.create_task(tg_notifier.send_async(
                                f"⚡ [{expiry}-{int(strike)}] 基差触发提前TWAP\n"
                                f"触发: {_reason_str}\n"
                                f"当前基差: {_basis_usd:+.1f} USD ({_basis_dir})\n"
                                f"{_tg_entry_line}"
                                f"Binance: {current_mark_price:.1f} | Deribit指数: {_deribit_index:.1f}\n"
                                f"距到期: {hours_to_expiry:.1f}h\n"
                                f"正在启动 TWAP 平仓..."))
                            state._settlement_twap_started = True
                            state._settlement_twap_result = None
                            if not getattr(state, '_settlement_twap_qty_snapshot', Decimal('0')) > _dust:
                                state._settlement_twap_qty_snapshot = state.binance_filled_qty
                            state._settlement_twap_task = asyncio.create_task(
                                self._run_settlement_twap(state))
                except Exception as _basis_err:
                    logger.info(f"[{expiry}-{int(strike)}] 基差监控异常: {_basis_err}")

            # 结算窗口 TWAP 预平仓：在 Deribit TWAP 窗口 (默认到期前 30 分钟) 内启动
            # Binance 分片平仓，使平均成交价贴近 Deribit 结算 TWAP，消除结算后价格漂移风险
            # 允许重试：TWAP task 已结束但仍有残仓且仍在窗口内 → 重置标志重新启动
            _twap_prev_task = getattr(state, '_settlement_twap_task', None)
            _twap_retry_count = getattr(state, '_settlement_twap_retry_count', 0)
            _twap_max_retries = 5
            _twap_can_retry = (
                getattr(state, '_settlement_twap_started', False)
                and (_twap_prev_task is None or _twap_prev_task.done())
                and state.binance_filled_qty > _dust
                and _twap_retry_count < _twap_max_retries
            )
            if _twap_can_retry:
                state._settlement_twap_started = False
                state._settlement_twap_retry_count = _twap_retry_count + 1
                logger.info(
                    f"[{expiry}-{int(strike)}] 结算TWAP前次已结束但仍有残仓 "
                    f"{state.binance_filled_qty}，重试 {_twap_retry_count + 1}/{_twap_max_retries}")
            if (getattr(self, 'settlement_twap_enabled', True)
                    and 0 < hours_to_expiry <= getattr(self, 'settlement_twap_minutes', 30) / 60.0
                    and state.state == 'position_open'
                    and state.binance_filled_qty > _dust
                    and not getattr(state, '_settlement_twap_started', False)):
                state._settlement_twap_started = True
                _prev_result = getattr(state, '_settlement_twap_result', None)
                if _prev_result and _prev_result.get('executedQty'):
                    _prev_filled = Decimal(str(_prev_result['executedQty']))
                    _prev_notional = Decimal(str(_prev_result.get('avgPrice', '0'))) * _prev_filled
                    state._settlement_twap_accumulated = {
                        'filled': float(_prev_filled),
                        'notional': float(_prev_notional),
                        'orderIds': list(_prev_result.get('orderIds', [])),
                        'slices': int(_prev_result.get('twapSlices', 0)),
                        'lastAvg': float(Decimal(str(_prev_result.get('avgPrice', '0')))),
                    }
                state._settlement_twap_result = None
                if not getattr(state, '_settlement_twap_qty_snapshot', Decimal('0')) > _dust:
                    state._settlement_twap_qty_snapshot = state.binance_filled_qty
                state._settlement_twap_task = asyncio.create_task(
                    self._run_settlement_twap(state))
                logger.info(
                    f"[{expiry}-{int(strike)}] 距到期 {hours_to_expiry:.2f}h (≤{getattr(self, 'settlement_twap_minutes', 30)}min)，"
                    f"启动结算TWAP预平仓 Binance {state.binance_future_symbol}")

            # =========================================================================
            # 🚨🚨🚨 实盘交易员护栏：单组合 PnL / 硬止损 🚨🚨🚨
            # =========================================================================
            # ⚠️ 关键修复：f_pos.unrealized_pnl 是该期货合约的【总仓位浮亏】（多个组合共享同一期货），
            # 不能直接用于单组合的止损判断。改用独立成本账本计算本组合的真实浮亏。

            # 🌟 RISK-2 修复: 只要有 entry_prices 就用成本账本（不再依赖 prices_confirmed）
            # entry_prices 在开仓时即设置（即使尚未确认成交价也有预期价），远优于共享期货总PnL
            if state.entry_prices:
                f_entry = state.entry_prices.get('future', Decimal('0'))
                c_entry = state.entry_prices.get('call', Decimal('0'))
                p_entry = state.entry_prices.get('put', Decimal('0'))
                opt_amt = state.entry_amount if state.entry_amount > 0 else self.trade_amount
                f_mark = current_mark_price  # 跨所时为 Binance 价格，否则为 Deribit 期货标记价
                # 🌟 Plan Bug #3 修复: 到期前 OTM 期权簿常为空, 真实价值要等交割结算
                # 降级链: ticker.mid → pos.mark → entry_price
                # 用 entry_price 的含义是"假设期权无变动", 让硬止损只基于期货腿真实变动判断
                # 避免因期权数据空洞导致风控全部静默跳过 (2026-04-11 11APR26-73000 交割时发生过)
                _option_data_degraded = False
                c_mark = call_ticker.mid_price if call_ticker and call_ticker.mid_price > 0 else (
                    c_pos.mark_price if c_pos and c_pos.mark_price > 0 else c_entry)
                p_mark = put_ticker.mid_price if put_ticker and put_ticker.mid_price > 0 else (
                    p_pos.mark_price if p_pos and p_pos.mark_price > 0 else p_entry)
                # 检测是否走了 entry_price fallback
                _c_from_live = (call_ticker and call_ticker.mid_price > 0) or (c_pos and c_pos.mark_price > 0)
                _p_from_live = (put_ticker and put_ticker.mid_price > 0) or (p_pos and p_pos.mark_price > 0)
                if not _c_from_live or not _p_from_live:
                    _option_data_degraded = True

                if f_entry > 0 and f_mark > 0 and c_mark > 0 and p_mark > 0:
                    # 🌟 P0-6 修复: 跨所模式用 Binance 线性公式, 非跨所用 Deribit inverse
                    # 期权 PnL 永远是 BTC 本位 (Deribit 期权是 inverse, 用 BTC 计价)
                    if state.strategy_type == 'sell_future_buy_synthetic':
                        # 卖期货 + 买Call + 卖Put → 期货涨亏、Call涨赚、Put涨亏
                        c_pnl_btc = (c_mark - c_entry) * opt_amt  # BTC 本位
                        p_pnl_btc = (p_entry - p_mark) * opt_amt  # BTC 本位
                    else:
                        # 买期货 + 卖Call + 买Put
                        c_pnl_btc = (c_entry - c_mark) * opt_amt
                        p_pnl_btc = (p_mark - p_entry) * opt_amt
                    # 期权 PnL 转 USD
                    option_pnl_usd = (c_pnl_btc + p_pnl_btc) * f_mark

                    # 期货 PnL: 跨所(Binance 线性) vs 非跨所(Deribit inverse) 区别对待
                    if _is_cross_exchange:
                        # Binance 线性合约: PnL_USD = (exit - entry) × qty
                        bn_qty = state.binance_filled_qty if state.binance_filled_qty > 0 else (
                            state.future_size_usd / f_entry if f_entry > 0 else opt_amt)
                        if state.strategy_type == 'sell_future_buy_synthetic':
                            # 做空永续 (bn_entry 高, 当前价 f_mark 低则盈利)
                            future_pnl_usd = float((f_entry - f_mark) * bn_qty)
                        else:
                            # 做多永续
                            future_pnl_usd = float((f_mark - f_entry) * bn_qty)
                        combo_pnl_usd = Decimal(str(future_pnl_usd)) + option_pnl_usd
                    else:
                        # Deribit inverse 合约: PnL_BTC = (f_entry - f_mark) / f_mark × notional
                        if state.strategy_type == 'sell_future_buy_synthetic':
                            f_pnl_btc = (f_entry - f_mark) / f_mark
                        else:
                            f_pnl_btc = (f_mark - f_entry) / f_mark
                        combo_pnl_btc = (f_pnl_btc * (state.future_size_usd / f_entry if f_entry > 0 else opt_amt)
                                         + c_pnl_btc + p_pnl_btc)
                        combo_pnl_usd = combo_pnl_btc * f_mark
                else:
                    # f_entry/f_mark 为0时的降级：跨所模式不应到这里（entry_prices应完整），仅作兜底
                    if f_pos and f_pos.size != 0:
                        _f_pnl = getattr(f_pos, 'unrealized_pnl', Decimal('0'))
                        _f_abs_size = abs(f_pos.size)
                        _combo_f_usd = state.future_size_usd if state.future_size_usd > 0 else self.trade_amount * current_mark_price
                        _f_ratio = min(_combo_f_usd / _f_abs_size, Decimal('1'))
                        combo_pnl_usd = (_f_pnl * _f_ratio +
                                         c_pos.unrealized_pnl + p_pos.unrealized_pnl) * current_mark_price
                    else:
                        # 跨所且无 f_entry: 仅计算期权 PnL
                        combo_pnl_usd = (c_pos.unrealized_pnl + p_pos.unrealized_pnl) * current_mark_price
            else:
                # 无 entry_prices（异常情况：如Redis恢复失败），按比例分摊期货PnL
                if f_pos and f_pos.size != 0:
                    _f_pnl = getattr(f_pos, 'unrealized_pnl', Decimal('0'))
                    _f_abs_size = abs(f_pos.size)
                    _combo_f_usd = state.future_size_usd if state.future_size_usd > 0 else self.trade_amount * current_mark_price
                    _f_ratio = min(_combo_f_usd / _f_abs_size, Decimal('1'))
                    combo_pnl_usd = (_f_pnl * _f_ratio +
                                     c_pos.unrealized_pnl + p_pos.unrealized_pnl) * current_mark_price
                else:
                    combo_pnl_usd = (c_pos.unrealized_pnl + p_pos.unrealized_pnl) * current_mark_price

            # W1 修复：单组合止损口径补扣费用（已发生开仓费 + 现在平仓预估费）
            # 避免仅按毛浮盈判断，导致硬止损触发偏慢。
            try:
                _opt_amt_fee = state.entry_amount if state.entry_amount > 0 else self.trade_amount
                _f_entry_fee = state.entry_prices.get('future', Decimal('0')) if state.entry_prices else Decimal('0')
                _c_entry_fee = state.entry_prices.get('call', Decimal('0')) if state.entry_prices else Decimal('0')
                _p_entry_fee = state.entry_prices.get('put', Decimal('0')) if state.entry_prices else Decimal('0')
                # 🌟 Plan Bug #3 修复: 费用估算同样用 entry_price 降级 (而非 0)
                # 否则 _close_fee_usd 会被跳过, 硬止损阈值失去费用缓冲
                _c_mark_fee = call_ticker.mid_price if call_ticker and call_ticker.mid_price > 0 else (
                    c_pos.mark_price if c_pos and c_pos.mark_price > 0 else _c_entry_fee)
                _p_mark_fee = put_ticker.mid_price if put_ticker and put_ticker.mid_price > 0 else (
                    p_pos.mark_price if p_pos and p_pos.mark_price > 0 else _p_entry_fee)
                _bn_qty_fee = state.binance_filled_qty if state.binance_filled_qty > 0 else Decimal('0')
                if _bn_qty_fee <= 0 and _f_entry_fee > 0 and state.future_size_usd > 0:
                    _bn_qty_fee = (state.future_size_usd / _f_entry_fee)

                _open_fee_usd = Decimal('0')
                if _f_entry_fee > 0 and _opt_amt_fee > 0 and _c_entry_fee > 0 and _p_entry_fee > 0:
                    _oc_btc = self.trade_executor.fee_calculator.calculate_option_fee(
                        _f_entry_fee, _c_entry_fee, _opt_amt_fee, is_taker=True)
                    _op_btc = self.trade_executor.fee_calculator.calculate_option_fee(
                        _f_entry_fee, _p_entry_fee, _opt_amt_fee, is_taker=True)
                    _open_fee_usd += (_oc_btc + _op_btc) * _f_entry_fee
                if _f_entry_fee > 0 and _bn_qty_fee > 0:
                    _open_fee_usd += self.trade_executor._calculate_binance_fee_usdt(
                        _f_entry_fee, _bn_qty_fee, is_taker=True)

                _close_fee_usd = Decimal('0')
                if current_mark_price > 0 and _opt_amt_fee > 0 and _c_mark_fee > 0 and _p_mark_fee > 0:
                    _cc_btc = self.trade_executor.fee_calculator.calculate_delivery_fee(
                        current_mark_price, _c_mark_fee, _opt_amt_fee, is_option=True)
                    _cp_btc = self.trade_executor.fee_calculator.calculate_delivery_fee(
                        current_mark_price, _p_mark_fee, _opt_amt_fee, is_option=True)
                    _close_fee_usd += (_cc_btc + _cp_btc) * current_mark_price
                if current_mark_price > 0 and _bn_qty_fee > 0:
                    _close_fee_usd += self.trade_executor._calculate_binance_fee_usdt(
                        current_mark_price, _bn_qty_fee, is_taker=True)

                _fee_drag_usd = _open_fee_usd + _close_fee_usd
                combo_pnl_usd -= _fee_drag_usd
            except Exception:
                pass

            # 仍保留旧变量名供下方巡检播报兼容
            deribit_real_pnl_usd = combo_pnl_usd
            # 🌟 daily drawdown 采集: 暴露当前 combo PnL 给 _check_global_risk 统计峰值
            try:
                state._last_combo_pnl_usd = float(combo_pnl_usd)
            except Exception:
                pass

            # 使用 getattr 防呆，防止 Position 类没加 delta 属性导致报错
            # 注: f_pos.delta 是共享期货的【总Delta】，不能与单组合期权Delta混合计算
            # 日志中仅展示期权Delta作为参考，全局Delta风控由 _check_global_risk() 负责
            c_delta = getattr(c_pos, 'delta', Decimal('0'))
            p_delta = getattr(p_pos, 'delta', Decimal('0'))
            opt_delta = c_delta + p_delta  # 仅期权腿Delta（不含共享期货）

            # 护栏: 单组合绝对硬止损 (适配 0.1 BTC 最小交易量，默认 300 USD 止损)
            # 🌟 P1-18 修复: prices_confirmed=False 时禁止基于 PnL 的平仓决策
            #   未确认时 entry_prices 可能是预期/降级值, PnL 基准不可信
            #   正常路径: 等待到期结算, 或手动 /stop_all
            #   极端保护: 浮亏 ≥ 硬止损 × 2 时仍触发 emergency_dump (防完全失控)
            hard_stop_loss = getattr(self, 'hard_stop_loss_usd', Decimal('300.0'))
            _settlement_hard_stop_guard = self._is_settlement_hard_stop_guard_active()
            _twap_task = getattr(state, '_settlement_twap_task', None)
            _twap_winding_down = (_twap_task is not None and not _twap_task.done())
            _twap_res = getattr(state, '_settlement_twap_result', None)
            _twap_fully_closed = (
                _twap_res is not None
                and _twap_res.get('status') == 'FILLED'
                and 0 < hours_to_expiry <= _twap_window_hours
            )
            _twap_partial_residual = (
                _twap_res is not None
                and _twap_res.get('status') != 'FILLED'
                and state.binance_filled_qty > Decimal('0.0001')
            )
            if _settlement_hard_stop_guard or _twap_winding_down or _twap_fully_closed:
                # 🌟 2026-04-24 补充: 保护激活时清零硬止损候选计数，防止"跨窗口继承旧计数"
                # 否则保护结束时可能只需 1 次新 hit 就触发（原本需要 2 次）
                if int(getattr(state, '_hard_stop_consecutive', 0)) > 0:
                    state._hard_stop_consecutive = 0
                _last_guard_log = getattr(state, '_settlement_guard_skip_log_ts', 0.0)
                _now_guard = time.time()
                if _now_guard - _last_guard_log >= 60:
                    state._settlement_guard_skip_log_ts = _now_guard
                    _guard_reason = ("结算TWAP平仓中" if _twap_winding_down
                                     else "结算TWAP已全部成交,等待到期" if _twap_fully_closed
                                     else "结算窗口")
                    logger.info(
                        f"[{expiry}-{strike}] 🛡️ {_guard_reason}硬止损保护生效，暂不触发单组合硬止损/极端护栏，"
                        f"由到期结算路径接管。当前组合PnL={combo_pnl_usd:.2f} USD")
            else:
                if _twap_partial_residual:
                    _last_partial_log = getattr(state, '_twap_partial_warn_log_ts', 0.0)
                    if time.time() - _last_partial_log >= 60:
                        state._twap_partial_warn_log_ts = time.time()
                        logger.warning(
                            f"[{expiry}-{strike}] ⚠️ TWAP部分成交但残仓未清(qty={state.binance_filled_qty})，"
                            f"硬止损保持激活。PnL={combo_pnl_usd:.2f} USD")
                # 🌟 2026-04-24 修复: 硬止损/极端护栏需"连续 N 次确认"后才开火，防假警报强平
                #   背景: 2026-04-23 16:17:48 [24APR26-84000] 在结算后 17 分钟 (grace window 已过)
                #         从 -30.71 USD 瞬跳 -315.70 USD 误触发强平，实际仅损失 -52.90 USD
                #   根因: Deribit 结算后 option mark_price 抖动几秒即回归，但单次触发即开火
                #   修复: 正常行情要求连续 2 次 <= 阈值；行情降级时要求 3 次（更保守）
                #        PnL 恢复到阈值以上立刻清零计数器
                _required_confirmations = 3 if _risk_data_degraded else 2
                _is_confirmed_path = state.prices_confirmed and combo_pnl_usd <= -hard_stop_loss
                _is_unconfirmed_path = (not state.prices_confirmed) and combo_pnl_usd <= -hard_stop_loss * 2

                if _is_confirmed_path or _is_unconfirmed_path:
                    _hs_cnt = int(getattr(state, '_hard_stop_consecutive', 0)) + 1
                    state._hard_stop_consecutive = _hs_cnt
                    _guard_tag = "致命护栏" if _is_confirmed_path else "极端护栏"
                    _degrade_tag = " (行情降级)" if _risk_data_degraded else ""
                    if _hs_cnt < _required_confirmations:
                        logger.warning(
                            f"[{expiry}-{strike}] ⚠️ {_guard_tag}候选 "
                            f"({_hs_cnt}/{_required_confirmations}){_degrade_tag}: "
                            f"浮亏 {combo_pnl_usd:.2f} USD ≤ -{float(hard_stop_loss)*(2 if _is_unconfirmed_path else 1):.0f}, "
                            f"等待下次巡检确认")
                    else:
                        logger.error(
                            f"🚨🚨 [{_guard_tag}触发] {expiry}-{strike} "
                            f"单组合浮亏 {combo_pnl_usd:.2f} USD 连续 {_hs_cnt} 次击穿"
                            f"{'硬止损' if _is_confirmed_path else f'硬止损 2× ({float(hard_stop_loss)*2:.0f} USD)'}线！")
                        if _is_unconfirmed_path:
                            asyncio.create_task(tg_notifier.send_async(
                                f"🚨 {state.combo_id} 极端护栏触发\n"
                                f"未确认价仓位浮亏 {combo_pnl_usd:.2f} USD\n"
                                f"连续 {_hs_cnt} 次确认，已强制市价强平"))
                        state._hard_stop_consecutive = 0
                        await self._emergency_dump_all(state, combination, f_pos, c_pos, p_pos)
                        return
                else:
                    if int(getattr(state, '_hard_stop_consecutive', 0)) > 0:
                        logger.info(
                            f"[{expiry}-{strike}] ℹ️ 硬止损候选解除: "
                            f"PnL {combo_pnl_usd:.2f} USD 恢复到阈值以上")
                        state._hard_stop_consecutive = 0

            # 注: 单组合Delta检查已移除。原因：
            # f_pos.delta 是该期货合约的【总仓位Delta】(多个组合共享同一期货)，
            # 而 c_pos/p_pos 仅是本组合的期权Delta，导致 net_delta 计算值虚高无意义。
            # 合成期货本身天然Delta中性，全局风控由 _check_global_risk() 负责。

            # =========================================================================
            # 护栏 4: 🚨 Gamma 敞口超限强平 (防到期日或 IV 暴涨导致的非线性爆仓)
            # =========================================================================
            # 🌟 TWAP 审计修复: 结算窗口/TWAP 运行期间抑制 Gamma 护栏 (与硬止损逻辑一致)
            # 原因: 到期前 30 分钟 ATM 期权 Gamma 极大, 合成仓位 net_gamma 微小差异即可突破
            #       收紧后的 0.01 阈值, 误触发 _emergency_dump_all 会中断 TWAP 且多付 ~$15 费用.
            #       系统设计为"等到期结算", 此窗口内不应因 Gamma 强平.
            if _settlement_hard_stop_guard or _twap_winding_down or _twap_fully_closed:
                state.gamma_exceed_start = 0
                state.gamma_exceed_count = 0
            elif c_pos is None or p_pos is None:
                # 🌟 Plan Bug #3 + R3 修复: 持仓缺失时跳过 Gamma 检查 (无数据不触发), 硬止损不受影响
                state.gamma_exceed_start = 0
                state.gamma_exceed_count = 0
            else:
                c_gamma = getattr(c_pos, 'gamma', Decimal('0'))
                p_gamma = getattr(p_pos, 'gamma', Decimal('0'))
                net_gamma = c_gamma + p_gamma

                # 🌟 R3 修复: 改为"连续 3 次检测周期超标"计数, 替代原"时间戳 3s 窗口"
                # 原逻辑在 net_gamma 阈值附近抖动时 (跌出 → 归零, 跳回 → 重新计时) 永远不累积
                if abs(net_gamma) > max_gamma_limit:
                    _cur_cnt = getattr(state, 'gamma_exceed_count', 0) + 1
                    state.gamma_exceed_count = _cur_cnt
                    if getattr(state, 'gamma_exceed_start', 0) == 0:
                        state.gamma_exceed_start = time.time()
                    # 双重门禁: 连续 3 次检测周期 AND 超过 3 秒 (比 OR 严格, 不易被抖动绕过)
                    if _cur_cnt >= 3 and (time.time() - state.gamma_exceed_start) > 3.0:
                        logger.error(
                            f"🚨🚨 [致命护栏触发] {expiry}-{strike} 净 Gamma敞口 ({net_gamma:.4f}) "
                            f"连续 {_cur_cnt} 次超标! 发生极端非线性偏离, 断尾求生!")
                        await self._emergency_dump_all(state, combination, f_pos, c_pos, p_pos)
                        return
                else:
                    state.gamma_exceed_start = 0
                    state.gamma_exceed_count = 0

            # =========================================================================
            # 护栏 5: 🚨 永续合约持仓超时强制平仓 (止 funding 出血)
            # 永续 funding 年化 ~10%+，远超基差收益，持仓超过 max_perpetual_hold_hours 必须平仓
            # =========================================================================
            _perpetual_timeout = False
            if (state.binance_future_type == 'perpetual' and state.binance_filled_qty > 0
                    and self.max_perpetual_hold_hours > 0):
                _hold_seconds = time.time() - state.start_time if state.start_time > 0 else 0
                _hold_hours = _hold_seconds / 3600
                if _hold_hours >= self.max_perpetual_hold_hours:
                    _pnl_tag = f"当前PnL: {deribit_real_pnl_usd:.2f} USD"
                    logger.warning(
                        f"🚨 [{expiry}-{strike}] 永续合约持仓 {_hold_hours:.1f}h ≥ 上限 "
                        f"{self.max_perpetual_hold_hours}h, 强制平仓止 funding 出血! {_pnl_tag}")
                    asyncio.create_task(tg_notifier.send_async(
                        f"🚨 [{expiry}-{strike}] 永续持仓超时 {_hold_hours:.0f}h\n"
                        f"强制平仓止 funding 出血\n{_pnl_tag}"))
                    state.exit_reason = f'永续超时({_hold_hours:.0f}h)'
                    if not state.prices_confirmed:
                        await self._emergency_dump_all(state, combination, f_pos, c_pos, p_pos)
                        return
                    _perpetual_timeout = True  # 标记: 跳过所有利润门槛，直接进入平仓执行

            # =========================================================================
            # 等待到期结算 (不主动平仓)
            # 理由: 主动平仓费用($7.5) + 期权滑点(~$15) = $22.5，吞噬大部分利润
            #       到期结算费$5.25 + 零滑点，净利高出 ~$17/笔
            # 上方硬止损/Gamma/永续超时已处理紧急情况，此处正常仓位一律等结算
            # =========================================================================
            if not _perpetual_timeout:
                current_time = time.time()
                last_log = self._last_pnl_log_time.get(state.expiry_strike, 0)
                if current_time - last_log > 60:
                    days_left = hours_to_expiry / 24
                    _bcast_twap_task = getattr(state, '_settlement_twap_task', None)
                    _twap_in_progress = (_bcast_twap_task is not None and not _bcast_twap_task.done())
                    _twap_done_waiting = (not _twap_in_progress
                                         and getattr(state, '_settlement_twap_result', None) is not None)
                    _hedge_tag = (" | 🕐 TWAP平仓中(PnL仅含剩余仓位)" if _twap_in_progress
                                  else " | ✅ TWAP已完成,等结算" if _twap_done_waiting
                                  else " | 对冲=Binance" if state.binance_filled_qty > 0
                                  else "")
                    status_icon = "⏳ 收敛中" if deribit_real_pnl_usd > 0 else "❄️ 浮亏/抗单中"
                    # 🌟 Plan Bug #3 修复: 期权数据降级时明确标记, 方便排查
                    _degraded_tag = " | ⚠️ 期权数据降级(entry 估值)" if locals().get('_option_data_degraded', False) else ""
                    logger.info(
                        f"[{expiry.zfill(7)}-{strike}] 🛡️ 持仓巡检: {status_icon} | "
                        f"官方净利 {deribit_real_pnl_usd:.2f} USD | "
                        f"净 Delta(期权): {opt_delta:.4f} | "
                        f"剩余 {days_left:.1f} 天 | 📦 等待到期结算{_hedge_tag}{_degraded_tag}")
                    self._last_pnl_log_time[state.expiry_strike] = current_time
                return

            # =========================================================================
            # 永续超时安全网：DTE≤72h 的期权应该已到期，_handle_delivery_settlement 应已处理
            # 走到这里说明到期检测失败，直接关闭 Binance 对冲
            # =========================================================================
            if self._is_deribit_settlement_core_window():
                logger.warning(
                    f"[{expiry}-{strike}] ⏸️ Deribit core settlement window active，"
                    f"永续超时安全网延后到窗口结束后执行")
                return

            logger.warning(
                f"[{expiry}-{strike}] 🚨 永续超时安全网触发! "
                f"期权应已到期但未被结算检测捕获，直接关闭 Binance 对冲")
            asyncio.create_task(tg_notifier.send_error_async(
                f"🚨 [{expiry}-{strike}] 永续超时安全网\n期权可能已到期但结算检测失败\n正在关闭 Binance 对冲",
                "perpetual_timeout_safety"))
            if state.binance_filled_qty > 0:
                _bn_close_result = await self._close_binance_hedge(state, emergency=True)
                # 🌟 P2 回归修复: 永续超时安全网成功关 Binance 也要保存 close IDs
                if _bn_close_result:
                    self._capture_binance_close_ids(state, _bn_close_result)
                # 🌟 P1-A 修复: 使用与 _close_binance_hedge 一致的 dust 阈值 0.0001
                # 旧逻辑用 > Decimal('0') 导致残余量在 (0, 0.0001] 时误判为平仓失败
                if not _bn_close_result or state.binance_filled_qty > Decimal('0.0001'):
                    logger.error(
                        f"[{expiry}-{strike}] 永续超时安全网: Binance 平仓未完成，"
                        f"剩余数量={state.binance_filled_qty}，保持 position_open 继续重试")
                    self._add_pause("Binance残余仓位")
                    state.state = 'position_open'
                    state.last_update = time.time()
                    await self._save_state_to_redis(state)
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 [{expiry}-{strike}] 永续超时安全网平仓失败\n"
                        f"Binance 残余: {state.binance_future_symbol} qty={state.binance_filled_qty}\n"
                        f"状态已保留并将持续重试，请人工关注。", "perpetual_timeout_close_failed"))
                    return
            state.state = 'exited'
            state.exit_reason = f'永续超时安全网'
            state.last_update = time.time()
            self.position_locks.discard(state.expiry_strike)
            self._bn_mark_missing_since.pop(state.expiry_strike, None)
            self._bn_mark_degraded_log_ts.pop(state.expiry_strike, None)
            self._broken_combo_first_seen.pop(state.expiry_strike, None)
            self._broken_combo_handling.discard(state.expiry_strike)
            self._combo_closing_locks.pop(state.expiry_strike, None)
            await self._delete_state_from_redis(expiry, strike)

        except Exception as e:
            logger.error(f"检查退出机会异常: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"检查退出机会异常: {e}", "exit_check_error"))
            import traceback
            traceback.print_exc()
