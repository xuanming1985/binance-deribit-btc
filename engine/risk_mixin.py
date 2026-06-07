"""engine/risk_mixin.py — 全局风控 + Binance 分腿对账 + 裸腿检测"""
from __future__ import annotations
import logging
import time
import asyncio
import aiohttp
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set
from collections import defaultdict

if TYPE_CHECKING:
    pass

from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


class RiskMixin:
    """Mixin: 全局风控 + Binance 分腿对账 + 裸腿检测 + Gamma 监控"""

    def _is_settlement_hard_stop_guard_active(self) -> bool:
        """结算窗口全局硬止损保护阀是否生效"""
        try:
            if not bool(getattr(self, 'settlement_hard_stop_guard', True)):
                return False

            # 仅在存在 Deribit 期权持仓时启用保护，避免掩盖纯 Binance 风险
            has_deribit_option_position = False
            for _inst, _pos in self.client.positions.items():
                if getattr(_pos, 'size', Decimal('0')) == 0:
                    continue
                if isinstance(_inst, str) and (_inst.endswith('-C') or _inst.endswith('-P')):
                    has_deribit_option_position = True
                    break
            if not has_deribit_option_position:
                return False

            return bool(
                self._is_settlement_risk_grace_window() or
                getattr(self, '_settlement_paused', False) or
                self._has_pause("结算窗口")
            )
        except Exception:
            return False

    def _reset_gamma_guard_counters(self, reason: str = "") -> None:
        """重置活跃组合 Gamma 超限计数器，防止断连恢复后沿用陈旧计数误触发。"""
        _reset_cnt = 0
        for _st in self.arbitrage_states.values():
            if getattr(_st, 'state', '') not in ('position_open', 'executing', 'exiting'):
                continue
            if getattr(_st, 'gamma_exceed_start', 0) != 0 or getattr(_st, 'gamma_exceed_count', 0) != 0:
                _st.gamma_exceed_start = 0
                _st.gamma_exceed_count = 0
                _reset_cnt += 1
        if _reset_cnt > 0:
            _tag = f"({reason})" if reason else ""
            logger.info(f"🧹 Gamma 计数器已重置 {_reset_cnt} 个活跃组合 {_tag}")

    async def _check_global_risk(self):
        """🌍 全局组合风险护栏：双层 Delta 阈值 + 裸腿诊断 + 自动减损"""
        try:
            # 日损跨日重置: 只依赖本地日期，不需要交易所连接，必须在断连保护之前运行
            # 否则断连跨越 UTC 午夜时，昨日的"日损熔断"暂停会错误延续到新的一天
            _daily_limit = float(getattr(self, 'daily_loss_limit_usd', 0))
            if _daily_limit > 0:
                from datetime import datetime, timezone
                _today_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                if self._daily_loss_date != _today_utc:
                    self._daily_loss_date = _today_utc
                    self._daily_realized_pnl = 0.0
                    self._daily_loss_triggered = False
                    self._remove_pause("日损熔断")
                    logger.info(f"📅 [日损追踪] 新交易日 {_today_utc}, 累计净盈亏已重置")

            # 与 Delta 熔断解耦的 Binance 分腿对账巡检（Hedge Mode 下防净额抵消掩盖异常）
            # 故意放在断连保护之前: 此巡检本身只查询/记录/告警, 不执行平仓动作,
            # 即使 Deribit 断连也可正常巡检 Binance 分腿一致性
            # (函数内有 self.binance_connected 守卫, Binance 断连时自动 no-op)
            _now = time.time()
            if _now - self._bn_side_integrity_last_check >= self._bn_side_integrity_interval:
                self._bn_side_integrity_last_check = _now
                await self._check_binance_side_integrity()

            # 🌟 断连保护阀: 任一交易所断连时，暂停所有风控动作（Delta熔断/硬止损/裸腿平仓）
            # 原因: 对冲仓位断连本身不产生风险，但用残缺数据做决策会误杀
            # 已保留: Binance 分腿对账巡检（在本 guard 之前运行）
            _deribit_down = not self.client.is_connected
            _binance_down = (self.binance_ws is not None and not self.binance_connected)
            if _deribit_down or _binance_down:
                _down_src = []
                if _deribit_down:
                    _down_src.append("Deribit")
                if _binance_down:
                    _down_src.append("Binance")
                _down_str = "+".join(_down_src)
                _last_dc_log = getattr(self, '_global_risk_disconnect_log_ts', 0.0)
                _log_gap = max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0)
                if time.time() - _last_dc_log >= _log_gap:
                    logger.warning(
                        f"🛡️ [断连保护] {_down_str} 未连接，暂停全局风控动作 "
                        f"(Delta熔断/硬止损/裸腿平仓)，等待恢复后重新评估")
                    self._global_risk_disconnect_log_ts = time.time()
                return  # 跳过 Delta/硬止损/裸腿计算（日损跨日重置 + 分腿对账巡检已在上方完成）

            # 统计正在执行中的组合数 (Maker 已成交但 Taker 尚未发射)
            executing_count = sum(
                1 for s in self.arbitrage_states.values() if s.state == 'executing'
            )

            total_delta = Decimal('0')
            total_unrealized_pnl_btc = Decimal('0')
            binance_unrealized_pnl_usd = Decimal('0')

            # 遍历所有已知持仓，累加全局敞口
            for inst, pos in self.client.positions.items():
                if pos.size != 0:
                    total_delta += getattr(pos, 'delta', Decimal('0'))
                    total_unrealized_pnl_btc += pos.unrealized_pnl

            # ===== 跨所: Binance 期货 delta =====
            binance_delta = Decimal('0')
            _bn_delta_source = "none"
            if self.binance_ws and self.binance_connected:
                # WS 已连接: 用实时持仓数据 (精确)
                _bn_delta_source = "ws_live"
                # Hedge 模式下必须按分腿汇总 unrealized_pnl，避免净仓抵消导致 PnL 被低估/漏算
                if self.binance_dual_side_mode and getattr(self.binance_ws, "positions_by_side", None):
                    for (_bn_sym, _ps), _bn_pos in self.binance_ws.positions_by_side.items():
                        if _bn_pos.quantity <= 0:
                            continue
                        _ps_u = str(_ps).upper()
                        if _ps_u == "LONG":
                            binance_delta += _bn_pos.quantity
                        elif _ps_u == "SHORT":
                            binance_delta -= _bn_pos.quantity
                        else:
                            _side_u = str(getattr(_bn_pos, 'side', '')).upper()
                            if _side_u == "LONG":
                                binance_delta += _bn_pos.quantity
                            elif _side_u == "SHORT":
                                binance_delta -= _bn_pos.quantity
                        # Binance 未实现盈亏已是 USDT 口径，可直接并入全局硬止损
                        binance_unrealized_pnl_usd += Decimal(str(getattr(_bn_pos, 'unrealized_pnl', 0)))
                else:
                    for _bn_sym, _bn_pos in self.binance_ws.positions.items():
                        if _bn_pos.quantity > 0:
                            if _bn_pos.side == "LONG":
                                binance_delta += _bn_pos.quantity
                            else:
                                binance_delta -= _bn_pos.quantity
                            # Binance 未实现盈亏已是 USDT 口径，可直接并入全局硬止损
                            binance_unrealized_pnl_usd += Decimal(str(getattr(_bn_pos, 'unrealized_pnl', 0)))

            # 🌟 修复: Binance 断连时从 arbitrage_states 估算 Delta 和 PnL
            # 防止重连间隙中只看到 Deribit 一半的对冲仓位 → 误判裸腿 + 误触硬止损
            if binance_delta == 0 and not self.binance_connected:
                _fallback_delta = Decimal('0')
                _fallback_pnl = Decimal('0')
                _fallback_count = 0
                for _st in self.arbitrage_states.values():
                    if _st.state not in ('position_open', 'executing', 'exiting'):
                        continue
                    if not _st.binance_future_symbol or _st.binance_filled_qty <= 0:
                        continue
                    # Delta 估算: LONG → +qty, SHORT → -qty
                    if _st.strategy_type == 'buy_future_sell_synthetic':
                        _fallback_delta += _st.binance_filled_qty
                    elif _st.strategy_type == 'sell_future_buy_synthetic':
                        _fallback_delta -= _st.binance_filled_qty
                    # PnL 估算: 无法精确计算 (不知道当前价), 用 0 代替
                    # 关键: 不产生虚假亏损, 比"只算 Deribit"安全得多
                    _fallback_count += 1
                if _fallback_count > 0:
                    binance_delta = _fallback_delta
                    _bn_delta_source = "state_fallback"
                    # PnL 标记为不精确, 跳过全局硬止损
                    _last_fb_log = getattr(self, '_bn_fallback_log_ts', 0.0)
                    if time.time() - _last_fb_log >= 30:
                        logger.warning(
                            f"⚠️ [全局风控] Binance 未连接, 从 arbitrage_states 估算 delta={_fallback_delta:.4f} "
                            f"({_fallback_count} 个组合), PnL 暂不计入硬止损")
                        self._bn_fallback_log_ts = time.time()
            # TWAP 补偿: 结算窗口内 TWAP 主动减仓导致 Binance delta 减少是预期行为，
            # 需要补偿回来避免全局 Delta 熔断误触发
            _twap_delta_compensation = Decimal('0')
            for _st in self.arbitrage_states.values():
                if _st.state not in ('position_open',):
                    continue
                if not getattr(_st, '_settlement_twap_started', False):
                    continue
                # 时效+持仓约束: combo 已清理或期权已归零时不补偿
                _st_combo = self.arbitrage_combinations.get(_st.expiry_strike)
                if not _st_combo:
                    continue
                _c_pos = self.client.positions.get(_st_combo.get('call', ''))
                _p_pos = self.client.positions.get(_st_combo.get('put', ''))
                _has_options = ((_c_pos and _c_pos.size != 0) or (_p_pos and _p_pos.size != 0))
                if not _has_options:
                    continue
                # 用 TWAP 实际成交量（而非 snap-remaining 推算），防止非 TWAP 原因的仓位变动被误补偿
                _twap_acc = getattr(_st, '_settlement_twap_accumulated', None)
                _twap_res = getattr(_st, '_settlement_twap_result', None)
                _closed_by_twap = Decimal('0')
                if isinstance(_twap_acc, dict):
                    try:
                        _closed_by_twap = Decimal(str(_twap_acc.get('filled', 0)))
                        if not _closed_by_twap.is_finite():
                            _closed_by_twap = Decimal('0')
                    except (ValueError, TypeError, ArithmeticError):
                        _closed_by_twap = Decimal('0')
                elif _twap_res and _twap_res.get('executedQty'):
                    try:
                        _closed_by_twap = Decimal(str(_twap_res['executedQty']))
                        if not _closed_by_twap.is_finite():
                            _closed_by_twap = Decimal('0')
                    except (ValueError, TypeError, ArithmeticError):
                        _closed_by_twap = Decimal('0')
                if _closed_by_twap > 0:
                    if _st.strategy_type == 'buy_future_sell_synthetic':
                        _twap_delta_compensation += _closed_by_twap
                    elif _st.strategy_type == 'sell_future_buy_synthetic':
                        _twap_delta_compensation -= _closed_by_twap

            # 合并双端 delta
            if binance_delta != 0:
                logger.debug(f"Binance delta: {binance_delta}, Deribit delta: {total_delta}")
                total_delta = total_delta + binance_delta + _twap_delta_compensation
                if _twap_delta_compensation != 0:
                    logger.info(f"TWAP delta 补偿: {_twap_delta_compensation}")
            else:
                total_delta = total_delta + _twap_delta_compensation
            # Deribit 未实现盈亏为 BTC 口径，换算 USD 时必须使用可靠 BTC/USD 参考价
            btc_usd_ref = await self._get_reference_btc_price()
            if btc_usd_ref > 0:
                deribit_unrealized_pnl_usd = total_unrealized_pnl_btc * btc_usd_ref
            else:
                deribit_unrealized_pnl_usd = Decimal('0')
                _last_warn = getattr(self, '_risk_ref_price_warn_ts', 0.0)
                if time.time() - _last_warn > 60:
                    logger.warning("⚠️ 全局风控: 无法获取 BTC/USD 参考价，Deribit 未实现PnL暂按0处理（避免误触发硬止损）")
                    self._risk_ref_price_warn_ts = time.time()
            total_unrealized_pnl_usd = deribit_unrealized_pnl_usd + binance_unrealized_pnl_usd

            # 🌟 采集每日最大浮亏, 供 Monitor 柱状图对比止损阈值设置是否合理
            try:
                self._update_daily_drawdown(float(total_unrealized_pnl_usd))
            except Exception as _dd_err:
                logger.info(f"daily drawdown 更新异常 (非致命): {_dd_err}")

            # ===================== 双层 Delta 阈值 =====================
            # 第一层 (警告)：Gamma 漂移监控，不暂停
            warn_limit = getattr(self, 'global_max_delta', Decimal('0.15'))
            # 第二层 (熔断)：裸腿/严重风险，暂停 + 自动减损
            hard_base = getattr(self, 'global_hard_delta', Decimal('0.50'))
            # 🌟 修复 #2: 限制 executing_count 的膨胀效应，最大不超过 hard_base 的 2 倍
            # 原来: 6个并发时 hard_limit = 0.5 + 3.0 = 3.5，几乎禁用了熔断
            # 修复: 改用小系数 + 硬顶，6个并发时 = 0.5 + 0.3 = 0.8，仍有保护效果
            hard_limit = min(hard_base + Decimal('0.05') * executing_count, hard_base * Decimal('2'))

            abs_delta = abs(total_delta)

            # ---------- 熔断层自动恢复 ----------
            hard_recovery = hard_limit * Decimal('0.8')  # 迟滞：熔断阈值的 80%
            is_hard_paused = self._has_pause("裸腿Delta熔断")
            if is_hard_paused and abs_delta <= hard_recovery:
                self._remove_pause("裸腿Delta熔断")
                logger.info(f"✅ 全局 Delta 已回落至 {total_delta:.4f}（< 熔断恢复阈值 {hard_recovery:.2f}），Delta熔断已解除"
                            f"{'，但其他暂停原因仍有效' if self.trading_paused else '，系统自动恢复交易'}")
                await tg_notifier.send_error_async(
                    f"✅ Delta 回落至 {total_delta:.4f}，系统已自动恢复", "global_risk_recovery")
                self._delta_warn_logged = False

            _settlement_risk_guard = self._is_settlement_hard_stop_guard_active()

            # ---------- 第二层：熔断（裸腿级别风险）----------
            if abs_delta > hard_limit:
                if _settlement_risk_guard:
                    _now_sg = time.time()
                    if _now_sg - getattr(self, '_delta_settlement_guard_log_ts', 0.0) >= max(
                            float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0):
                        self._delta_settlement_guard_log_ts = _now_sg
                        logger.warning(
                            f"⏸️ [Delta熔断] 结算风险保护中，暂缓自动减损 "
                            f"(Delta {total_delta:.4f} > 熔断 {hard_limit:.2f})")
                elif not is_hard_paused:
                    # 🔍 诊断：找出具体哪些是裸腿
                    naked_legs = self._identify_naked_legs()
                    naked_detail = "\n".join(naked_legs) if naked_legs else "未能定位具体裸腿，请人工检查"

                    logger.error(
                        f"🚨🚨【裸腿风险】账户总 Delta {total_delta:.4f} 超熔断阈值 {hard_limit:.2f}！\n"
                        f"  诊断结果:\n  " + "\n  ".join(naked_legs) if naked_legs else naked_detail
                    )
                    self._add_pause("裸腿Delta熔断")
                    self._delta_alert_last_log = time.time()

                    tg_msg = (f"🚨 裸腿风险！Delta {total_delta:.4f}（熔断 {hard_limit:.2f}）\n"
                              f"📋 诊断:\n{naked_detail}\n"
                              f"⚡ 正在自动平仓裸腿...")
                    await tg_notifier.send_error_async(tg_msg, "naked_leg_risk")

                    # ⚡ 自动减损：平掉未跟踪的裸腿仓位
                    await self._auto_close_naked_legs()
                else:
                    # 已熔断：按配置节流提醒（默认每 5 分钟）
                    now = time.time()
                    _risk_alert_gap = max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0)
                    if now - getattr(self, '_delta_alert_last_log', 0) >= _risk_alert_gap:
                        logger.warning(f"⏸️ 裸腿熔断持续中: Delta {total_delta:.4f}（熔断 {hard_limit:.2f}）")
                        self._delta_alert_last_log = now

            # ---------- 第一层：警告（Gamma 漂移，不暂停）----------
            elif abs_delta > warn_limit:
                if not getattr(self, '_delta_warn_logged', False):
                    logger.warning(f"⚠️ [Gamma漂移] 账户总 Delta {total_delta:.4f} 超监控阈值 {warn_limit}，属正常市场波动，继续交易")
                    self._delta_warn_logged = True
                    self._delta_warn_time = time.time()
                elif time.time() - getattr(self, '_delta_warn_time', 0) >= max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0):
                    # 按配置节流提醒（默认每 5 分钟）
                    logger.warning(f"⚠️ [Gamma漂移] Delta 持续偏高: {total_delta:.4f}（阈值 {warn_limit}）")
                    self._delta_warn_time = time.time()
            else:
                # Delta 正常，清除警告标记
                if getattr(self, '_delta_warn_logged', False):
                    self._delta_warn_logged = False

            # ===================== 🌟 已移除: 全局硬止损 (global_hard_stop_loss) =====================
            # 策略本身是 delta-neutral 合成期货套利, 价格 spike 天然被对冲,
            # 账户级瞬时浮亏信号质量差 (basis 暂时扩大 ≠ 真实风险).
            # 风控责任转交:
            #   - 单点异常: hard_stop_loss_usd (per combo) + Gamma/Delta 监控
            #   - 日累计 :  daily_loss_limit_usd (日内连亏熔断)
            # 见 _update_daily_drawdown docstring 的两层保护说明.
            # ===================== 🌟 P0-2.2 + P1-5: 日损 kill-switch =====================
            # 今日净亏损 = -(已实现净盈亏 + 当前浮盈亏), 超过阈值则熔断
            # 盈利自动抵扣亏损, 避免 "5亏+1大盈" 被误熔断
            # _daily_limit 和跨日重置已在函数开头处理（断连保护之前）
            if _daily_limit > 0:
                # 今日净盈亏 = 已实现净盈亏 + 当前总浮盈亏 (两者都带符号)
                _today_net_pnl = self._daily_realized_pnl + float(total_unrealized_pnl_usd)
                _today_net_loss = -_today_net_pnl  # 正值表示净亏损

                if _today_net_loss >= _daily_limit and not self._daily_loss_triggered:
                    # 结算窗口保护：core window + grace 内暂缓触发，防止 TWAP/mark 抖动误触发
                    if self._is_settlement_hard_stop_guard_active():
                        _now_dl = time.time()
                        if _now_dl - getattr(self, '_daily_loss_settle_guard_log_ts', 0.0) >= 60:
                            self._daily_loss_settle_guard_log_ts = _now_dl
                            logger.warning(
                                f"⏸️ [日损追踪] 结算窗口保护中, 暂缓触发日损熔断 "
                                f"(净亏损 ${_today_net_loss:.2f} ≥ 阈值 ${_daily_limit:.2f})")
                    else:
                        self._daily_loss_triggered = True
                        self._add_pause("日损熔断")
                        try:
                            await asyncio.wait_for(self._save_daily_pnl_to_redis(), timeout=2.0)
                        except (asyncio.TimeoutError, Exception):
                            logger.warning("⚠️ 日损熔断 Redis 持久化超时/失败，状态已在内存生效")
                        logger.error(
                            f"🚨🚨 【日损熔断】今日净亏损 ${_today_net_loss:.2f} ≥ 阈值 ${_daily_limit:.2f}! "
                            f"(已实现 ${self._daily_realized_pnl:+.2f} + 浮盈亏 ${float(total_unrealized_pnl_usd):+.2f})")
                        if self.daily_loss_auto_close:
                            asyncio.create_task(tg_notifier.send_error_async(
                                f"🚨 日损熔断触发! 净亏损 ${_today_net_loss:.2f}\n"
                                f"阈值 ${_daily_limit:.2f} | 自动清仓中...", "daily_loss_limit"))
                            await self.emergency_liquidate_all(full_stop=False)
                        else:
                            asyncio.create_task(tg_notifier.send_error_async(
                                f"🚨 日损熔断触发! 净亏损 ${_today_net_loss:.2f}\n"
                                f"阈值 ${_daily_limit:.2f}\n"
                                f"⚠️ 已暂停交易, 到明日 UTC 00:00 自动重置\n"
                                f"或手动 /stop_all 清仓", "daily_loss_limit"))
                elif self._daily_loss_triggered and _today_net_loss < _daily_limit * 0.5:
                    self._daily_loss_triggered = False
                    self._remove_pause("日损熔断")
                    try:
                        await asyncio.wait_for(self._save_daily_pnl_to_redis(), timeout=2.0)
                    except (asyncio.TimeoutError, Exception):
                        logger.warning("⚠️ 日损解除 Redis 持久化超时/失败，状态已在内存生效")
                    logger.info(f"✅ [日损追踪] 净亏损已回落至 ${_today_net_loss:.2f} < 50% 阈值, 自动解除熔断")

        except Exception as e:
            logger.error(f"全局风控检查异常: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"全局风控检查异常: {e}", "global_risk_error"))

    def _build_tracked_binance_signed(self) -> Dict[str, Decimal]:
        """统计状态机跟踪到的 Binance 净持仓（按 symbol，带方向）"""
        tracked = defaultdict(lambda: Decimal('0'))
        for _es, _st in self.arbitrage_states.items():
            if _st.state not in ('position_open', 'executing', 'exiting'):
                continue
            if not _st.binance_future_symbol or _st.binance_filled_qty <= 0:
                continue
            if _st.strategy_type == 'buy_future_sell_synthetic':
                tracked[_st.binance_future_symbol] += _st.binance_filled_qty   # LONG
            elif _st.strategy_type == 'sell_future_buy_synthetic':
                tracked[_st.binance_future_symbol] -= _st.binance_filled_qty   # SHORT
        return dict(tracked)

    def _build_tracked_binance_by_side(self) -> Dict[Tuple[str, str], Decimal]:
        """统计状态机跟踪到的 Binance 分腿持仓（symbol + positionSide）"""
        tracked = defaultdict(lambda: Decimal('0'))
        for _es, _st in self.arbitrage_states.items():
            if _st.state not in ('position_open', 'executing', 'exiting'):
                continue
            if not _st.binance_future_symbol or _st.binance_filled_qty <= 0:
                continue
            _ps = (_st.binance_position_side or "").upper()
            if _ps not in ("LONG", "SHORT"):
                _ps = "LONG" if _st.strategy_type == 'buy_future_sell_synthetic' else "SHORT"
            tracked[(_st.binance_future_symbol, _ps)] += _st.binance_filled_qty
        return dict(tracked)

    async def _check_binance_side_integrity(self):
        """Hedge Mode 分腿仓位对账巡检（与 Delta 熔断解耦）"""
        try:
            if not self.binance_ws or not self.binance_connected:
                return
            if not self.binance_dual_side_mode:
                return
            if not getattr(self.binance_ws, "positions_by_side", None):
                return
            # 先执行组合级护栏，避免"单组合数量膨胀"直接污染分腿对账汇总
            await self._sanitize_combo_binance_qty("side_integrity")

            tracked_side = self._build_tracked_binance_by_side()
            actual_side: Dict[Tuple[str, str], Decimal] = {}
            for (bn_sym, ps), bn_pos in self.binance_ws.positions_by_side.items():
                if bn_pos.quantity > 0:
                    actual_side[(bn_sym, ps)] = bn_pos.quantity

            tolerance = self._bn_side_integrity_tolerance
            mismatches = []
            has_untracked_actual = False
            has_missing_tracked = False
            for _k in set(actual_side.keys()) | set(tracked_side.keys()):
                _actual_qty = actual_side.get(_k, Decimal('0'))
                _tracked_qty = tracked_side.get(_k, Decimal('0'))
                _diff = _actual_qty - _tracked_qty
                if abs(_diff) > tolerance:
                    _sym, _ps = _k
                    mismatches.append((_sym, _ps, _actual_qty, _tracked_qty, _diff))
                    if _actual_qty > _tracked_qty:
                        has_untracked_actual = True
                    elif _tracked_qty > _actual_qty:
                        has_missing_tracked = True

            if not mismatches:
                return

            _head = mismatches[:6]
            _detail = "\n".join(
                f"{_sym} {_ps}: 实际={_a} | 状态机={_t} | 差额={_d:+f}"
                for _sym, _ps, _a, _t, _d in _head
            )
            if len(mismatches) > 6:
                _detail += f"\n... 其余 {len(mismatches) - 6} 条已省略"

            logger.error(
                f"🚨 [Binance分腿对账] 检测到 {len(mismatches)} 条差额（容差>{tolerance}）\n{_detail}"
            )

            _now = time.time()
            _integrity_alert_gap = max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0)
            if _now - self._bn_side_integrity_alert_ts >= _integrity_alert_gap:
                self._bn_side_integrity_alert_ts = _now
                if has_untracked_actual and has_missing_tracked:
                    _action_hint = "系统将执行双向处置：未跟踪实仓自动减损 + 状态机分组自愈。"
                elif has_untracked_actual:
                    _action_hint = "系统将对未跟踪实仓执行自动减损。"
                else:
                    _action_hint = "系统将执行状态机分组自愈，并持续监控是否存在对冲缺失。"
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 Binance 分腿对账异常（Hedge）\n"
                    f"共 {len(mismatches)} 条差额:\n{_detail}\n"
                    f"{_action_hint}", "binance_side_mismatch"))

            # 仅当"交易所实仓 > 状态机跟踪"时，执行自动减损；避免对账方向相反时误操作
            if has_untracked_actual:
                # 🛡️ 修复 A: 残余仓位 pause 期间禁止自动减损（人工介入优先，防误平真实对冲）
                # 配合修复 B（降级重建主动 pause），让"启动期状态不一致"走 pause 路径而非自动平仓
                if self._has_pause("Binance残余仓位"):
                    _now_a = time.time()
                    if _now_a - getattr(self, '_path2_pause_block_log_ts', 0.0) >= 60:
                        logger.warning("⏸️ [Binance分腿对账] 残余仓位 pause 期间，自动减损已屏蔽，等待人工/自动恢复")
                        self._path2_pause_block_log_ts = _now_a
                else:
                    # 🛡️ 修复 D: 自写 + 复用 _bn_ghost_first_seen 的 20s 宽限期
                    # 与 _ghost_and_integrity_check (path①) 共享首见时间字典，互为兜底。
                    # 仅对"实仓 > 跟踪"方向(_diff > 0)生效；自愈方向放行（不下单不动钱）。
                    _now_d = time.time()
                    _grace_seconds = 20.0
                    _grace_violated = False
                    for _sym_d, _ps_d, _a_d, _t_d, _diff_d in mismatches:
                        if _diff_d <= 0:
                            continue
                        _k_d = f"{_sym_d}:{_ps_d}"
                        if _k_d not in self._bn_ghost_first_seen:
                            self._bn_ghost_first_seen[_k_d] = _now_d
                            _grace_violated = True
                        elif (_now_d - self._bn_ghost_first_seen[_k_d]) < _grace_seconds:
                            _grace_violated = True
                    if _grace_violated:
                        if _now_d - getattr(self, '_path2_grace_block_log_ts', 0.0) >= 30:
                            logger.info(
                                f"[Binance分腿对账] {int(_grace_seconds)}s 宽限期未到，本次仅记录差额，不执行减损")
                            self._path2_grace_block_log_ts = _now_d
                    else:
                        await self._auto_close_naked_legs()

            # 若"状态机跟踪 > 交易所实仓"，优先做分组自愈：
            # 同 symbol+side 的多个组合共享同一 Binance 分腿时，实仓是总量，不能逐组合直接对齐总仓。
            # 这里按 entry_amount 权重把"总仓位"回填到各组合，修复误写导致的膨胀（例如 0.6 被写成 6×0.6=3.6）。
            if has_missing_tracked:
                _groups = defaultdict(list)  # (symbol, positionSide) -> [(expiry_strike, state)]
                for _es, _st in list(self.arbitrage_states.items()):
                    if _st.state not in ('position_open', 'executing', 'exiting'):
                        continue
                    if not _st.binance_future_symbol:
                        continue
                    _ps = (_st.binance_position_side or "").upper()
                    if _ps not in ("LONG", "SHORT"):
                        _ps = "LONG" if _st.strategy_type == 'buy_future_sell_synthetic' else "SHORT"
                    _groups[(_st.binance_future_symbol, _ps)].append((_es, _st))

                _healed_groups = 0
                for (_sym, _ps), _items in _groups.items():
                    _actual_qty = actual_side.get((_sym, _ps), Decimal('0'))
                    _tracked_total = sum((it[1].binance_filled_qty for it in _items), Decimal('0'))
                    _excess = _tracked_total - _actual_qty
                    if _excess <= tolerance:
                        continue

                    # TWAP 平仓中的组合跳过自愈：TWAP 在 event loop yield 期间
                    # 交易所仓位可能已更新但 state.binance_filled_qty 尚未递减，
                    # 此时自愈会错误覆写，导致后续 TWAP 递减时双重扣减
                    # 仅跳过 task 仍在运行的组合，已完成/已失败的不影响其他组合
                    _items = [
                        (es, st) for es, st in _items
                        if not (getattr(st, '_settlement_twap_task', None) is not None
                                and not getattr(st, '_settlement_twap_task').done())
                    ]
                    if not _items:
                        continue
                    # 重算过滤后的 tracked vs actual
                    _tracked_total = sum((it[1].binance_filled_qty for it in _items), Decimal('0'))
                    _excess = _tracked_total - _actual_qty
                    if _excess <= tolerance:
                        continue

                    # 单组合可直接对齐
                    if len(_items) == 1:
                        _es, _st = _items[0]
                        if abs(_st.binance_filled_qty - _actual_qty) > tolerance:
                            logger.warning(
                                f"[Binance分腿对账-自愈] {_sym} {_ps}: 单组合修正 "
                                f"{_st.binance_filled_qty} -> {_actual_qty}")
                            _st.binance_filled_qty = max(_actual_qty, Decimal('0'))
                            _st.last_update = time.time()
                            await self._save_state_to_redis(_st)
                            _healed_groups += 1
                        continue

                    # 多组合共享分腿：按 entry_amount 权重重标定到"总实仓"
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
                        if abs(_st.binance_filled_qty - _new_qty) > tolerance:
                            _st.binance_filled_qty = _new_qty
                            _st.last_update = time.time()
                            await self._save_state_to_redis(_st)
                            _changed += 1
                    if _changed > 0:
                        _healed_groups += 1
                        logger.warning(
                            f"[Binance分腿对账-自愈] {_sym} {_ps}: 多组合重标定 "
                            f"tracked_total={_tracked_total} -> actual_total={_actual_qty} | 组合数={len(_items)}")

                if _healed_groups > 0:
                    # 自愈后等待下一轮再告警，避免同一轮继续使用旧快照重复报警
                    return
        except Exception as e:
            logger.error(f"Binance 分腿对账巡检异常: {e}")

    def _identify_naked_legs(self) -> List[str]:
        """🔍 诊断裸腿：找出交易所有仓但状态机未跟踪的仓位（含 Binance）"""
        tracked_instruments = set()
        for es, st in self.arbitrage_states.items():
            if st.state in ('position_open', 'executing', 'exiting'):
                combo = self.arbitrage_combinations.get(es)
                if combo:
                    tracked_instruments.add(combo['future'])
                    tracked_instruments.add(combo['call'])
                    tracked_instruments.add(combo['put'])

        tracked_binance_signed = self._build_tracked_binance_signed()

        naked_legs = []
        # ===== Deribit 仓位诊断 =====
        for inst, pos in self.client.positions.items():
            if pos.size == 0:
                continue
            delta = getattr(pos, 'delta', Decimal('0'))
            if inst not in tracked_instruments:
                direction = '多' if pos.size > 0 else '空'
                naked_legs.append(
                    f"🚨 Deribit 裸腿: {inst} | {direction} {abs(pos.size)} | Delta={delta:.4f} | 状态机无记录"
                )
            else:
                if abs(delta) > Decimal('0.1'):
                    naked_legs.append(
                        f"📊 Deribit 已跟踪: {inst} | 数量={pos.size} | Delta={delta:.4f}"
                    )

        # ===== Binance 仓位诊断 =====
        if self.binance_ws:
            if self.binance_dual_side_mode and getattr(self.binance_ws, "positions_by_side", None):
                tracked_side = self._build_tracked_binance_by_side()
                for (bn_sym, ps), bn_pos in self.binance_ws.positions_by_side.items():
                    if bn_pos.quantity <= 0:
                        continue
                    tracked_qty = tracked_side.get((bn_sym, ps), Decimal('0'))
                    residual = bn_pos.quantity - tracked_qty
                    if abs(residual) > Decimal('0.001'):
                        naked_legs.append(
                            f"🚨 Binance 未跟踪: {bn_sym} {ps} | 实际={bn_pos.quantity} "
                            f"(状态机={tracked_qty}, 差额={residual})"
                        )
                    else:
                        naked_legs.append(
                            f"📊 Binance 已跟踪: {bn_sym} {ps} | 实际={bn_pos.quantity} | 状态机={tracked_qty}"
                        )
            else:
                for bn_sym, bn_pos in self.binance_ws.positions.items():
                    if bn_pos.quantity <= 0:
                        continue
                    actual_signed = bn_pos.quantity if bn_pos.side == "LONG" else -bn_pos.quantity
                    tracked_signed = tracked_binance_signed.get(bn_sym, Decimal('0'))
                    untracked_signed = actual_signed - tracked_signed
                    actual_dir = '多' if actual_signed > 0 else '空'
                    if abs(untracked_signed) > Decimal('0.001'):
                        extra_dir = '多' if untracked_signed > 0 else '空'
                        naked_legs.append(
                            f"🚨 Binance 未跟踪: {bn_sym} | 实际={actual_dir} {abs(actual_signed)} "
                            f"(状态机净额={tracked_signed:+f}, 差额={extra_dir} {abs(untracked_signed)})"
                        )
                    else:
                        naked_legs.append(
                            f"📊 Binance 已跟踪: {bn_sym} | 实际={actual_dir} {abs(actual_signed)} | 状态机净额={tracked_signed:+f}"
                        )
        return naked_legs

    async def _post_reconnect_naked_leg_check(self):
        """🌟 恢复后裸腿核查: 检测断连期间是否有期权交割导致 Binance 裸腿

        场景: 断连期间 Deribit 期权到期结算 → 持仓归零 → 但 Binance 对冲未平
        此时 Deribit 端无持仓，Binance 端有裸露的永续仓位 → 必须告警

        处理方式: 只检测+告警，不自动平仓（由用户决定）
        """
        if not self.binance_ws or not self.binance_connected:
            return

        _alerts = []
        _perp = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'

        for es, state in list(self.arbitrage_states.items()):
            if state.state not in ('position_open', 'executing', 'exiting'):
                continue
            if not state.binance_future_symbol or state.binance_filled_qty <= Decimal('0.0001'):
                continue

            # 检查 Deribit 端期权是否还有持仓
            combo = self.arbitrage_combinations.get(es)
            if not combo:
                continue

            c_pos = self.client.positions.get(combo['call'])
            p_pos = self.client.positions.get(combo['put'])
            c_alive = c_pos and c_pos.size != 0
            p_alive = p_pos and p_pos.size != 0

            if not c_alive and not p_alive:
                # Deribit 期权已全部归零（可能交割了），但 Binance 仍有对冲仓位
                expiry, strike = es
                _alerts.append(
                    f"  ⚠️ [{expiry}-{strike}] Deribit 期权已归零，"
                    f"Binance {state.binance_future_symbol} {state.binance_position_side} "
                    f"qty={state.binance_filled_qty} 可能裸露！")
                logger.error(
                    f"🚨 [恢复后裸腿核查] [{expiry}-{strike}] Deribit 两腿已归零，"
                    f"Binance 对冲 {state.binance_future_symbol} qty={state.binance_filled_qty} 仍在！"
                    f"可能是断连期间交割导致的裸腿，将由 monitor_positions 的交割检测接管处理")

        if _alerts:
            _msg = (f"🚨 恢复后裸腿核查发现 {len(_alerts)} 个异常:\n"
                    + "\n".join(_alerts) +
                    f"\n\n系统将自动尝试交割结算流程处理。\n"
                    f"如需手动处理: /stop_all")
            asyncio.create_task(tg_notifier.send_error_async(_msg, "reconnect_naked_leg"))
            logger.error(f"🚨 恢复后裸腿核查: 发现 {len(_alerts)} 个可能的裸腿")
        else:
            logger.info("✅ 恢复后裸腿核查: 所有对冲仓位正常")

    async def _auto_close_naked_legs(self):
        """⚡ 自动平仓裸腿仓位，减少损失（Deribit + Binance）"""
        if self._is_deribit_settlement_core_window():
            self._add_pause("结算窗口")
            _now_core = time.time()
            if _now_core - getattr(self, '_naked_close_settlement_block_log_ts', 0.0) >= 30:
                logger.warning("⏸️ [自动减损] Deribit core settlement window active，本次跳过裸腿自动平仓")
                self._naked_close_settlement_block_log_ts = _now_core
            return

        # 🛡️ 修复 C: 启动稳定窗口（60s 内拒绝执行）
        # 防止启动早期 WS/Redis/状态机异步加载顺序差导致的瞬时不一致触发误平真实对冲腿。
        # 这是最后一道屏障 — 即使上游修复全部失效，本守卫也能挡住启动期事故。
        _start_ts = getattr(self, '_engine_start_ts', 0.0)
        if _start_ts > 0:
            _uptime = time.time() - _start_ts
            if _uptime < 60.0:
                _now_c = time.time()
                if _now_c - getattr(self, '_naked_close_startup_block_log_ts', 0.0) >= 30:
                    logger.warning(
                        f"⏸️ [自动减损] 启动稳定窗口未过 (uptime={_uptime:.1f}s/60s)，本次跳过")
                    self._naked_close_startup_block_log_ts = _now_c
                return
        tracked_instruments = set()
        for es, st in self.arbitrage_states.items():
            if st.state in ('position_open', 'executing', 'exiting'):
                combo = self.arbitrage_combinations.get(es)
                if combo:
                    tracked_instruments.add(combo['future'])
                    tracked_instruments.add(combo['call'])
                    tracked_instruments.add(combo['put'])

        tracked_binance_signed = self._build_tracked_binance_signed()
        tracked_binance_by_side = self._build_tracked_binance_by_side()
        closed_deribit = 0
        closed_binance = 0

        # ===== Deribit 裸腿处置 =====
        for inst, pos in list(self.client.positions.items()):
            if pos.size == 0 or inst in tracked_instruments:
                continue

            direction = '多' if pos.size > 0 else '空'
            logger.warning(f"⚡ [自动减损] 正在平仓 Deribit 裸腿: {inst} {direction} {abs(pos.size)}")

            try:
                if self.client._is_option_instrument(inst):
                    await self._close_option_position(inst, "[自动减损]")
                else:
                    close_resp = await self.client.send_request({
                        "jsonrpc": "2.0",
                        "id": self.client._get_next_request_id(),
                        "method": "private/close_position",
                        "params": {"instrument_name": inst, "type": "market"}
                    }, is_private=True)
                    if 'error' in close_resp:
                        logger.error(f"[自动减损] Deribit 期货平仓失败: {inst} -> {close_resp['error']}")
                        continue

                closed_deribit += 1
                logger.info(f"✅ [自动减损] Deribit 裸腿已平仓: {inst}")
            except Exception as e:
                logger.error(f"[自动减损] Deribit 平仓异常: {inst} -> {e}")

        # ===== Binance 裸腿处置 =====
        if self.binance_ws:
            if self.binance_dual_side_mode and getattr(self.binance_ws, "positions_by_side", None):
                for (bn_sym, ps), bn_pos in list(self.binance_ws.positions_by_side.items()):
                    if bn_pos.quantity <= 0:
                        continue
                    tracked_qty = tracked_binance_by_side.get((bn_sym, ps), Decimal('0'))
                    # 仅处置"交易所实仓 > 状态机跟踪"的超额部分；
                    # tracked > actual 说明是状态机滞后，不能反向去平正常仓位。
                    residual_qty = bn_pos.quantity - tracked_qty
                    if residual_qty <= Decimal('0.001'):
                        continue
                    close_side = "SELL" if ps == "LONG" else "BUY"
                    close_qty = residual_qty
                    logger.warning(
                        f"⚡ [自动减损] 正在平仓 Binance 裸腿: {bn_sym} {ps} {close_side} {close_qty} "
                        f"(实际={bn_pos.quantity}, 状态机={tracked_qty})")
                    try:
                        _res = None
                        if self.binance_executor:
                            _res = await self.binance_executor.place_market_order(
                                bn_sym, close_side, close_qty, reduce_only=True, position_side=ps)
                        elif getattr(self, 'binance_auth', None):
                            _session = None
                            try:
                                _session = aiohttp.ClientSession(
                                    headers=self.binance_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                                _params = self.binance_auth.sign({
                                    "symbol": bn_sym,
                                    "side": close_side,
                                    "positionSide": ps,
                                    "type": "MARKET",
                                    "quantity": str(close_qty),
                                    "newOrderRespType": "RESULT"
                                })
                                _res = await self._binance_rest_fallback(
                                    self.binance_auth, _session, "POST", "/fapi/v1/order", _params, signed=False)
                            finally:
                                if _session:
                                    await _session.close()
                        if _res and _res.get('status') in ('FILLED', 'PARTIALLY_FILLED'):
                            closed_binance += 1
                            logger.info(f"✅ [自动减损] Binance 裸腿处置已执行: {bn_sym} {ps} {close_side} {close_qty}")
                        else:
                            logger.error(f"[自动减损] Binance 裸腿平仓失败: {bn_sym} {ps} -> {_res}")
                    except Exception as e:
                        logger.error(f"[自动减损] Binance 平仓异常: {bn_sym} {ps} -> {e}")
            else:
                for bn_sym, bn_pos in list(self.binance_ws.positions.items()):
                    if bn_pos.quantity <= 0:
                        continue
                    actual_signed = bn_pos.quantity if bn_pos.side == "LONG" else -bn_pos.quantity
                    tracked_signed = tracked_binance_signed.get(bn_sym, Decimal('0'))
                    # 仅平"与实际方向同向的超额仓位"：
                    # - 实际为多: 仅比较 tracked 的多仓部分
                    # - 实际为空: 仅比较 tracked 的空仓部分
                    # 防止 tracked 绝对值更大(状态机滞后)时误平真实对冲腿。
                    if actual_signed > 0:
                        _tracked_same_dir = max(tracked_signed, Decimal('0'))
                    else:
                        _tracked_same_dir = min(tracked_signed, Decimal('0'))
                    residual_signed = actual_signed - _tracked_same_dir
                    if abs(residual_signed) <= Decimal('0.001'):
                        continue

                    close_side = "SELL" if residual_signed > 0 else "BUY"
                    close_qty = abs(residual_signed)
                    logger.warning(
                        f"⚡ [自动减损] 正在平仓 Binance 裸腿: {bn_sym} {close_side} {close_qty} "
                        f"(实际={actual_signed:+f}, 状态机={tracked_signed:+f})")

                    try:
                        _res = None
                        if self.binance_executor:
                            _res = await self.binance_executor.place_market_order(
                                bn_sym, close_side, close_qty, reduce_only=True)
                        elif getattr(self, 'binance_auth', None):
                            _session = None
                            try:
                                _session = aiohttp.ClientSession(
                                    headers=self.binance_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                                _params = self.binance_auth.sign({
                                    "symbol": bn_sym,
                                    "side": close_side,
                                    "type": "MARKET",
                                    "quantity": str(close_qty),
                                    "reduceOnly": "true",
                                    "newOrderRespType": "RESULT"
                                })
                                _res = await self._binance_rest_fallback(
                                    self.binance_auth, _session, "POST", "/fapi/v1/order", _params, signed=False)
                            finally:
                                if _session:
                                    await _session.close()

                        if _res and _res.get('status') in ('FILLED', 'PARTIALLY_FILLED'):
                            closed_binance += 1
                            logger.info(f"✅ [自动减损] Binance 裸腿处置已执行: {bn_sym} {close_side} {close_qty}")
                        else:
                            logger.error(f"[自动减损] Binance 裸腿平仓失败: {bn_sym} -> {_res}")
                    except Exception as e:
                        logger.error(f"[自动减损] Binance 平仓异常: {bn_sym} -> {e}")

        if closed_deribit > 0 or closed_binance > 0:
            await tg_notifier.send_error_async(
                f"⚡ 自动减损完成：Deribit={closed_deribit}，Binance={closed_binance}", "naked_leg_closed")
            await self.client.get_positions(self.target_currency, silent=True)
            if self.binance_ws:
                _sym_set = set(self.binance_ws.positions.keys())
                for _sym, _ps in getattr(self.binance_ws, "positions_by_side", {}).keys():
                    _sym_set.add(_sym)
                for _sym in list(_sym_set):
                    try:
                        _ = await self.binance_ws.get_position_risk(_sym)
                    except Exception:
                        pass
        else:
            logger.warning("[自动减损] 未找到可自动平仓的裸腿，Delta 偏移可能来自已跟踪仓位的 Gamma 漂移")
            await tg_notifier.send_error_async(
                "⚠️ 未找到裸腿仓位，Delta 偏移来自已跟踪持仓的 Gamma 漂移，需人工评估",
                "naked_leg_not_found")

    async def _margin_emergency_shutdown(self, exchange: str, detail: str):
        """保证金不足时：暂停新开仓，但保留平仓监控继续运行"""
        if getattr(self, '_margin_shutdown_active', False):
            return  # 防止并发重复触发
        self._margin_shutdown_active = True
        log_prefix = "【保证金不足】"
        try:
            logger.error(f"{log_prefix} {exchange} 保证金不足: {detail}")

            # 1. 暂停新开仓（不设 manual_stop，不设 emergency_stop）
            #    trading_paused 会阻止主循环扫描新机会，但 monitor_positions 独立运行
            #    平仓监控 (_check_exit_opportunity) 不受 trading_paused 影响
            self._add_pause(f"{exchange}保证金不足")

            # 2. 撤销开仓相关的挂单（保留平仓挂单）
            _msgs = []
            _cancelled = 0
            try:
                for oid, order in list(self.client.active_orders.items()):
                    # 只撤销开仓挂单（label 含 arb_ 前缀），保留平仓/L2 挂单
                    if order.label and order.label.startswith('arb_'):
                        await self.client.cancel_order(oid, log_prefix=log_prefix)
                        _cancelled += 1
                if _cancelled > 0:
                    _msgs.append(f"✅ 已撤销 {_cancelled} 个开仓挂单")
                else:
                    _msgs.append("ℹ️ 无开仓挂单需要撤销")
            except Exception as e:
                _msgs.append(f"⚠️ 撤单失败: {e}")

            # 3. 统计当前持仓组合数
            _open_combos = len([s for s in self.arbitrage_states.values() if s.state == 'position_open'])
            _msgs.append(f"📊 当前持有 {_open_combos} 个套利组合，平仓监控持续运行中")

            # 4. 发送 Telegram 通知
            _msg = (
                f"⚠️ 【{exchange} 保证金不足 - 已暂停开仓】\n"
                f"{detail}\n\n"
                + "\n".join(_msgs)
                + "\n\n📌 已暂停新开仓，平仓监控正常运行。"
                  "\n充值后发送 /start 恢复开仓。"
            )
            asyncio.create_task(tg_notifier.send_error_async(_msg, "margin_insufficient"))
            logger.warning(f"{log_prefix} 已暂停开仓，平仓监控继续运行")
        except Exception as e:
            logger.error(f"{log_prefix} 执行异常: {e}")
        finally:
            self._margin_shutdown_active = False
