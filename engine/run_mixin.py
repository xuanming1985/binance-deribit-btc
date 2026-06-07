"""engine/run_mixin.py — 主循环 + 价差记录 + BTC 参考价"""
from __future__ import annotations
import logging
import time
import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    pass

import config
import aiohttp
from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


class RunMixin:
    """Mixin: 引擎主循环 + 价差快照记录 + BTC 参考价获取"""

    async def _get_reference_btc_price(self, preferred_symbol: str = "") -> Decimal:
        """获取 BTC/USD 风控参考价（Binance 优先，Deribit 次之）"""
        # 1) Binance 永续盘口 / 标记价 / 最新成交
        _perp = preferred_symbol or ''
        if not _perp:
            _perp = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
        if self.binance_ws:
            try:
                _ob = self.binance_ws.order_books.get(_perp)
                if _ob and _ob.mid_price is not None and _ob.mid_price > 0:
                    return Decimal(str(_ob.mid_price))
                _mk = self.binance_ws.mark_prices.get(_perp, Decimal('0'))
                if _mk and _mk > 0:
                    return Decimal(str(_mk))
                _last = self.binance_ws.last_prices.get(_perp, Decimal('0'))
                if _last and _last > 0:
                    return Decimal(str(_last))
            except Exception:
                pass

        # 2) Deribit 期货 ticker 中间价
        try:
            for _inst, _t in self.client.tickers.items():
                if _inst.count('-') == 1 and _t.mid_price > 0:
                    return Decimal(str(_t.mid_price))
        except Exception:
            pass

        # 3) Deribit 账户持仓中的期货标记价
        try:
            for _inst, _pos in self.client.positions.items():
                if _inst.count('-') == 1 and _pos.mark_price > 0:
                    return Decimal(str(_pos.mark_price))
        except Exception:
            pass

        # 4) Deribit 指数价 API（最慢但最稳）
        try:
            _idx_resp = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_index_price",
                "params": {"index_name": f"{self.target_currency.lower()}_usd"}
            })
            if 'result' in _idx_resp:
                _idx = Decimal(str(_idx_resp['result'].get('index_price', 0)))
                if _idx > 0:
                    return _idx
        except Exception:
            pass
        return Decimal('0')

    async def _refresh_binance_contracts(self):
        """保留方法以兼容调用方，仅使用永续合约无需刷新"""
        logger.info("[合约刷新] 仅使用永续合约，无需刷新合约列表")

    def _record_spread_snapshot(self):
        """记录所有组合的当前价差快照 (42 列 v3 口径, 与 main-spread-his-binance.py 对齐)

        新增字段 (相比旧 19 列):
          - 期权原始盘口: c_bid/c_ask/p_bid/p_ask + 深度 bid_sz/ask_sz
          - Maker 视角: maker_anchor, maker_spread_sell/buy, maker_net_sell/buy
          - 自适应 aggression: maker_aggr_call/put (与主引擎 T1 定价一致)
          - 过滤标记: dte_pass / depth_pass / funding_pass / executable
          - Binance 深度: bn_bid_sz / bn_ask_sz

        这样主网/测试网两边数据可以用同一套分析脚本处理.
        """
        try:
            if not getattr(self, 'record_spread_snapshots', True):
                self._spread_last_record = time.time()
                return

            rows = []
            ts = time.strftime('%Y-%m-%d %H:%M:%S')
            amount = float(self.trade_amount) if getattr(self, 'trade_amount', 0) else 0.1
            # 主引擎配置 (用于 executable 过滤标记)
            _min_dte_h = float(getattr(self, 'min_option_dte_hours', 12) or 0)
            _min_depth_ratio = float(getattr(self, 'min_depth_ratio', 0.2) or 0.2)
            _min_depth_btc = amount * _min_depth_ratio
            _max_funding_pct = float(getattr(self, 'max_funding_rate_pct', 0.001) or 0.001)
            _aggr_upper = float(getattr(self, 'maker_price_aggression', 0.8) or 0.8)
            from datetime import datetime, timezone, timedelta

            def _adaptive_aggr(spread_usd: float) -> float:
                # 与主引擎 T1 定价一致: upper - clamp((spread-50)/3000, 0, 0.20), 夹到 [0.5, 0.95]
                _decay = max(0.0, min(0.20, (spread_usd - 50.0) / 3000.0))
                return max(0.5, min(0.95, _aggr_upper - _decay))

            for (expiry, strike), combo in self.arbitrage_combinations.items():
                call_t = self.client.tickers.get(combo['call'])
                put_t = self.client.tickers.get(combo['put'])
                if (not call_t or not put_t or
                        call_t.bid <= 0 or put_t.ask <= 0 or
                        call_t.ask <= 0 or put_t.bid <= 0):
                    continue

                bn_symbol = combo.get('binance_future', '')
                bn_type = combo.get('binance_future_type', '')
                if not bn_symbol or not self.binance_ws:
                    continue
                bn_ob = self.binance_ws.order_books.get(bn_symbol)
                if not bn_ob or bn_ob.mid_price is None or bn_ob.mid_price <= 0:
                    continue

                bn_mid = float(bn_ob.mid_price)
                dr_fwd_t = self.client.tickers.get(combo['future'])
                dr_fwd = float(dr_fwd_t.mid_price) if dr_fwd_t and dr_fwd_t.mid_price > 0 else 0

                # 期权原始盘口
                c_bid = float(call_t.bid)
                c_ask = float(call_t.ask)
                p_bid = float(put_t.bid)
                p_ask = float(put_t.ask)
                c_bid_sz = float(getattr(call_t, 'bid_size', 0) or 0)
                c_ask_sz = float(getattr(call_t, 'ask_size', 0) or 0)
                p_bid_sz = float(getattr(put_t, 'bid_size', 0) or 0)
                p_ask_sz = float(getattr(put_t, 'ask_size', 0) or 0)
                # Binance 首档深度
                try:
                    bn_bid_sz = float(bn_ob.bids[0][1]) if bn_ob.bids else 0.0
                    bn_ask_sz = float(bn_ob.asks[0][1]) if bn_ob.asks else 0.0
                except Exception:
                    bn_bid_sz, bn_ask_sz = 0.0, 0.0

                # 两个策略方向的 Taker 价差
                syn_sell = float(strike) + (c_bid - p_ask) * bn_mid
                spread_sell = syn_sell - bn_mid  # buy_future_sell_synthetic
                syn_buy = float(strike) + (c_ask - p_bid) * bn_mid
                spread_buy = bn_mid - syn_buy    # sell_future_buy_synthetic

                # DTE 计算
                try:
                    _raw_dt = datetime.strptime(expiry, "%d%b%y")
                    _expiry_dt = _raw_dt.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                    dte_hours = (_expiry_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                except Exception:
                    dte_hours = 0.0

                # 费用与 funding (动态费率, 与主引擎扫描方向口径一致)
                try:
                    _amt_d = Decimal(str(amount))
                    _bn_mid_d = Decimal(str(bn_mid))
                    _c_open_ref = Decimal(str(max(c_bid, c_ask)))
                    _p_open_ref = Decimal(str(max(p_bid, p_ask)))
                    _oc_btc = self.fee_calculator.calculate_option_fee(
                        _bn_mid_d, _c_open_ref, _amt_d, is_taker=True)
                    _op_btc = self.fee_calculator.calculate_option_fee(
                        _bn_mid_d, _p_open_ref, _amt_d, is_taker=True)
                    _bn_open_fee = self.trade_executor._calculate_binance_fee_usdt(
                        _bn_mid_d, _amt_d, is_taker=True)
                    open_fee = float((_oc_btc + _op_btc) * _bn_mid_d + _bn_open_fee)

                    _del_c = self.fee_calculator.calculate_delivery_fee(
                        _bn_mid_d, _c_open_ref, _amt_d, is_option=True)
                    _del_p = self.fee_calculator.calculate_delivery_fee(
                        _bn_mid_d, _p_open_ref, _amt_d, is_option=True)
                    _bn_close_fee = self.trade_executor._calculate_binance_fee_usdt(
                        _bn_mid_d, _amt_d, is_taker=True)
                    settle_fee = float((_del_c + _del_p) * _bn_mid_d + _bn_close_fee)
                except Exception:
                    _bn_rate = float(Decimal(str(getattr(self.binance_fee_calc, 'taker_rate', Decimal('0.0004')))))
                    _opt_rate = float(self.fee_calculator.current_rates['option']['taker'])
                    _del_rate = 0.00015
                    open_fee = (amount * _opt_rate * 2 + amount * _bn_rate) * bn_mid
                    settle_fee = (amount * _del_rate * 2 + amount * _bn_rate) * bn_mid

                funding_rate = float(self.binance_ws.funding_rates.get(bn_symbol, Decimal('0')))
                funding_hours = max(dte_hours, 8.0) if dte_hours > 0 else 48.0
                funding_cost = bn_mid * amount * funding_rate * (funding_hours / 8.0)

                # Taker 净利
                net_sell = spread_sell * amount - open_fee - settle_fee - funding_cost
                net_buy = spread_buy * amount - open_fee - settle_fee + funding_cost

                # ============ Maker 视角 (对齐 main-spread-his-binance.py v3) ============
                c_spread_usd = (c_ask - c_bid) * bn_mid
                p_spread_usd = (p_ask - p_bid) * bn_mid
                aggr_call = _adaptive_aggr(c_spread_usd)
                aggr_put = _adaptive_aggr(p_spread_usd)

                # Binance 单腿 taker 费 (USD), 用于 Maker 开仓费估算
                try:
                    _bn_leg_fee = float(self.trade_executor._calculate_binance_fee_usdt(
                        Decimal(str(bn_mid)), Decimal(str(amount)), is_taker=True))
                except Exception:
                    _bn_leg_fee = bn_mid * amount * float(Decimal(str(
                        getattr(self.binance_fee_calc, 'taker_rate', Decimal('0.0004')))))

                def _opt_taker_fee_usd(premium_ref: float) -> float:
                    """Maker-Taker 场景下单腿 Taker 期权费 (USD) - 含 12.5% premium cap"""
                    try:
                        _pref = Decimal(str(max(premium_ref, 0.000001)))
                        _fee_btc = self.fee_calculator.calculate_option_fee(
                            Decimal(str(bn_mid)), _pref, Decimal(str(amount)), is_taker=True)
                        return float(_fee_btc) * bn_mid
                    except Exception:
                        return amount * 0.0003 * bn_mid

                # --- sell 方向 (buy_future_sell_synthetic): 卖C + 买P ---
                if c_spread_usd >= p_spread_usd:
                    # Call 更宽 → Call 做 Maker (卖高), Put 做 Taker
                    maker_anchor_sell = "call"
                    _aggr_s = aggr_call
                    c_maker_sell = c_bid + (c_ask - c_bid) * _aggr_s
                    maker_syn_sell = float(strike) + (c_maker_sell - p_ask) * bn_mid
                    maker_open_fee_sell = _opt_taker_fee_usd(p_ask) + _bn_leg_fee
                else:
                    maker_anchor_sell = "put"
                    _aggr_s = aggr_put
                    p_maker_sell = p_ask - (p_ask - p_bid) * _aggr_s
                    maker_syn_sell = float(strike) + (c_bid - p_maker_sell) * bn_mid
                    maker_open_fee_sell = _opt_taker_fee_usd(c_bid) + _bn_leg_fee
                maker_spread_sell = maker_syn_sell - bn_mid
                maker_net_sell = maker_spread_sell * amount - maker_open_fee_sell - settle_fee - funding_cost

                # --- buy 方向 (sell_future_buy_synthetic): 买C + 卖P ---
                if c_spread_usd >= p_spread_usd:
                    maker_anchor_buy = "call"
                    _aggr_b = aggr_call
                    c_maker_buy = c_ask - (c_ask - c_bid) * _aggr_b
                    maker_syn_buy = float(strike) + (c_maker_buy - p_bid) * bn_mid
                    maker_open_fee_buy = _opt_taker_fee_usd(p_bid) + _bn_leg_fee
                else:
                    maker_anchor_buy = "put"
                    _aggr_b = aggr_put
                    p_maker_buy = p_bid + (p_ask - p_bid) * _aggr_b
                    maker_syn_buy = float(strike) + (c_ask - p_maker_buy) * bn_mid
                    maker_open_fee_buy = _opt_taker_fee_usd(c_ask) + _bn_leg_fee
                maker_spread_buy = bn_mid - maker_syn_buy
                maker_net_buy = maker_spread_buy * amount - maker_open_fee_buy - settle_fee + funding_cost

                maker_anchor = maker_anchor_sell  # 以 sell 方向锚定腿为代表

                # ============ 过滤标记 (与主引擎实际开仓门槛一致) ============
                dte_pass = 1 if dte_hours >= _min_dte_h else 0
                depth_pass = 1 if (
                    min(c_bid_sz, c_ask_sz, p_bid_sz, p_ask_sz) >= _min_depth_btc and
                    min(bn_bid_sz, bn_ask_sz) >= _min_depth_btc
                ) else 0
                funding_pass = 0 if abs(funding_rate) > _max_funding_pct else 1
                executable = 1 if (dte_pass and depth_pass and funding_pass) else 0

                # 真实活跃持仓状态 (主引擎独有优势: 采集器版本永远是 0)
                state = self.arbitrage_states.get((expiry, strike))
                has_pos = 1 if state and state.state == 'position_open' else 0

                rows.append(
                    f"{ts},{expiry},{int(float(strike))},{bn_symbol},{bn_type},"
                    f"{bn_mid:.2f},{dr_fwd:.2f},{syn_sell:.2f},{syn_buy:.2f},"
                    f"{spread_sell:.2f},{spread_buy:.2f},{has_pos},"
                    f"{dte_hours:.1f},{funding_rate:.6f},"
                    f"{open_fee:.2f},{settle_fee:.2f},{funding_cost:.2f},"
                    f"{net_sell:.2f},{net_buy:.2f},"
                    # v2 Maker 视角
                    f"{c_bid:.6f},{c_ask:.6f},{p_bid:.6f},{p_ask:.6f},"
                    f"{c_spread_usd:.2f},{p_spread_usd:.2f},"
                    f"{maker_anchor},{maker_spread_sell:.2f},{maker_net_sell:.2f},"
                    f"{maker_spread_buy:.2f},{maker_net_buy:.2f},"
                    # v3 深度 + aggression + 过滤标记
                    f"{c_bid_sz:.4f},{c_ask_sz:.4f},{p_bid_sz:.4f},{p_ask_sz:.4f},"
                    f"{bn_bid_sz:.4f},{bn_ask_sz:.4f},"
                    f"{aggr_call:.3f},{aggr_put:.3f},"
                    f"{depth_pass},{funding_pass},{dte_pass},{executable}"
                )

            if rows:
                header_keys = [
                    'timestamp', 'expiry', 'strike', 'bn_symbol', 'bn_type',
                    'bn_mid', 'dr_fwd_mid', 'syn_sell', 'syn_buy',
                    'spread_sell', 'spread_buy', 'has_position',
                    'dte_hours', 'funding_rate',
                    'open_fee', 'settle_fee', 'funding_cost',
                    'net_sell', 'net_buy',
                    'c_bid', 'c_ask', 'p_bid', 'p_ask',
                    'c_spread_usd', 'p_spread_usd',
                    'maker_anchor', 'maker_spread_sell', 'maker_net_sell',
                    'maker_spread_buy', 'maker_net_buy',
                    'c_bid_sz', 'c_ask_sz', 'p_bid_sz', 'p_ask_sz',
                    'bn_bid_sz', 'bn_ask_sz',
                    'maker_aggr_call', 'maker_aggr_put',
                    'depth_pass', 'funding_pass', 'dte_pass', 'executable',
                ]
                spread_dicts = []
                for row_str in rows:
                    vals = row_str.split(',')
                    if len(vals) == len(header_keys):
                        spread_dicts.append(dict(zip(header_keys, vals)))
                if spread_dicts:
                    try:
                        self._spread_store.insert_batch_sync(spread_dicts)
                    except Exception as _db_err:
                        logger.warning(f"[价差记录] SQLite 写入失败: {_db_err}")

            self._spread_last_record = time.time()
        except Exception as e:
            logger.info(f"价差记录失败: {e}")

    async def run(self):
        """运行引擎 (重构版：并发批量执行)"""
        self.running = True
        # 🛡️ 修复 C: 记录引擎启动时间，供 _auto_close_naked_legs 的启动冷启动窗使用
        # 在 self.running = True 之后立即赋值，确保整个 initialize() 期间已生效
        self._engine_start_ts = time.time()

        try:
            # 🌟 C4 修复: 获取单例锁，防止多实例同时运行
            if not await self._acquire_singleton_lock():
                logger.error("🚨 单例锁获取失败，引擎退出。请确认没有其他实例在运行。")
                return

            # 初始化
            await self.initialize()

            if not self.initialized:
                logger.error("初始化失败，无法运行引擎")
                await self._release_singleton_lock()
                return

            logger.info("开始全市场套利扫描 (并发模式)...")

            # 启动持仓监控
            self._monitor_task = asyncio.create_task(self.monitor_positions())
            # ================= 🌟 机构级新增：启动异步落盘进程 =================
            self.persist_task = asyncio.create_task(self._trade_persistence_worker())
            # 🌟 C4: 启动单例锁心跳
            self._singleton_hb_task = asyncio.create_task(self._singleton_heartbeat())

            # 主循环
            while True:
                try:
                    if self._fatal_shutdown:
                        logger.warning("🛑 致命关机信号已触发，主循环退出进入清理流程...")
                        break

                    # ========= 看门狗 (在 running 检查之前，确保 stop_all 后基础设施仍受监护) =========
                    # 🌟 H3 修复: 看门狗 — 检测 monitor_positions 是否存活，死亡则自动重启
                    if hasattr(self, '_monitor_task') and self._monitor_task.done() and not self._fatal_shutdown:
                        _exc = self._monitor_task.exception() if not self._monitor_task.cancelled() else None
                        logger.error(f"🚨 monitor_positions 任务已死亡！异常: {_exc}，正在自动重启...")
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 monitor_positions 看门狗触发重启! 异常: {str(_exc)[:200]}", "watchdog"))
                        self._monitor_task = asyncio.create_task(self.monitor_positions())

                    # 🌟 看门狗: 落盘守护进程存活检测，死亡则自动重启
                    if self.persist_task and self.persist_task.done() and not self._fatal_shutdown:
                        _p_exc = self.persist_task.exception() if not self.persist_task.cancelled() else None
                        logger.error(f"🚨 落盘守护进程已死亡！异常: {_p_exc}，正在自动重启...")
                        self.persist_task = asyncio.create_task(self._trade_persistence_worker())

                    # 🌟 看门狗: 单例锁心跳任务存活检测，死亡则自动重启（被抢占场景除外）
                    if hasattr(self, '_singleton_hb_task') and self._singleton_hb_task and self._singleton_hb_task.done() and not self._fatal_shutdown:
                        _hb_exc = self._singleton_hb_task.exception() if not self._singleton_hb_task.cancelled() else None
                        if self._has_pause("单例锁被抢占"):
                            logger.error('🚨 单例锁心跳任务已结束，且系统处于"单例锁被抢占"暂停状态，不自动重启。')
                        else:
                            logger.error(f"🚨 单例锁心跳任务已死亡！异常: {_hb_exc}，正在自动重启...")
                            asyncio.create_task(tg_notifier.send_error_async(
                                f"🚨 单例锁心跳看门狗触发重启! 异常: {str(_hb_exc)[:200]}", "singleton_watchdog"))
                            self._singleton_hb_task = asyncio.create_task(self._singleton_heartbeat())

                    # stop_all 后 running=False 仍要检查结算窗口:
                    # 若 /stop_all 在 core settlement window 内被延后，这里负责在窗口结束后恢复执行清仓。
                    if not self.running:
                        await self._check_settlement_window()

                    # stop_all 后 running=False，等待 start 恢复（看门狗已在上方执行）
                    if not self.running:
                        await asyncio.sleep(2)
                        continue

                    # ================= 断线重连与成功通知 =================
                    if not self.client.is_connected:
                        # 维护期间不恢复交易动作，但必须保证独立探测任务仍在运行；
                        # 探测成功后会清除 Deribit维护，下一轮再走完整 initialize。
                        if getattr(self.client, 'maintenance_sleep_active', False) or self._has_pause("Deribit维护"):
                            if hasattr(self.client, '_start_maintenance_cooldown'):
                                self.client._start_maintenance_cooldown(source="locked_by_admin", is_first_maintenance=False)
                            await asyncio.sleep(10)
                            continue

                        logger.warning("检测到 WebSocket 断开，启动网络恢复流程...")
                        try:
                            # 1. 彻底清理残余的阻塞任务和死链接
                            await self.client.cleanup()
                            # 2. 重新走一遍完整的引擎初始化
                            # (这会自动完成：重连WS -> 重新账号认证 -> 重新拉取真实持仓 -> 重新订阅期权/期货频道)
                            await self.initialize()
                            # 3. 发送重连成功的喜报
                            if self.initialized:
                                # 重置连接失败通知标记，确保下次断连能正常发通知
                                self.client._ws_fail_notified = False
                                self._reset_gamma_guard_counters("Deribit重连恢复")
                                # 仅移除 Deribit WS断连原因，其他暂停原因不受影响
                                self._remove_pause("Deribit WS断连")
                                if self.trading_paused:
                                    logger.info(f"网络恢复，但仍有其他暂停原因: {self._pause_reason}")
                                else:
                                    logger.info("网络恢复，自动解除网络断连暂停")
                                asyncio.create_task(tg_notifier.notify_network_reconnect())

                                # 🌟 Deribit 恢复后核查: 断连期间是否有交割导致 Binance 裸腿
                                try:
                                    await self._post_reconnect_naked_leg_check()
                                except Exception as _nlc_err:
                                    logger.error(f"⚠️ Deribit恢复后裸腿核查异常: {_nlc_err}")

                            continue  # 恢复成功后，跳过本次死循环，重新开始健康扫描

                        except Exception as e:
                            logger.error(f"自动重连失败，5秒后重试: {e}")
                            await asyncio.sleep(5)
                            continue

                    # ================= 跨所: Binance 连接健康检查 =================
                    if self.binance_ws is not None:
                        prev_bn_connected = self.binance_connected
                        self.binance_connected = self.binance_ws.connected

                        if prev_bn_connected and not self.binance_connected:
                            # Binance 刚断开
                            logger.warning("⚠️ Binance WS 断开，暂停开新仓")
                            self._add_pause("Binance WS断连")
                            asyncio.create_task(tg_notifier.send_async(
                                "⚠️ 【Binance 断开】期货对冲通道中断，已暂停开新仓"))

                        elif not prev_bn_connected and self.binance_connected:
                            # Binance 刚恢复
                            logger.info("✅ Binance WS 已恢复")
                            self._reset_gamma_guard_counters("Binance重连恢复")
                            self._remove_pause("Binance WS断连")
                            # 🌟 P1-D 修复: 恢复后通过 REST 刷新持仓快照
                            # WS 只推增量 (ACCOUNT_UPDATE)，断连期间的变动不会被推送
                            # 不刷新 → positions_by_side 用断连前的陈旧数据 → 对账/平仓数量错误
                            try:
                                _perp = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
                                _risks = await self.binance_ws.get_position_risk_all(_perp)
                                if _risks is not None:
                                    _refreshed = 0
                                    from binance_futures import BinanceFuturesPosition
                                    # 🌟 缺陷A修复: 先收集 REST 返回的有效分腿 key，
                                    # 然后清除不在返回列表中的陈旧条目
                                    _live_side_keys = set()
                                    for _rk in _risks:
                                        _pos_amt = Decimal(str(_rk.get("positionAmt", "0")))
                                        _pos_side = str(_rk.get("positionSide", "BOTH")).upper()
                                        if _pos_side in ("LONG", "SHORT"):
                                            _live_side_keys.add((_perp, _pos_side))
                                            if _pos_amt == 0:
                                                # 已平仓: 从内存中删除陈旧条目
                                                self.binance_ws.positions_by_side.pop((_perp, _pos_side), None)
                                                continue
                                            self.binance_ws.positions_by_side[(_perp, _pos_side)] = BinanceFuturesPosition(
                                                symbol=_perp, side=_pos_side, position_side=_pos_side,
                                                quantity=abs(_pos_amt),
                                                entry_price=Decimal(str(_rk.get("entryPrice", "0"))),
                                                unrealized_pnl=Decimal(str(_rk.get("unRealizedProfit", "0"))),
                                                mark_price=Decimal(str(_rk.get("markPrice", "0"))))
                                            _refreshed += 1
                                        else:
                                            if _pos_amt == 0:
                                                self.binance_ws.positions.pop(_perp, None)
                                                continue
                                            _side = "LONG" if _pos_amt > 0 else "SHORT"
                                            self.binance_ws.positions[_perp] = BinanceFuturesPosition(
                                                symbol=_perp, side=_side, position_side="BOTH",
                                                quantity=abs(_pos_amt),
                                                entry_price=Decimal(str(_rk.get("entryPrice", "0"))),
                                                unrealized_pnl=Decimal(str(_rk.get("unRealizedProfit", "0"))),
                                                mark_price=Decimal(str(_rk.get("markPrice", "0"))))
                                            _refreshed += 1
                                    # 清除 REST 未返回但内存中仍存在的分腿（可能是断连期间清算的）
                                    for _stale_key in [k for k in self.binance_ws.positions_by_side if k[0] == _perp and k not in _live_side_keys]:
                                        self.binance_ws.positions_by_side.pop(_stale_key, None)
                                        logger.info(f"✅ 清除陈旧 Binance 分腿: {_stale_key}")
                                    # 🌟 缺陷B修复: 同步 positions 字典（与启动路径和 WS 推送一致）
                                    if hasattr(self.binance_ws, '_rebuild_net_position'):
                                        self.binance_ws._rebuild_net_position(_perp)
                                    logger.info(f"✅ Binance 持仓快照已刷新 ({_refreshed} 条活跃持仓)")
                                else:
                                    logger.warning("⚠️ Binance 恢复后持仓快照刷新失败，等待下次 WS 推送修正")
                            except Exception as _refresh_err:
                                logger.warning(f"⚠️ Binance 恢复后持仓刷新异常: {_refresh_err}")

                            # 🌟 P1-1 修复: Binance 重连后重试参数设置 (杠杆/保证金模式)
                            # 初始化时若因网络抖动设置失败, "Binance参数设置失败" 暂停永远不会被清除
                            if self._has_pause("Binance参数设置失败"):
                                try:
                                    _bn_cfg = getattr(config, 'BINANCE_CONFIG', {}) or {}
                                    _perp_sym = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
                                    _lev_ok = True
                                    _mt_ok = True
                                    _target_lev = int(_bn_cfg.get("leverage", 3))
                                    if _target_lev > 0:
                                        _lev_ok = await self.binance_ws.set_leverage(_perp_sym, _target_lev)
                                    _target_mt = str(_bn_cfg.get("margin_type", "ISOLATED")).upper()
                                    if _target_mt in ("ISOLATED", "CROSSED"):
                                        _mt_ok = await self.binance_ws.set_margin_type(_perp_sym, _target_mt)
                                    if _lev_ok and _mt_ok:
                                        self._remove_pause("Binance参数设置失败")
                                        logger.info(f"✅ Binance 重连后参数设置成功 (leverage={_target_lev}x, margin={_target_mt})，自动解除暂停")
                                        asyncio.create_task(tg_notifier.send_async(
                                            f"✅ Binance 参数设置已恢复\nleverage={_target_lev}x, margin={_target_mt}"))
                                    else:
                                        logger.warning(f"⚠️ Binance 重连后参数设置仍失败 (lev={_lev_ok}, mt={_mt_ok})，保持暂停")
                                except Exception as _param_retry_err:
                                    logger.warning(f"⚠️ Binance 重连后参数重试异常: {_param_retry_err}")

                            # 🌟 恢复后立即核查: 断连期间是否有期权交割导致 Binance 裸腿
                            # 场景: 期权到期结算 → Deribit 持仓归零 → 但 Binance 对冲未平
                            try:
                                await self._post_reconnect_naked_leg_check()
                            except Exception as _nlc_err:
                                logger.error(f"⚠️ 恢复后裸腿核查异常: {_nlc_err}")

                            if not self.trading_paused:
                                logger.info("两端连接均正常，恢复交易")
                            asyncio.create_task(tg_notifier.send_async(
                                "✅ 【Binance 恢复】期货对冲通道已恢复，持仓已重新同步"))

                        # 严格Hedge模式自动复核与恢复（修复：此前只会暂停不会自动解除）
                        if self.binance_use_hedge_mode and self.binance_connected:
                            _now_mode = time.time()
                            if _now_mode - self._last_hedge_mode_check_ts >= self._hedge_mode_check_interval:
                                self._last_hedge_mode_check_ts = _now_mode
                                try:
                                    _mode_now = await self.binance_ws.get_position_mode()
                                    if _mode_now is not None:
                                        self.binance_dual_side_mode = bool(_mode_now)
                                except Exception as _hm_err:
                                    logger.info(f"[严格Hedge巡检] 持仓模式查询失败: {_hm_err}")

                                if self.binance_strict_hedge_mode:
                                    if self.binance_dual_side_mode:
                                        if self._has_pause("Hedge模式未就绪"):
                                            self._remove_pause("Hedge模式未就绪")
                                            logger.info("✅ 严格Hedge巡检通过：账户已恢复到Hedge模式，自动解除暂停")
                                            asyncio.create_task(tg_notifier.send_async(
                                                "✅ Binance 已恢复 Hedge 双向持仓，系统自动解除严格Hedge暂停"))
                                    else:
                                        if not self._has_pause("Hedge模式未就绪"):
                                            self._add_pause("Hedge模式未就绪")
                                            logger.error("🚨 严格Hedge巡检失败：账户处于One-way，已暂停开仓")
                                        if _now_mode - self._strict_hedge_alert_ts >= 300:
                                            self._strict_hedge_alert_ts = _now_mode
                                            asyncio.create_task(tg_notifier.send_error_async(
                                                "🚨 Binance 严格Hedge模式异常：当前为 One-way\n"
                                                "系统保持暂停开仓，请先平仓并切回 Hedge。",
                                                "strict_hedge_mode_runtime"))

                    # ================= 价差历史记录 (均值回归分析) =================
                    if (self.binance_ws and self.binance_connected and
                            time.time() - self._spread_last_record > self._spread_record_interval):
                        self._record_spread_snapshot()

                    # ================= 每日结算窗口自动规避 =================
                    # 必须在 trading_paused 检查之前调用，否则结算窗口结束后无法自动恢复
                    await self._check_settlement_window()
                    # Binance残余仓位暂停原因自动解锁巡检（仅解除该原因，不影响其他风控暂停）
                    await self._refresh_binance_residual_pause()

                    # ================= 每日账户权益快照 (UTC 日切) =================
                    try:
                        await self._record_daily_account_equity()
                    except Exception as _eq_err:
                        logger.warning(f"账户权益快照周期写入失败: {_eq_err}")

                    # ================= 周期性费率刷新（独立于扫描路径） =================
                    _fee_now = time.time()
                    _need_hourly_refresh = (_fee_now - self._fee_refresh_time) > self._fee_refresh_interval
                    _retry_cooldown_passed = (
                        (_fee_now - getattr(self, '_fee_refresh_last_attempt_time', 0.0)) >
                        max(float(getattr(self, '_fee_refresh_retry_interval', 300.0)), 30.0)
                    )
                    if _need_hourly_refresh and _retry_cooldown_passed:
                        await self._refresh_exchange_fee_rates(reason="hourly")

                    if getattr(self, 'trading_paused', False):
                        if getattr(self.client, 'maintenance_sleep_active', False) or self._has_pause("Deribit维护"):
                            await asyncio.sleep(2)
                            continue
                        # 普通暂停期间仍执行组合完整性/幽灵巡检；Deribit core settlement
                        # window 例外，只读取/记录/撤单，不启动自动处置。
                        _now_pause = time.time()
                        if self._is_deribit_settlement_core_window():
                            if _now_pause - getattr(self, '_settlement_core_freeze_loop_log_ts', 0.0) >= 30.0:
                                logger.info(
                                    "⏸️ [结算核心窗口] 暂停 ghost/integrity/自动处置；仅保留撤单、读取和日志")
                                self._settlement_core_freeze_loop_log_ts = _now_pause
                        elif _now_pause - getattr(self, '_paused_integrity_check_ts', 0.0) >= 5.0:
                            await self._ghost_and_integrity_check()
                            self._paused_integrity_check_ts = _now_pause
                        # 每5分钟输出一次暂停原因日志（避免刷屏）
                        if not hasattr(self, '_last_pause_log_time') or time.time() - self._last_pause_log_time > 300:
                            reason = getattr(self, '_pause_reason', '未知')
                            if getattr(self, '_manual_stop', False):
                                logger.info(f"🛑 [手动暂停] 系统已被人工停止 | 原因: {reason} | 发送 start 命令恢复")
                            else:
                                logger.info(f"⏸️ [自动暂停] 系统暂停中 | 原因: {reason} | 条件消除后将自动恢复")
                            self._last_pause_log_time = time.time()
                        await asyncio.sleep(2)
                        continue

                    # 0. 持仓上限检查（per-cycle 级别，避免 per-task 重复通知）
                    active_count = sum(1 for s in self.arbitrage_states.values() if s.state in ('position_open', 'executing', 'exiting'))
                    if active_count >= self.max_total_positions:
                        if not getattr(self, '_max_pos_notified', False):
                            self._max_pos_notified = True
                            logger.warning(
                                f"⚠️ 活跃组合 {active_count} 已达上限 {self.max_total_positions}，"
                                f"暂停开仓，平仓监控继续运行")
                            asyncio.create_task(tg_notifier.send_async(
                                f"⚠️ 持仓已达上限 ({active_count}/{self.max_total_positions})\n"
                                f"已暂停新开仓，平仓监控正常运行。\n"
                                f"平仓释放仓位后将自动恢复开仓。"))
                        # 每5分钟输出一次心跳日志 + 持仓利润概况
                        if not hasattr(self, '_last_maxpos_log_time') or time.time() - self._last_maxpos_log_time > 300:
                            pnl_summary = self._calc_positions_pnl_summary()
                            logger.info(
                                f"⏸️ [持仓上限] 活跃组合 {active_count}/{self.max_total_positions}，等待平仓释放...\n{pnl_summary}")
                            self._last_maxpos_log_time = time.time()
                        # 无机会时也执行幽灵/完整性检测
                        await self._ghost_and_integrity_check()
                        await asyncio.sleep(self.scan_interval_ms / 1000)
                        continue
                    else:
                        if getattr(self, '_max_pos_notified', False):
                            self._max_pos_notified = False
                            logger.info(f"✅ 活跃组合 {active_count} 已低于上限 {self.max_total_positions}，恢复开仓")
                            asyncio.create_task(tg_notifier.send_async(
                                f"✅ 持仓已低于上限 ({active_count}/{self.max_total_positions})，已恢复开仓。"))

                    # 1. 扫描所有机会
                    opportunities = await self.scan_arbitrage_opportunities()

                    if opportunities:
                        # 按利润从高到低排序
                        opportunities.sort(key=lambda x: x['net_profit'], reverse=True)

                        # 🌟 每到期日持仓上限: 分散风险，避免同一到期日过度集中
                        _max_per_expiry = getattr(self, 'max_positions_per_expiry', 3)
                        _expiry_counts = {}
                        for s in self.arbitrage_states.values():
                            if s.state in ('position_open', 'executing', 'exiting'):
                                _exp = s.expiry_strike[0]
                                _expiry_counts[_exp] = _expiry_counts.get(_exp, 0) + 1
                        _filtered = []
                        for op in opportunities:
                            _exp = op.get('expiry', '')
                            _cur = _expiry_counts.get(_exp, 0) + sum(1 for o in _filtered if o.get('expiry') == _exp)
                            if _cur < _max_per_expiry:
                                _filtered.append(op)
                        if len(_filtered) < len(opportunities):
                            logger.info(f"到期日分散过滤: {len(opportunities)} → {len(_filtered)} "
                                       f"(每到期日上限 {_max_per_expiry})")
                        opportunities = _filtered

                        # 每轮循环实时读取配置，支持 Telegram 热重载
                        batch_size = self.concurrent_batch_size
                        batch_interval = self.batch_interval

                        logger.info(f"扫描发现 {len(opportunities)} 个潜在机会，准备分批并发执行 (Batch: {batch_size})")

                        # 2. 分批处理 (Batch Processing)
                        # 例如 100 个机会，每次执行 10 个
                        for i in range(0, len(opportunities), batch_size):
                            # 如果程序停止或暂停，中断循环
                            if not self.running or self.trading_paused:
                                logger.info(f"扫描批次循环中断 (running={self.running}, paused={self.trading_paused})")
                                break

                            # 批次间重新检查持仓上限，防止超开
                            _batch_active = sum(1 for s in self.arbitrage_states.values()
                                                if s.state in ('position_open', 'executing', 'exiting'))
                            if _batch_active >= self.max_total_positions:
                                logger.info(f">>> 批次 {i // batch_size + 1}: 持仓已达上限 "
                                           f"{_batch_active}/{self.max_total_positions}，停止开仓")
                                break

                            # 获取当前批次
                            batch_ops = opportunities[i: i + batch_size]
                            logger.info(f">>> 执行批次 {i // batch_size + 1}: 包含 {len(batch_ops)} 个组合 "
                                       f"(活跃 {_batch_active}/{self.max_total_positions})")

                            # 3. 构建并发任务
                            # 使用 asyncio.create_task 或 gather 来实现"多线程"效果 (Python协程)
                            tasks = []
                            for op in batch_ops:
                                # 批次内逐个检查暂停，stop 后立即停止提交新任务
                                if self.trading_paused or not self.running:
                                    break
                                task = asyncio.create_task(self._verify_and_execute_task(op))
                                tasks.append(task)
                                # 5毫秒对交易速度没有任何影响 (比你到东京机房的物理延迟还低)
                                # 但在底层，它强制 Python 将指令分拆到不同的 TCP 数据包中发送，完美欺骗交易所的防刷机制
                                await asyncio.sleep(0.005)

                            # 4. 并发等待当前批次完成
                            if tasks:
                                await asyncio.gather(*tasks)
                                # 确保整批任务处理完后，再统一同步一次全局持仓，杜绝 API 重复请求和刷屏
                                try:
                                    # 传入 silent=True 配合我们上一个回合的修改，让它静默更新内存即可
                                    await self.client.get_positions(self.target_currency, silent=True)
                                    logger.info(f"🔄 [批次护栏] 批次 {i // batch_size + 1} 执行完毕，已统一强制刷新账户真实持仓。")

                                    # 🌟 Layer 3: 幽灵仓位检测 + 组合完整性 + L2清理 (已提取为独立方法)
                                    await self._ghost_and_integrity_check()

                                except Exception as sync_e:
                                    logger.error(f"批次状态机护栏同步失败: {sync_e}")

                            # 批次完成，重置保证金预留（防止异常路径泄漏）
                            if self._bn_reserved_margin > Decimal('1'):
                                logger.warning(f"⚠️ 批次结束时保证金预留未归零: {self._bn_reserved_margin:.2f}，存在泄漏路径")
                            self._bn_reserved_margin = Decimal('0')

                            # 批次间歇，防止 API 限频
                            if i + batch_size < len(opportunities):
                                await asyncio.sleep(batch_interval)

                        logger.info("本轮扫描所有批次执行完毕")

                        # 稍微多休息一下，避免过度扫描
                        await asyncio.sleep(1)

                    else:
                        # 无机会时也执行幽灵/完整性检测，防止启动前残留仓位无人发现
                        await self._ghost_and_integrity_check()
                        await asyncio.sleep(self.scan_interval_ms / 1000)

                except Exception as e:
                    logger.error(f"主循环异常: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    asyncio.create_task(tg_notifier.send_error_async(str(e)[:200], "main_loop_error"))
                    await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("收到停止信号 (KeyboardInterrupt)")
        except asyncio.CancelledError:
            logger.info("收到停止信号 (CancelledError)")
        except Exception as e:
            logger.error(f"引擎运行异常: {e}")
        finally:
            self._fatal_shutdown = True
            self.running = False
            # ================= 🌟 安全退出：撤销所有未成交挂单 =================
            try:
                logger.info("🛑 正在撤销所有活跃挂单...")
                await asyncio.wait_for(
                    self.client.cancel_all_orders(self.target_currency), timeout=5.0)
                logger.info("✅ 所有挂单已撤销")
            except asyncio.TimeoutError:
                logger.warning("⚠️ 撤单超时 (5s)，部分挂单可能残留")
            except Exception as e:
                logger.warning(f"⚠️ 撤单异常: {e}")
            # ================= 🌟 安全退出：撤销 Binance 活跃订单 =================
            if self.binance_ws and self.binance_connected:
                try:
                    logger.info("🛑 正在撤销 Binance 活跃订单...")
                    _perp = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
                    await asyncio.wait_for(
                        self.binance_executor.ws_client._rest_request(
                            "DELETE", "/fapi/v1/allOpenOrders",
                            {"symbol": _perp}, signed=True), timeout=5.0)
                    logger.info("✅ Binance 挂单已撤销")
                except (asyncio.TimeoutError, Exception) as e:
                    logger.warning(f"⚠️ Binance 撤单异常: {e}")
            # 🌟 C4: 释放单例锁
            await self._release_singleton_lock()
            # 取消 monitor_positions 任务
            if hasattr(self, '_monitor_task') and not self._monitor_task.done():
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass
            # 🌟 C4: 取消单例锁心跳
            if hasattr(self, '_singleton_hb_task') and not self._singleton_hb_task.done():
                self._singleton_hb_task.cancel()
            # ================= 🌟 补充清理：关闭持久化守护进程 =================
            if hasattr(self, 'trade_queue'):
                try:
                    if not self.trade_queue.empty():
                        await asyncio.wait_for(self.trade_queue.join(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("⚠️ 退出前落盘队列冲刷超时，可能仍有少量记录待写入")
                except Exception:
                    pass
            if self.persist_task and not self.persist_task.done():
                self.persist_task.cancel()
                try:
                    await self.persist_task
                except asyncio.CancelledError:
                    pass
            # ================= 🌟 补充清理：关闭 Binance WS 客户端 =================
            if self.binance_ws:
                try:
                    await self.binance_ws.close()
                except Exception as _bn_close_err:
                    logger.warning(f"⚠️ 关闭 Binance WS 客户端异常: {_bn_close_err}")
                finally:
                    self.binance_ws = None
                    self.binance_executor = None
                    self.binance_connected = False
                    self._binance_tasks = []
            # ================= 🌟 补充清理：安全关闭 Redis 连接 =================
            if hasattr(self, 'redis'):
                try:
                    await asyncio.wait_for(self.redis.aclose(), timeout=5.0)
                except (asyncio.TimeoutError, Exception):
                    pass

            await self.client.stop_listening()
            await self.client.close()
            logger.info("\n套利引擎停止，底层资源已全部安全释放。")
            self._print_final_stats()

    async def _binance_rest_fallback(self, auth, session, method, path, params=None, signed=False):
        """Binance REST 降级通道：WS 未连接时直接用 aiohttp 发请求"""
        if params is None:
            params = {}
        _method = method.upper()
        _base_params = dict(params or {})
        _needs_signature = signed or ('signature' in _base_params)
        _max_retries = 3
        url = f"{auth.rest_base}{path}"
        for _attempt in range(_max_retries):
            _params = dict(_base_params)
            if _needs_signature:
                _params.pop('timestamp', None)
                _params.pop('signature', None)
                _params = auth.sign(_params)
            try:
                if _method == "GET":
                    _ctx = session.get(url, params=_params)
                elif _method == "POST":
                    _ctx = session.post(url, params=_params)
                elif _method == "DELETE":
                    _ctx = session.delete(url, params=_params)
                else:
                    logger.error(f"[Binance REST 降级] 不支持的 HTTP 方法: {method}")
                    return None

                async with _ctx as resp:
                    _content_type = resp.headers.get('Content-Type', '')
                    try:
                        _body = await resp.read()
                        _body_head = _body[:200].decode('utf-8', errors='replace').replace('\n', ' ') if _body else ''
                        _looks_json = (
                            not _body or
                            'json' in _content_type.lower() or
                            _body.lstrip().startswith((b'{', b'['))
                        )
                        if not _looks_json:
                            raise ValueError(
                                f"非JSON响应 HTTP={resp.status} content-type={_content_type or '-'} body={_body_head!r}")
                        import orjson as _orjson
                        _result = _orjson.loads(_body) if _body else {}
                    except Exception as _json_err:
                        if _attempt < _max_retries - 1 and resp.status in (418, 429, 500, 502, 503, 504):
                            _delay = min(2.0, 0.4 * (2 ** _attempt))
                            logger.warning(
                                f"[Binance REST 降级] {method} {path} 响应解析失败: {_json_err}，"
                                f"第 {_attempt + 1}/{_max_retries} 次重试")
                            await asyncio.sleep(_delay)
                            continue
                        raise
                    _code = _result.get('code') if isinstance(_result, dict) else None
                    if _needs_signature and _code == -1021 and _attempt < _max_retries - 1:
                        await auth.sync_server_time()
                        _delay = min(1.0, 0.2 * (2 ** _attempt))
                        logger.warning(
                            f"[Binance REST 降级] {method} {path} timestamp 超出 recvWindow，"
                            f"已重同步 serverTime，第 {_attempt + 1}/{_max_retries} 次重试")
                        await asyncio.sleep(_delay)
                        continue
                    return _result
            except Exception as e:
                if _attempt < _max_retries - 1:
                    _delay = min(2.0, 0.4 * (2 ** _attempt))
                    logger.warning(
                        f"[Binance REST 降级] {method} {path} 失败: {e}，"
                        f"第 {_attempt + 1}/{_max_retries} 次重试")
                    await asyncio.sleep(_delay)
                    continue
                logger.error(f"[Binance REST 降级] {method} {path} 失败: {e}")
                return None
        return None
