"""engine/balance_mixin.py — 余额快照 + 盈亏报告 + 回撤追踪"""
from __future__ import annotations
import asyncio
import logging
import time
import os
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    pass

import aiohttp
import db_store
from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


class BalanceMixin:
    """Mixin: 余额快照 + 盈亏报告 + 回撤追踪"""

    async def _init_account_equity_store(self) -> None:
        """初始化每日账户总权益 SQLite 存储。"""
        await self._account_equity_store.init()

    async def _record_daily_account_equity(self, snapshot: dict = None, force: bool = False) -> bool:
        """按 UTC 日期写入每日账户总权益快照。

        返回 True 表示本次已写入；查询不完整或当天已写过则返回 False。
        """
        from datetime import datetime, timezone
        _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if not force and getattr(self, '_account_equity_date', None) == _today:
            return False

        snap = snapshot or await self._snapshot_balances()
        if not snap.get('deribit_ok') or not snap.get('binance_ok'):
            logger.warning(
                f"账户权益快照跳过: Deribit/Binance 查询不完整 "
                f"(deribit_ok={snap.get('deribit_ok')}, binance_ok={snap.get('binance_ok')})")
            return False

        btc_price = Decimal(str(snap.get('btc_price', '0') or '0'))
        if btc_price <= 0:
            logger.warning("账户权益快照跳过: BTC/USD 参考价不可用")
            return False

        deribit_equity = Decimal(str(snap.get('deribit_equity', '0') or '0'))
        deribit_balance = Decimal(str(snap.get('deribit_balance', '0') or '0'))
        binance_equity = Decimal(str(snap.get('binance_equity', '0') or '0'))
        binance_balance = Decimal(str(snap.get('binance_balance', '0') or '0'))
        total_usd = deribit_equity * btc_price + binance_equity
        total_btc = deribit_equity + (binance_equity / btc_price)

        record = {
            'timestamp': float(snap.get('timestamp', time.time()) or time.time()),
            'deribit_equity_btc': float(deribit_equity),
            'deribit_balance_btc': float(deribit_balance),
            'binance_equity_usdt': float(binance_equity),
            'binance_balance_usdt': float(binance_balance),
            'btc_usd_price': float(btc_price),
            'total_equity_usd': float(total_usd),
            'total_equity_btc': float(total_btc),
        }
        await self._account_equity_store.upsert(_today, record)
        self._account_equity_date = _today
        logger.info(
            f"📈 每日账户权益快照已写入: {_today} | "
            f"USD={total_usd:.2f} | BTC={total_btc:.6f} | BTC/USD={btc_price:.0f}")
        return True

    async def _init_drawdown_store(self) -> None:
        """🌟 初始化 daily_drawdown SQLite 存储 + 读回今日峰值
        只在首次启动调用一次 (重连不重复做)。
        🌟 2026-04-18: 删除 CSV → SQLite import 逻辑, SQLite 为唯一权威数据源.
        """
        from datetime import datetime, timezone
        _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        await self._drawdown_store.init()
        logger.info(f"📊 drawdown SQLite 存储就绪: {self._drawdown_db_path}")
        _today_row = await self._drawdown_store.get_by_date(_today)
        if _today_row:
            self._drawdown_date = _today
            self._drawdown_max_single_loss = float(_today_row['max_single_loss_usd'] or 0)
            self._drawdown_max_single_gain = float(_today_row['max_single_gain_usd'] or 0)
            self._drawdown_max_total_loss  = float(_today_row['max_total_loss_usd']  or 0)
            self._drawdown_max_total_gain  = float(_today_row['max_total_gain_usd']  or 0)
            # 🌟 新增字段: SQLite 中若无此列 (老 schema), get() 返回 None → 默认 0
            self._drawdown_max_daily_net_loss = float(_today_row.get('max_daily_net_loss_usd') or 0)
            logger.info(
                f"📊 drawdown 重启恢复 (SQLite): 今日 ({_today}) "
                f"single(loss={self._drawdown_max_single_loss}/gain={self._drawdown_max_single_gain}), "
                f"total(loss={self._drawdown_max_total_loss}/gain={self._drawdown_max_total_gain}), "
                f"daily_net_loss={self._drawdown_max_daily_net_loss}")
        else:
            self._drawdown_date = _today

    def _update_daily_drawdown(self, total_unrealized_pnl_usd: float) -> None:
        """🌟 采集每日最大浮盈/浮亏, 供 Monitor 面板柱状图对比止损阈值是否合理

        每秒被 _check_global_risk 调用; 内部每分钟 + 跨日切换时落盘 SQLite。

        🌟 v4 口径 (用户 2026-04-18 明确定义):
          所有柱都基于 combo-level 的 _last_combo_pnl_usd (扣开仓费+预估平仓费后),
          差异仅在"max 单个 combo" vs "sum 所有 combo".

        采集 5 个指标:
          浮亏侧 (与风控阈值对比):
            1. 单组合最大浮亏 = max(abs(combo.pnl)) 对 pnl<0 的 combo
               → 任意时刻某一个组合亏得最多的值 (绝对值)
               → 对比 hard_stop_loss_usd=$300 (同口径)
               → 触发源: _check_exit_opportunity 里 `combo_pnl_usd <= -hard_stop_loss`
            2. 全局最大浮亏 = abs(sum(combo.pnl)) 对所有活跃 combo
               → 任意时刻所有组合合计浮亏最多的值 (绝对值)
               → 与 single 同口径, 无对应阈值 (global_hard_stop_loss 已移除)
               → 1 combo 时必然 global == single, 多 combo 时 global >= single
            3. 今日最大净亏损 = -(realized + unrealized) 峰值
               → 对比 daily_loss_limit_usd=$2000 (混合口径: 已实现+账户级浮动)
               → 触发源: _check_global_risk 里 `_today_net_loss >= _daily_limit`
          浮盈侧 (参考, 判断盈利天花板):
            4. 单组合最大浮盈 = max(combo.pnl) 对 pnl>0 的 combo
            5. 全局最大浮盈 = sum(combo.pnl) > 0 时的峰值

        关键示例 (用户说明):
          3 组合 PnL = [-100, +50, -100] 时:
            single_loss = 100 (组合 1 或 3)
            single_gain = 50  (组合 2)
            global_loss = abs(-100 + 50 - 100) = 150
        """
        from datetime import datetime, timezone
        _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # 跨日切换 → 先固化昨天最终值, 再重置
        # 🌟 POC: 走 _persist_drawdown_async 主写 SQLite
        if self._drawdown_date != _today:
            if self._drawdown_date is not None:
                _yd_date = self._drawdown_date
                _yd_sl = self._drawdown_max_single_loss
                _yd_sg = self._drawdown_max_single_gain
                _yd_tl = self._drawdown_max_total_loss
                _yd_tg = self._drawdown_max_total_gain
                _yd_dnl = self._drawdown_max_daily_net_loss
                try:
                    asyncio.create_task(self._persist_drawdown_async(
                        _yd_date, _yd_sl, _yd_sg, _yd_tl, _yd_tg, _yd_dnl))
                except Exception as _e:
                    logger.warning(f"drawdown 跨日固化任务调度失败: {_e}")
            self._drawdown_date = _today
            self._drawdown_max_single_loss = 0.0
            self._drawdown_max_single_gain = 0.0
            self._drawdown_max_total_loss = 0.0
            self._drawdown_max_total_gain = 0.0
            self._drawdown_max_daily_net_loss = 0.0
            self._drawdown_last_persist_ts = 0.0  # 强制下次立即落盘

        # === 柱 1+2+3: 基于活跃 combo PnL 的三个指标 (全部 combo-level 扣费后) ===
        # 🌟 v4 口径 (用户 2026-04-18 明确定义):
        #   柱 1 "单组合最大浮亏" = max(abs(combo.pnl)) 对 pnl<0 的 combo
        #        → 任意时刻某一个组合亏得最多的数值, 绝对值
        #        → 对比 hard_stop_loss_usd (同口径: per-combo 扣费后)
        #   柱 2 "单组合最大浮盈" = max(combo.pnl) 对 pnl>0 的 combo
        #        → 任意时刻某一个组合赚得最多的数值 (参考, 无阈值)
        #   柱 3 "全局最大浮亏" = abs(sum(combo.pnl)) 对所有活跃 combo
        #        → 任意时刻所有组合合计浮亏最多的数值, 绝对值
        #        → 与 single 同口径 (都是 combo 扣费后), 只是"max 单个" vs "求和全部"
        #        → 1 个 combo 时必然 global == single (符合用户直觉的不变式)
        #
        # 回归修复: 只看当前活跃组合, 避免已退出组合的陈旧 _last_combo_pnl_usd
        #   污染跨日统计 (arbitrage_states 清理窗口 5 分钟, 跨日后陈旧值会被误采)
        #
        # _has_any_pnl 护栏: combo 刚开仓还没跑完 exit_check 时 _last_combo_pnl_usd=None,
        #   此时跳过 global 更新 (而非用账户级 total_unrealized_pnl_usd fallback),
        #   保证 single 和 global 永远 combo 级同口径.
        _ACTIVE_STATES = ('position_open', 'executing', 'exiting')
        _worst_single_loss = 0.0
        _best_single_gain = 0.0
        _total_combo_pnl = 0.0   # 所有活跃组合 PnL 代数和 (含浮盈抵消)
        _has_any_pnl = False
        for _st in self.arbitrage_states.values():
            if getattr(_st, 'state', '') not in _ACTIVE_STATES:
                continue
            _pnl = getattr(_st, '_last_combo_pnl_usd', None)
            if _pnl is None:
                continue
            _has_any_pnl = True
            _total_combo_pnl += float(_pnl)
            if _pnl < 0:
                if abs(_pnl) > _worst_single_loss:
                    _worst_single_loss = abs(_pnl)
            elif _pnl > 0:
                if _pnl > _best_single_gain:
                    _best_single_gain = _pnl
        # 柱 1+2: 单组合 max/min 峰值
        if _worst_single_loss > self._drawdown_max_single_loss:
            self._drawdown_max_single_loss = _worst_single_loss
        if _best_single_gain > self._drawdown_max_single_gain:
            self._drawdown_max_single_gain = _best_single_gain
        # 柱 3: 当日最大亏损 = Σ combo.pnl 取负值时的绝对值峰值 (sum all, 会被浮盈抵消)
        # 1 combo 时恒等 single_loss; 多 combo 同时存在且有浮盈时, 可能被抵消为 0
        if _has_any_pnl:
            if _total_combo_pnl < 0:
                _abs_total = abs(_total_combo_pnl)
                if _abs_total > self._drawdown_max_total_loss:
                    self._drawdown_max_total_loss = _abs_total
            elif _total_combo_pnl > 0:
                if _total_combo_pnl > self._drawdown_max_total_gain:
                    self._drawdown_max_total_gain = _total_combo_pnl

        # === 日损柱 (已实现+浮动, 混合口径) → 对比 daily_loss_limit_usd ===
        # 🌟 与 line 8826-8829 的触发公式完全一致:
        #   _today_net_pnl = self._daily_realized_pnl + total_unrealized_pnl_usd
        #   _today_net_loss = -_today_net_pnl  (正值=净亏损)
        # 跨日自动重置: _daily_realized_pnl 在 line 8819-8820 按 UTC date 清零, 与 _drawdown_date 同步
        _today_net_pnl = float(getattr(self, '_daily_realized_pnl', 0.0)) + float(total_unrealized_pnl_usd)
        if _today_net_pnl < 0:
            _abs_daily_loss = abs(_today_net_pnl)
            if _abs_daily_loss > self._drawdown_max_daily_net_loss:
                self._drawdown_max_daily_net_loss = _abs_daily_loss

        # 落盘节流: 每 60 秒写一次
        # 🌟 POC: _persist_drawdown_async 主走 SQLite (内部用 to_thread)
        _now = time.time()
        if _now - self._drawdown_last_persist_ts >= 60:
            self._drawdown_last_persist_ts = _now  # 先更新时间戳, 避免异步落盘中重复触发
            _date = self._drawdown_date
            _sl = self._drawdown_max_single_loss
            _sg = self._drawdown_max_single_gain
            _tl = self._drawdown_max_total_loss
            _tg = self._drawdown_max_total_gain
            _dnl = self._drawdown_max_daily_net_loss
            try:
                asyncio.create_task(self._persist_drawdown_async(
                    _date, _sl, _sg, _tl, _tg, _dnl))
            except Exception as _e:
                logger.info(f"drawdown 落盘任务调度失败 (非致命): {_e}")

    async def _persist_drawdown_async(self, date_str: str,
                                        max_single_loss: float, max_single_gain: float,
                                        max_total_loss: float, max_total_gain: float,
                                        max_daily_net_loss: float = 0.0) -> None:
        """drawdown 落盘: 只走 SQLite (WAL 模式), 失败则告警, 不再降级 CSV.

        🌟 2026-04-18 简化: 删除 CSV fallback. SQLite 是唯一权威数据源.
        被 _update_daily_drawdown 里 asyncio.create_task 异步调用, 不阻塞巡检.
        """
        try:
            await self._drawdown_store.upsert(
                date_str, max_single_loss, max_single_gain,
                max_total_loss, max_total_gain, max_daily_net_loss)
        except Exception as _sqlite_err:
            # 🌟 只记日志, 不发 Telegram (drawdown 是观察指标, 丢几条落盘不影响交易)
            logger.error(f"🚨 drawdown SQLite 写入失败: {_sqlite_err}")

    async def _get_account_equity_btc_usd_price(self) -> Decimal:
        """账户权益换算使用现货/指数口径，避免到期期货 mark 污染 BTC/USD。"""
        _perp = (
            self.binance_matcher.perpetual_symbol
            if hasattr(self, 'binance_matcher') else f'{self.target_currency}USDT'
        )
        if self.binance_ws and getattr(self, 'binance_connected', False):
            try:
                _bn_ob = self.binance_ws.order_books.get(_perp)
                _mid = getattr(_bn_ob, 'mid_price', None) if _bn_ob else None
                if _mid is not None and _mid > 0:
                    return Decimal(str(_mid))

                _mark = Decimal(str(self.binance_ws.mark_prices.get(_perp, '0') or '0'))
                if _mark > 0:
                    return _mark

                _last = Decimal(str(self.binance_ws.last_prices.get(_perp, '0') or '0'))
                if _last > 0:
                    return _last
            except Exception:
                pass

        try:
            _idx_resp = await self.client.send_request({
                "jsonrpc": "2.0", "id": self.client._get_next_request_id(),
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

    async def _snapshot_balances(self) -> dict:
        """查询 Deribit + Binance 当前余额快照"""
        snap = {'deribit_equity': Decimal('0'), 'deribit_balance': Decimal('0'),
                'binance_balance': Decimal('0'), 'binance_equity': Decimal('0'),
                'btc_price': Decimal('0'), 'timestamp': time.time(),
                'deribit_ok': False, 'binance_ok': False}
        # Deribit
        try:
            resp = await self.client.send_request({
                "jsonrpc": "2.0", "id": self.client._get_next_request_id(),
                "method": "private/get_account_summary",
                "params": {"currency": self.target_currency}
            }, is_private=True)
            if 'result' in resp:
                snap['deribit_equity'] = Decimal(str(resp['result'].get('equity', 0)))
                snap['deribit_balance'] = Decimal(str(resp['result'].get('balance', 0)))
                snap['deribit_ok'] = True
        except Exception as e:
            logger.warning(f"Deribit 余额查询失败: {e}")
        # Binance
        if self.binance_ws and self.binance_connected:
            try:
                acct = await self.binance_ws.get_account_info()
                if acct:
                    snap['binance_balance'] = Decimal(str(acct.get('totalWalletBalance', '0')))
                    snap['binance_equity'] = Decimal(str(acct.get('totalMarginBalance', '0')))
                    snap['binance_ok'] = True
            except Exception as e:
                logger.warning(f"Binance 余额查询失败: {e}")
        elif getattr(self, 'binance_auth', None):
            # WS 未连接时 REST 降级
            try:
                async with aiohttp.ClientSession(
                    headers=self.binance_auth.headers, timeout=aiohttp.ClientTimeout(total=10)) as s:
                    params = self.binance_auth.sign({})
                    async with s.get(f"{self.binance_auth.rest_base}/fapi/v2/account", params=params) as r:
                        data = await r.json()
                        if data and 'totalWalletBalance' in data:
                            snap['binance_balance'] = Decimal(str(data.get('totalWalletBalance', '0')))
                            snap['binance_equity'] = Decimal(str(data.get('totalMarginBalance', '0')))
                            snap['binance_ok'] = True
            except Exception as e:
                logger.warning(f"Binance REST 余额查询失败: {e}")
        snap['btc_price'] = await self._get_account_equity_btc_usd_price()
        return snap

    def _calc_positions_pnl_summary(self) -> str:
        """计算所有活跃组合的利润概况（用于持仓上限心跳日志）"""
        closeable = []   # ✅ 达到平仓目标
        converging = []  # 🔄 正收益但未达标
        losing = []      # 📉 浮亏中
        total_pnl = Decimal('0')
        threshold = self.min_profit_threshold

        for es, st in list(self.arbitrage_states.items()):
            if st.state != 'position_open':
                continue
            combo = self.arbitrage_combinations.get(es)
            if not combo:
                continue

            c_t = self.client.tickers.get(combo['call'])
            p_t = self.client.tickers.get(combo['put'])
            f_t = self.client.tickers.get(combo['future'])
            if not all([c_t, p_t]):
                continue

            f_e = st.entry_prices.get('future', Decimal('0'))
            c_e = st.entry_prices.get('call', Decimal('0'))
            p_e = st.entry_prices.get('put', Decimal('0'))
            amt = st.entry_amount
            fsz = st.future_size_usd or Decimal('0')
            if f_e == 0 or amt == 0:
                continue

            # Binance 盘口价格 (跨所模式)
            _bn_px = Decimal('0')
            if st.binance_filled_qty > 0 and self.binance_ws:
                _chk_perp = st.binance_future_symbol or (self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT')
                _ob = self.binance_ws.order_books.get(_chk_perp)
                if _ob and _ob.mid_price is not None and _ob.mid_price > 0:
                    _bn_px = _ob.mid_price

            try:
                if st.strategy_type == 'sell_future_buy_synthetic':
                    f_exit = _bn_px if _bn_px > 0 else (f_t.ask if f_t else Decimal('0'))
                    c_exit = c_t.bid if c_t.bid > 0 else Decimal('0')
                    p_exit = p_t.ask if p_t.ask > 0 else Decimal('999')
                    f_pnl = fsz * (Decimal('1') / f_exit - Decimal('1') / f_e) if f_exit > 0 else Decimal('0')
                    o_pnl = ((c_exit - c_e) + (p_e - p_exit)) * amt
                else:
                    f_exit = _bn_px if _bn_px > 0 else (f_t.bid if f_t else Decimal('0'))
                    c_exit = c_t.ask if c_t.ask > 0 else Decimal('999')
                    p_exit = p_t.bid if p_t.bid > 0 else Decimal('0')
                    f_pnl = fsz * (Decimal('1') / f_e - Decimal('1') / f_exit) if f_exit > 0 else Decimal('0')
                    o_pnl = ((c_e - c_exit) + (p_exit - p_e)) * amt

                mark = _bn_px if _bn_px > 0 else (f_t.mid_price if f_t else Decimal('0'))
                if mark <= 0:
                    continue
                est_pnl = (f_pnl + o_pnl) * mark
            except Exception:
                continue

            total_pnl += est_pnl
            exp, stk = es
            label = f"{exp}-{int(stk)}"
            pnl_val = float(est_pnl)

            if est_pnl >= threshold:
                closeable.append((label, pnl_val))  # ⏳ 平仓中（已达标，monitor_positions 正在处理）
            elif est_pnl > 0:
                converging.append((label, pnl_val))
            else:
                losing.append((label, pnl_val))

        # 按利润排序（高→低）
        closeable.sort(key=lambda x: x[1], reverse=True)
        converging.sort(key=lambda x: x[1], reverse=True)
        losing.sort(key=lambda x: x[1])

        lines = [f"  📊 利润概况 (参考门槛≥{threshold} USD):"]
        if closeable:
            items = " | ".join(f"{lb}:{pv:+.1f}" for lb, pv in closeable)
            lines.append(f"  ⏳ 平仓中({len(closeable)}): {items}")
        if converging:
            items = " | ".join(f"{lb}:{pv:+.1f}" for lb, pv in converging)
            lines.append(f"  🔄 收敛中({len(converging)}): {items}")
        if losing:
            items = " | ".join(f"{lb}:{pv:+.1f}" for lb, pv in losing)
            lines.append(f"  📉 浮亏中({len(losing)}): {items}")
        lines.append(f"  合计: {float(total_pnl):+.2f} USD | 可平仓: {len(closeable)}个")
        return "\n".join(lines)

    def _format_profit_report(self, start_snap: dict, current_snap: dict) -> str:
        """生成盈利报告文本"""
        d_start = start_snap.get('deribit_equity', Decimal('0'))
        d_now = current_snap.get('deribit_equity', Decimal('0'))
        d_change = d_now - d_start

        b_start = start_snap.get('binance_equity', Decimal('0'))
        b_now = current_snap.get('binance_equity', Decimal('0'))
        b_change = b_now - b_start

        btc_price = current_snap.get('btc_price', Decimal('0'))
        d_change_usd = d_change * btc_price if btc_price > 0 else Decimal('0')
        total_usd = d_change_usd + b_change

        runtime_secs = current_snap.get('timestamp', 0) - start_snap.get('timestamp', 0)
        hours = runtime_secs / 3600
        mins = (runtime_secs % 3600) / 60

        lines = [
            f"📊 盈利报告",
            f"{'='*30}",
            f"运行时长: {int(hours)}h {int(mins)}m",
            f"成交笔数: {self.trades_executed}",
            f"",
            f"--- Deribit ({self.target_currency}) ---",
            f"启动权益: {d_start:.6f}",
            f"当前权益: {d_now:.6f}",
            f"变化: {'+' if d_change >= 0 else ''}{d_change:.6f} {self.target_currency}",
            f"折USD: {'+' if d_change_usd >= 0 else ''}{d_change_usd:.2f} USD",
            f"",
            f"--- Binance (USDT) ---",
            f"启动权益: {b_start:.2f}",
            f"当前权益: {b_now:.2f}",
            f"变化: {'+' if b_change >= 0 else ''}{b_change:.2f} USDT",
            f"",
            f"--- 合计 ---",
            f"💰 总盈亏: {'+' if total_usd >= 0 else ''}{total_usd:.2f} USD",
        ]
        return "\n".join(lines)

    def _realized_summary_since(self, start_ts: float) -> dict:
        """统计 SQLite 中自启动以来的已平仓净利润（交易口径）"""
        try:
            return self._trade_store.realized_summary_since_sync(start_ts)
        except Exception as e:
            logger.info(f"读取SQLite已平仓统计失败: {e}")
            return {"available": False, "close_count": 0, "realized_pnl_usd": 0.0}

    def _format_balance_change(self, start_snap: dict, current_snap: dict) -> str:
        """生成余额变化报告（基于 balance，仅反映已实现盈亏）"""
        d_start = start_snap.get('deribit_balance', Decimal('0'))
        d_now = current_snap.get('deribit_balance', Decimal('0'))
        d_change = d_now - d_start

        b_start = start_snap.get('binance_balance', Decimal('0'))
        b_now = current_snap.get('binance_balance', Decimal('0'))
        b_change = b_now - b_start

        btc_price = current_snap.get('btc_price', Decimal('0'))
        d_change_usd = d_change * btc_price if btc_price > 0 else Decimal('0')
        total_usd = d_change_usd + b_change

        runtime_secs = current_snap.get('timestamp', 0) - start_snap.get('timestamp', 0)
        hours = runtime_secs / 3600
        mins = (runtime_secs % 3600) / 60
        _start_ts = float(start_snap.get('timestamp', 0) or 0)
        _db_summary = self._realized_summary_since(_start_ts)
        _start_open = int(getattr(self, '_start_open_positions', 0))

        lines = [
            f"📊 余额变化 (已实现)",
            f"{'='*30}",
            f"运行时长: {int(hours)}h {int(mins)}m",
            f"成交笔数: {self.trades_executed}",
            f"",
            f"--- Deribit ({self.target_currency}) ---",
            f"启动余额: {d_start:.6f}",
            f"当前余额: {d_now:.6f}",
            f"变化: {'+' if d_change >= 0 else ''}{d_change:.6f} {self.target_currency}",
            f"折USD: {'+' if d_change_usd >= 0 else ''}{d_change_usd:.2f} USD",
            f"",
            f"--- Binance (USDT) ---",
            f"启动余额: {b_start:.2f}",
            f"当前余额: {b_now:.2f}",
            f"变化: {'+' if b_change >= 0 else ''}{b_change:.2f} USDT",
            f"",
            f"--- 合计 ---",
            f"💰 已实现盈亏: {'+' if total_usd >= 0 else ''}{total_usd:.2f} USD",
        ]
        if _db_summary.get("available"):
            _db_realized = float(_db_summary.get("realized_pnl_usd", 0.0))
            _db_count = int(_db_summary.get("close_count", 0))
            lines.extend([
                f"",
                f"--- 交易口径对照 ---",
                f"已平仓净利(完整往返): {'+' if _db_realized >= 0 else ''}{_db_realized:.2f} USD",
                f"已平仓笔数: {_db_count}",
            ])
        if _start_open > 0:
            lines.extend([
                f"",
                f"⚠️ 口径提示: 启动快照时系统已继承 {_start_open} 个持仓，",
                f"账户余额变化与单笔完整往返净利可能暂时不一致。",
            ])
        # 🌟 Plan Bug #1 修复: 明确此报告是账户级, 防止与单笔交割 PnL 混淆
        _active_combos = sum(1 for s in self.arbitrage_states.values()
                              if getattr(s, 'state', '') == 'position_open')
        lines.extend([
            "",
            "⚠️ 以上为全局账户余额变化 (含所有持仓 funding/滑点/Deribit 其他费用)",
            "   单仓位盈亏请参考交割通知或交易记录, 数字通常不完全相等",
        ])
        if _active_combos > 0:
            lines.append(f"   当前活跃持仓: {_active_combos} 个组合 (浮盈浮亏尚未计入)")
        return "\n".join(lines)

    def _print_final_stats(self):
        """打印最终统计"""
        logger.info("=" * 60)
        logger.info("最终统计:")
        logger.info(f"  总扫描次数: {self.scan_count}")
        logger.info(f"  发现机会数: {self.opportunities_found}")
        logger.info(f"  成交交易数: {self.trades_executed}")
        logger.info(f"  当前持仓锁: {len(self.position_locks)}")
        logger.info("=" * 60)
