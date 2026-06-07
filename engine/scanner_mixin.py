"""engine/scanner_mixin.py — 市场扫描 + 合约刷新 + 费率同步"""
from __future__ import annotations
import logging
import time
import asyncio
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set, Any
from collections import defaultdict

if TYPE_CHECKING:
    pass

import binance_futures
from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


class ScannerMixin:
    """Mixin: 全市场套利扫描 + 合约刷新 + 费率同步"""

    def _record_scan_maker_top_profits(self, maker_profits: List[float]) -> None:
        """Aggregate scan maker profits and emit one Top5 line per configured window."""
        now = time.time()
        try:
            interval = float(getattr(self, 'maker_top5_log_interval_seconds', 300.0))
        except Exception:
            interval = 300.0
        if interval <= 0:
            interval = 300.0

        samples = getattr(self, '_scan_maker_top_profit_samples', None)
        if samples is None:
            samples = []
            self._scan_maker_top_profit_samples = samples

        window_started = float(getattr(self, '_scan_maker_top_profit_window_started', 0.0) or 0.0)
        if not maker_profits and not samples:
            return

        if window_started <= 0:
            self._scan_maker_top_profit_window_started = now

        if maker_profits:
            samples.extend(float(p) for p in maker_profits)
        if now - float(getattr(self, '_scan_maker_top_profit_window_started', now)) < interval:
            return

        maker_top_5 = sorted(samples, reverse=True)[:5]
        maker_top_5_formatted = [round(p, 2) for p in maker_top_5]
        logger.info(f"测算利润【Maker Top5/{int(interval)}s】: {maker_top_5_formatted} USD")
        self._scan_maker_top_profit_samples = []
        self._scan_maker_top_profit_window_started = now

    def _estimate_scan_maker_open_fee_usd(
            self,
            binance_price: Decimal,
            option_taker_price: Decimal,
            amount: Decimal,
            option_maker_price: Decimal = None) -> Decimal:
        """扫描阶段 Maker 口径开仓费：Binance taker + 期权 Maker/Taker 腿。

        锚定腿按 Maker 挂单进入候选，扫描阶段不再用三腿全 taker 费用把候选提前杀掉。
        真实成交后仍由验证/VWAP/锚定腿复核负责兜底。
        """
        if option_maker_price is None:
            option_maker_price = Decimal('0')
        try:
            _opt_fee_btc = self.fee_calculator.calculate_option_fee(
                binance_price, option_taker_price, amount, is_taker=True)
            if option_maker_price and option_maker_price > 0:
                _opt_fee_btc += self.fee_calculator.calculate_option_fee(
                    binance_price, option_maker_price, amount, is_taker=False)
            _opt_fee_usd = _opt_fee_btc * binance_price
        except Exception:
            _opt_fee_usd = amount * Decimal('0.0003') * binance_price

        try:
            _bn_fee_usd = self.trade_executor._calculate_binance_fee_usdt(
                binance_price, amount, is_taker=True)
        except Exception:
            _calc = getattr(self, 'binance_fee_calc', None)
            _rate = Decimal(str(getattr(_calc, 'taker_rate', Decimal('0.0004'))))
            _bn_fee_usd = binance_price * amount * _rate
        return Decimal(str(_opt_fee_usd)) + Decimal(str(_bn_fee_usd))

    async def _calculate_three_leg_scan_vwap(
        self, strategy_type: str,
        future_name: str, call_name: str, put_name: str,
        option_amount: Decimal, future_amount_usd: Decimal,
        call_ticker, put_ticker, binance_symbol: str = ""
    ) -> Optional[Dict[str, Decimal]]:
        """扫描专用三腿混合定价：
        - 锚定腿（spread 更大的期权）: mid-price，模拟 Maker 挂单
        - Taker 腿（另一期权 + 期货）: 自适应（首档够用首档，不够才 VWAP）
        比全 Taker VWAP 更贴近实际执行定价，减少假阴性漏掉可盈利机会。"""
        if strategy_type == 'sell_future_buy_synthetic':
            f_side, c_side, p_side = 'sell', 'buy', 'sell'
        else:
            f_side, c_side, p_side = 'buy', 'sell', 'buy'

        # 确定锚定腿（与 _execute_maker2_taker1_strategy 相同逻辑：spread 更大 → Maker）
        c_spread = call_ticker.ask - call_ticker.bid if call_ticker.ask > 0 and call_ticker.bid > 0 else Decimal('999')
        p_spread = put_ticker.ask - put_ticker.bid if put_ticker.ask > 0 and put_ticker.bid > 0 else Decimal('999')
        call_is_anchor = (c_spread >= p_spread)

        # 锚定腿用激进价（Maker 挂在靠近对手方的位置，提高被扫概率）
        # 🌟 使用 spread 自适应 aggression:
        #   upper = maker_price_aggression, 再根据期权 bid-ask 宽度动态下调
        _ref_price = Decimal('0')
        try:
            if binance_symbol and self.binance_ws:
                _bn_ob = self.binance_ws.order_books.get(binance_symbol)
                if _bn_ob and _bn_ob.mid_price is not None and _bn_ob.mid_price > 0:
                    _ref_price = _bn_ob.mid_price
            if _ref_price <= 0:
                _ft = self.client.tickers.get(future_name)
                if _ft and _ft.mid_price > 0:
                    _ref_price = _ft.mid_price
        except Exception:
            pass
        if _ref_price <= 0:
            _ref_price = Decimal('75000')  # 最终兜底, 仅用于 aggression 估算
        if call_is_anchor:
            _aggr = self._compute_dynamic_aggression(call_ticker.bid, call_ticker.ask, _ref_price)
            # Call 锚定: 根据 c_side 确定"对手方"方向
            if c_side == 'buy':
                c_price = call_ticker.bid + (call_ticker.ask - call_ticker.bid) * _aggr
            else:
                c_price = call_ticker.ask - (call_ticker.ask - call_ticker.bid) * _aggr
            p_price = await self._calculate_adaptive_price(put_name, p_side, option_amount)
        else:
            _aggr = self._compute_dynamic_aggression(put_ticker.bid, put_ticker.ask, _ref_price)
            c_price = await self._calculate_adaptive_price(call_name, c_side, option_amount)
            if p_side == 'buy':
                p_price = put_ticker.bid + (put_ticker.ask - put_ticker.bid) * _aggr
            else:
                p_price = put_ticker.ask - (put_ticker.ask - put_ticker.bid) * _aggr

        # 期货腿：跨所模式必须使用 Binance 永续盘口，不回退 Deribit 远期
        if binance_symbol and self.binance_ws:
            _scan_ob = self.binance_ws.order_books.get(binance_symbol)
            if (not _scan_ob or _scan_ob.mid_price is None or _scan_ob.mid_price <= 0 or
                    (_scan_ob.update_time and (time.time() - _scan_ob.update_time) > 30)):
                return None
            f_price = _scan_ob.best_bid if f_side == 'sell' else _scan_ob.best_ask
        else:
            f_price = await self._calculate_adaptive_price(future_name, f_side, future_amount_usd)

        if f_price is None or c_price is None or p_price is None:
            return None

        return {'future': f_price, 'call': c_price, 'put': p_price}

    async def _refresh_volume_filter(self):
        """周期性刷新24h交易量缓存，过滤零交易量合约"""
        try:
            resp = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_book_summary_by_currency",
                "params": {"currency": self.target_currency, "kind": "option"}
            })
            active = set()
            low_count = 0
            _min_vol = self.min_option_volume
            for s in resp.get('result', []):
                vol = float(s.get('volume', 0) or 0)
                if vol >= _min_vol and vol > 0:
                    active.add(s['instrument_name'])
                else:
                    low_count += 1
            self._active_options = active
            self._volume_refresh_time = time.time()
            logger.info(f"📊 交易量刷新 (阈值≥{_min_vol}): {len(active)} 活跃 / {low_count} 低量期权")
        except Exception as e:
            logger.warning(f"⚠️ 刷新交易量失败({e})，沿用上次缓存")

    async def _refresh_instruments(self):
        """周期性发现新上线的合约（新到期日/新行权价），自动添加到套利组合并订阅行情"""
        try:
            # 1. 获取当前所有未过期期货
            futures_response = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_instruments",
                "params": {"currency": self.target_currency, "kind": "future", "expired": False}
            })

            future_by_expiry = {}
            _sorted_futures_refresh = sorted(futures_response.get('result', []),
                                             key=lambda x: x.get('expiration_timestamp', 0))
            for item in _sorted_futures_refresh[0:self.futures_numbers]:
                parsed = self._parse_instrument_name(item['instrument_name'])
                if parsed and parsed[2] is None:
                    _, expiry, _, _ = parsed
                    if expiry == 'PERPETUAL':
                        continue
                    future_by_expiry[expiry] = item['instrument_name']
                    c_size = item.get('contract_size', 10)
                    self.contract_sizes[item['instrument_name']] = Decimal(str(c_size))
                    self.client.instrument_cache[item['instrument_name']] = item

            # 2. 一次性获取所有未过期期权（比 initialize 按到期日循环更高效）
            options_response = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_instruments",
                "params": {"currency": self.target_currency, "kind": "option", "expired": False}
            })

            calls_by_key = {}
            puts_by_key = {}
            _future_prices = {}  # 缓存每个到期日的期货价格，避免重复查询

            for item in options_response.get('result', []):
                parsed = self._parse_instrument_name(item['instrument_name'])
                if not parsed:
                    continue
                currency, item_expiry, strike, option_type = parsed

                # 只保留有对应期货的到期日
                if item_expiry not in future_by_expiry:
                    continue

                # Moneyness 过滤：优先用 WS 行情，新期货回退 REST
                future_name = future_by_expiry[item_expiry]
                if item_expiry not in _future_prices:
                    future_ticker = self.client.tickers.get(future_name)
                    if future_ticker and future_ticker.mid_price and future_ticker.mid_price > 0:
                        _future_prices[item_expiry] = future_ticker.mid_price
                    else:
                        # 新期货尚无 WS 行情，回退 REST（与 initialize 一致）
                        try:
                            _tk_resp = await self.client.send_request({
                                "jsonrpc": "2.0",
                                "id": self.client._get_next_request_id(),
                                "method": "public/ticker",
                                "params": {"instrument_name": future_name}
                            })
                            _mk = Decimal(str(_tk_resp.get('result', {}).get('mark_price', 0)))
                            if _mk > 0:
                                _future_prices[item_expiry] = _mk
                        except Exception:
                            pass

                current_future_price = _future_prices.get(item_expiry)
                if not current_future_price or current_future_price <= 0:
                    continue
                price_diff_pct = abs(strike - current_future_price) / current_future_price
                if price_diff_pct > Decimal(str(self.moneyness_threshold)):
                    continue

                self.client.instrument_cache[item['instrument_name']] = item

                key = (item_expiry, strike)
                if option_type == 'call' and strike:
                    calls_by_key[key] = item['instrument_name']
                elif option_type == 'put' and strike:
                    puts_by_key[key] = item['instrument_name']

            # 3. 找出新增的 (expiry, strike) 组合
            new_combos = {}
            for key in set(calls_by_key.keys()) & set(puts_by_key.keys()):
                if key in self.arbitrage_combinations:
                    continue

                call_name = calls_by_key[key]
                put_name = puts_by_key[key]
                expiry, strike = key

                # 24h交易量过滤
                if self._active_options is not None:
                    if call_name not in self._active_options or put_name not in self._active_options:
                        continue

                _new_combo = {
                    'call': call_name,
                    'put': put_name,
                    'future': future_by_expiry[expiry]
                }
                if getattr(self, 'binance_matcher', None) is not None:
                    _bn_sym = getattr(self.binance_matcher, 'perpetual_symbol', '')
                    if _bn_sym:
                        _new_combo['binance_future'] = _bn_sym
                        _new_combo['binance_future_type'] = 'perpetual'
                new_combos[key] = _new_combo

            self._instrument_refresh_time = time.time()

            if not new_combos:
                return

            # 4. 添加到 arbitrage_combinations
            for key, combo in new_combos.items():
                self.arbitrage_combinations[key] = combo

            # 5. 订阅新合约的行情频道（去重：同一期货可能已订阅）
            new_instruments = set()
            for combo in new_combos.values():
                new_instruments.add(combo['call'])
                new_instruments.add(combo['put'])
                new_instruments.add(combo['future'])

            channels = []
            for inst in new_instruments:
                channels.append(f"ticker.{inst}.100ms")
                channels.append(f"book.{inst}.raw")

            batch_size = 20
            for i in range(0, len(channels), batch_size):
                batch = channels[i:i + batch_size]
                await self.client.send_request({
                    "jsonrpc": "2.0",
                    "id": self.client._get_next_request_id(),
                    "method": "public/subscribe",
                    "params": {"channels": batch}
                })
                await asyncio.sleep(0.5)

            logger.info(
                f"🔍 新合约发现: 新增 {len(new_combos)} 个套利组合, "
                f"订阅 {len(channels)} 个频道, 当前总数 {len(self.arbitrage_combinations)}"
            )
            for key, combo in new_combos.items():
                logger.info(f"  新组合: [{key[0]}-{key[1]}] C={combo['call']} P={combo['put']} F={combo['future']}")

        except Exception as e:
            logger.warning(f"⚠️ 新合约发现失败({e})，下次重试")
            self._instrument_refresh_time = time.time()

    def _extract_deribit_commission_pair(self, instruments: Any, kind_tag: str = "") -> Optional[Tuple[Decimal, Decimal, str]]:
        """从 Deribit public/get_instruments 提取 commission（按众数聚合，避免首条不稳定）"""
        if not isinstance(instruments, list):
            return None

        _counts: Dict[Tuple[Decimal, Decimal], int] = {}
        _sample_inst: Dict[Tuple[Decimal, Decimal], str] = {}
        for item in instruments:
            if not isinstance(item, dict):
                continue
            _mk = item.get("maker_commission")
            _tk = item.get("taker_commission")
            if _mk is None or _tk is None:
                continue
            try:
                _pair = (Decimal(str(_mk)), Decimal(str(_tk)))
            except Exception:
                continue
            _counts[_pair] = _counts.get(_pair, 0) + 1
            if _pair not in _sample_inst:
                _sample_inst[_pair] = str(item.get("instrument_name", ""))

        if not _counts:
            return None

        _ranked = sorted(_counts.items(), key=lambda kv: (-kv[1], str(kv[0][0]), str(kv[0][1])))
        (_mk, _tk), _cnt = _ranked[0]
        if len(_ranked) > 1:
            _alts = ", ".join(f"{k[0]}/{k[1]} x{v}" for k, v in _ranked[:3])
            logger.info(
                f"⚠️ Deribit {kind_tag or 'instrument'} commission 存在多组值，按众数选取 {_mk}/{_tk} x{_cnt}; 候选: {_alts}"
            )
        return _mk, _tk, _sample_inst.get((_mk, _tk), "")

    async def _sync_deribit_fee_from_instruments(self) -> bool:
        """Deribit 期权费率同步：仅使用 public/get_instruments 基础费率。
        注意：本系统只交易 Deribit 期权，不交易 Deribit 期货，因此只同步期权费率。
        """
        try:
            _option_resp = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_instruments",
                "params": {"currency": self.target_currency, "kind": "option", "expired": False}
            })

            _o_pair = self._extract_deribit_commission_pair(_option_resp.get("result", []), "option")
            if _o_pair is None:
                logger.warning("⚠️ Deribit get_instruments 未返回期权手续费字段，保留当前费率")
                return False

            _o_maker, _o_taker, _o_inst = _o_pair
            _source = "public/get_instruments"

            _cur = getattr(self.fee_calculator, 'current_rates', {}) or {}
            try:
                _cur_o_maker = Decimal(str(_cur.get('option', {}).get('maker', _o_maker)))
                _cur_o_taker = Decimal(str(_cur.get('option', {}).get('taker', _o_taker)))
            except Exception:
                _cur_o_maker, _cur_o_taker = _o_maker, _o_taker

            _eps = Decimal('0.0000000001')
            _changed = (
                abs(_o_maker - _cur_o_maker) > _eps or
                abs(_o_taker - _cur_o_taker) > _eps
            )
            self.fee_calculator.update_option_rates(_o_maker, _o_taker)
            self._deribit_fee_source = _source
            if _changed:
                logger.warning(
                    f"🔄 Deribit期权费率更新(来源: {_source}): "
                    f"option {_cur_o_maker}/{_cur_o_taker} -> {_o_maker}/{_o_taker} ({_o_inst})"
                )
            else:
                logger.info(
                    f"✅ Deribit期权费率核验(无变化, 来源: {_source}): "
                    f"option={_o_maker}/{_o_taker}"
                )
            return True
        except Exception as e:
            logger.warning(f"⚠️ Deribit get_instruments 同步费率失败: {e}")
            return False

    async def _sync_binance_fee_from_commission_rate(self, symbol: str) -> bool:
        """使用 Binance /fapi/v1/commissionRate 同步真实 maker/taker 费率"""
        if not getattr(self, 'binance_fee_calc', None) or not getattr(self, 'binance_ws', None):
            return False
        _symbol = (symbol or '').upper()
        if not _symbol:
            return False
        try:
            # 启动前可能尚未执行 WS.start()，这里主动做一次 serverTime 同步避免 -1021/-1022
            if getattr(self, 'binance_auth', None) is not None:
                try:
                    await self.binance_auth.sync_server_time()
                except Exception:
                    pass
            _resp = await self.binance_ws._rest_request(
                "GET", "/fapi/v1/commissionRate", {"symbol": _symbol}, signed=True
            )
            if not isinstance(_resp, dict):
                logger.warning(f"⚠️ Binance commissionRate 返回异常: {_resp}")
                return False

            _mk_raw = _resp.get("makerCommissionRate")
            _tk_raw = _resp.get("takerCommissionRate")
            if _mk_raw is None or _tk_raw is None:
                logger.warning(f"⚠️ Binance commissionRate 未返回 maker/taker 字段: {_resp}")
                return False

            _mk = Decimal(str(_mk_raw))
            _tk = Decimal(str(_tk_raw))
            _old_mk = Decimal(str(getattr(self.binance_fee_calc, 'maker_rate', _mk)))
            _old_tk = Decimal(str(getattr(self.binance_fee_calc, 'taker_rate', _tk)))
            self.binance_fee_calc.maker_rate = _mk
            self.binance_fee_calc.taker_rate = _tk

            _eps = Decimal('0.0000000001')
            if abs(_mk - _old_mk) > _eps or abs(_tk - _old_tk) > _eps:
                logger.warning(
                    f"🔄 Binance费率更新(来源: /fapi/v1/commissionRate): "
                    f"symbol={_symbol}, maker {_old_mk}->{_mk}, taker {_old_tk}->{_tk}"
                )
            else:
                logger.info(
                    f"✅ Binance费率核验(无变化): symbol={_symbol}, maker={_mk}, taker={_tk}"
                )
            return True
        except Exception as e:
            logger.warning(f"⚠️ Binance commissionRate 同步费率失败(symbol={_symbol}): {e}")
            return False

    async def _refresh_exchange_fee_rates(self, reason: str = "periodic") -> None:
        """刷新 Deribit + Binance 实时费率（启动校验 + 每小时轮询）"""
        _now = time.time()
        _lock = getattr(self, "_fee_refresh_lock", None)
        if _lock is None:
            self._fee_refresh_lock = asyncio.Lock()
            _lock = self._fee_refresh_lock
        if _lock.locked():
            return

        async with _lock:
            self._fee_refresh_last_attempt_time = _now
            _deribit_ok = await self._sync_deribit_fee_from_instruments()
            _binance_ok = False
            _perp = None
            _binance_required = False
            try:
                if getattr(self, 'binance_matcher', None) is not None:
                    _perp = self.binance_matcher.perpetual_symbol
            except Exception:
                _perp = None
            if _perp and getattr(self, 'binance_auth', None) is not None:
                _binance_required = True
                _binance_ok = await self._sync_binance_fee_from_commission_rate(_perp)

            _all_ok = _deribit_ok and (_binance_ok if _binance_required else True)
            if _all_ok:
                self._fee_refresh_time = _now

            logger.info(
                f"📊 费率刷新[{reason}] 完成: "
                f"Deribit={'OK' if _deribit_ok else 'FAIL'} | "
                f"Binance={'OK' if _binance_ok else ('FAIL' if _binance_required else 'SKIP')} | "
                f"overall={'OK' if _all_ok else 'RETRY'}"
            )
            if not _all_ok:
                _retry = int(max(getattr(self, '_fee_refresh_retry_interval', 300.0), 30.0))
                logger.warning(f"⚠️ 费率刷新[{reason}] 未完全成功，将在约 {_retry}s 后重试")

    def _cleanup_expired_instruments(self):
        """🌟 M3 修复: 清理已过期合约的 ticker / orderbook / cache，防止长期运行内存泄漏"""
        from datetime import datetime, timezone, timedelta
        now_utc = datetime.now(timezone.utc)
        expired_keys = []

        for (expiry, strike) in list(self.arbitrage_combinations.keys()):
            try:
                raw_dt = datetime.strptime(expiry, "%d%b%y")
                expiry_dt = raw_dt.replace(tzinfo=timezone.utc) + timedelta(hours=8)
            except Exception:
                continue

            if expiry_dt >= now_utc:
                continue  # 未过期，跳过

            # 检查是否有活跃持仓状态 — 有则不清理
            state = self.arbitrage_states.get((expiry, strike))
            if state and state.state in ('position_open', 'executing', 'exiting'):
                continue

            expired_keys.append((expiry, strike))

        if not expired_keys:
            return

        # 收集需要清理的合约名称
        expired_instruments = set()
        for key in expired_keys:
            combo = self.arbitrage_combinations.pop(key, None)
            if combo:
                expired_instruments.add(combo['call'])
                expired_instruments.add(combo['put'])
                expired_instruments.add(combo['future'])
            # 清理终态的 arbitrage_states 和各类辅助字典
            self.arbitrage_states.pop(key, None)
            self._exit_attempt_notified.discard(key)
            self._last_pnl_log_time.pop(key, None)
            # _trailing_log_time / _peak_save_time: 已废弃，不再使用

        # 检查合约是否仍被其他组合引用（同一期货可能服务多个 strike）
        still_in_use = set()
        for combo in self.arbitrage_combinations.values():
            still_in_use.add(combo['call'])
            still_in_use.add(combo['put'])
            still_in_use.add(combo['future'])

        # 只清理不再被任何组合引用的合约
        cleanup_count = 0
        for inst in expired_instruments:
            if inst in still_in_use:
                continue
            self.client.tickers.pop(inst, None)
            self.client.local_orderbooks.pop(inst, None)
            self.client.instrument_cache.pop(inst, None)
            self.contract_sizes.pop(inst, None)
            cleanup_count += 1

        logger.info(
            f"🧹 过期合约清理: 移除 {len(expired_keys)} 个组合, "
            f"清理 {cleanup_count} 个合约缓存, 剩余 {len(self.arbitrage_combinations)} 个组合"
        )

    async def scan_arbitrage_opportunities(self) -> List[Dict]:
        """扫描套利机会：Maker 口径找候选，保留保守口径用于日志对照。"""
        self.scan_count += 1
        opportunities = []
        all_maker_profits = []

        # 过滤阶段计数器（诊断静默过滤）
        _fc_total = 0
        _fc_dte = 0
        _fc_volume = 0
        _fc_lock = 0
        _fc_ticker = 0
        _fc_stale = 0
        _fc_binance = 0
        _fc_binance_reasons = defaultdict(int)
        _fc_funding_rate = Decimal('0')  # 当前 funding rate 值（所有组合共用同一永续合约，值相同）
        _fc_moneyness = 0
        _fc_spread = 0
        _fc_min_dte = 0
        _fc_depth = 0
        _fc_passed = 0

        try:
            # 周期性刷新24h交易量缓存（每30分钟）
            if time.time() - self._volume_refresh_time > self._volume_refresh_interval:
                await self._refresh_volume_filter()

            # 周期性发现新上线的合约 + 清理过期合约（每1小时）
            if time.time() - self._instrument_refresh_time > self._instrument_refresh_interval:
                await self._refresh_instruments()
                self._cleanup_expired_instruments()

            # 从配置读取风控参数
            moneyness_threshold = self.moneyness_threshold
            max_spread_pct = self.max_spread_pct

            for (expiry, strike), combination in self.arbitrage_combinations.items():
                _fc_total += 1
                # 0. DTE过滤：只交易距到期≤max_option_dte_hours的期权
                _dte_hours = 0
                try:
                    from datetime import datetime, timezone, timedelta
                    _raw_dt = datetime.strptime(expiry, "%d%b%y")
                    _expiry_dt = _raw_dt.replace(tzinfo=timezone.utc) + timedelta(hours=8)
                    _dte_hours = (_expiry_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                    if _dte_hours > self.max_option_dte_hours:
                        logger.debug(f"[{expiry}-{strike}] 跳过：DTE {_dte_hours:.1f}h > 上限 {self.max_option_dte_hours}h")
                        _fc_dte += 1
                        continue
                    if _dte_hours < 0:
                        _fc_dte += 1
                        continue  # 已过期
                except Exception:
                    _fc_dte += 1
                    continue  # 到期日解析失败，跳过

                # 0b. 24h交易量过滤：跳过零成交合约
                if self._active_options is not None:
                    if combination['call'] not in self._active_options or combination['put'] not in self._active_options:
                        _fc_volume += 1
                        continue

                # 1. 基础检查 (持仓锁、冷却锁)
                if self._check_position_lock(expiry, strike):
                    _fc_lock += 1
                    continue
                if self._check_processing_lock(expiry, strike):
                    _fc_lock += 1
                    continue

                # 2. 获取行情
                future_ticker = self.client.tickers.get(combination['future'])
                call_ticker = self.client.tickers.get(combination['call'])
                put_ticker = self.client.tickers.get(combination['put'])

                # 动态获取合约面值
                default_size = Decimal('1') if self.target_currency == 'ETH' else Decimal('10')
                contract_size = self.contract_sizes.get(combination['future'], default_size)

                # 3. 数据完整性检查
                _is_cross_exchange = bool(combination.get('binance_future'))
                if _is_cross_exchange:
                    # 跨所模式: 期货腿在 Binance，Deribit 期货 ticker 仅作参考不做硬过滤
                    if not all([call_ticker, put_ticker]):
                        _fc_ticker += 1
                        continue
                    if any(t.bid <= 0 or t.ask <= 0 for t in [call_ticker, put_ticker]):
                        _fc_ticker += 1
                        continue
                else:
                    if not all([future_ticker, call_ticker, put_ticker]):
                        _fc_ticker += 1
                        continue
                    if any(t.bid <= 0 or t.ask <= 0 for t in [future_ticker, call_ticker, put_ticker]):
                        _fc_ticker += 1
                        continue
                # 🌟 P2-4 修复: 扫描阶段增加 Deribit 期权 ticker 时间戳检查
                # 验证阶段已有 30s stale 检查, 但扫描阶段缺失会导致 WS 恢复期用过期行情触发无效验证
                _scan_now = time.time()
                if any(hasattr(t, 'timestamp') and t.timestamp > 0 and (_scan_now - t.timestamp) > 30
                       for t in [call_ticker, put_ticker]):
                    _fc_stale += 1
                    continue

                # ===== 跨所: 获取 Binance 期货价格 =====
                bn_symbol = combination.get('binance_future', '')
                bn_type = combination.get('binance_future_type', '')
                _bn_ready, _bn_reason = self._binance_market_ready(bn_symbol, max_age_sec=30.0)
                if not _bn_ready:
                    _fc_binance += 1
                    _fc_binance_reasons[str(_bn_reason).split(':', 1)[0]] += 1
                    logger.debug(f"[{expiry}-{strike}] 跳过：Binance 市场未就绪 ({_bn_reason})")
                    continue  # 无匹配的 Binance 合约或 Binance 行情未就绪

                _entry_threshold = self.min_profit_threshold

                bn_ob = self.binance_ws.order_books.get(bn_symbol) if self.binance_ws else None
                if not bn_ob:
                    _fc_binance += 1
                    _fc_binance_reasons["orderbook_missing"] += 1
                    continue
                if bn_ob.mid_price is None:
                    _fc_binance += 1
                    _fc_binance_reasons["orderbook_mid_missing"] += 1
                    continue
                if bn_ob.mid_price <= 0:
                    _fc_binance += 1
                    _fc_binance_reasons["orderbook_mid_invalid"] += 1
                    continue
                if not bn_ob.update_time or (time.time() - bn_ob.update_time) > 30:
                    _fc_binance += 1
                    _fc_binance_reasons["orderbook_stale"] += 1
                    continue  # Binance 盘口数据不可用或过期

                binance_future_bid = bn_ob.best_bid
                binance_future_ask = bn_ob.best_ask
                binance_future_mid = bn_ob.mid_price

                # ===== 永续 funding 预估（按方向保留符号：成本为正，收入为负） =====
                # 注：交割阶段 funding_net_usd 使用"收入为正"口径；这里是成本口径变量。
                # funding 对多/空方向影响相反，不做 abs() 门控，由下游利润模拟按方向扣减。
                funding_deduction_usd = Decimal('0')
                if bn_type == "perpetual" and self.binance_ws:
                    current_funding = self.binance_ws.funding_rates.get(bn_symbol, Decimal('0'))
                    _fc_funding_rate = current_funding
                    position_value = binance_future_mid * self.trade_amount
                    _funding_hours = max(_dte_hours, 8) if _dte_hours > 0 else 48
                    funding_deduction_usd = binance_futures.BinanceFeeCalculator.estimate_funding_cost_usdt(
                        position_value, current_funding, _funding_hours)

                # 4. 深度实值/虚值过滤 (Moneyness Filter)
                # 🌟 P2-8 修复: 跨所模式优先用 Binance 永续价 (无远期基差溢价)
                current_price = binance_future_mid if binance_future_mid > 0 else (future_ticker.mid_price if future_ticker else Decimal('0'))
                if current_price > 0:
                    price_diff_pct = abs(strike - current_price) / current_price
                    if price_diff_pct > moneyness_threshold:
                        _fc_moneyness += 1
                        continue

                # 5. 价差率过滤 (流动性风控)
                # 🌟 扫描优化: 仅对 Taker 腿（价差窄的）检查 spread_pct
                # Maker 腿挂中间价，宽价差反而有利（排队位置远离竞争）；Taker 腿吃单才真正受滑点影响
                call_spread_pct = (call_ticker.ask - call_ticker.bid) / call_ticker.ask if call_ticker.ask > 0 else Decimal('1')
                put_spread_pct = (put_ticker.ask - put_ticker.bid) / put_ticker.ask if put_ticker.ask > 0 else Decimal('1')
                # 价差窄的做 Taker，只检查 Taker 腿的 spread
                taker_spread = min(call_spread_pct, put_spread_pct)
                if taker_spread > max_spread_pct:
                    _fc_spread += 1
                    continue

                # 临近到期保护（可配置）— 距到期不足 min_option_dte_hours 不开新仓
                if _dte_hours <= float(getattr(self, 'min_option_dte_hours', 12)):
                    _fc_min_dte += 1
                    continue

                # ===== 快速深度预过滤：三腿最优档 size ≥ trade_amount × min_depth_ratio =====
                # 跨所模式下期货腿深度使用 Binance 永续盘口数量（BTC），而非 Deribit 远期期货
                # 🌟 min_depth_ratio 可在 config 中调整 (默认 0.2)
                min_depth_ratio = Decimal(str(getattr(self, 'min_depth_ratio', Decimal('0.2'))))
                opt_min_size = self.trade_amount * min_depth_ratio
                fut_min_qty = self.trade_amount * min_depth_ratio
                bn_bid_size = bn_ob.bids[0][1] if bn_ob.bids else Decimal('0')
                bn_ask_size = bn_ob.asks[0][1] if bn_ob.asks else Decimal('0')

                skip_strat1 = (bn_bid_size < fut_min_qty or
                               call_ticker.ask_size < opt_min_size or
                               put_ticker.bid_size < opt_min_size)

                skip_strat2 = (bn_ask_size < fut_min_qty or
                               call_ticker.bid_size < opt_min_size or
                               put_ticker.ask_size < opt_min_size)

                if skip_strat1 and skip_strat2:
                    _fc_depth += 1
                    continue
                _fc_passed += 1

                # -------------------------------------------------------------------------
                # 策略1: 卖期货 + 买合成 (扫描准入使用 Maker 口径)
                # -------------------------------------------------------------------------
                if not skip_strat1:
                    future_amount_usd = self.trade_amount * binance_future_bid
                    future_amount_usd = (future_amount_usd / contract_size).quantize(Decimal('1'), rounding='ROUND_HALF_UP') * contract_size

                    vwap_prices = await self._calculate_three_leg_scan_vwap(
                        'sell_future_buy_synthetic',
                        combination['future'], combination['call'], combination['put'],
                        self.trade_amount, future_amount_usd,
                        call_ticker, put_ticker,
                        binance_symbol=bn_symbol)

                    if vwap_prices:
                        trade_result = await self.trade_executor.simulate_trade(
                            strategy_type='sell_future_buy_synthetic',
                            future_price=binance_future_bid,
                            call_price=vwap_prices['call'],
                            put_price=vwap_prices['put'],
                            strike=strike,
                            future_amount_usd=future_amount_usd,
                            option_btc_amount=self.trade_amount
                        )
                        _scan_settle_fee_usd = self._estimate_round_settle_fee_usd(
                            binance_future_mid, vwap_prices['call'], vwap_prices['put'], self.trade_amount)
                        # 策略1 = Binance 空头：rate>0 时 funding 为收入（负成本）
                        _scan_funding_adj = -funding_deduction_usd
                        reference_net_profit = (
                            Decimal(str(trade_result['net_profit'])) -
                            _scan_settle_fee_usd - _scan_funding_adj)
                        _call_is_anchor = (
                            (call_ticker.ask - call_ticker.bid) >=
                            (put_ticker.ask - put_ticker.bid))
                        _taker_option_price = vwap_prices['put'] if _call_is_anchor else vwap_prices['call']
                        _maker_option_price = vwap_prices['call'] if _call_is_anchor else vwap_prices['put']
                        _maker_open_fee_usd = self._estimate_scan_maker_open_fee_usd(
                            binance_future_mid, _taker_option_price, self.trade_amount,
                            option_maker_price=_maker_option_price)
                        maker_net_profit = (
                            Decimal(str(trade_result.get('gross_profit_usd', trade_result.get('gross_profit', 0)))) -
                            _maker_open_fee_usd - _scan_settle_fee_usd - _scan_funding_adj)
                        all_maker_profits.append(float(maker_net_profit))

                        if maker_net_profit >= _entry_threshold:
                            opportunities.append({
                                'expiry_strike': (expiry, strike),
                                'expiry': expiry,
                                'type': 'sell_future_buy_synthetic',
                                'gross_profit': float(trade_result.get('gross_profit_usd', 0)),
                                'net_profit': float(maker_net_profit),
                                'scan_profit_basis': 'maker',
                                'scan_maker_net_profit': float(maker_net_profit),
                                'scan_taker_fee_net_profit': float(reference_net_profit),
                                'strike': float(strike),
                                'future_price': float(binance_future_bid),
                                'future_symbol': combination['future'],
                                'call_symbol': combination['call'],
                                'put_symbol': combination['put'],
                                'future_ticker': future_ticker,
                                'call_ticker': call_ticker,
                                'put_ticker': put_ticker,
                                'scan_vwap': vwap_prices,
                                'scan_maker_open_fee_usd': float(_maker_open_fee_usd),
                                'scan_settle_fee_usd': float(_scan_settle_fee_usd),
                                'scan_funding_adj_usd': float(_scan_funding_adj),
                                'binance_symbol': bn_symbol,
                                'binance_type': bn_type,
                                'binance_price': float(binance_future_bid),
                            })

                # -------------------------------------------------------------------------
                # 策略2: 买期货 + 卖合成 (扫描准入使用 Maker 口径)
                # -------------------------------------------------------------------------
                if not skip_strat2:
                    future_amount_usd = self.trade_amount * binance_future_ask
                    future_amount_usd = (future_amount_usd / contract_size).quantize(Decimal('1'), rounding='ROUND_HALF_UP') * contract_size

                    vwap_prices = await self._calculate_three_leg_scan_vwap(
                        'buy_future_sell_synthetic',
                        combination['future'], combination['call'], combination['put'],
                        self.trade_amount, future_amount_usd,
                        call_ticker, put_ticker,
                        binance_symbol=bn_symbol)

                    if vwap_prices:
                        trade_result = await self.trade_executor.simulate_trade(
                            strategy_type='buy_future_sell_synthetic',
                            future_price=binance_future_ask,
                            call_price=vwap_prices['call'],
                            put_price=vwap_prices['put'],
                            strike=strike,
                            future_amount_usd=future_amount_usd,
                            option_btc_amount=self.trade_amount
                        )
                        _scan_settle_fee_usd = self._estimate_round_settle_fee_usd(
                            binance_future_mid, vwap_prices['call'], vwap_prices['put'], self.trade_amount)
                        # 策略2 = Binance 多头：rate>0 时 funding 为成本（正成本）
                        _scan_funding_adj = funding_deduction_usd
                        reference_net_profit = (
                            Decimal(str(trade_result['net_profit'])) -
                            _scan_settle_fee_usd - _scan_funding_adj)
                        _call_is_anchor = (
                            (call_ticker.ask - call_ticker.bid) >=
                            (put_ticker.ask - put_ticker.bid))
                        _taker_option_price = vwap_prices['put'] if _call_is_anchor else vwap_prices['call']
                        _maker_option_price = vwap_prices['call'] if _call_is_anchor else vwap_prices['put']
                        _maker_open_fee_usd = self._estimate_scan_maker_open_fee_usd(
                            binance_future_mid, _taker_option_price, self.trade_amount,
                            option_maker_price=_maker_option_price)
                        maker_net_profit = (
                            Decimal(str(trade_result.get('gross_profit_usd', trade_result.get('gross_profit', 0)))) -
                            _maker_open_fee_usd - _scan_settle_fee_usd - _scan_funding_adj)
                        all_maker_profits.append(float(maker_net_profit))

                        if maker_net_profit >= _entry_threshold:
                            opportunities.append({
                                'expiry_strike': (expiry, strike),
                                'expiry': expiry,
                                'type': 'buy_future_sell_synthetic',
                                'gross_profit': float(trade_result.get('gross_profit_usd', 0)),
                                'net_profit': float(maker_net_profit),
                                'scan_profit_basis': 'maker',
                                'scan_maker_net_profit': float(maker_net_profit),
                                'scan_taker_fee_net_profit': float(reference_net_profit),
                                'strike': float(strike),
                                'future_price': float(binance_future_ask),
                                'future_symbol': combination['future'],
                                'call_symbol': combination['call'],
                                'put_symbol': combination['put'],
                                'future_ticker': future_ticker,
                                'call_ticker': call_ticker,
                                'put_ticker': put_ticker,
                                'scan_vwap': vwap_prices,
                                'scan_maker_open_fee_usd': float(_maker_open_fee_usd),
                                'scan_settle_fee_usd': float(_scan_settle_fee_usd),
                                'scan_funding_adj_usd': float(_scan_funding_adj),
                                'binance_symbol': bn_symbol,
                                'binance_type': bn_type,
                                'binance_price': float(binance_future_ask),
                            })

        except Exception as e:
            logger.error(f"扫描机会时发生异常: {e}")
            # 即使发生异常，也返回空列表，而不是 None
            return []

        # ================= 🌟 聚合打印 Maker Top5 利润，避免每轮扫描刷屏 =================
        if all_maker_profits:
            self._record_scan_maker_top_profits(all_maker_profits)
        elif _fc_total > 0:
            self._record_scan_maker_top_profits([])
            # 所有组合被过滤，每5分钟输出一次诊断（避免刷屏）
            _diag_now = time.time()
            if _diag_now - getattr(self, '_scan_diag_last_log', 0.0) >= 300:
                self._scan_diag_last_log = _diag_now
                _parts = []
                if _fc_dte: _parts.append(f"DTE={_fc_dte}")
                if _fc_volume: _parts.append(f"交易量={_fc_volume}")
                if _fc_lock: _parts.append(f"持仓锁={_fc_lock}")
                if _fc_ticker: _parts.append(f"行情缺失={_fc_ticker}")
                if _fc_stale: _parts.append(f"行情过期={_fc_stale}")
                if _fc_binance:
                    _bn_detail = ""
                    if _fc_binance_reasons:
                        _reason_parts = [
                            f"{k}={v}" for k, v in sorted(
                                _fc_binance_reasons.items(),
                                key=lambda kv: (-kv[1], kv[0]))
                        ]
                        _bn_detail = "(" + ",".join(_reason_parts[:6]) + ")"
                    _parts.append(f"Binance={_fc_binance}{_bn_detail}")
                if _fc_moneyness: _parts.append(f"Moneyness={_fc_moneyness}")
                if _fc_spread: _parts.append(f"价差={_fc_spread}")
                if _fc_min_dte: _parts.append(f"临近到期={_fc_min_dte}")
                if _fc_depth: _parts.append(f"深度={_fc_depth}")
                _diag_msg = f"📊 扫描诊断: {_fc_total}个组合全部过滤 | 通过={_fc_passed} | " + " | ".join(_parts)
                if _fc_funding_rate != 0:
                    _fr_pct = float(_fc_funding_rate) * 100
                    _thr_pct = float(self.binance_max_funding_rate) * 100
                    if _fc_funding_rate > 0:
                        _diag_msg += f" | 💰 funding={_fr_pct:+.4f}%(多头付费，空头收益"
                    else:
                        _diag_msg += f" | 💰 funding={_fr_pct:+.4f}%(空头付费，多头收益"
                    if abs(_fc_funding_rate) > self.binance_max_funding_rate:
                        _diag_msg += f"，超警戒{_thr_pct:.2f}%)"
                    else:
                        _diag_msg += ")"
                logger.info(_diag_msg)
        # ===============================================================

        # 确保函数结束时返回列表
        return opportunities
