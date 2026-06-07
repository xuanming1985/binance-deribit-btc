"""engine/startup_mixin.py — 初始化 + 单例锁 + 订阅设置"""
from __future__ import annotations
import logging
import os
import time
import asyncio
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set

if TYPE_CHECKING:
    pass

import config
import binance_futures
from telegram_handler import tg_notifier
from engine.models import ArbitrageState

logger = logging.getLogger(__name__)


class StartupMixin:
    """Mixin: 引擎初始化 + Redis 单例锁 + WS 订阅"""

    @staticmethod
    def _parse_instrument_name(instrument_name: str) -> Optional[
        Tuple[str, str, Optional[Decimal], Optional[str]]]:
        """解析合约名称"""
        try:
            parts = instrument_name.split('-')
            if len(parts) < 2:
                return None

            currency = parts[0]
            expiry = parts[1]

            if len(parts) == 2:  # 期货
                return currency, expiry, None, None
            elif len(parts) >= 4:  # 期权
                strike = Decimal(parts[2]) if parts[2].replace('.', '').isdigit() else None
                option_type = parts[3].upper()
                if option_type in ['C', 'CALL']:
                    option_type = 'call'
                elif option_type in ['P', 'PUT']:
                    option_type = 'put'
                else:
                    option_type = None
                return currency, expiry, strike, option_type

            return None
        except Exception as e:
            logger.info(f"解析合约名称失败: {instrument_name}, 错误: {e}")
            return None

    async def _resolve_anchor_ws_disconnect_pause(self):
        """重连后核查 WS 断线时遗留的锚定 Maker 单，再决定是否恢复扫描。"""
        reason = "锚定腿WS断连待核查"
        pending_orders = set(getattr(self, '_anchor_ws_disconnect_pending_orders', set()) or set())
        if not pending_orders and not self._has_pause(reason):
            return

        filled_orders = []
        unresolved_orders = []
        dust = Decimal('0.0001')

        for order_id in list(pending_orders):
            order = self.client.get_order_by_id(order_id)
            if order is None or str(getattr(order, 'status', '')).lower() not in ('filled', 'cancelled', 'rejected'):
                try:
                    msg = {
                        "jsonrpc": "2.0",
                        "id": self.client._get_next_request_id(),
                        "method": "private/get_order_state",
                        "params": {"order_id": order_id},
                    }
                    resp = await self.client.send_request(msg, is_private=True, timeout=3.0)
                    if isinstance(resp, dict) and 'result' in resp:
                        existing = self.client.get_order_by_id(order_id)
                        order = self.client._order_from_api_data(resp['result'], existing=existing)
                        self.client._store_order_snapshot(order)
                except Exception as exc:
                    logger.warning(f"[锚定腿WS断连] 重连后订单状态核查失败 {order_id}: {exc}")

            order = self.client.get_order_by_id(order_id)
            if order is None:
                unresolved_orders.append(order_id)
                continue

            status = str(getattr(order, 'status', '') or '').lower()
            filled = Decimal(str(getattr(order, 'filled_amount', 0) or 0))
            amount = Decimal(str(getattr(order, 'amount', 0) or 0))
            if filled > dust:
                filled_orders.append((order_id, getattr(order, 'instrument_name', ''), filled, status))
            elif status not in ('filled', 'cancelled', 'rejected') and (amount <= 0 or filled < amount - dust):
                unresolved_orders.append(order_id)

        if filled_orders:
            self._remove_pause(reason)
            self._add_pause("锚定腿回滚失败")
            self._anchor_ws_disconnect_pending_orders = set(unresolved_orders)
            if unresolved_orders:
                self._add_pause(reason)
            detail = ", ".join(f"{inst or oid}#{oid} filled={filled} status={status}"
                               for oid, inst, filled, status in filled_orders)
            logger.error(f"🚨 [锚定腿WS断连] 重连核查发现锚定单已成交，保持暂停等待幽灵仓位清理: {detail}")
            now = time.time()
            if now - getattr(self, '_anchor_ws_disconnect_alert_ts', 0.0) >= max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0):
                self._anchor_ws_disconnect_alert_ts = now
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 锚定腿 WS 断线后发现成交\n{detail}\n系统保持暂停，等待幽灵仓位/人工处理。",
                    "anchor_ws_disconnect_filled"))
            return

        if unresolved_orders:
            self._anchor_ws_disconnect_pending_orders = set(unresolved_orders)
            self._add_pause(reason)
            logger.warning(f"[锚定腿WS断连] 重连后仍有订单状态未确认，继续暂停扫描: {unresolved_orders}")
            return

        self._anchor_ws_disconnect_pending_orders = set()
        self._remove_pause(reason)
        logger.info("[锚定腿WS断连] 重连核查完成：未发现成交或活跃锚定单，解除专项暂停")

    async def _acquire_singleton_lock(self) -> bool:
        """🌟 C4 修复: Redis 分布式单例锁，防止多实例同时运行"""
        self._singleton_key = f"arb_engine_lock:{self.target_currency}"
        import socket
        self._singleton_value = f"{socket.gethostname()}:{os.getpid()}:{time.time()}"
        try:
            # SET NX + EX: 仅在 key 不存在时设置，TTL 120 秒（给事件循环阻塞留足余量）
            acquired = await self.redis.set(self._singleton_key, self._singleton_value, nx=True, ex=120)
            if not acquired:
                existing = await self.redis.get(self._singleton_key)
                logger.warning(f"⚠️ 单例锁已被持有: {existing}，检查旧进程是否存活...")
                # 解析锁值: hostname:pid:timestamp
                if existing:
                    try:
                        parts = existing.split(":")
                        lock_host = parts[0]
                        lock_pid = int(parts[1])
                        import socket as _sock
                        my_host = _sock.gethostname()
                        if lock_host == my_host:
                            # 同一台机器，检查 PID 是否存活
                            try:
                                os.kill(lock_pid, 0)  # 不发信号，仅检测进程存在
                                # 进程仍在运行 — 真正的冲突
                                logger.error(f"🚨 单例锁获取失败！PID {lock_pid} 仍在运行: {existing}")
                                logger.error(f"如确认旧实例已死，手动清理: redis-cli DEL {self._singleton_key}")
                                return False
                            except ProcessLookupError:
                                # 进程已死，强制接管锁
                                logger.warning(f"🔄 旧进程 PID {lock_pid} 已死亡，强制接管单例锁")
                                await self.redis.delete(self._singleton_key)
                                taken = await self.redis.set(self._singleton_key, self._singleton_value, nx=True, ex=120)
                                if taken:
                                    logger.info(f"✅ 单例锁已强制接管: {self._singleton_value}")
                                    return True
                                else:
                                    logger.error("🚨 强制接管失败，可能有第三个实例同时启动")
                                    return False
                            except PermissionError:
                                # 进程存在但无权限检查（不应发生），保守拒绝
                                logger.error(f"🚨 无法检测 PID {lock_pid} 状态 (权限不足)，拒绝启动")
                                return False
                        else:
                            # 不同主机，无法检测进程状态
                            logger.error(f"🚨 单例锁由其他主机 {lock_host} 持有: {existing}")
                            logger.error(f"如确认旧实例已死，手动清理: redis-cli DEL {self._singleton_key}")
                            return False
                    except (ValueError, IndexError):
                        logger.error(f"🚨 锁值格式异常: {existing}，无法自动判断")
                        logger.error(f"手动清理: redis-cli DEL {self._singleton_key}")
                        return False
                else:
                    # existing 为空说明锁刚好过期，重试一次
                    retry = await self.redis.set(self._singleton_key, self._singleton_value, nx=True, ex=120)
                    if retry:
                        logger.info(f"✅ 单例锁在重试时获取成功: {self._singleton_value}")
                        return True
                    return False
            logger.info(f"✅ 单例锁已获取: {self._singleton_value}")
            return True
        except Exception as e:
            # 🌟 E2 修复: Redis 不可用时仍允许运行（单机部署场景需要），
            # 但标记降级状态，心跳失败时发出强告警
            logger.warning(f"⚠️ Redis 单例锁获取异常 (降级允许运行，无多实例保护): {e}")
            self._singleton_redis_degraded = True
            self._add_pause("Redis不可用")
            asyncio.create_task(tg_notifier.send_error_async(
                f"⚠️ Redis 不可用，单例锁降级运行\n"
                f"⚠️ 已暂停新开仓（monitor 持仓监控不受影响）\n"
                f"⚠️ 请确保没有其他实例在运行！\n"
                f"错误: {str(e)[:100]}", "singleton_redis_degraded"))
            return True

    async def _singleton_heartbeat(self):
        """单例锁心跳续期 (每 10 秒续期一次，TTL 120 秒)"""
        while True:
            try:
                await asyncio.sleep(10)
                current = await self.redis.get(self._singleton_key)
                if current == self._singleton_value:
                    # 正常续期
                    await self.redis.expire(self._singleton_key, 120)
                    self._singleton_hb_fail_count = 0
                    if getattr(self, '_singleton_redis_degraded', False):
                        self._singleton_redis_degraded = False
                        self._remove_pause("Redis不可用")
                        logger.info("✅ Redis 已恢复，解除 'Redis不可用' 暂停，恢复新开仓")
                elif current is None:
                    # key 已过期（事件循环曾长时间阻塞），尝试重新获取而非误判为抢占
                    reacquired = await self.redis.set(self._singleton_key, self._singleton_value, nx=True, ex=120)
                    if reacquired:
                        logger.warning("⚠️ 单例锁曾过期，已重新获取 (事件循环可能发生过长时间阻塞)")
                        self._singleton_hb_fail_count = 0
                        if getattr(self, '_singleton_redis_degraded', False):
                            self._singleton_redis_degraded = False
                            self._remove_pause("Redis不可用")
                            logger.info("✅ Redis 已恢复，解除 'Redis不可用' 暂停，恢复新开仓")
                    else:
                        # 过期后被另一个实例抢走了
                        new_owner = await self.redis.get(self._singleton_key)
                        logger.error(f"🚨 单例锁过期后被其他实例抢占: {new_owner}，停止当前实例")
                        self._add_pause("单例锁被抢占")
                        self._fatal_shutdown = True
                        self.running = False
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 单例锁被抢占！新持有者: {new_owner}\n当前实例正在关机，请检查是否有重复进程",
                            "singleton_lock"))
                        break
                else:
                    # key 存在但值不同 — 真正的抢占（另一个实例覆盖了我们的锁）
                    logger.error(f"🚨 单例锁被抢占！当前持有者: {current}，停止当前实例")
                    self._add_pause("单例锁被抢占")
                    self._fatal_shutdown = True
                    self.running = False
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 单例锁被抢占！当前持有者: {current}\n当前实例正在关机，请检查是否有重复进程",
                        "singleton_lock"))
                    break
            except Exception as e:
                # 🌟 E2 修复: Redis 心跳异常从 debug 升级为计数告警
                _hb_fail_count = getattr(self, '_singleton_hb_fail_count', 0) + 1
                self._singleton_hb_fail_count = _hb_fail_count
                if _hb_fail_count <= 3:
                    logger.warning(f"⚠️ 单例锁心跳异常 ({_hb_fail_count}/3): {e}")
                elif _hb_fail_count == 4:
                    logger.error(f"🚨 单例锁心跳连续失败 {_hb_fail_count} 次，Redis 可能不可用！"
                                 f"锁将在 ~{120 - _hb_fail_count * 10}s 后过期，存在双实例风险")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 Redis 心跳连续失败 {_hb_fail_count} 次\n"
                        f"单例锁即将过期，请确保无其他实例！\n"
                        f"错误: {str(e)[:100]}", "singleton_heartbeat_fail"))
                # 超过 10 次 (100s) 仍失败，每 60s 提醒一次
                elif _hb_fail_count % 6 == 0:
                    logger.error(f"🚨 Redis 心跳持续异常 ({_hb_fail_count} 次)，单例保护已失效")
                # 连续失败 ≥3 次，重新暂停开仓（与 _acquire_singleton_lock 失败路径对称）
                if _hb_fail_count >= 3 and not getattr(self, '_singleton_redis_degraded', False):
                    self._singleton_redis_degraded = True
                    self._add_pause("Redis不可用")
                    logger.warning("⚠️ Redis 心跳连续失败，已暂停新开仓直到 Redis 恢复")

    async def _release_singleton_lock(self):
        """释放单例锁"""
        try:
            current = await self.redis.get(self._singleton_key)
            if current == self._singleton_value:
                await self.redis.delete(self._singleton_key)
                logger.info("✅ 单例锁已释放")
        except Exception:
            pass

    async def initialize(self):
        """初始化引擎（优化版：期权按选定的期货到期日过滤）"""
        try:
            # 🌟 重连安全: 重初始化期间标记未就绪，阻止 monitor_positions 用过期数据做风控决策
            self.initialized = False

            # 连接WebSocket
            if not await self.client.connect_with_retry():
                raise ConnectionError("WebSocket连接失败")

            # 启动消息监听器
            await self.client.start_listening()
            self.client.process_task = asyncio.create_task(self.client.process_messages())

            # 同步持仓
            logger.info("正在同步持仓...")
            await self.client.get_positions(self.target_currency)
            await self.client.get_open_orders(self.target_currency)

            # 🌟 重连安全: 清理上一个会话的残留挂单
            # WS 断连期间 Maker 挂单可能无人追踪，若成交将产生裸腿风险
            # 🌟 P1-5.5: 只清理本程序生成的订单 (label 以 arb_ / em_ / fb_ / stop_all_ 开头)
            # 防止误杀用户手动在 Deribit 网页挂的订单
            _ARB_LABEL_PREFIXES = ('arb_', 'em_', 'fb_', 'stop_all_', 'l2a_', 'l2t_', 'l2f_', 'ghost_')
            _our_orders = []
            for _oid, _order in list(self.client.active_orders.items()):
                _label = getattr(_order, 'label', '') or ''
                if any(_label.startswith(p) for p in _ARB_LABEL_PREFIXES):
                    _our_orders.append(_oid)
            orphan_count = len(_our_orders)
            total_count = len(self.client.active_orders)
            if orphan_count > 0:
                logger.warning(
                    f"⚠️ 发现 {orphan_count}/{total_count} 个本程序残留挂单, 正在清理防止裸腿风险...")
                _filled_orphan_orders = []
                for _oid in _our_orders:
                    try:
                        await self.client.cancel_order(_oid)
                        _ord = self.client.get_order_by_id(_oid)
                        _filled = Decimal(str(getattr(_ord, 'filled_amount', 0) or 0)) if _ord else Decimal('0')
                        _label = getattr(_ord, 'label', '') if _ord else ''
                        if _label.startswith('arb_') and _filled > Decimal('0.0001'):
                            _filled_orphan_orders.append((_oid, getattr(_ord, 'instrument_name', ''), _filled))
                    except Exception as _ce:
                        logger.info(f"清理残留挂单失败 {_oid}: {_ce}")
                logger.info(f"✅ 已清理 {orphan_count} 个本程序残留挂单 (保留 {total_count - orphan_count} 个非本程序挂单)")
                if _filled_orphan_orders:
                    self._add_pause("锚定腿回滚失败")
                    detail = ", ".join(f"{inst or oid}#{oid} filled={filled}" for oid, inst, filled in _filled_orphan_orders)
                    logger.error(f"🚨 [残留挂单清理] 发现本程序锚定挂单已有成交，保持暂停等待幽灵仓位清理: {detail}")
                    now = time.time()
                    if now - getattr(self, '_anchor_ws_disconnect_alert_ts', 0.0) >= max(float(getattr(self, 'risk_alert_throttle_seconds', 300.0)), 30.0):
                        self._anchor_ws_disconnect_alert_ts = now
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 残留锚定挂单清理后发现成交\n{detail}\n系统保持暂停，等待幽灵仓位/人工处理。",
                            "orphan_anchor_order_filled"))
            elif total_count > 0:
                logger.info(f"ℹ️ 发现 {total_count} 个挂单, 均非本程序创建(无 arb_/em_ label), 保留不动")

            await self._resolve_anchor_ws_disconnect_pause()

            # 订阅持仓和订单变化
            await self.client.subscribe_positions(self.target_currency)
            await self.client.subscribe_orders(self.target_currency)

            # ========== 1. 先获取并筛选期货合约 ==========
            logger.info("正在获取期货合约...")
            futures_response = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_instruments",
                "params": {"currency": self.target_currency, "kind": "future", "expired": False}
            })

            future_by_expiry = {}
            future_list = []

            # 按到期时间升序排列后取前 N 个，确保短期到期合约优先入选
            _sorted_futures = sorted(futures_response.get('result', []),
                                     key=lambda x: x.get('expiration_timestamp', 0))
            for item in _sorted_futures[0:self.futures_numbers]:
                parsed = self._parse_instrument_name(item['instrument_name'])
                if parsed and parsed[2] is None:  # 期货
                    _, expiry, _, _ = parsed
                    if expiry == 'PERPETUAL':
                        continue  # 永续合约没有对应期权，跳过
                    future_by_expiry[expiry] = item['instrument_name']
                    future_list.append(item['instrument_name'])
                    # 【新增】保存该期货的合约面值
                    c_size = item.get('contract_size', 10)  # 默认为10以防万一
                    self.contract_sizes[item['instrument_name']] = Decimal(str(c_size))
                    self.client.instrument_cache[item['instrument_name']] = item
                    logger.info(f"合约 {item['instrument_name']} 面值: {c_size}")

            logger.info(f"找到 【{len(future_by_expiry)}】 个不同到期日的期货合约")
            logger.info(f"目标到期日: {list(future_by_expiry.keys())}")

            # ========== 2. 只获取对应到期日的期权合约 ==========
            logger.info("正在获取期权合约，并执行深度风控过滤...")

            calls_by_expiry_strike = {}
            puts_by_expiry_strike = {}

            for expiry, future_name in future_by_expiry.items():
                logger.info(f"正在获取 {expiry} 到期日的期权合约...")

                # ================= 🌟 方案 B 新增：预先获取期货价格，作为过滤基准 =================
                try:
                    ticker_resp = await self.client.send_request({
                        "jsonrpc": "2.0",
                        "id": self.client._get_next_request_id(),
                        "method": "public/ticker",
                        "params": {"instrument_name": future_name}
                    })

                    # 更安全的解析防呆机制
                    result_data = ticker_resp.get('result')
                    if not result_data:
                        current_future_price = Decimal('0')
                    else:
                        current_future_price = Decimal(str(result_data.get('mark_price', 0)))

                except Exception as e:
                    logger.error(f"获取期货 {future_name} 价格失败，跳过: {e}")
                    continue

                if current_future_price <= 0:
                    continue
                # =========================================================================

                options_response = await self.client.send_request({
                    "jsonrpc": "2.0",
                    "id": self.client._get_next_request_id(),
                    "method": "public/get_instruments",
                    "params": {
                        "currency": self.target_currency,
                        "kind": "option",
                        "expired": False
                    }
                })

                # 只处理当前到期日的期权
                for item in options_response.get('result', []):
                    parsed = self._parse_instrument_name(item['instrument_name'])
                    if not parsed:
                        continue

                    currency, item_expiry, strike, option_type = parsed

                    # 只保留我们需要的到期日
                    if item_expiry != expiry:
                        continue

                    # ================= 🌟 方案 B 新增：在源头掐断无效合约的注册 =================
                    # 根据配置中的 moneyness_threshold 剔除没有套利价值的边缘期权
                    price_diff_pct = abs(strike - current_future_price) / current_future_price

                    # 安全读取配置，如果没配则默认 0.1 (即 10%)
                    # ================= 智能读取配置兜底 =================
                    # config 已在文件顶部导入
                    # 优先使用引擎已加载的配置值，其次按币种读取 config.py，最后兜底 0.1
                    if hasattr(self, 'moneyness_threshold') and self.moneyness_threshold > 0:
                        max_moneyness = self.moneyness_threshold
                    else:
                        cfg_name = 'ETH_CONFIG' if self.target_currency == 'ETH' else 'BTC_CONFIG'
                        cfg_dict = getattr(config, cfg_name, {})
                        max_moneyness = cfg_dict.get('moneyness_threshold', 0.1) if isinstance(cfg_dict, dict) else 0.1
                    # =======================================================

                    if price_diff_pct > Decimal(str(max_moneyness)):
                        continue  # 核心拦截：不放入组合，后续就不会订阅它的高频盘口！
                    # =========================================================================

                    # 只有通过了过滤的优质期权，才放入缓存
                    self.client.instrument_cache[item['instrument_name']] = item

                    if option_type == 'call' and strike:
                        calls_by_expiry_strike[(expiry, strike)] = item['instrument_name']
                    elif option_type == 'put' and strike:
                        puts_by_expiry_strike[(expiry, strike)] = item['instrument_name']

                await asyncio.sleep(0.1)

            # ========== 2.5 获取24h交易量，过滤零交易量合约 ==========
            _active_options = set()  # 24h有成交的期权集合
            try:
                _summary_resp = await self.client.send_request({
                    "jsonrpc": "2.0",
                    "id": self.client._get_next_request_id(),
                    "method": "public/get_book_summary_by_currency",
                    "params": {"currency": self.target_currency, "kind": "option"}
                })
                _low_vol_count = 0
                _min_vol = self.min_option_volume
                for _s in _summary_resp.get('result', []):
                    _vol = float(_s.get('volume', 0) or 0)
                    if _vol >= _min_vol and _vol > 0:
                        _active_options.add(_s['instrument_name'])
                    else:
                        _low_vol_count += 1
                logger.info(f"📊 24h交易量过滤 (阈值≥{_min_vol}): {len(_active_options)} 个活跃期权 / {_low_vol_count} 个低量已排除")
                # 同步到实例属性，供扫描循环周期刷新使用
                self._active_options = _active_options
                self._volume_refresh_time = time.time()
            except Exception as e:
                logger.warning(f"⚠️ 获取交易量摘要失败({e})，跳过交易量过滤")
                _active_options = None  # None 表示跳过过滤

            # ========== 3. 构建套利组合 ==========
            _vol_filtered = 0
            for (expiry, strike) in set(calls_by_expiry_strike.keys()) & set(puts_by_expiry_strike.keys()):
                call_name = calls_by_expiry_strike[(expiry, strike)]
                put_name = puts_by_expiry_strike[(expiry, strike)]
                future_name = future_by_expiry.get(expiry)

                if future_name:
                    # 24h交易量过滤：Call 和 Put 任一无成交则跳过
                    if _active_options is not None:
                        if call_name not in _active_options or put_name not in _active_options:
                            _vol_filtered += 1
                            continue

                    self.arbitrage_combinations[(expiry, strike)] = {
                        'call': call_name,
                        'put': put_name,
                        'future': future_name
                    }

            if _vol_filtered > 0:
                logger.info(f"📊 交易量过滤淘汰 {_vol_filtered} 个组合（Call或Put 24h零成交）")
            logger.info(f"成功构建 {len(self.arbitrage_combinations)} 个套利组合")

            # 记录合约发现时间，避免首次扫描立即重复刷新
            self._instrument_refresh_time = time.time()

            # 订阅市场数据
            await self._setup_subscriptions()

            # ================= 🌟 Redis 连通性探测 + 同环境冲突检查 =================
            logger.info(f"正在探测 Redis 数据库连接状态 (db={self._redis_db}, {self._env_label})...")
            try:
                await self.redis.ping()
                logger.info(f"✅ Redis 已连接 (db={self._redis_db}, {self._env_label})")
                # 检查是否有其他同环境引擎在运行（排除自己）
                _lock_key = f"arb_engine_lock:{self.target_currency}"
                _existing = await self.redis.get(_lock_key)
                if _existing:
                    try:
                        _parts = _existing.split(":")
                        _lock_pid = int(_parts[1])
                        if _lock_pid != os.getpid():  # 排除自己持有的锁
                            os.kill(_lock_pid, 0)
                            logger.error(f"🚨 检测到同环境 ({self._env_label}) 另一个引擎在运行! PID={_lock_pid}")
                            logger.error(f"   请先停止旧实例，或手动清锁: redis-cli -n {self._redis_db} DEL {_lock_key}")
                    except ProcessLookupError:
                        logger.info(f"发现残留锁 ({_existing})，旧进程已死亡，启动时将自动接管")
                    except (ValueError, IndexError):
                        pass
            except Exception as e:
                logger.error(f"❌ 无法连接到本地 Redis！已暂停开仓，等待 Redis 恢复后心跳自动解锁。")
                self._singleton_redis_degraded = True
                self._add_pause("Redis不可用")
            # =================================================================

            # ================= 🌟 新增：系统启动时，自动接管并监控已有持仓 =================
            await self._recover_states_from_redis()

            # ========== 手续费同步: Deribit 期权费率 (public基础 + private折扣) ==========
            logger.info("正在尝试同步 Deribit 期权费率...")
            _deribit_fee_ok = await self._sync_deribit_fee_from_instruments()
            self._fee_refresh_last_attempt_time = time.time()
            if not _deribit_fee_ok:
                logger.warning("⚠️ Deribit 期权费率同步未应用，继续使用当前费率配置")
            elif self.binance_auth is None:
                # 仅 Deribit 模式：本次可视为完整成功
                self._fee_refresh_time = self._fee_refresh_last_attempt_time

            # ================= 跨交易所: 初始化 Binance 期货客户端 =================
            if self.binance_auth is not None:
                try:
                    logger.info("正在连接 Binance 期货...")
                    # 重初始化时先关闭旧 Binance WS 客户端，避免僵尸连接/重复任务泄漏
                    if self.binance_ws is not None:
                        try:
                            logger.warning("检测到旧 Binance WS 实例，先执行关闭再重建连接...")
                            await self.binance_ws.close()
                        except Exception as _bn_close_err:
                            logger.warning(f"关闭旧 Binance WS 实例异常: {_bn_close_err}")
                        finally:
                            self.binance_ws = None
                            self.binance_executor = None
                            self.binance_connected = False
                            self._binance_tasks = []
                    self.binance_ws = binance_futures.BinanceFuturesWSClient(self.binance_auth)
                    self.binance_ws.on_market_disconnect = self._on_binance_market_disconnect
                    self.trade_executor.binance_ws = self.binance_ws  # 注入引用，利润守卫需要
                    self.binance_executor = binance_futures.BinanceFuturesExecutor(self.binance_ws)

                    # 加载合约精度信息 (tick_size, step_size)
                    await self.binance_executor.load_contract_info()

                    # 仅使用永续合约，为所有组合设置 perpetual
                    perpetual_sym = self.binance_matcher.perpetual_symbol
                    # 启动前先同步一次真实手续费 (maker/taker)
                    # 来源: GET /fapi/v1/commissionRate?symbol=BTCUSDT
                    _bn_fee_ok = await self._sync_binance_fee_from_commission_rate(perpetual_sym)
                    self._fee_refresh_last_attempt_time = time.time()
                    if _deribit_fee_ok and _bn_fee_ok:
                        self._fee_refresh_time = self._fee_refresh_last_attempt_time
                    for _, combo in self.arbitrage_combinations.items():
                        combo['binance_future'] = perpetual_sym
                        combo['binance_future_type'] = "perpetual"

                    # 启动 Binance WS — 仅订阅永续合约
                    self._binance_tasks = await self.binance_ws.start(symbols=[perpetual_sym])
                    self.binance_connected = self.binance_ws.connected

                    if self.binance_connected:
                        self._remove_pause("Binance WS断连")
                        # Hedge Mode：允许同一 symbol 多空并存，避免反向机会被单向净仓拦截
                        self.binance_dual_side_mode = bool(getattr(self.binance_ws, "dual_side_mode", False))
                        if self.binance_use_hedge_mode:
                            try:
                                if not self.binance_dual_side_mode:
                                    _set_ok = await self.binance_ws.set_position_mode(True)
                                    if _set_ok:
                                        self.binance_dual_side_mode = True
                                _mode_now = await self.binance_ws.get_position_mode()
                                if _mode_now is not None:
                                    self.binance_dual_side_mode = bool(_mode_now)
                            except Exception as _m_err:
                                logger.warning(f"Binance Hedge Mode 设置失败: {_m_err}")

                        # 严格Hedge模式：要求账户最终必须处于双向持仓，否则暂停开仓并告警
                        if self.binance_use_hedge_mode and self.binance_strict_hedge_mode and not self.binance_dual_side_mode:
                            self._add_pause("Hedge模式未就绪")
                            logger.error("🚨 严格Hedge模式开启，但当前账户仍为One-way。系统已暂停开仓。")
                            asyncio.create_task(tg_notifier.send_error_async(
                                "🚨 Binance 严格Hedge模式检查失败：当前账户不是双向持仓(Hedge)\n"
                                "系统已自动暂停开仓。\n"
                                "请先平掉Binance持仓并手动切到Hedge后再恢复。",
                                "strict_hedge_mode"))
                        elif self.binance_use_hedge_mode and not self.binance_dual_side_mode:
                            logger.warning("⚠️ 账户当前为One-way模式，将按单向逻辑运行（反向机会可能被跳过）。")

                        # 🌟 P0-2.1 + P0-1 强化: 启动时显式设置杠杆和保证金模式
                        # 防止账户默认 20x 杠杆 + 全仓导致爆仓风险
                        # 🛡️ P0-1: 任一失败 → 暂停开仓 "Binance参数设置失败"
                        #         避免以危险参数继续交易
                        # 🐛 Fix: 之前误引用 main() 局部变量 bn_cfg (NameError),
                        #   改为在方法内直接取 config.BINANCE_CONFIG
                        _bn_cfg = getattr(config, 'BINANCE_CONFIG', {}) or {}
                        _lev_ok = False
                        _mt_ok = False
                        try:
                            _target_lev = int(_bn_cfg.get("leverage", 3))
                            if _target_lev > 0:
                                _lev_ok = await self.binance_ws.set_leverage(perpetual_sym, _target_lev)
                            else:
                                _lev_ok = True  # 未配置时视为跳过 OK
                        except Exception as _lev_err:
                            logger.error(f"🚨 Binance 杠杆设置异常: {_lev_err}")
                            _lev_ok = False
                        try:
                            _target_mt = str(_bn_cfg.get("margin_type", "ISOLATED")).upper()
                            if _target_mt in ("ISOLATED", "CROSSED"):
                                _mt_ok = await self.binance_ws.set_margin_type(perpetual_sym, _target_mt)
                            else:
                                _mt_ok = True
                        except Exception as _mt_err:
                            logger.error(f"🚨 Binance 保证金模式设置异常: {_mt_err}")
                            _mt_ok = False

                        if not _lev_ok or not _mt_ok:
                            _msg = []
                            if not _lev_ok:
                                _msg.append(f"杠杆设置失败(目标={_bn_cfg.get('leverage', 3)}x)")
                            if not _mt_ok:
                                _msg.append(f"保证金模式失败(目标={_bn_cfg.get('margin_type', 'ISOLATED')})")
                            _reason = " | ".join(_msg)
                            logger.error(f"🚨 Binance 参数设置失败, 暂停开仓! {_reason}")
                            self._add_pause("Binance参数设置失败")
                            asyncio.create_task(tg_notifier.send_error_async(
                                f"🚨 Binance 参数设置失败, 已暂停开仓!\n"
                                f"{_reason}\n"
                                f"⚠️ 账户可能仍为默认 20x 全仓模式\n"
                                f"请手动在 Binance 网页设置后发 /start 恢复", "binance_param_fail"))
                        else:
                            logger.warning(f"✅ Binance 参数就绪: leverage={_bn_cfg.get('leverage')}x, "
                                        f"margin={_bn_cfg.get('margin_type')}")

                        logger.info(f"✅ Binance 期货客户端初始化成功, 仅使用永续合约 {perpetual_sym}")
                        logger.info(f"✅ Binance 持仓模式: {'Hedge(双向)' if self.binance_dual_side_mode else 'One-way(单向)'}")
                        # 绑定 Binance 执行器到 TradeExecutor
                        self.trade_executor._binance_fee_calc = self.binance_fee_calc
                        self.trade_executor._binance_executor = self.binance_executor
                        self.trade_executor._binance_hedge_order_type = self.binance_hedge_order_type
                        self.trade_executor._binance_max_slippage_usd = self.binance_max_slippage_usd
                    else:
                        logger.error("⚠️ Binance 连接超时，跨所套利将不可用")

                except Exception as e:
                    logger.error(f"Binance 初始化异常: {e}")
                    import traceback
                    traceback.print_exc()
                    self.binance_connected = False

                # Binance 初始化完成后，重新验��降级重建组合的对冲分配
                # 原因: _rebuild_states_from_positions 在 Binance 初始化前运行，
                # 当时 binance_dual_side_mode 默认 False，Hedge Mode 下查询可能按净额合并
                if self.binance_connected and self.binance_dual_side_mode:
                    _rebuild_states = [(es, s) for es, s in self.arbitrage_states.items()
                                       if 'rebuild' in str(getattr(s, 'combo_id', ''))]
                    if _rebuild_states:
                        logger.info(
                            f"🔍 Binance Hedge模式已确认，按分腿重新查询 {len(_rebuild_states)} 个降级重建组合的实仓")
                        _requery_fixed = 0
                        _rb_groups: dict = {}
                        _rebuild_keys = set()
                        for _es, _rs in _rebuild_states:
                            _rebuild_keys.add(_es)
                            if not _rs.binance_future_symbol:
                                continue
                            _ps = _rs.binance_position_side or ('SHORT' if _rs.strategy_type == 'sell_future_buy_synthetic' else 'LONG')
                            _gk = (_rs.binance_future_symbol, _ps)
                            _rb_groups.setdefault(_gk, []).append((_es, _rs, _ps))
                        for (_sym, _side), _members in _rb_groups.items():
                            try:
                                _actual_qty, _actual_side, _actual_entry, _known = await self._get_binance_actual_position(_sym, _side)
                                if not _known:
                                    logger.warning(f"Hedge重建: Binance {_sym} {_side} 查询失败，{len(_members)} 个组合保留当前值")
                                    continue
                                _tol = Decimal('0.0001')
                                _non_rebuild_reserved = Decimal('0')
                                for _aes, _ast in self.arbitrage_states.items():
                                    if _aes in _rebuild_keys:
                                        continue
                                    if _ast.state not in ('position_open', 'executing', 'exiting'):
                                        continue
                                    if _ast.binance_future_symbol != _sym:
                                        continue
                                    _ast_side = _ast.binance_position_side or (
                                        'SHORT' if _ast.strategy_type == 'sell_future_buy_synthetic' else 'LONG')
                                    if _ast_side != _side:
                                        continue
                                    _entry = getattr(_ast, 'entry_amount', Decimal('0'))
                                    _qty = getattr(_ast, 'binance_filled_qty', Decimal('0'))
                                    if _entry <= 0 or _qty <= 0:
                                        continue
                                    _non_rebuild_reserved += min(_qty, _entry)

                                _rebuild_pool = _actual_qty - _non_rebuild_reserved
                                if _rebuild_pool < 0:
                                    logger.warning(
                                        f"Hedge重建 {_sym} {_side}: Binance总仓={_actual_qty} 小于非rebuild已占用="
                                        f"{_non_rebuild_reserved}，rebuild组合按0分配")
                                    _rebuild_pool = Decimal('0')

                                _total_entry = sum(
                                    getattr(_m[1], 'entry_amount', Decimal('0'))
                                    for _m in _members if getattr(_m[1], 'entry_amount', Decimal('0')) > 0)
                                if _rebuild_pool - _total_entry > _tol:
                                    logger.warning(
                                        f"Hedge重建 {_sym} {_side}: 可分配残量={_rebuild_pool} > rebuild名义={_total_entry}，"
                                        f"按名义上限分配，超额={_rebuild_pool - _total_entry} 保留给残余仓位巡检")
                                    _rebuild_pool = _total_entry

                                _allocated_sum = Decimal('0')
                                _valid_members = [(_es, _rs, _ps) for _es, _rs, _ps in _members
                                                  if getattr(_rs, 'entry_amount', Decimal('0')) > 0]
                                for _idx, (_es, _rs, _ps) in enumerate(_valid_members):
                                    _ea = getattr(_rs, 'entry_amount', Decimal('0'))
                                    if _ea <= 0 or _total_entry <= 0:
                                        continue
                                    if _idx == len(_valid_members) - 1:
                                        _alloc = max(_rebuild_pool - _allocated_sum, Decimal('0'))
                                    else:
                                        _alloc = (_rebuild_pool * _ea / _total_entry).quantize(Decimal('0.0001'))
                                    _alloc = min(_alloc, _ea)
                                    _allocated_sum += _alloc
                                    _old_qty = _rs.binance_filled_qty
                                    if abs(_alloc - _old_qty) > _tol:
                                        _rs.binance_filled_qty = _alloc
                                        _rs.binance_open_qty = _alloc
                                        if _actual_entry > 0 and _rs.binance_entry_price <= 0:
                                            _rs.binance_entry_price = _actual_entry
                                        _rs.binance_position_side = _ps
                                        _rs.last_update = time.time()
                                        await self._save_state_to_redis(_rs)
                                        _requery_fixed += 1
                                        logger.warning(
                                            f"[{_es[0]}-{_es[1]}] Hedge重建修正: Binance {_ps} qty {_old_qty}→{_alloc} "
                                            f"(总仓={_actual_qty}, 非rebuild已占用={_non_rebuild_reserved}, "
                                            f"可分配={_rebuild_pool}, 权重={_ea}/{_total_entry})")
                            except Exception as _rqe:
                                logger.warning(f"Hedge重建 {_sym} {_side} 查询异常: {_rqe}")
                        if _requery_fixed > 0:
                            logger.warning(f"[Binance分腿对账/post_init] 已按Hedge模式修正 {_requery_fixed} 个组合")
                        try:
                            await self._sanitize_combo_binance_qty("post_binance_init_hedge")
                        except Exception:
                            pass

            self.initialized = True
            # ================= SQLite 迁移: 旧文件 → 新命名 + CSV → SQLite =================
            await self._migrate_to_sqlite()
            # SQLite 就绪后，用终态记录校准日损账本 (弥补 Redis fire-and-forget 崩溃窗口)
            await self._calibrate_daily_pnl_from_sqlite()
            # 🌟 POC: 启动时初始化 SQLite 存储 + 一次性迁移 CSV 历史 + 读回今日峰值
            #   (仅首次 initialize 需做; 重连时 arbitrage_state 还在, 跳过)
            if self._drawdown_date is None:
                try:
                    await self._init_drawdown_store()
                except Exception as _dd_err:
                    # 🌟 SQLite 初始化失败: 只记日志, 不发 Telegram, 当日从 0 开始
                    logger.error(f"🚨 drawdown SQLite 初始化失败: {_dd_err}")
                    from datetime import datetime, timezone
                    self._drawdown_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            try:
                await self._init_account_equity_store()
            except Exception as _eq_err:
                logger.error(f"🚨 account_equity SQLite 初始化失败: {_eq_err}")
            # ================= 🌟 启动余额快照：用于盈利报告与退出报告 =================
            # 仅首次初始化时设置，重连时保留原始快照，确保统计覆盖完整运行周期
            if not hasattr(self, '_start_balance_snap') or self._start_balance_snap is None:
                self._start_balance_snap = await self._snapshot_balances()
                _sb = self._start_balance_snap
                self._start_open_positions = sum(
                    1 for s in self.arbitrage_states.values()
                    if s.state in ('position_open', 'executing', 'exiting')
                )
                logger.info(
                    f"📊 启动余额快照: Deribit 权益={_sb['deribit_equity']:.6f} {self.target_currency} | "
                    f"Binance 权益={_sb['binance_equity']:.2f} USDT | BTC≈${_sb['btc_price']:.0f}")
                if self._start_open_positions > 0:
                    logger.info(
                        f"📌 启动快照时已检测到继承持仓: {self._start_open_positions} 个，"
                        f"/balancechg 将按账户余额变化口径统计。")
                try:
                    await self._record_daily_account_equity(snapshot=_sb, force=True)
                except Exception as _eq_write_err:
                    logger.warning(f"账户权益快照启动写入失败: {_eq_write_err}")
            else:
                logger.info("📊 重连模式：保留原始启动余额快照，不重置盈利统计基准")
                try:
                    await self._record_daily_account_equity(force=True)
                except Exception as _eq_write_err:
                    logger.warning(f"账户权益快照重连写入失败: {_eq_write_err}")
            logger.info("实时套利引擎初始化完成")

            # ================= 🌟 启动幽灵检测：立即扫描启动前残留仓位 =================
            # 宽限期设为0 (通过 _ghost_first_seen 预填时间戳)，让首次检测 30s 后强平
            # 这里先触发一轮注册，30s 后主循环中的周期检测会确认并强平
            logger.info("🔍 启动残留仓位扫描...")
            await self._ghost_and_integrity_check()
            # 🛡️ 修复 E+F 已重构: 启动期不做持仓数据判断，所有对账/诊断延后到稳定窗口
            # 触发位置: monitor_positions 循环内, uptime >= 60s 时调用 _initial_post_startup_audit()
            # 原因: 启动期 WS 加载/dual_side 探测/状态机恢复异步进行，立即对账容易误判

        except Exception as e:
            logger.error(f"初始化失败: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"引擎初始化失败: {str(e)[:200]}", "init_failed"))
            self.initialized = False
            raise

    async def _migrate_to_sqlite(self) -> None:
        """一次性迁移: 旧 db 重命名 + WAL 残留回放 + CSV 导入 + 旧文件删除"""
        _currency = getattr(self, 'target_currency', 'BTC')
        _db_path = getattr(self, '_db_path', '')
        if not _db_path:
            return

        # --- 步骤 1: 旧数据库文件重命名 ---
        _old_db = f"trading_{_currency}.db"
        if os.path.exists(_old_db) and not os.path.exists(_db_path):
            for suffix in ['', '-wal', '-shm']:
                _src = _old_db + suffix
                _dst = _db_path + suffix
                if os.path.exists(_src):
                    os.rename(_src, _dst)
            logger.info(f"📦 旧数据库 {_old_db} → {_db_path}")

        # --- 步骤 1b: 初始化 TradeStore + SpreadStore schema ---
        try:
            await self._trade_store.init()
            await self._spread_store.init()
        except Exception as _e:
            logger.error(f"❌ TradeStore/SpreadStore schema 初始化失败: {_e}")
            return

        # --- 步骤 2: WAL 残留回放 ---
        _wal_replayed = 0
        _wal_path = f"arbitrage_trades_{_currency}.queue.wal.jsonl"
        _ack_path = f"arbitrage_trades_{_currency}.queue.wal.ack"
        if os.path.isfile(_wal_path):
            try:
                import orjson
                _acked = set()
                if os.path.isfile(_ack_path):
                    with open(_ack_path, 'r', encoding='utf-8') as _af:
                        for _line in _af:
                            _id = _line.strip()
                            if _id:
                                _acked.add(_id)
                _replayed = 0
                with open(_wal_path, 'rb') as _wf:
                    for _line in _wf:
                        _line = _line.strip()
                        if not _line:
                            continue
                        try:
                            _obj = orjson.loads(_line)
                        except Exception:
                            continue
                        _wal_id = str(_obj.get('wal_id', '')).strip()
                        if not _wal_id or _wal_id in _acked:
                            continue
                        _rec = _obj.get('record')
                        if isinstance(_rec, dict):
                            try:
                                self._trade_store.insert_sync(_rec)
                                _replayed += 1
                            except Exception:
                                pass
                _wal_replayed = _replayed
                if _replayed:
                    logger.info(f"♻️ WAL 残留回放: {_replayed} 条记录已写入 SQLite")
            except Exception as _e:
                logger.error(f"WAL 回放失败 (不影响后续): {_e}")
            for _f in [_wal_path, _ack_path]:
                try:
                    os.remove(_f)
                except OSError:
                    pass

        # --- 步骤 3: CSV 导入并归档 (rename, 不删除源文件) ---
        _csv_path = f"arbitrage_trades_{_currency}.csv"
        if os.path.isfile(_csv_path):
            try:
                _existing_trades = 0
                try:
                    from db_store import _open_conn as _oc
                    _c = _oc(_db_path)
                    _existing_trades = _c.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
                    _c.close()
                except Exception:
                    pass
                if _existing_trades > _wal_replayed + 5:
                    os.rename(_csv_path, _csv_path + '.imported')
                    logger.info(f"⏭️ trades 表已有 {_existing_trades} 条 (WAL回放{_wal_replayed}条), 跳过CSV重复导入, 已归档 → {_csv_path}.imported")
                else:
                    _n = await self._trade_store.import_from_csv(_csv_path)
                    os.rename(_csv_path, _csv_path + '.imported')
                    logger.info(f"✅ 交易CSV → SQLite: {_n} 条, 已归档 → {_csv_path}.imported")
            except Exception as _e:
                logger.error(f"交易CSV导入失败 (源文件保留): {_e}")

        _spread_csv = f"spread_history_{_currency}.csv"
        if os.path.isfile(_spread_csv):
            try:
                _existing_spreads = 0
                try:
                    from db_store import _open_conn as _oc
                    _c = _oc(_db_path)
                    _existing_spreads = _c.execute("SELECT COUNT(*) FROM spread_snapshots").fetchone()[0]
                    _c.close()
                except Exception:
                    pass
                if _existing_spreads > 100:
                    os.rename(_spread_csv, _spread_csv + '.imported')
                    logger.info(f"⏭️ spread_snapshots 表已有 {_existing_spreads} 条, 跳过CSV重复导入, 已归档 → {_spread_csv}.imported")
                else:
                    _n = await self._spread_store.import_from_csv(_spread_csv)
                    os.rename(_spread_csv, _spread_csv + '.imported')
                    logger.info(f"✅ 价差CSV → SQLite: {_n} 条, 已归档 → {_spread_csv}.imported")
            except Exception as _e:
                logger.error(f"价差CSV导入失败 (源文件保留): {_e}")

        # 清理遗留文件
        for _f in [f"{_csv_path}.lock", f"arbitrage_trades_{_currency}.queue.wal.jsonl",
                    f"arbitrage_trades_{_currency}.queue.wal.ack"]:
            try:
                if os.path.exists(_f):
                    os.remove(_f)
            except OSError:
                pass

    async def _setup_subscriptions(self):
        """订阅市场数据"""
        if not self.arbitrage_combinations:
            return

        channels = []
        unique_instruments = set()

        for combination in self.arbitrage_combinations.values():
            unique_instruments.add(combination['call'])
            unique_instruments.add(combination['put'])
            unique_instruments.add(combination['future'])

        for instrument in unique_instruments:
            channels.append(f"ticker.{instrument}.100ms")
            # 🌟 机构级升级：订阅最高规格的 raw 无延迟增量频道
            channels.append(f"book.{instrument}.raw")

        # 分批订阅
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

        logger.info(f"订阅 {len(channels)} 个数据频道完成")
