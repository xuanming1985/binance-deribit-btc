"""binance_deribit.py — 跨所套利引擎入口: preflight + main()"""
import sys
import os
import logging
import asyncio
import shutil
from decimal import Decimal
from typing import List

import redis.asyncio as redisf
import aiohttp

import config
import binance_futures
from engine import RealTimeArbitrageEngine
from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


async def _preflight_check(allow_force: bool = False) -> bool:
    """启动前环境完整性检查

    检查项:
      [Critical] .env 文件存在
      [Critical] Deribit API key 已配置
      [Critical] Binance API key 已配置 (跨所必需)
      [Critical] Redis 连通性 (P0-3: 单点故障防护)
      [Critical] Deribit REST + 认证 + 持仓查询
      [Critical] Binance REST + 认证 + 持仓查询
      [Critical] 磁盘剩余空间 ≥ 100 MB
      [Warning]  Telegram 配置
      [Warning]  磁盘剩余空间 < 1 GB

    持仓保护:
      - 交易所有持仓 + Critical 失败 → 强制拒绝启动
      - 无持仓 + Critical 失败 + allow_force=True → 允许启动
      - 无持仓 + Critical 失败 + allow_force=False → 拒绝启动
    """
    issues_critical: List[str] = []
    issues_warning: List[str] = []
    has_exchange_positions = False
    deribit_positions_count = 0
    binance_positions_count = 0

    base_cfg = config.BASE_CONFIG
    target_currency = base_cfg.get("target_currency", "BTC")
    is_testnet = bool(base_cfg.get("test_trading", True))
    currency_cfg = config.BTC_CONFIG if target_currency == "BTC" else config.ETH_CONFIG
    bn_cfg = config.BINANCE_CONFIG
    tg_cfg = getattr(config, 'TELEGRAM_CONFIG', {}) or {}

    logger.info("=" * 70)
    logger.info(f"🔍 [PREFLIGHT] 启动前环境完整性检查 ({'测试网' if is_testnet else '实盘'}-{target_currency})")
    logger.info("=" * 70)

    # ===== Check 1: .env 文件 =====
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.isfile(_env_path):
        issues_critical.append(f".env 文件不存在: {_env_path}")
    else:
        logger.info(f"✅ [1/8] .env 文件存在: {_env_path}")

    # ===== Check 2: Deribit API key =====
    if not currency_cfg.get("CLIENT_ID"):
        issues_critical.append(f"DERIBIT_{target_currency}_CLIENT_ID 未配置 (检查 .env)")
    if not currency_cfg.get("CLIENT_SECRET"):
        issues_critical.append(f"DERIBIT_{target_currency}_CLIENT_SECRET 未配置 (检查 .env)")
    if currency_cfg.get("CLIENT_ID") and currency_cfg.get("CLIENT_SECRET"):
        logger.info(f"✅ [2/8] Deribit {target_currency} API key 已配置")

    # ===== Check 3: Binance API key =====
    bn_configured = bool(bn_cfg.get("API_KEY") and bn_cfg.get("API_SECRET"))
    if not bn_configured:
        issues_critical.append("BINANCE_API_KEY / BINANCE_API_SECRET 未配置 (检查 .env, 跨所套利必需)")
    else:
        logger.info("✅ [3/8] Binance API key 已配置")

    # ===== Check 4: Telegram (Warning, 不阻塞) =====
    if not tg_cfg.get("TG_BOT_TOKEN"):
        issues_warning.append("TG_BOT_TOKEN 未配置 - 告警通道不可用")
    if not tg_cfg.get("TG_CHAT_ID"):
        issues_warning.append("TG_CHAT_ID 未配置 - 告警通道不可用")
    if tg_cfg.get("TG_BOT_TOKEN") and tg_cfg.get("TG_CHAT_ID"):
        logger.info("✅ [4/8] Telegram 配置就绪")

    # ===== Check 5: Redis 连通性 =====
    _db_map = {(False, "BTC"): 0, (False, "ETH"): 1, (True, "BTC"): 2, (True, "ETH"): 3}
    redis_db = _db_map.get((is_testnet, target_currency), 0)
    _r_pre = None
    try:
        _r_pre = redis.Redis(host='localhost', port=6379, db=redis_db,
                             decode_responses=True, socket_connect_timeout=3)
        await _r_pre.ping()
        logger.info(f"✅ [5/8] Redis 连通性 OK (db={redis_db})")
    except Exception as _re:
        issues_critical.append(
            f"Redis 不可达 (localhost:6379, db={redis_db}): {type(_re).__name__}: {str(_re)[:100]}")
    finally:
        if _r_pre is not None:
            try:
                await _r_pre.close()
            except Exception:
                pass

    # ===== Check 6: Deribit REST + 认证 + 持仓 =====
    deribit_url = "https://test.deribit.com" if is_testnet else "https://www.deribit.com"
    if currency_cfg.get("CLIENT_ID") and currency_cfg.get("CLIENT_SECRET"):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as _s:
                async with _s.get(f"{deribit_url}/api/v2/public/test") as _resp:
                    if _resp.status != 200:
                        issues_critical.append(f"Deribit REST 不可达 ({deribit_url}, HTTP {_resp.status})")
                        raise RuntimeError("test failed")

                async with _s.get(
                        f"{deribit_url}/api/v2/public/auth",
                        params={
                            "grant_type": "client_credentials",
                            "client_id": currency_cfg["CLIENT_ID"],
                            "client_secret": currency_cfg["CLIENT_SECRET"]
                        }) as _resp:
                    _auth_data = await _resp.json()
                    _token = _auth_data.get("result", {}).get("access_token")
                    if not _token:
                        _err = _auth_data.get("error", _auth_data)
                        issues_critical.append(f"Deribit 认证失败 (检查 API key): {str(_err)[:200]}")
                        raise RuntimeError("auth failed")

                async with _s.get(
                        f"{deribit_url}/api/v2/private/get_positions",
                        params={"currency": target_currency},
                        headers={"Authorization": f"Bearer {_token}"}) as _resp:
                    _pos_data = await _resp.json()
                    if "result" in _pos_data:
                        _active = [p for p in _pos_data["result"] if p.get("size", 0) != 0]
                        deribit_positions_count = len(_active)
                        if deribit_positions_count > 0:
                            has_exchange_positions = True
                            logger.info(
                                f"✅ [6/8] Deribit REST + 认证 OK | ⚠️ 持仓数: {deribit_positions_count}")
                            for p in _active[:5]:
                                logger.info(
                                    f"          - {p.get('instrument_name')}: size={p.get('size')}, mark={p.get('mark_price')}")
                            if len(_active) > 5:
                                logger.info(f"          ... 其余 {len(_active) - 5} 个持仓已省略")
                        else:
                            logger.info("✅ [6/8] Deribit REST + 认证 OK, 无持仓")
                    else:
                        issues_critical.append(f"Deribit 持仓查询失败: {_pos_data.get('error', _pos_data)}")
        except RuntimeError:
            pass
        except Exception as _de:
            issues_critical.append(
                f"Deribit REST 异常 ({deribit_url}): {type(_de).__name__}: {str(_de)[:100]}")

    # ===== Check 7: Binance REST + 认证 + 持仓 =====
    if bn_configured:
        bn_url = "https://testnet.binancefuture.com" if bn_cfg.get("use_testnet", True) else "https://fapi.binance.com"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as _s:
                async with _s.get(f"{bn_url}/fapi/v1/ping") as _resp:
                    if _resp.status != 200:
                        issues_critical.append(f"Binance REST 不可达 ({bn_url}, HTTP {_resp.status})")
                        raise RuntimeError("ping failed")

                _bn_auth_pre = binance_futures.BinanceAuth(
                    api_key=bn_cfg["API_KEY"],
                    api_secret=bn_cfg["API_SECRET"],
                    is_testnet=bn_cfg.get("use_testnet", True))
                _params = _bn_auth_pre.sign({"recvWindow": "5000"})
                _query = "&".join(f"{k}={v}" for k, v in _params.items())
                async with _s.get(
                        f"{bn_url}/fapi/v2/positionRisk?{_query}",
                        headers=_bn_auth_pre.headers) as _resp:
                    if _resp.status == 200:
                        _pos_data = await _resp.json()
                        _bn_active = [p for p in _pos_data if abs(float(p.get("positionAmt", 0))) > 0]
                        binance_positions_count = len(_bn_active)
                        if binance_positions_count > 0:
                            has_exchange_positions = True
                            logger.info(
                                f"✅ [7/8] Binance REST + 认证 OK | ⚠️ 持仓数: {binance_positions_count}")
                            for p in _bn_active[:5]:
                                logger.info(
                                    f"          - {p.get('symbol')} {p.get('positionSide')}: amt={p.get('positionAmt')}, entry={p.get('entryPrice')}")
                            if len(_bn_active) > 5:
                                logger.info(f"          ... 其余 {len(_bn_active) - 5} 个持仓已省略")
                        else:
                            logger.info("✅ [7/8] Binance REST + 认证 OK, 无持仓")
                    elif _resp.status in (401, 403):
                        _err_text = await _resp.text()
                        issues_critical.append(
                            f"Binance API 认证失败 (HTTP {_resp.status}, 检查 API key): {_err_text[:200]}")
                    else:
                        _err_text = await _resp.text()
                        issues_critical.append(f"Binance 持仓查询失败 (HTTP {_resp.status}): {_err_text[:200]}")
        except RuntimeError:
            pass
        except Exception as _bne:
            issues_critical.append(f"Binance REST 异常 ({bn_url}): {type(_bne).__name__}: {str(_bne)[:100]}")

    # ===== Check 8: 磁盘空间 =====
    try:
        _stat = shutil.disk_usage(os.path.dirname(os.path.abspath(__file__)))
        _free_mb = _stat.free / 1024 / 1024
        if _free_mb < 100:
            issues_critical.append(f"磁盘剩余空间不足: {_free_mb:.1f} MB < 100 MB")
        elif _free_mb < 1024:
            issues_warning.append(f"磁盘剩余空间偏低: {_free_mb:.0f} MB < 1 GB")
            logger.info(f"✅ [8/8] 磁盘空间偏低但可用 ({_free_mb:.0f} MB)")
        else:
            logger.info(f"✅ [8/8] 磁盘空间 OK ({_free_mb:.0f} MB free)")
    except Exception as _se:
        issues_warning.append(f"磁盘空间检查失败: {_se}")

    # ===== 汇总结果 =====
    logger.info("-" * 70)
    if issues_warning:
        logger.warning(f"⚠️ [PREFLIGHT] {len(issues_warning)} 条警告 (允许启动):")
        for _i in issues_warning:
            logger.warning(f"  ⚠️  {_i}")

    if issues_critical:
        logger.error("=" * 70)
        logger.error(f"🚨 [PREFLIGHT] 发现 {len(issues_critical)} 条关键问题:")
        for _i in issues_critical:
            logger.error(f"  ❌ {_i}")
        logger.error("=" * 70)
        logger.error(f"📊 交易所持仓汇总: Deribit={deribit_positions_count}, Binance={binance_positions_count}")

        if has_exchange_positions:
            logger.error("=" * 70)
            logger.error("🚫 拒绝启动: 检测到交易所有持仓 + 关键检查失败")
            logger.error("    原因: 持仓存在时状态机重建可能错配 (本次事故根因)")
            logger.error("    步骤:")
            logger.error("      1. 修复上述关键问题 (检查 .env / Redis / 网络)")
            logger.error("      2. 或手动平掉交易所持仓后再启动")
            logger.error("      3. --force-startup 标志在持仓场景下被忽略 (强制保护)")
            logger.error("=" * 70)
            return False

        if allow_force:
            logger.warning("=" * 70)
            logger.warning("⚠️ 启用 --force-startup: 关键检查失败但无持仓, 允许启动")
            logger.warning("    建议: 启动后立即修复上述问题")
            logger.warning("=" * 70)
            return True

        logger.error("=" * 70)
        logger.error("🚫 拒绝启动: 关键检查失败")
        logger.error("    步骤:")
        logger.error("      1. 修复上述关键问题")
        logger.error("      2. 或确认无持仓后加 --force-startup 标志强制启动")
        logger.error("=" * 70)
        return False

    logger.info(f"📊 交易所持仓汇总: Deribit={deribit_positions_count}, Binance={binance_positions_count}")
    logger.info("✅ [PREFLIGHT] 所有关键检查通过，启动引擎...")
    logger.info("=" * 70)
    return True


async def main():
    """主函数"""
    try:
        import signal
        _engine_ref = None
        _shutdown_count = 0

        def _graceful_shutdown(sig, _frame):
            nonlocal _engine_ref, _shutdown_count
            _shutdown_count += 1
            if _engine_ref and _shutdown_count == 1:
                logger.info(f"🛑 收到信号 {sig}，正在优雅退出...")
                _engine_ref._fatal_shutdown = True
                _engine_ref.running = False
            else:
                logger.warning(f"🛑 收到第{_shutdown_count}次退出信号，强制中断...")
                raise KeyboardInterrupt

        signal.signal(signal.SIGINT, _graceful_shutdown)
        signal.signal(signal.SIGTERM, _graceful_shutdown)

        # ====== 1. 解析字典配置 ======
        base_cfg = config.BASE_CONFIG
        target_currency = base_cfg["target_currency"]
        currency_cfg = config.BTC_CONFIG if target_currency == "BTC" else config.ETH_CONFIG
        client_id = currency_cfg["CLIENT_ID"]
        client_secret = currency_cfg["CLIENT_SECRET"]
        is_testnet = base_cfg["test_trading"]

        # ====== 1.5 Preflight 检查 ======
        _allow_force = '--force-startup' in sys.argv
        _preflight_ok = await _preflight_check(allow_force=_allow_force)
        if not _preflight_ok:
            logger.error("⛔ Preflight 检查未通过, 引擎拒绝启动")
            sys.exit(1)

        logger.info(f"正在初始化实时套利引擎 (币种: {target_currency})...")

        # ====== 2. 实例化引擎 ======
        engine = RealTimeArbitrageEngine(
            client_id=client_id,
            client_secret=client_secret,
            fee_tier=base_cfg["current_tier"],
            is_testnet=is_testnet
        )

        _engine_ref = engine

        # 动态绑定参数到引擎
        engine.target_currency = target_currency
        engine.min_profit_threshold = Decimal(str(currency_cfg["min_profit_threshold"]))
        engine.max_option_dte_hours = int(currency_cfg.get("max_option_dte_hours", 72))
        engine.min_option_dte_hours = int(currency_cfg.get("min_option_dte_hours", 12))
        if engine.min_option_dte_hours < 0:
            engine.min_option_dte_hours = 0
        if engine.min_option_dte_hours >= engine.max_option_dte_hours:
            logger.warning(
                f"⚠️ min_option_dte_hours={engine.min_option_dte_hours} 非法(>= max_option_dte_hours={engine.max_option_dte_hours})，已回退为 12")
            engine.min_option_dte_hours = min(12, max(engine.max_option_dte_hours - 1, 0))
        engine.max_positions_per_expiry = int(currency_cfg.get("max_positions_per_expiry", 3))
        engine.trade_amount = Decimal(str(currency_cfg["trade_amount"]))
        engine.moneyness_threshold = Decimal(str(currency_cfg["moneyness_threshold"]))
        engine.max_spread_pct = Decimal(str(currency_cfg["max_spread_pct"]))
        engine.min_option_volume = float(currency_cfg.get("min_option_volume", 0))
        engine.maker_price_aggression = float(currency_cfg.get("maker_price_aggression", 0.8))
        engine.max_net_gamma = Decimal(str(currency_cfg.get("max_net_gamma", "0.02")))

        engine.scan_interval_ms = base_cfg["scan_interval_ms"]
        engine.futures_numbers = base_cfg["futures_numbers"]
        engine.max_wait_time = base_cfg["max_wait_time"]
        engine.concurrent_batch_size = base_cfg["concurrent_batch_size"]
        engine.batch_interval = base_cfg.get("batch_interval", 0.5)
        engine._settlement_pause_seconds = float(base_cfg.get("settlement_pause_seconds", 120))
        engine.settlement_hard_stop_guard = bool(base_cfg.get("settlement_hard_stop_guard", True))
        engine.settlement_hard_stop_grace_seconds = max(
            float(base_cfg.get("settlement_hard_stop_grace_seconds", 1200.0)), 0.0)
        engine.risk_alert_throttle_seconds = max(float(base_cfg.get("risk_alert_throttle_seconds", 300)), 30.0)
        engine.maker_top5_log_interval_seconds = max(
            float(base_cfg.get("maker_top5_log_interval_seconds", 300)), 1.0)
        engine.record_spread_snapshots = bool(base_cfg.get("record_spread_snapshots", True))
        # 全局风控参数
        engine.global_max_delta = Decimal(str(base_cfg.get("global_max_delta", 0.15)))
        engine.global_hard_delta = Decimal(str(base_cfg.get("global_hard_delta", 0.50)))
        # 日损失熔断
        engine.daily_loss_limit_usd = float(base_cfg.get("daily_loss_limit_usd", 0))
        engine.daily_loss_auto_close = bool(base_cfg.get("daily_loss_auto_close", False))
        engine.max_total_positions = int(base_cfg.get("max_total_positions", 10))
        engine.hard_stop_loss_usd = Decimal(str(currency_cfg.get("hard_stop_loss_usd", 300)))
        engine.post_anchor_min_profit_usd = Decimal(str(currency_cfg.get("post_anchor_min_profit_usd", 12)))
        if engine.post_anchor_min_profit_usd < 0:
            logger.warning(
                f"⚠️ post_anchor_min_profit_usd={engine.post_anchor_min_profit_usd} 非法，已回退为 12")
            engine.post_anchor_min_profit_usd = Decimal('12')
        engine.rollback_ioc_aggressive_ticks = max(
            1, min(2000, int(currency_cfg.get("rollback_ioc_aggressive_ticks", 100))))
        engine.trade_executor.engine = engine
        engine.post_fill_negative_action = str(currency_cfg.get("post_fill_negative_action", "hold")).lower()
        if engine.post_fill_negative_action not in ("hold", "rollback"):
            logger.warning(
                f"⚠️ post_fill_negative_action={engine.post_fill_negative_action} 非法，已回退为 hold")
            engine.post_fill_negative_action = "hold"

        # ====== 跨交易所: Binance 期货配置 ======
        bn_cfg = config.BINANCE_CONFIG
        if bn_cfg.get("API_KEY") and bn_cfg.get("API_SECRET"):
            bn_is_testnet = bn_cfg.get("use_testnet", True)
            engine.binance_auth = binance_futures.BinanceAuth(
                api_key=bn_cfg["API_KEY"],
                api_secret=bn_cfg["API_SECRET"],
                is_testnet=bn_is_testnet
            )
            engine.binance_fee_calc = binance_futures.BinanceFeeCalculator(
                tier=bn_cfg.get("fee_tier", "standard")
            )
            engine.binance_matcher = binance_futures.BinanceFuturesMatcher(
                priority="perpetual",
                currency=target_currency
            )
            engine.binance_hedge_order_type = "MARKET"
            engine.binance_max_slippage_usd = Decimal(str(currency_cfg.get("binance_max_slippage_usd", 5.0)))
            engine.min_depth_ratio = Decimal(str(currency_cfg.get("min_depth_ratio", 0.2)))
            engine.binance_max_funding_rate = Decimal(str(currency_cfg.get("max_funding_rate_pct", 0.001)))
            engine.binance_use_hedge_mode = bool(bn_cfg.get("use_hedge_mode", True))
            engine.binance_strict_hedge_mode = bool(bn_cfg.get("strict_hedge_mode", False))
            engine.max_perpetual_hold_hours = int(currency_cfg.get("max_perpetual_hold_hours", 80))
            engine.binance_close_twap_slices = min(
                max(1, int(currency_cfg.get("binance_close_twap_slices", 4))), 20)
            engine.binance_close_twap_interval_sec = min(
                max(0.05, float(currency_cfg.get("binance_close_twap_interval_sec", 0.25))), 2.0)
            engine.settlement_twap_enabled = bool(currency_cfg.get("settlement_twap_enabled", True))
            engine.settlement_twap_minutes = min(
                max(5, int(currency_cfg.get("settlement_twap_minutes", 30))), 60)
            engine.settlement_twap_slices = min(
                max(2, int(currency_cfg.get("settlement_twap_slices", 30))), 30)
            engine.basis_monitor_hours = min(
                max(0.5, float(currency_cfg.get("basis_monitor_hours", 3.0))), 12.0)
            engine.basis_early_trigger_usd = min(
                max(50.0, float(currency_cfg.get("basis_early_trigger_usd", 300.0))), 2000.0)
            engine.basis_deterioration_trigger_usd = min(
                max(30.0, float(currency_cfg.get("basis_deterioration_trigger_usd", 150.0))), 1000.0)
            logger.info(
                f"Binance 配置已加载: testnet={bn_is_testnet}, 仅使用永续合约, "
                f"hedge_mode={'on' if engine.binance_use_hedge_mode else 'off'}, "
                f"strict_hedge={'on' if engine.binance_strict_hedge_mode else 'off'}")
        else:
            logger.warning("⚠️ BINANCE_CONFIG 未配置 API Key，跨所套利不可用")

        # ====== 3. 绑定并启动 Telegram 后台监听 ======
        tg_notifier.bind_engine(engine)
        asyncio.create_task(tg_notifier.start_polling())

        # ====== 4. 发送启动通知 ======
        env_str = "🧪 测试网 (Testnet)" if is_testnet else "💰 实盘环境 (Mainnet)"
        startup_msg = (
            f"🚀 套利机器人已启动\n"
            f"环境: {env_str}\n"
            f"目标标的: {target_currency}\n"
            f"费率等级: {base_cfg['current_tier']}\n"
            f"------------------------\n"
            f"📊 风控参数:\n"
            f"• 交易量: {engine.trade_amount}\n"
            f"• 最低利润: {engine.min_profit_threshold} USD\n"
            f"• 期权DTE窗口: {engine.min_option_dte_hours}h ~ {engine.max_option_dte_hours}h\n"
            f"• 单组合Gamma上限: {engine.max_net_gamma}\n"
            f"• 期货扫描数量: {engine.futures_numbers}\n"
            f"• 实值/虚值容忍: {engine.moneyness_threshold}\n"
            f"• 价差率限制: {engine.max_spread_pct}\n"
            f"• 并发数: {engine.concurrent_batch_size}\n"
            f"• 挂单最大等待: {engine.max_wait_time} 秒\n"
            f"• 锚定腿成交后继续门槛: {engine.post_anchor_min_profit_usd} USD\n"
            f"• 负净利动作: {engine.post_fill_negative_action}\n"
            f"• Binance 平仓TWAP: {int(getattr(engine, 'binance_close_twap_slices', 4))}片 / "
            f"{float(getattr(engine, 'binance_close_twap_interval_sec', 0.25)):.2f}s\n"
            f"• 结算TWAP: {'开启' if getattr(engine, 'settlement_twap_enabled', True) else '关闭'} "
            f"({getattr(engine, 'settlement_twap_slices', 30)}片/{getattr(engine, 'settlement_twap_minutes', 30)}min)\n"
            f"• 基差监控: 到期前{getattr(engine, 'basis_monitor_hours', 3.0)}h，绝对阈值${getattr(engine, 'basis_early_trigger_usd', 300.0)}/恶化阈值${getattr(engine, 'basis_deterioration_trigger_usd', 150.0)}\n"
            f"• 结算窗口暂停: 固定开启 (±{int(getattr(engine, '_settlement_pause_seconds', 120))}s)\n"
            f"• 结算窗口硬止损保护: {'开启' if getattr(engine, 'settlement_hard_stop_guard', True) else '关闭'} "
            f"(缓冲 {int(getattr(engine, 'settlement_hard_stop_grace_seconds', 1200))}s)\n"
            f"• 风控告警节流: {int(getattr(engine, 'risk_alert_throttle_seconds', 300))} 秒\n"
            f"• Binance 对冲: {'已配置' if engine.binance_auth else '未配置'}\n\n"
            f"• Binance 严格Hedge: {'开启' if getattr(engine, 'binance_strict_hedge_mode', False) else '关闭'}\n\n"
            f"提示：发送 `t` 查看日志，发送 `stop` 暂停，发送 `config` 查看配置"
        )
        asyncio.create_task(tg_notifier.send_async(startup_msg))

        await engine.run()

    except Exception as e:
        logger.error(f"程序异常: {e}")
        asyncio.create_task(tg_notifier.send_async(f"【系统崩溃】\n发生致命异常：\n{e}"))
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
