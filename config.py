# config.py

import os

# 轻量级 .env 加载器 (不依赖 python-dotenv, 避免额外依赖)
def _load_env_file(path: str = ".env"):
    """从 .env 文件加载环境变量 (仅当环境变量未设置时)"""
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    if not os.path.isfile(_env_path):
        return
    try:
        with open(_env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as _e:
        print(f"[config] 警告: 加载 .env 失败: {_e}")

_load_env_file()


def _env(name: str, default: str = "") -> str:
    """从环境变量读取, 未设置则返回 default"""
    return os.environ.get(name, default)


########################Telegram 机器人配置########################
# Key 读取自 .env 文件 (TG_BOT_TOKEN / TG_CHAT_ID)
TELEGRAM_CONFIG = {
    "TG_BOT_TOKEN": _env("TG_BOT_TOKEN"),
    "TG_CHAT_ID": _env("TG_CHAT_ID"),
    # 可选: 限制哪些 user_id 可以发命令 (逗号分隔), 留空则只验证 chat_id
    "TG_ALLOWED_USER_IDS": _env("TG_ALLOWED_USER_IDS", ""),
}
########################基础配置######################
BASE_CONFIG = {  
    "target_currency" : "BTC",      ########### 修 改 币 种 #############
    "test_trading" : True,          ######## False实盘，True测试网 #######
    "scan_interval_ms" : 500,       # 扫描间隔 (ms)
    "futures_numbers" : 4,          # 只扫描最近几个到期日，DTE>72h 的期权会被过滤
    "current_tier" : "standard",    ###后续要验证获取的费用信息跟vip是否一致。Deribit 费率等级 'standard' 或 'vip1'
    "concurrent_batch_size" : 4,    # 每批并发执行数 (建议 3-5)
    # ============ 全局风控参数 ============
    "global_max_delta" : 0.15,      # 第一层警告阈值：Gamma漂移监控，超过只记日志不暂停
    "global_hard_delta" : 0.50,     # 第二层熔断阈值：裸腿风险，超过暂停+自动平仓裸腿
    "record_spread_snapshots": False,  # 是否记录 spread_snapshots 价差快照；关闭可降低 SQLite 增长速度
}

########################BTC配置######################
# API Key 读取自 .env 文件 (DERIBIT_BTC_CLIENT_ID / DERIBIT_BTC_CLIENT_SECRET)
BTC_CONFIG = {
    "CLIENT_ID": _env("DERIBIT_BTC_CLIENT_ID"),
    "CLIENT_SECRET": _env("DERIBIT_BTC_CLIENT_SECRET"),
    "min_profit_threshold": 20,     # 开仓前净利润门槛(USD): 扫描/验证/T3 使用；锚定腿成交后用 post_anchor_min_profit_usd
    "trade_amount": 0.1,           # Deribit 期权最小单位 0.1 BTC
    "moneyness_threshold": 0.2,    # 虚实值容忍度: ±20%
    "max_spread_pct": 0.2,         # 最大买卖价差率: 价差超过卖一价 20% 视为流动性不足
    "min_option_volume": 1,        # 24h 最小交易量(BTC), 0=只过滤零成交
    # ============ Binance 永续 funding 风控 ============
    "max_funding_rate_pct": 0.001, # 当前8h funding rate > 0.1% 跳过开仓 (正常≈0.01%, 0.1%是10倍异常)
    "max_net_gamma": 0.02,         # 单组合净Gamma上限 (到期<24h 自动收紧50% → 0.01)
    "hard_stop_loss_usd": 500,     # 单组合硬止损(USD) - 按 0.1 BTC 面值 ~$7500 计算 = 4%
    "post_anchor_min_profit_usd": 2,     # 锚定腿成交后复算净利门槛；低于此值才回滚，避免正收益组合因原门槛过严变裸腿
    "rollback_ioc_aggressive_ticks": 100, # 锚定腿回滚 IOC 激进 tick 偏移；未满额会自动二次重试 + L2 兜底
    "post_fill_negative_action": "hold",  # 开仓后若全成本净利<0: hold=继续持有到结算 / rollback=立即回滚
    # ============ 结算 TWAP + 基差监控 ============
    "settlement_twap_minutes": 30,        # 结算前 N 分钟启动 TWAP (匹配 Deribit 30 分钟 TWAP 窗口)
    "basis_monitor_hours": 3.0,           # 到期前 N 小时开始监控基差
    "basis_early_trigger_usd": 300.0,     # 绝对基差超此值触发提前 TWAP
    "basis_deterioration_trigger_usd": 150.0,  # 基差从开仓起恶化超此值也触发提前 TWAP
}

########################Binance 期货配置######################
# API Key 读取自 .env 文件 (BINANCE_API_KEY / BINANCE_API_SECRET)
BINANCE_CONFIG = {
    "API_KEY": _env("BINANCE_API_KEY"),
    "API_SECRET": _env("BINANCE_API_SECRET"),
    "use_testnet": True,                # True=测试网, False=实盘
    "use_hedge_mode": True,             # True=双向持仓(允许同一symbol多空并存)
    "strict_hedge_mode": False,         # True=若无法切到Hedge则暂停交易并告警
    "fee_tier": "standard",             # "standard" 或 "vip1" (实盘升级VIP后可改)
    "leverage": 20,                     # 强制杠杆 (1-20, 实盘建议 3-5)
    "margin_type": "ISOLATED",          # "ISOLATED" 隔离保证金 / "CROSSED" 全仓
}
