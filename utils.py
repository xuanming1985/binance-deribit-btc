"""utils.py — 全局工具类与常量 (从 binance-deribit.py 提取)"""
import orjson
import logging
import time
import asyncio
import config
from decimal import getcontext

# 设置Decimal精度
getcontext().prec = 28

# ================= 🌟 方案 A：超高速 JSON 引擎 (偷梁换柱) =================
class FastJSON:
    @staticmethod
    def loads(obj):
        return orjson.loads(obj)

    @staticmethod
    def dumps(obj):
        # orjson.dumps 返回的是 bytes，我们解码为字符串以兼容原生 WebSocket
        return orjson.dumps(obj).decode('utf-8')


# 自定义 Handler：每条日志写完立即 flush，确保监控面板可实时读取
class _FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


# 交易记录 CSV 固定列（禁止随 record.keys() 漂移，避免监控统计错位）
TRADE_CSV_COLUMNS = [
    '订单ID', '成交时间', '策略方向', '到期日', '行权价', '标的',
    '期权数量', '期货面值(USD)',
    '模拟_Future价格', '实际_Future均价',
    '模拟_Call价格', '实际_Call均价',
    '模拟_Put价格', '实际_Put均价',
    '模拟_手续费(USD)', '实际_手续费(USD)',
    '开仓手续费(USD)', '预估结算手续费(USD)', '已实现funding(USD)',
    '模拟_净利润(USD)', '实际_净利润(USD)', '滑点与偏差损失(USD)',
    'Call_ID', 'Put_ID', 'Future_ID',
    '交易类型', '平仓原因',
    '实际对冲关闭时间',
]

# ================= 新增：API 令牌桶限速器 =================
class TokenBucketRateLimiter:
    """毫秒级令牌桶限速器，防止瞬间并发打穿交易所 API 限制"""

    def __init__(self, calls_per_second: int):
        self.rate = calls_per_second
        self.capacity = calls_per_second
        self.tokens = float(calls_per_second)
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.last_update = now
                self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                wait_time = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)


def setup_logging():
    """配置全局日志（控制台 + 文件）"""
    current_currency = config.BASE_CONFIG.get("target_currency", "SYS")
    log_filename = f"{current_currency}-log.txt"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[_FlushFileHandler(log_filename, encoding='utf-8'), logging.StreamHandler()]
    )


# 模块导入时自动初始化日志
setup_logging()
