"""engine/models.py — 数据模型 (从 binance-deribit.py 提取)"""
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set, Any
import time


@dataclass
class MarketData:
    """市场数据结构"""
    bid: Decimal = Decimal('0')
    ask: Decimal = Decimal('0')
    bid_size: Decimal = Decimal('0')
    ask_size: Decimal = Decimal('0')
    min_price: Decimal = Decimal('0')  # Deribit 下单最低价限制
    max_price: Decimal = Decimal('0')  # Deribit 下单最高价限制
    timestamp: float = 0.0

    @property
    def mid_price(self) -> Decimal:
        """获取中间价"""
        if self.bid > Decimal('0') and self.ask > Decimal('0'):
            return (self.bid + self.ask) / Decimal('2')
        elif self.bid > Decimal('0'):
            return self.bid
        elif self.ask > Decimal('0'):
            return self.ask
        return Decimal('0')


@dataclass
class Position:
    """持仓信息"""
    instrument_name: str
    size: Decimal
    average_price: Decimal
    mark_price: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    timestamp: float
    # ====== 🚨 护栏核心：官方 Delta & Gamma 敞口 ======
    delta: Decimal = Decimal('0')
    gamma: Decimal = Decimal('0')

class LocalOrderBook:
    """本地高精度实时订单簿 (Local Order Book)，含序列号丢包检测"""
    def __init__(self):
        self.bids = {}  # 结构: {price_decimal: amount_decimal}
        self.asks = {}  # 结构: {price_decimal: amount_decimal}
        self.last_change_id = None  # 上一次的 change_id，用于丢包检测
        self.is_valid = False  # 标记订单簿是否完整可用（收到 snapshot 后才为 True）

    def update(self, data: dict) -> bool:
        """处理 Raw 级别的增量推送。返回 True=正常，False=检测到丢包需重订阅"""
        current_id = data.get('change_id')
        prev_id = data.get('prev_change_id')

        # 1. 遇到快照，清空重建
        if data.get('type') == 'snapshot':
            self.bids.clear()
            self.asks.clear()
            self.last_change_id = current_id
            self.is_valid = True
            self._apply_updates(data)
            return True

        # 2. 增量更新：检查序列号连续性
        if self.last_change_id is not None and prev_id is not None:
            if prev_id != self.last_change_id:
                # 丢包！标记订单簿无效
                self.is_valid = False
                return False

        # 3. 正常增量更新
        self._apply_updates(data)
        if current_id is not None:
            self.last_change_id = current_id
        return True

    def _apply_updates(self, data: dict):
        """应用 bids/asks 增量到内存"""
        for action, price, amount in data.get('bids', []):
            p_dec = Decimal(str(price))
            if action in ['new', 'change']:
                self.bids[p_dec] = Decimal(str(amount))
            elif action == 'delete':
                self.bids.pop(p_dec, None)

        for action, price, amount in data.get('asks', []):
            p_dec = Decimal(str(price))
            if action in ['new', 'change']:
                self.asks[p_dec] = Decimal(str(amount))
            elif action == 'delete':
                self.asks.pop(p_dec, None)

    def get_top_levels(self, action_side: str) -> list:
        """
        获取排序后的前排深度
        :param action_side: 'buy' (我要买，看卖盘Asks) 或 'sell' (我要卖，看买盘Bids)
        :return: [(price_dec, amount_dec), ...]
        """
        if action_side == 'buy':
            # 我要买，吃掉对手的卖单 (Asks)。价格越低对我越有利，所以按价格【升序】排列
            sorted_prices = sorted(self.asks.keys())
            return [(p, self.asks[p]) for p in sorted_prices]
        else:
            # 我要卖，砸给对手的买单 (Bids)。价格越高对我越有利，所以按价格【降序】排列
            sorted_prices = sorted(self.bids.keys(), reverse=True)
            return [(p, self.bids[p]) for p in sorted_prices]

@dataclass
class Order:
    """订单信息"""
    order_id: str
    instrument_name: str
    side: str  # buy/sell
    amount: Decimal
    price: Optional[Decimal]
    order_type: str  # limit/market
    label: str  # 订单标签，用于追踪策略
    status: str  # open/filled/cancelled/rejected
    timestamp: float
    filled_amount: Decimal = Decimal('0')
    average_price: Decimal = Decimal('0')


@dataclass
class ArbitrageState:
    """套利状态机"""
    expiry_strike: Tuple[str, Decimal]  # (到期日, 行权价)
    state: str  # scanning, anchor_order_placed, waiting_anchor, hedge_orders_placed, position_open, exiting
    strategy_type: Optional[str] = None  # sell_future_buy_synthetic / buy_future_sell_synthetic
    anchor_order_id: Optional[str] = None
    hedge_order_ids: List[str] = field(default_factory=list)
    position_ids: List[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    # ====== 【新增】独立成本账本 ======
    # ====== 实盘级 Synthetic PnL Ledger ======
    entry_prices: Dict[str, Decimal] = field(default_factory=dict)
    entry_amount: Decimal = Decimal('0')  # 锁定：期权的真实成交数量 (BTC)
    future_size_usd: Decimal = Decimal('0')  # 🌟【新增】锁定：期货的真实成交面值 (USD)，彻底杜绝汇率漂移
    combo_id: str = ''  # 🌟 本地订单ID，关联开仓和平仓记录
    # ====== 成交价确认标志：未确认前禁止平仓决策 ======
    prices_confirmed: bool = False
    # ====== Trailing Stop: 浮盈最高点跟踪 ======
    peak_pnl_usd: Decimal = Decimal('0')  # 已废弃: Trailing Stop 已删除
    exit_reason: str = ''
    gamma_exceed_start: float = 0.0
    gamma_exceed_count: int = 0
    _delivery_csv_written: bool = False
    # ====== 跨交易所 Binance 对冲腿字段 ======
    binance_future_symbol: str = ''           # Binance 合约名 (如 "BTCUSDT" 或 "BTCUSDT_260626")
    binance_future_type: str = ''             # "deliverable" 或 "perpetual"
    binance_position_side: str = ''           # Hedge Mode: "LONG"/"SHORT"，单向模式为空
    binance_order_id: str = ''                # Binance 对冲开仓订单 ID
    binance_close_order_id: str = ''           # 🌟 Binance 对冲平仓订单 ID (多 TWAP 分片时管道分隔)
    binance_entry_price: Decimal = Decimal('0')  # Binance 期货实际成交均价 (USDT)
    binance_open_qty: Decimal = Decimal('0')     # Binance 对冲开仓成交数量快照（用于结算开仓费口径）
    binance_filled_qty: Decimal = Decimal('0')   # Binance 期货实际成交数量 (BTC)
    accumulated_funding: Decimal = Decimal('0')  # 永续合约累计 funding 费用 (USDT)
    last_exit_check_ts: float = 0.0  # 平仓风控检查节流时间戳（与 last_update 解耦）
    # 🌟 P1-17 回归修复: 期权订单 ID 必须 dataclass 字段 + 序列化, 否则重启后丢失
    call_order_id: str = ''                    # Deribit Call 期权订单 ID (开仓时保存)
    put_order_id: str = ''                     # Deribit Put 期权订单 ID (开仓时保存)
