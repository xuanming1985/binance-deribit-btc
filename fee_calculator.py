"""fee_calculator.py — Deribit 手续费计算器 (从 binance-deribit.py 提取)"""
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)


class FeeCalculator:
    """
    精确的费用计算器 (基于 Deribit 官方费率文档)
    支持 Standard 和 VIP 1 等级自动切换
    """

    def __init__(self, tier: str = 'standard'):
        """
        初始化费率计算器
        :param tier: 用户等级，支持 'standard' 或 'vip1'
        """
        self.tier = tier.lower()

        # 定义费率结构 (单位: 小数，例如 0.0005 代表 0.05%)
        # 数据来源: Deribit Fees PDF Page 6
        self.fee_structure = {
            'standard': {
                # 期货: Maker 0%, Taker 0.05%
                'future': {'maker': Decimal('0.0000'), 'taker': Decimal('0.0005')},
                # 期权: Maker 0.03%, Taker 0.03% (注意：Standard等级Maker不免费)
                'option': {'maker': Decimal('0.0003'), 'taker': Decimal('0.0003')}
            },
            'vip1': {
                # 期货: Maker -0.01% (返佣), Taker 0.035%
                'future': {'maker': Decimal('-0.0001'), 'taker': Decimal('0.00035')},
                # 期权: Maker 0.025%, Taker 0.025% (VIP1 Maker 不是免费，官方费率表 2.5/2.5 bps)
                'option': {'maker': Decimal('0.00025'), 'taker': Decimal('0.00025')}
            }
        }

        if self.tier not in self.fee_structure:
            logger.warning(f"未知费率等级 {tier}, 降级使用 Standard 费率")
            self.tier = 'standard'

        self.current_rates = self.fee_structure[self.tier]
        # 期权手续费上限: Standard 12.5%, VIP1 10.42% (= 83.33% × 12.5%), 官方费率表 Fee Cap 列
        self._fee_cap_by_tier = {
            'standard': Decimal('0.125'),
            'vip1': Decimal('0.1042'),
        }
        self.option_fee_cap_rate = self._fee_cap_by_tier.get(self.tier, Decimal('0.125'))

        # 最小手续费 (防止精度问题导致为0，虽官方未明确最低值但保留作为防呆)
        self.min_fee = Decimal('0.00000001')

    def update_option_rates(self, o_maker, o_taker):
        """由引擎调用，更新期权费率为 API 返回的真实费率（期货费率保持初始值不变）"""
        self.current_rates['option'] = {
            'maker': Decimal(str(o_maker)), 'taker': Decimal(str(o_taker))
        }
        logger.info(f"📊 期权费率已同步: maker={o_maker}, taker={o_taker}")

    def calculate_option_fee(self, future_price: Decimal, option_price: Decimal, amount: Decimal, is_taker: bool = True) -> Decimal:
        """
        计算期权手续费 (BTC)
        :param future_price: 期货价格 (USD)，用于防呆
        :param option_price: 期权权利金 (BTC)
        :param amount: 交易数量 (BTC)
        :param is_taker: 是否为吃单
        """
        if future_price <= Decimal('0') or option_price <= Decimal('0'):
            return Decimal('0')

        rates = self.current_rates['option']
        fee_rate = rates['taker'] if is_taker else rates['maker']

        # Deribit期权手续费为标的资产(BTC)数量的固定比例。
        # 由于 amount 的单位已经是 BTC，手续费 (BTC) = amount * fee_rate。
        base_fee = amount * fee_rate  # 单位：BTC

        # 上限：权利金的 fee_cap% (Standard 12.5%, VIP1 10.42%)
        max_fee = amount * option_price * self.option_fee_cap_rate
        fee = min(base_fee, max_fee)
        return max(fee, Decimal('0'))

    def calculate_future_fee(self, price: Decimal, amount_usd: Decimal, is_taker: bool = True) -> Decimal:
        """
        计算期货手续费 (BTC)
        :param price: 期货价格 (USD)
        :param amount_usd: 期货交易面值 (USD)
        :param is_taker: 是否为吃单
        """
        if price <= Decimal('0'):
            return Decimal('0')

        # 1. 获取对应等级的费率
        rates = self.current_rates['future']
        fee_rate = rates['taker'] if is_taker else rates['maker']

        # 2. 计算合约总价值 (USD) -> 转换为 BTC
        # 传入的参数 amount_usd 已经是 USD 面值总和，无需再乘 10
        total_value_usd = amount_usd
        total_value_btc = total_value_usd / price

        # 3. 计算手续费 (BTC)
        fee = total_value_btc * fee_rate

        if fee > 0:
            return max(fee, self.min_fee)
        else:
            return fee  # 允许返回负数（返佣）

    def calculate_delivery_fee(self, future_price: Decimal, option_price: Decimal,
                               amount: Decimal, is_option: bool = True) -> Decimal:
        """
        🌟 C3 修复 + 费率修正: 计算交割手续费 (BTC/ETH)
        官方费率 (Fee discounts 不适用于交割费，所有等级相同):
        - 期货: 0.025% (notional_btc * 0.00025)
        - 期权: 0.015% (amount * 0.00015), capped at 12.5% of option_price
        - 周合约期货 / 日期权: 豁免 (0%)，由调用方判断
        :param future_price: 期货/标的价格 (USD)
        :param option_price: 期权权利金 (BTC)，期货时传 0
        :param amount: BTC 数量 (期权) 或 USD 面值 (期货)
        :param is_option: True=期权, False=期货
        """
        if is_option:
            delivery_rate = Decimal('0.00015')  # 期权 0.015%
            base_fee = amount * delivery_rate
            # 期权交割费同样有 12.5% premium cap
            # 注意：OTM 到期时 option_price=0，应触发 cap=0（即该腿交割费为0）
            _premium = max(Decimal(str(option_price)), Decimal('0'))
            max_fee = amount * _premium * Decimal('0.125')
            return min(base_fee, max_fee)
        else:
            delivery_rate = Decimal('0.00025')  # 期货 0.025% (官方: BTC/ETH Futures 0.025%)
            if future_price <= 0:
                return Decimal('0')
            return (amount / future_price) * delivery_rate
