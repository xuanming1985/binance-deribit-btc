"""engine/ — 跨所套利引擎模块化包

通过 Mixin 模式将 RealTimeArbitrageEngine 按职责拆分为多个文件，
所有方法仍通过 self 共享状态，零行为变更。
"""
from engine.core import RealTimeArbitrageEngineCore
from engine.persistence_mixin import PersistenceMixin
from engine.redis_mixin import RedisMixin
from engine.ghost_mixin import GhostMixin
from engine.scanner_mixin import ScannerMixin
from engine.execution_mixin import ExecutionMixin
from engine.risk_mixin import RiskMixin
from engine.settlement_mixin import SettlementMixin
from engine.monitor_mixin import MonitorMixin
from engine.balance_mixin import BalanceMixin
from engine.startup_mixin import StartupMixin
from engine.run_mixin import RunMixin


class RealTimeArbitrageEngine(
    RunMixin,
    StartupMixin,
    MonitorMixin,
    SettlementMixin,
    RiskMixin,
    ExecutionMixin,
    ScannerMixin,
    GhostMixin,
    RedisMixin,
    PersistenceMixin,
    BalanceMixin,
    RealTimeArbitrageEngineCore,
):
    """跨所套利引擎 — Deribit 期权 + Binance 永续"""
    pass
