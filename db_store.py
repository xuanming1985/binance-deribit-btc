# db_store.py
# ================================================================
# 🌟 SQLite POC: 轻量的本地 SQLite 存储层, 替代 CSV 文件持久化
#
# POC 目标: 先把 daily_drawdown 这个小文件迁到 SQLite, 验证可行性,
#           再决定是否推广到 trades / spread_history 等其他 CSV
#
# 设计要点:
#   1. 标准库 sqlite3 (不引入新依赖) + asyncio.to_thread 避免阻塞 event loop
#   2. WAL 模式 (journal_mode=WAL) 支持多进程并发读 + 单进程写
#      主引擎写, Monitor 读, 不会冲突
#   3. synchronous=NORMAL 平衡性能与崩溃安全性
#   4. 所有方法提供 async + sync 两个版本: 主引擎用 async, Flask Monitor 用 sync
#   5. busy_timeout=30s 防锁冲突抖动
# ================================================================

import sqlite3
import asyncio
import time
import os
from decimal import Decimal
from typing import Optional, List, Dict


# ================= 内部 helper =================

def _open_conn(db_path: str) -> sqlite3.Connection:
    """打开 SQLite 连接, 应用连接级 PRAGMA
    性能说明:
      - journal_mode=WAL 是数据库级 (持久在 .db 文件里), 只在 _ensure_schema_sync
        首次建表时设置即可, 每次 open 不再重复执行 (省 ~0.1ms)
      - synchronous 和 busy_timeout 是 connection-level, 必须每次设
    """
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit-ish
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")  # 30s 等待锁, 防并发抖动
    conn.row_factory = sqlite3.Row  # 结果按列名访问
    return conn


def _init_db_pragmas(db_path: str) -> None:
    """首次建表时调用一次, 设置数据库级 PRAGMA (持久化到 .db 文件)"""
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    finally:
        conn.close()


# ================= DrawdownStore =================

class DrawdownStore:
    """每日最大浮盈/浮亏持久化 — SQLite 版本

    表 schema:
        CREATE TABLE daily_drawdown (
            date TEXT PRIMARY KEY,          -- 'YYYY-MM-DD' UTC
            max_single_loss_usd REAL NOT NULL DEFAULT 0,
            max_single_gain_usd REAL NOT NULL DEFAULT 0,
            max_total_loss_usd  REAL NOT NULL DEFAULT 0,
            max_total_gain_usd  REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL        -- 'YYYY-MM-DD HH:MM:SS' UTC
        );

    使用:
        store = DrawdownStore("trading_BTC.db")
        await store.init()                                  # 建表 + WAL
        await store.upsert("2026-04-18", 200, 50, 500, 80)  # 幂等写入
        row = await store.get_by_date("2026-04-18")          # 读单日
        rows = await store.recent_days(30)                    # 读近 30 天 (升序)
    """

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS daily_drawdown (
            date TEXT PRIMARY KEY,
            max_single_loss_usd REAL NOT NULL DEFAULT 0,
            max_single_gain_usd REAL NOT NULL DEFAULT 0,
            max_total_loss_usd  REAL NOT NULL DEFAULT 0,
            max_total_gain_usd  REAL NOT NULL DEFAULT 0,
            max_daily_net_loss_usd REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """
    INDEX = "CREATE INDEX IF NOT EXISTS idx_drawdown_date_desc ON daily_drawdown(date DESC)"

    # 🌟 新字段列名 (用于 ALTER TABLE 幂等迁移)
    _NEW_COLUMNS = (
        ('max_daily_net_loss_usd', 'REAL NOT NULL DEFAULT 0'),
    )

    def __init__(self, db_path: str):
        self._path = db_path
        self._init_done = False

    # ---------- schema 初始化 ----------
    def _ensure_schema_sync(self) -> None:
        # 数据库级 PRAGMA (只做一次, WAL 模式持久化到 .db 文件)
        _init_db_pragmas(self._path)
        conn = _open_conn(self._path)
        try:
            conn.execute(self.SCHEMA)
            conn.execute(self.INDEX)
            # 🌟 ALTER TABLE 幂等迁移: 旧数据库 (schema v1) 缺列时自动补齐,
            # 不删老数据. SQLite 的 ADD COLUMN 只在列不存在时有意义, 所以先查
            # PRAGMA table_info 看已有列, 避免重复 ALTER 报错.
            _existing_cols = set()
            for _row in conn.execute("PRAGMA table_info(daily_drawdown)"):
                # PRAGMA table_info 返回 (cid, name, type, notnull, dflt_value, pk)
                _existing_cols.add(_row[1])
            for _col_name, _col_type in self._NEW_COLUMNS:
                if _col_name not in _existing_cols:
                    conn.execute(f"ALTER TABLE daily_drawdown ADD COLUMN {_col_name} {_col_type}")
        finally:
            conn.close()

    async def init(self) -> None:
        """异步初始化 schema (只需调一次)"""
        if self._init_done:
            return
        await asyncio.to_thread(self._ensure_schema_sync)
        self._init_done = True

    # ---------- upsert ----------
    def _upsert_sync(self, date: str, max_single_loss: float, max_single_gain: float,
                      max_total_loss: float, max_total_gain: float,
                      max_daily_net_loss: float = 0.0) -> None:
        _ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        conn = _open_conn(self._path)
        try:
            conn.execute(
                """
                INSERT INTO daily_drawdown
                    (date, max_single_loss_usd, max_single_gain_usd,
                     max_total_loss_usd, max_total_gain_usd,
                     max_daily_net_loss_usd, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    max_single_loss_usd    = excluded.max_single_loss_usd,
                    max_single_gain_usd    = excluded.max_single_gain_usd,
                    max_total_loss_usd     = excluded.max_total_loss_usd,
                    max_total_gain_usd     = excluded.max_total_gain_usd,
                    max_daily_net_loss_usd = excluded.max_daily_net_loss_usd,
                    updated_at             = excluded.updated_at
                """,
                (date, float(max_single_loss), float(max_single_gain),
                 float(max_total_loss), float(max_total_gain),
                 float(max_daily_net_loss), _ts)
            )
        finally:
            conn.close()

    async def upsert(self, date: str, max_single_loss: float, max_single_gain: float,
                      max_total_loss: float, max_total_gain: float,
                      max_daily_net_loss: float = 0.0) -> None:
        await asyncio.to_thread(
            self._upsert_sync, date, max_single_loss, max_single_gain,
            max_total_loss, max_total_gain, max_daily_net_loss)

    # ---------- 读单日 ----------
    def get_by_date_sync(self, date: str) -> Optional[Dict]:
        conn = _open_conn(self._path)
        try:
            row = conn.execute(
                "SELECT date, max_single_loss_usd, max_single_gain_usd, "
                "max_total_loss_usd, max_total_gain_usd, max_daily_net_loss_usd "
                "FROM daily_drawdown WHERE date = ?",
                (date,)
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        return {
            'date': row['date'],
            'max_single_loss_usd':    float(row['max_single_loss_usd']    or 0),
            'max_single_gain_usd':    float(row['max_single_gain_usd']    or 0),
            'max_total_loss_usd':     float(row['max_total_loss_usd']     or 0),
            'max_total_gain_usd':     float(row['max_total_gain_usd']     or 0),
            'max_daily_net_loss_usd': float(row['max_daily_net_loss_usd'] or 0),
        }

    async def get_by_date(self, date: str) -> Optional[Dict]:
        return await asyncio.to_thread(self.get_by_date_sync, date)

    # ---------- 读近 N 天 ----------
    def recent_days_sync(self, n: int = 30) -> List[Dict]:
        conn = _open_conn(self._path)
        try:
            rows = conn.execute(
                "SELECT date, max_single_loss_usd, max_single_gain_usd, "
                "max_total_loss_usd, max_total_gain_usd, max_daily_net_loss_usd, "
                "updated_at FROM daily_drawdown ORDER BY date DESC LIMIT ?",
                (int(n),)
            ).fetchall()
        finally:
            conn.close()
        # 升序返回, 与原 CSV API 保持一致
        return [
            {
                'date': r['date'],
                'max_single_loss_usd':    float(r['max_single_loss_usd']    or 0),
                'max_single_gain_usd':    float(r['max_single_gain_usd']    or 0),
                'max_total_loss_usd':     float(r['max_total_loss_usd']     or 0),
                'max_total_gain_usd':     float(r['max_total_gain_usd']     or 0),
                'max_daily_net_loss_usd': float(r['max_daily_net_loss_usd'] or 0),
                'updated_at':             r['updated_at'] or '',
            }
            for r in reversed(rows)
        ]

    async def recent_days(self, n: int = 30) -> List[Dict]:
        return await asyncio.to_thread(self.recent_days_sync, n)

    # ---------- CSV 一次性迁移 ----------
    def import_from_csv_sync(self, csv_path: str) -> int:
        """从旧 CSV 文件导入历史数据到 SQLite
        返回导入的行数 (同 date 用 INSERT OR REPLACE 覆盖, 不会出现重复行)
        """
        import csv as _csv
        if not os.path.isfile(csv_path):
            return 0
        conn = _open_conn(self._path)
        _n = 0
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = _csv.DictReader(f)
                _ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                for r in reader:
                    _date = (r.get('date') or '').strip()
                    if not _date:
                        continue
                    try:
                        conn.execute(
                            """
                            INSERT INTO daily_drawdown
                                (date, max_single_loss_usd, max_single_gain_usd,
                                 max_total_loss_usd, max_total_gain_usd,
                                 max_daily_net_loss_usd, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(date) DO UPDATE SET
                                max_single_loss_usd    = excluded.max_single_loss_usd,
                                max_single_gain_usd    = excluded.max_single_gain_usd,
                                max_total_loss_usd     = excluded.max_total_loss_usd,
                                max_total_gain_usd     = excluded.max_total_gain_usd,
                                max_daily_net_loss_usd = excluded.max_daily_net_loss_usd,
                                updated_at             = excluded.updated_at
                            """,
                            (_date,
                             float(r.get('max_single_loss_usd', 0) or 0),
                             float(r.get('max_single_gain_usd', 0) or 0),
                             float(r.get('max_total_loss_usd',  0) or 0),
                             float(r.get('max_total_gain_usd',  0) or 0),
                             float(r.get('max_daily_net_loss_usd', 0) or 0),
                             _ts)
                        )
                        _n += 1
                    except (ValueError, TypeError, sqlite3.Error):
                        continue  # 跳过格式异常的行
        finally:
            conn.close()
        return _n

    async def import_from_csv(self, csv_path: str) -> int:
        return await asyncio.to_thread(self.import_from_csv_sync, csv_path)


# ================= AccountEquityStore =================

class AccountEquityStore:
    """每日账户总权益快照 — SQLite 版本

    date 使用 UTC 日期；USD/BTC 总权益均在写入时计算并固化，Monitor 只读展示。
    """

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS daily_account_equity (
            date                 TEXT PRIMARY KEY,
            timestamp            REAL NOT NULL,
            deribit_equity_btc   REAL NOT NULL,
            deribit_balance_btc  REAL NOT NULL,
            binance_equity_usdt  REAL NOT NULL,
            binance_balance_usdt REAL NOT NULL,
            btc_usd_price        REAL NOT NULL,
            total_equity_usd     REAL NOT NULL,
            total_equity_btc     REAL NOT NULL,
            updated_at           TEXT NOT NULL
        )
    """
    INDEX = "CREATE INDEX IF NOT EXISTS idx_account_equity_date ON daily_account_equity(date)"

    def __init__(self, db_path: str):
        self._path = db_path
        self._init_done = False

    def _ensure_schema_sync(self) -> None:
        _init_db_pragmas(self._path)
        conn = _open_conn(self._path)
        try:
            conn.execute(self.SCHEMA)
            conn.execute(self.INDEX)
        finally:
            conn.close()

    async def init(self) -> None:
        if self._init_done:
            return
        await asyncio.to_thread(self._ensure_schema_sync)
        self._init_done = True

    def upsert_sync(self, date: str, snapshot: dict) -> None:
        _ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        def _decimal_field(name: str) -> Decimal:
            try:
                return Decimal(str(snapshot.get(name, 0) or 0))
            except Exception:
                return Decimal('0')

        _deribit_equity_btc = _decimal_field('deribit_equity_btc')
        _deribit_balance_btc = _decimal_field('deribit_balance_btc')
        _binance_equity_usdt = _decimal_field('binance_equity_usdt')
        _binance_balance_usdt = _decimal_field('binance_balance_usdt')
        _btc_usd_price = _decimal_field('btc_usd_price')
        _total_equity_usd = (
            _deribit_equity_btc * _btc_usd_price + _binance_equity_usdt
            if _btc_usd_price > 0 else Decimal('0')
        )
        _total_equity_btc = (
            _deribit_equity_btc + (_binance_equity_usdt / _btc_usd_price)
            if _btc_usd_price > 0 else Decimal('0')
        )
        conn = _open_conn(self._path)
        try:
            conn.execute(
                """
                INSERT INTO daily_account_equity
                    (date, timestamp, deribit_equity_btc, deribit_balance_btc,
                     binance_equity_usdt, binance_balance_usdt, btc_usd_price,
                     total_equity_usd, total_equity_btc, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    timestamp            = excluded.timestamp,
                    deribit_equity_btc   = excluded.deribit_equity_btc,
                    deribit_balance_btc  = excluded.deribit_balance_btc,
                    binance_equity_usdt  = excluded.binance_equity_usdt,
                    binance_balance_usdt = excluded.binance_balance_usdt,
                    btc_usd_price        = excluded.btc_usd_price,
                    total_equity_usd     = excluded.total_equity_usd,
                    total_equity_btc     = excluded.total_equity_btc,
                    updated_at           = excluded.updated_at
                """,
                (
                    date,
                    float(snapshot.get('timestamp', time.time()) or 0),
                    float(_deribit_equity_btc),
                    float(_deribit_balance_btc),
                    float(_binance_equity_usdt),
                    float(_binance_balance_usdt),
                    float(_btc_usd_price),
                    float(_total_equity_usd),
                    float(_total_equity_btc),
                    _ts,
                )
            )
        finally:
            conn.close()

    async def upsert(self, date: str, snapshot: dict) -> None:
        await asyncio.to_thread(self.upsert_sync, date, snapshot)

    def all_days_sync(self) -> List[Dict]:
        conn = _open_conn(self._path)
        try:
            rows = conn.execute(
                "SELECT date, timestamp, deribit_equity_btc, deribit_balance_btc, "
                "binance_equity_usdt, binance_balance_usdt, btc_usd_price, "
                "total_equity_usd, total_equity_btc, updated_at "
                "FROM daily_account_equity ORDER BY date ASC"
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                'date': r['date'],
                'timestamp': float(r['timestamp'] or 0),
                'deribit_equity_btc': float(r['deribit_equity_btc'] or 0),
                'deribit_balance_btc': float(r['deribit_balance_btc'] or 0),
                'binance_equity_usdt': float(r['binance_equity_usdt'] or 0),
                'binance_balance_usdt': float(r['binance_balance_usdt'] or 0),
                'btc_usd_price': float(r['btc_usd_price'] or 0),
                'total_equity_usd': float(r['total_equity_usd'] or 0),
                'total_equity_btc': float(r['total_equity_btc'] or 0),
                'updated_at': r['updated_at'] or '',
            }
            for r in rows
        ]

    async def all_days(self) -> List[Dict]:
        return await asyncio.to_thread(self.all_days_sync)


# ================= TradeStore =================

class TradeStore:
    """交易记录持久化 — SQLite 版本 (替代 arbitrage_trades CSV)"""

    _CN_TO_EN = {
        '订单ID': 'order_id',
        '成交时间': 'trade_time',
        '策略方向': 'strategy_type',
        '到期日': 'expiry',
        '行权价': 'strike',
        '标的': 'underlying',
        '期权数量': 'option_qty',
        '期货面值(USD)': 'future_notional_usd',
        '模拟_Future价格': 'sim_future_price',
        '实际_Future均价': 'actual_future_price',
        '模拟_Call价格': 'sim_call_price',
        '实际_Call均价': 'actual_call_price',
        '模拟_Put价格': 'sim_put_price',
        '实际_Put均价': 'actual_put_price',
        '模拟_手续费(USD)': 'sim_fee_usd',
        '实际_手续费(USD)': 'actual_fee_usd',
        '开仓手续费(USD)': 'open_fee_usd',
        '预估结算手续费(USD)': 'est_settle_fee_usd',
        '已实现funding(USD)': 'realized_funding_usd',
        '模拟_净利润(USD)': 'sim_profit_usd',
        '实际_净利润(USD)': 'actual_profit_usd',
        '滑点与偏差损失(USD)': 'slippage_usd',
        'Call_ID': 'call_order_id',
        'Put_ID': 'put_order_id',
        'Future_ID': 'future_order_id',
        '交易类型': 'trade_type',
        '平仓原因': 'close_reason',
        '实际对冲关闭时间': 'hedge_close_time',
    }
    _EN_TO_CN = {v: k for k, v in _CN_TO_EN.items()}

    _EN_COLUMNS = [
        'order_id', 'trade_time', 'strategy_type', 'expiry', 'strike',
        'underlying', 'option_qty', 'future_notional_usd',
        'sim_future_price', 'actual_future_price',
        'sim_call_price', 'actual_call_price',
        'sim_put_price', 'actual_put_price',
        'sim_fee_usd', 'actual_fee_usd',
        'open_fee_usd', 'est_settle_fee_usd', 'realized_funding_usd',
        'sim_profit_usd', 'actual_profit_usd', 'slippage_usd',
        'call_order_id', 'put_order_id', 'future_order_id',
        'trade_type', 'close_reason', 'hedge_close_time',
        'record_key',
    ]

    _REAL_COLUMNS = {
        'strike', 'option_qty', 'future_notional_usd',
        'sim_future_price', 'actual_future_price',
        'sim_call_price', 'actual_call_price',
        'sim_put_price', 'actual_put_price',
        'sim_fee_usd', 'actual_fee_usd',
        'open_fee_usd', 'est_settle_fee_usd', 'realized_funding_usd',
        'sim_profit_usd', 'actual_profit_usd', 'slippage_usd',
    }

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS trades (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id             TEXT,
            trade_time           TEXT NOT NULL,
            strategy_type        TEXT,
            expiry               TEXT,
            strike               REAL,
            underlying           TEXT,
            option_qty           REAL,
            future_notional_usd  REAL,
            sim_future_price     REAL,
            actual_future_price  REAL,
            sim_call_price       REAL,
            actual_call_price    REAL,
            sim_put_price        REAL,
            actual_put_price     REAL,
            sim_fee_usd          REAL,
            actual_fee_usd       REAL,
            open_fee_usd         REAL,
            est_settle_fee_usd   REAL,
            realized_funding_usd REAL,
            sim_profit_usd       REAL,
            actual_profit_usd    REAL,
            slippage_usd         REAL,
            call_order_id        TEXT,
            put_order_id         TEXT,
            future_order_id      TEXT,
            trade_type           TEXT,
            close_reason         TEXT,
            hedge_close_time     TEXT,
            created_at           TEXT NOT NULL,
            record_key           TEXT
        )
    """
    INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(trade_time)",
        "CREATE INDEX IF NOT EXISTS idx_trades_type ON trades(trade_type)",
        "CREATE INDEX IF NOT EXISTS idx_trades_expiry_strike ON trades(expiry, strike)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_record_key ON trades(record_key) WHERE record_key IS NOT NULL",
    ]

    def __init__(self, db_path: str):
        self._path = db_path
        self._init_done = False

    def _ensure_schema_sync(self) -> None:
        _init_db_pragmas(self._path)
        conn = _open_conn(self._path)
        try:
            conn.execute(self.SCHEMA)
            # 增量迁移: 已有 trades 表可能缺少新列
            for _col_sql in (
                "ALTER TABLE trades ADD COLUMN record_key TEXT",
            ):
                try:
                    conn.execute(_col_sql)
                except sqlite3.OperationalError:
                    pass
            for idx in self.INDEXES:
                conn.execute(idx)
        finally:
            conn.close()

    async def init(self) -> None:
        if self._init_done:
            return
        await asyncio.to_thread(self._ensure_schema_sync)
        self._init_done = True

    def _normalize_record(self, record: dict) -> dict:
        """将中文或英文 key 统一翻译为英文列名，REAL 列转 float"""
        out = {}
        for k, v in record.items():
            en_key = self._CN_TO_EN.get(k, k)
            if en_key not in self._EN_COLUMNS:
                continue
            if en_key in self._REAL_COLUMNS:
                try:
                    v = float(v) if v not in (None, '') else None
                except (ValueError, TypeError):
                    v = None
            out[en_key] = v
        return out

    def insert_sync(self, record: dict) -> bool:
        """写入交易记录。返回 True=新插入, False=record_key 重复。"""
        normed = self._normalize_record(record)
        if not normed.get('trade_time'):
            normed['trade_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        cols = list(normed.keys()) + ['created_at']
        vals = list(normed.values()) + [time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())]
        placeholders = ', '.join(['?'] * len(cols))
        col_names = ', '.join(cols)
        verb = 'INSERT OR IGNORE' if normed.get('record_key') else 'INSERT'
        conn = _open_conn(self._path)
        try:
            cur = conn.execute(f"{verb} INTO trades ({col_names}) VALUES ({placeholders})", vals)
            return cur.rowcount > 0
        finally:
            conn.close()

    async def insert(self, record: dict) -> None:
        await asyncio.to_thread(self.insert_sync, record)

    def realized_summary_since_sync(self, start_ts: float) -> dict:
        result = {"available": False, "close_count": 0, "realized_pnl_usd": 0.0}
        start_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_ts)) if start_ts else '1970-01-01 00:00:00'
        conn = _open_conn(self._path)
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt, COALESCE(SUM(actual_profit_usd), 0) as pnl "
                "FROM trades "
                "WHERE trade_type IN ('平仓', '紧急强平', '交割结算', '紧急清仓', '紧急清仓(部分)') "
                "  AND trade_time >= ?",
                (start_str,)
            ).fetchone()
            if row:
                result["close_count"] = int(row["cnt"])
                result["realized_pnl_usd"] = float(row["pnl"])
                result["available"] = True
        except sqlite3.Error:
            pass
        finally:
            conn.close()
        return result

    async def realized_summary_since(self, start_ts: float) -> dict:
        return await asyncio.to_thread(self.realized_summary_since_sync, start_ts)

    def query_all_sync(self, limit: int = 5000) -> list:
        """返回最新 limit 条记录 (中文 key, Monitor 前端兼容), 按时间升序"""
        conn = _open_conn(self._path)
        try:
            rows = conn.execute(
                "SELECT * FROM (SELECT * FROM trades ORDER BY id DESC LIMIT ?) sub ORDER BY id ASC",
                (limit,)
            ).fetchall()
        finally:
            conn.close()
        result = []
        for row in rows:
            d = dict(row)
            cn_row = {}
            for en_key, val in d.items():
                if en_key in ('id', 'created_at') or en_key not in self._EN_TO_CN:
                    continue
                cn_key = self._EN_TO_CN.get(en_key, en_key)
                cn_row[cn_key] = '' if val is None else str(val)
            result.append(cn_row)
        return result

    def import_from_csv_sync(self, csv_path: str) -> int:
        import csv as _csv
        if not os.path.isfile(csv_path):
            return 0
        conn = _open_conn(self._path)
        n = 0
        try:
            conn.execute("BEGIN")
            with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
                reader = _csv.DictReader(f)
                _ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                for r in reader:
                    normed = self._normalize_record(r)
                    if not normed.get('trade_time'):
                        continue
                    cols = list(normed.keys()) + ['created_at']
                    vals = list(normed.values()) + [_ts]
                    placeholders = ', '.join(['?'] * len(cols))
                    col_names = ', '.join(cols)
                    try:
                        conn.execute(
                            f"INSERT INTO trades ({col_names}) VALUES ({placeholders})", vals)
                        n += 1
                    except (ValueError, TypeError, sqlite3.Error):
                        continue
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return n

    async def import_from_csv(self, csv_path: str) -> int:
        return await asyncio.to_thread(self.import_from_csv_sync, csv_path)


# ================= SpreadStore =================

class SpreadStore:
    """价差快照持久化 — SQLite 版本 (替代 spread_history CSV)"""

    SPREAD_COLUMNS = [
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

    _TEXT_COLUMNS = {'timestamp', 'expiry', 'bn_symbol', 'bn_type', 'maker_anchor'}
    _INT_COLUMNS = {'strike', 'has_position', 'depth_pass', 'funding_pass', 'dte_pass', 'executable'}

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS spread_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            expiry           TEXT NOT NULL,
            strike           INTEGER NOT NULL,
            bn_symbol        TEXT,
            bn_type          TEXT,
            bn_mid           REAL,
            dr_fwd_mid       REAL,
            syn_sell         REAL,
            syn_buy          REAL,
            spread_sell      REAL,
            spread_buy       REAL,
            has_position     INTEGER,
            dte_hours        REAL,
            funding_rate     REAL,
            open_fee         REAL,
            settle_fee       REAL,
            funding_cost     REAL,
            net_sell         REAL,
            net_buy          REAL,
            c_bid            REAL,
            c_ask            REAL,
            p_bid            REAL,
            p_ask            REAL,
            c_spread_usd     REAL,
            p_spread_usd    REAL,
            maker_anchor     TEXT,
            maker_spread_sell REAL,
            maker_net_sell   REAL,
            maker_spread_buy REAL,
            maker_net_buy    REAL,
            c_bid_sz         REAL,
            c_ask_sz         REAL,
            p_bid_sz         REAL,
            p_ask_sz         REAL,
            bn_bid_sz        REAL,
            bn_ask_sz        REAL,
            maker_aggr_call  REAL,
            maker_aggr_put   REAL,
            depth_pass       INTEGER,
            funding_pass     INTEGER,
            dte_pass         INTEGER,
            executable       INTEGER
        )
    """
    INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_spread_ts ON spread_snapshots(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_spread_expiry ON spread_snapshots(expiry, strike)",
    ]

    def __init__(self, db_path: str):
        self._path = db_path
        self._init_done = False

    def _ensure_schema_sync(self) -> None:
        _init_db_pragmas(self._path)
        conn = _open_conn(self._path)
        try:
            conn.execute(self.SCHEMA)
            for idx in self.INDEXES:
                conn.execute(idx)
        finally:
            conn.close()

    async def init(self) -> None:
        if self._init_done:
            return
        await asyncio.to_thread(self._ensure_schema_sync)
        self._init_done = True

    def _coerce_value(self, col: str, val):
        if val is None or val == '':
            return None
        if col in self._TEXT_COLUMNS:
            return str(val)
        if col in self._INT_COLUMNS:
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def insert_batch_sync(self, rows: list) -> int:
        if not rows:
            return 0
        conn = _open_conn(self._path)
        n = 0
        try:
            conn.execute("BEGIN")
            for row in rows:
                cols_present = [c for c in self.SPREAD_COLUMNS if c in row]
                if not cols_present or 'timestamp' not in row:
                    continue
                vals = [self._coerce_value(c, row[c]) for c in cols_present]
                placeholders = ', '.join(['?'] * len(cols_present))
                col_names = ', '.join(cols_present)
                try:
                    conn.execute(
                        f"INSERT INTO spread_snapshots ({col_names}) VALUES ({placeholders})",
                        vals)
                    n += 1
                except sqlite3.Error:
                    continue
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return n

    async def insert_batch(self, rows: list) -> int:
        return await asyncio.to_thread(self.insert_batch_sync, rows)

    def import_from_csv_sync(self, csv_path: str) -> int:
        import csv as _csv
        if not os.path.isfile(csv_path):
            return 0
        conn = _open_conn(self._path)
        n = 0
        try:
            conn.execute("BEGIN")
            with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
                reader = _csv.DictReader(f)
                for r in reader:
                    cols_present = [c for c in self.SPREAD_COLUMNS if c in r]
                    if not cols_present or 'timestamp' not in r:
                        continue
                    vals = [self._coerce_value(c, r[c]) for c in cols_present]
                    placeholders = ', '.join(['?'] * len(cols_present))
                    col_names = ', '.join(cols_present)
                    try:
                        conn.execute(
                            f"INSERT INTO spread_snapshots ({col_names}) VALUES ({placeholders})",
                            vals)
                        n += 1
                    except (ValueError, TypeError, sqlite3.Error):
                        continue
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return n

    async def import_from_csv(self, csv_path: str) -> int:
        return await asyncio.to_thread(self.import_from_csv_sync, csv_path)
