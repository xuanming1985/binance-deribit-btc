"""engine/persistence_mixin.py — SQLite 落盘 + trade_queue 异步写入"""
from __future__ import annotations
import logging
import os
import time
import asyncio
from typing import TYPE_CHECKING

import orjson

if TYPE_CHECKING:
    pass

from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


class PersistenceMixin:
    """Mixin: 交易记录 SQLite 落盘 + trade_queue 异步写入"""

    def _write_jsonl_fallback_sync(self, record: dict, reason: str) -> None:
        """同步写 JSONL fallback (极端场景兜底: 磁盘满/DB损坏)"""
        try:
            _coin = getattr(self, 'target_currency', 'SYS')
            _fallback_file = f"arbitrage_trades_{_coin}.failed.jsonl"
            with open(_fallback_file, mode='a', encoding='utf-8') as _ff:
                _payload = {
                    'fallback_ts': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'reason': reason,
                    'record': record,
                }
                _ff.write(orjson.dumps(_payload, default=str).decode('utf-8') + "\n")
        except Exception as _fb_err:
            logger.error(f"❌ JSONL fallback 也失败: {_fb_err} (记录将丢失!)")

    _TERMINAL_PHASE_MAP = {
        '交割结算': 'delivery',
        '紧急强平': 'emergency',
        '紧急清仓': 'stop_all',
        '紧急清仓(部分)': 'stop_all',
    }

    async def _persist_terminal_record(self, record: dict) -> bool:
        """终态记录同步落盘: 交割结算/紧急强平/stop_all 必须写入 SQLite 后才能设标志。
        绕过内存队列，直接写入 SQLite；失败时降级 JSONL。
        自动生成 record_key={combo_id}:{phase} 实现幂等，防止 Redis 标记丢失后重复计入。
        不同 phase (delivery/emergency/stop_all) 各自独立去重，同一 combo 可有多条终态记录。
        返回 True=新插入, False=幂等跳过。失败时抛异常（调用方标志不会被设置）。"""
        _record = dict(record) if isinstance(record, dict) else {'raw_record': str(record)}
        _order_id = _record.get('订单ID') or _record.get('order_id') or ''
        if _order_id:
            _trade_type = _record.get('交易类型') or _record.get('trade_type') or ''
            _phase = self._TERMINAL_PHASE_MAP.get(_trade_type, 'terminal')
            _record['record_key'] = f"{_order_id}:{_phase}"
        try:
            _inserted = await asyncio.to_thread(self._trade_store.insert_sync, _record)
            if _inserted:
                logger.info("💾 终态记录已同步落盘至 SQLite")
            else:
                logger.info("💾 终态记录为重复写入，已跳过 (幂等)")
            return _inserted
        except Exception as _db_err:
            logger.error(f"❌ 终态记录 SQLite 同步落盘失败: {_db_err}, 降级 JSONL")
            try:
                await asyncio.to_thread(
                    self._write_jsonl_fallback_sync, _record, f'terminal_sync_failed: {str(_db_err)[:200]}')
            except Exception as _fb_err:
                logger.error(f"❌ 终态记录 JSONL fallback 也失败: {_fb_err} (记录将丢失!)")
            raise

    async def _enqueue_trade_record(self, record: dict) -> None:
        """交易记录入队: 入内存队列异步写 SQLite, 入队失败降级 JSONL。"""
        _record = dict(record) if isinstance(record, dict) else {'raw_record': str(record)}

        try:
            self.trade_queue.put_nowait(_record)
            return
        except asyncio.QueueFull:
            _now = time.time()
            if _now - getattr(self, '_trade_queue_backpressure_ts', 0.0) > 60:
                self._trade_queue_backpressure_ts = _now
                _qsize = self.trade_queue.qsize() if hasattr(self.trade_queue, 'qsize') else '?'
                logger.error(
                    f"🚨 trade_queue 已满 (size={_qsize}/{self.trade_queue.maxsize}), "
                    f"将尝试短等待入队，失败则降级 JSONL")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"🚨 落盘队列满 (size={_qsize}), 请检查磁盘空间",
                    "trade_queue_full"))
            try:
                await asyncio.wait_for(self.trade_queue.put(_record), timeout=2.0)
                return
            except Exception:
                _reason = 'trade_queue_full_timeout'
        except Exception as _e:
            logger.error(f"trade_queue 入队异常 (非满): {_e}, 降级 JSONL")
            _reason = f'enqueue_exception: {str(_e)[:80]}'

        try:
            await asyncio.to_thread(self._write_jsonl_fallback_sync, _record, _reason)
        except Exception as _async_err:
            logger.error(f"❌ 入队失败后的 JSONL 处理异常: {_async_err}")

    async def _trade_persistence_worker(self):
        """独立的消息队列消费者任务 — 写入 SQLite
        生命周期绑定 _fatal_shutdown 而非 running，确保 stop_all 后仍可消费终态记录。"""
        logger.info("💾 异步落盘守护进程已启动 (SQLite)")
        while not self._fatal_shutdown or not self.trade_queue.empty():
            try:
                try:
                    record = await asyncio.wait_for(self.trade_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    await asyncio.to_thread(self._trade_store.insert_sync, record)
                    logger.info("💾 交易记录已成功异步落盘至 SQLite")
                except Exception as _db_err:
                    logger.error(f"❌ SQLite 落盘失败: {_db_err}, 降级 JSONL")
                    try:
                        await asyncio.to_thread(
                            self._write_jsonl_fallback_sync, record, f'db_write_failed: {str(_db_err)[:200]}')
                    except Exception as _fb_err:
                        logger.error(f"❌ Fallback JSONL 也失败: {_fb_err} (记录将丢失!)")
                self.trade_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"异步落盘守护进程异常: {e}")
