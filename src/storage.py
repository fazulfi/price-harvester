import asyncio
import json
import time
from typing import Any, Dict, List

import aiosqlite

from config.config import config
from src.metrics import increment as metrics_increment


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    ts INTEGER,
    price REAL,
    qty REAL,
    side TEXT,
    raw TEXT
);
"""

INSERT_SQL = """
INSERT INTO ticks (symbol, ts, price, qty, side, raw)
VALUES (?, ?, ?, ?, ?, ?);
"""


class AsyncStorage:
    def __init__(
        self,
        db_path: str = "./data/price.db",
        batch_size: int = 100,
        flush_interval: float = 1.0,
        retention_ms: int | None = None,
    ):
        self.db_path = db_path
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.retention_ms = retention_ms if retention_ms is not None else int(config.DATA_RETENTION_HOURS * 3600 * 1000)

        self._queue: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._running = False
        self._worker_task: asyncio.Task | None = None

    async def start(self):
        """Start background flush worker"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute(CREATE_TABLE_SQL)
            await db.commit()

        self._running = True
        self._worker_task = asyncio.create_task(self._worker())

    async def stop(self):
        """Stop worker and flush remaining data"""
        self._running = False

        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        await self._flush()

    async def insert_tick(self, tick: Dict[str, Any]):
        async with self._lock:
            self._queue.append(tick)
            if len(self._queue) >= self.batch_size:
                await self._flush_locked()

    async def _worker(self):
        try:
            while self._running:
                await asyncio.sleep(self.flush_interval)
                await self._flush()
        except asyncio.CancelledError:
            pass

    async def _flush(self):
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self):
        batch = self._queue
        self._queue = []

        params = [
            (
                t.get("symbol"),
                t.get("ts"),
                t.get("price"),
                t.get("qty"),
                t.get("side"),
                json.dumps(t.get("raw")) if t.get("raw") is not None else None,
            )
            for t in batch
        ]

        try:
            async with aiosqlite.connect(self.db_path, timeout=30) as db:
                if params:
                    await db.executemany(INSERT_SQL, params)
                    try:
                        metrics_increment("messages_stored", len(params))
                    except Exception:
                        pass

                await self._cleanup_old_ticks(db)
                await db.commit()
        except Exception as e:
            print("storage insert error:", e)

    async def _cleanup_old_ticks(self, db: aiosqlite.Connection):
        if not self.retention_ms or self.retention_ms <= 0:
            return

        cutoff_ms = int(time.time() * 1000) - int(self.retention_ms)
        await db.execute("DELETE FROM ticks WHERE ts IS NOT NULL AND ts < ?", (cutoff_ms,))
