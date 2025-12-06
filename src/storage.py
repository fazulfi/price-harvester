# src/storage.py
"""
Async storage layer for price-harvester.

Features:
- Uses aiosqlite for async writes
- WAL mode enabled for better concurrency
- Batching: accumulate ticks and flush as batch by size or interval
- Graceful shutdown (flush on stop)
- Simple enqueue_tick coroutine for producers (ws/parser)
- run_storage_loop coroutine wrapper for main.py to run forever
"""

from __future__ import annotations
import asyncio
import aiosqlite
import os
import time
from typing import Any, Dict, List, Optional, Tuple

# default config (can be overridden when creating AsyncStorage)
DEFAULT_DB_PATH = os.environ.get("DATABASE_PATH", "./data/price.db")
DEFAULT_BATCH_SIZE = 200
DEFAULT_FLUSH_INTERVAL = 1.0  # seconds

# SQL DDL
_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ts INTEGER NOT NULL, -- timestamp in ms
    price REAL NOT NULL,
    qty REAL NOT NULL,
    side TEXT,
    raw TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);
CREATE TABLE IF NOT EXISTS aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    ts INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_agg_symbol_interval_ts ON aggregates(symbol, interval, ts);
"""

class AsyncStorage:
    """
    Async storage worker.

    Usage:
      st = AsyncStorage(db_path="./data/price.db")
      await st.start()
      await st.insert_tick({...})
      await st.stop()
    """
    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        max_queue: int = 10000,
    ) -> None:
        self.db_path = db_path
        self.batch_size = int(batch_size)
        self.flush_interval = float(flush_interval)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._conn: Optional[aiosqlite.Connection] = None

    # --- public API for producers ---
    async def insert_tick(self, tick: Dict[str, Any], block: bool = True) -> bool:
        """
        Enqueue a single normalized tick.

        If block is False and queue is full, returns False (dropped).
        """
        try:
            if block:
                await self._queue.put(tick)
                return True
            else:
                self._queue.put_nowait(tick)
                return True
        except asyncio.QueueFull:
            # drop tick
            return False

    # --- lifecycle ---
    async def start(self) -> None:
        if self._running:
            return
        # ensure dir exists
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        # open connection and apply schema
        self._conn = await aiosqlite.connect(self.db_path)
        # enable WAL and pragmas
        await self._conn.execute("PRAGMA journal_mode = WAL;")
        await self._conn.execute("PRAGMA synchronous = NORMAL;")
        # apply schema (create tables if not exists)
        await self._conn.executescript(_SCHEMA_SQL)
        await self._conn.commit()
        # worker
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        return

    async def stop(self) -> None:
        # stop worker task and flush remaining
        if not self._running:
            return
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        # close connection
        if self._conn:
            try:
                await self._conn.commit()
                await self._conn.close()
            except Exception:
                pass
            self._conn = None

    # --- internal worker ---
    async def _worker_loop(self) -> None:
        """
        Consume queue and write in batches.
        """
        if not self._conn:
            # safety
            self._conn = await aiosqlite.connect(self.db_path)
            await self._conn.execute("PRAGMA journal_mode = WAL;")
            await self._conn.execute("PRAGMA synchronous = NORMAL;")
            await self._conn.executescript(_SCHEMA_SQL)
            await self._conn.commit()

        batch: List[Dict[str, Any]] = []
        last_flush = time.time()
        try:
            while True:
                try:
                    # Wait for first tick (with timeout to allow periodic flush)
                    timeout = max(0.0, self.flush_interval - (time.time() - last_flush))
                    item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                    batch.append(item)
                except asyncio.TimeoutError:
                    item = None

                # flush conditions: size or time
                now = time.time()
                if (len(batch) >= self.batch_size) or (now - last_flush >= self.flush_interval and len(batch) > 0):
                    await self._write_batch(batch)
                    batch = []
                    last_flush = now

                # if queue empty and not running, break
                if not self._running and self._queue.empty():
                    if batch:
                        await self._write_batch(batch)
                        batch = []
                    break
        except asyncio.CancelledError:
            # flush on cancel
            if batch:
                await self._write_batch(batch)
            raise
        except Exception:
            # log but keep running
            import traceback
            traceback.print_exc()
            # attempt to continue loop
            await asyncio.sleep(1.0)
            return

    async def _write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Insert a batch of ticks into the DB using executemany.
        Each tick should have keys: symbol, ts (ms int), price (float), qty (float), side, raw (optional)
        """
        if not batch:
            return

        # defensively ensure connection open
        if not self._conn:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._conn.execute("PRAGMA journal_mode = WAL;")
            await self._conn.execute("PRAGMA synchronous = NORMAL;")
            await self._conn.executescript(_SCHEMA_SQL)

        # prepare rows
        rows: List[Tuple[Any, ...]] = []
        for t in batch:
            symbol = t.get("symbol") or t.get("s") or ""
            ts = int(t.get("ts") or t.get("T") or 0)
            price = float(t.get("price") or t.get("p") or 0.0)
            qty = float(t.get("qty") or t.get("v") or 0.0)
            side = t.get("side") or t.get("S") or ""
            raw = t.get("raw")
            # normalize to tuple for DB
            rows.append((symbol, ts, price, qty, side, str(raw)))

        try:
            await self._conn.executemany(
                "INSERT INTO ticks(symbol, ts, price, qty, side, raw) VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
            await self._conn.commit()
        except Exception:
            # on failure, try single inserts (slower) to avoid losing data
            try:
                for r in rows:
                    await self._conn.execute(
                        "INSERT INTO ticks(symbol, ts, price, qty, side, raw) VALUES (?, ?, ?, ?, ?, ?)",
                        r,
                    )
                await self._conn.commit()
            except Exception:
                # last resort: drop batch but print error
                import traceback
                traceback.print_exc()

# --- Convenience singleton + run loop wrapper for main.py and enqueue helper ---

_storage_singleton: Optional[AsyncStorage] = None

def get_storage_singleton() -> AsyncStorage:
    global _storage_singleton
    if _storage_singleton is None:
        _storage_singleton = AsyncStorage()
    return _storage_singleton

async def enqueue_tick(tick: Dict[str, Any], block: bool = True) -> bool:
    """
    Coroutine to enqueue a tick into the global storage queue.

    Producers call: await enqueue_tick(parsed_tick)
    Returns True if queued, False if dropped (queue full and block=False).
    """
    st = get_storage_singleton()
    return await st.insert_tick(tick, block=block)

async def run_storage_loop(db_path: Optional[str] = None, batch_size: Optional[int] = None, flush_interval: Optional[float] = None) -> None:
    """
    Top-level awaitable expected by main.py: starts storage worker and runs until cancelled.
    Use optional parameters to override defaults.
    """
    st = get_storage_singleton()
    if db_path:
        st.db_path = db_path
    if batch_size:
        st.batch_size = int(batch_size)
    if flush_interval:
        st.flush_interval = float(flush_interval)

    await st.start()
    try:
        # just sleep forever; main will cancel this coroutine on shutdown
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        # graceful shutdown: stop storage (flushes remaining)
        await st.stop()
        raise
