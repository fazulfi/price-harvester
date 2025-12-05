"""
Async SQLite writer with batching using aiosqlite.

Usage:
    storage = AsyncStorage(batch_size=200, flush_interval=1.0)
    await storage.start()
    await storage.insert_tick(tick_dict)   # many times
    await storage.stop()   # flushes and exits
"""

import asyncio
import json
import time
from typing import Dict, Any, List, Optional
import aiosqlite
from config.config import config

INSERT_SQL = """
INSERT INTO ticks (symbol, ts, price, qty, side, raw)
VALUES (?, ?, ?, ?, ?, ?)
"""

class AsyncStorage:
    def __init__(
        self,
        db_path: Optional[str] = None,
        batch_size: int = 500,
        flush_interval: float = 1.0,
        queue_maxsize: int = 10000,
    ):
        self.db_path = db_path or config.DATABASE_PATH
        self.batch_size = int(batch_size)
        self.flush_interval = float(flush_interval)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._worker_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._running = False

    async def start(self):
        if self._worker_task is None:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker())
    
    async def stop(self):
        """Signal the worker to stop and wait for it to finish (flush remaining)."""
        self._stop_event.set()
        self._running = False
        if self._worker_task:
            await self._worker_task
            self._worker_task = None
        # reset stop for possible restart
        self._stop_event.clear()

    async def insert_tick(self, tick: Dict[str, Any], block: bool = True):
        """
        Put a tick into the queue.
        tick: dict with keys (symbol, ts, price, qty, side, raw)
        block: if False, drop tick on full queue (to avoid backpressure)
        """
        try:
            if block:
                await self.queue.put(tick)
            else:
                self.queue.put_nowait(tick)
        except asyncio.QueueFull:
            # optional policy: drop old / log
            # For now: drop and continue
            print("storage: queue full, dropping tick")

    async def _worker(self):
        """
        Background worker: gathers items into a buffer and writes in batches.
        Uses WAL and executes many in a single transaction for speed.
        """
        buffer: List[Dict[str, Any]] = []
        last_flush = time.time()
        while self._running or not self.queue.empty() or buffer:
            try:
                # Wait for next item up to flush_interval
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
                    buffer.append(item)
                except asyncio.TimeoutError:
                    # nothing new -> flush if buffer not empty
                    pass

                # Drain queue up to batch_size
                while len(buffer) < self.batch_size:
                    try:
                        item = self.queue.get_nowait()
                        buffer.append(item)
                    except asyncio.QueueEmpty:
                        break

                now = time.time()
                # Condition to flush: batch full or interval passed
                if buffer and (len(buffer) >= self.batch_size or (now - last_flush) >= self.flush_interval):
                    await self._flush_buffer(buffer)
                    buffer.clear()
                    last_flush = now

                # check stop event loop condition
                if self._stop_event.is_set() and self.queue.empty() and not buffer:
                    break

            except Exception as exc:
                # Unexpected error in worker loop: log and continue
                print("storage worker error:", exc)
                await asyncio.sleep(1.0)

        # Final flush (if any left)
        if buffer:
            try:
                await self._flush_buffer(buffer)
            except Exception as e:
                print("storage final flush failed:", e)

    async def _flush_buffer(self, buffer: List[Dict[str, Any]]):
        """
        Convert buffer to params and insert using executemany inside transaction.
        """
        if not buffer:
            return
        params = []
        for b in buffer:
            # normalize fields and guard types
            symbol = b.get("symbol")
            ts = None
            try:
                ts_val = b.get("ts")
                ts = int(ts_val) if ts_val is not None else None
            except Exception:
                ts = None
            price = None
            try:
                price = float(b.get("price")) if b.get("price") is not None else None
            except Exception:
                price = None
            qty = None
            try:
                qty = float(b.get("qty")) if b.get("qty") is not None else None
            except Exception:
                qty = None
            side = b.get("side")
            raw = b.get("raw")
            try:
                raw_json = json.dumps(raw, ensure_ascii=False)
            except Exception:
                raw_json = str(raw)
            params.append((symbol, ts, price, qty, side, raw_json))

        # actual DB write
        async with aiosqlite.connect(self.db_path, timeout=30) as db:
            # ensure WAL (redundant if init_db already set, but safe)
            try:
                await db.execute("PRAGMA journal_mode = WAL;")
            except Exception:
                pass
            async with db.execute_many(INSERT_SQL, params) as _:
                # Note: some aiosqlite versions don't support execute_many context manager
                # So do executemany instead if needed:
                pass
            # Fallback to executemany (most compatible)
            try:
                await db.executemany(INSERT_SQL, params)
                await db.commit()
            except Exception as e:
                # If insertion fails, log and rethrow or drop
                print("storage: insert error:", e)
                # (optional) write failed batch to disk for later recovery
                # For now, we drop to avoid infinite retry loop

