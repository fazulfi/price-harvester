# src/storage.py
import asyncio
import logging
import os
import time
from typing import Dict, Any, List

import aiosqlite

from src import metrics
import config  # expects config.DATABASE_PATH

logger = logging.getLogger("storage")
logger.setLevel(logging.INFO)

DATABASE = getattr(config, "DATABASE_PATH", os.path.join("data", "price.db"))
_BATCH_SIZE = getattr(config, "STORAGE_BATCH_SIZE", 200)
_FLUSH_INTERVAL = getattr(config, "STORAGE_FLUSH_INTERVAL", 0.5)  # seconds

# internal shared writer queue
_queue: asyncio.Queue = asyncio.Queue()
_last_ticks: Dict[str, int] = {}  # symbol -> last ts (ms)
_writer_task: asyncio.Task = None


async def enqueue_tick(tick: Dict[str, Any]) -> None:
    """Public: push a normalized tick into the background writer queue."""
    await _queue.put(tick)
    metrics.QUEUE_SIZE.set(_queue.qsize())


async def _ensure_db(conn: aiosqlite.Connection):
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            side TEXT,
            raw TEXT
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS aggregates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            ts INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
        """
    )
    await conn.commit()


async def _insert_batch(conn: aiosqlite.Connection, rows: List[tuple]):
    if not rows:
        return
    start = time.perf_counter()
    try:
        await conn.executemany(
            "INSERT INTO ticks (symbol, ts, price, qty, side, raw) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        await conn.commit()
        latency = time.perf_counter() - start
        metrics.DB_WRITE_LATENCY.observe(latency)
        metrics.MESSAGES_STORED.inc(len(rows))
    except Exception:
        metrics.ERRORS.inc()
        logger.exception("failed to write batch to db")


async def run_storage_loop(dry_run: bool = False):
    """Forever loop that drains the queue in batches and writes to sqlite."""
    os.makedirs(os.path.dirname(DATABASE), exist_ok=True)
    conn = await aiosqlite.connect(DATABASE)
    await _ensure_db(conn)

    logger.info("storage: writer loop starting (dry_run=%s)", dry_run)
    try:
        while True:
            # gather a batch with timeout
            batch = []
            try:
                first = await asyncio.wait_for(_queue.get(), timeout=_FLUSH_INTERVAL)
                batch.append(first)
            except asyncio.TimeoutError:
                # no items during interval
                pass

            # drain up to batch size
            while len(batch) < _BATCH_SIZE:
                try:
                    item = _queue.get_nowait()
                    batch.append(item)
                except asyncio.QueueEmpty:
                    break

            metrics.QUEUE_SIZE.set(_queue.qsize())

            if not batch:
                # idle - sleep a bit
                await asyncio.sleep(_FLUSH_INTERVAL)
                continue

            # prepare rows
            rows = []
            for t in batch:
                symbol = t.get("symbol")
                ts = int(t.get("ts", 0))
                price = float(t.get("price", 0.0))
                qty = float(t.get("qty", 0.0))
                side = t.get("side", "")
                raw = str(t.get("raw", ""))

                # update last tick map
                _last_ticks[symbol] = ts

                rows.append((symbol, ts, price, qty, side, raw))

            if dry_run:
                logger.debug("storage dry_run would insert %d rows", len(rows))
                metrics.MESSAGES_STORED.inc(len(rows))
            else:
                await _insert_batch(conn, rows)

    except asyncio.CancelledError:
        logger.info("storage: loop cancelled")
    finally:
        await conn.close()


# helpers for external health endpoint
def get_metrics_snapshot():
    return {
        "queue_size": _queue.qsize(),
        "last_ticks": dict(_last_ticks),
    }


# optional convenience to start writer as a background task from main
def start_writer(loop: asyncio.AbstractEventLoop, dry_run: bool = False) -> asyncio.Task:
    global _writer_task
    if _writer_task and not _writer_task.done():
        return _writer_task
    _writer_task = loop.create_task(run_storage_loop(dry_run=dry_run))
    return _writer_task
