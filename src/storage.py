# src/storage.py
"""
Async-safe sqlite writer for price-harvester.

Provides:
 - enqueue_tick(tick) coroutine to push parsed ticks into the writer queue
 - run_storage_loop() coroutine to be started as a background task
"""
import asyncio
import aiosqlite
import logging
import time
from typing import Dict, Any, List

from config.config import config
from src.logging_setup import setup_logging
from src.metrics import increment as metrics_increment

LOG = setup_logging("storage")

DB_PATH = getattr(config, "DATABASE_PATH", "./data/price.db")
BATCH_SIZE = getattr(config, "STORAGE_BATCH_SIZE", 200)
FLUSH_INTERVAL = getattr(config, "STORAGE_FLUSH_INTERVAL_S", 1.0)

_queue: asyncio.Queue | None = None
_task: asyncio.Task | None = None


def get_queue() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue


async def enqueue_tick(tick: Dict[str, Any]) -> None:
    """
    Put one parsed tick into the storage queue.
    tick should be normalized dict with keys:
      - symbol (str)
      - ts (int milliseconds)
      - price (float)
      - qty (float)
      - side (str) e.g. "Buy" / "Sell"
      - raw (optional)
    """
    q = get_queue()
    await q.put(tick)


async def _apply_pragmas(conn: aiosqlite.Connection) -> None:
    # WAL mode for better concurrency and performance on VPS
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.commit()


async def _insert_batch(conn: aiosqlite.Connection, rows: List[tuple]) -> None:
    if not rows:
        return
    await conn.executemany(
        "INSERT INTO ticks(symbol, ts, price, qty, side) VALUES (?, ?, ?, ?, ?);",
        rows,
    )
    await conn.commit()
    metrics_increment("messages_stored", len(rows))


async def _drain_batch(q: asyncio.Queue, max_count: int, timeout: float) -> List[tuple]:
    """
    Collect up to max_count items from queue. Wait up to timeout for first extra item.
    Returns list of prepared tuples for DB insertion.
    """
    rows: List[tuple] = []
    try:
        # always wait for at least one item (blocking)
        item = await q.get()
    except asyncio.CancelledError:
        raise
    except Exception:
        return rows

    # convert first item
    rows.append(
        (
            item.get("symbol"),
            int(item.get("ts") or 0),
            float(item.get("price") or 0.0),
            float(item.get("qty") or 0.0),
            item.get("side"),
        )
    )
    # gather more without blocking too long
    start = time.time()
    while len(rows) < max_count:
        try:
            # try to get quickly, with small timeout
            timeout_left = max(0.0, timeout - (time.time() - start))
            if timeout_left <= 0:
                break
            item = await asyncio.wait_for(q.get(), timeout=timeout_left)
            rows.append(
                (
                    item.get("symbol"),
                    int(item.get("ts") or 0),
                    float(item.get("price") or 0.0),
                    float(item.get("qty") or 0.0),
                    item.get("side"),
                )
            )
        except asyncio.TimeoutError:
            break
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("Error while draining queue; skipping item")
            continue
    return rows


async def run_storage_loop() -> None:
    """
    Long-running task saving ticks from queue into SQLite in batches.
    Start this with: asyncio.create_task(run_storage_loop())
    """
    q = get_queue()
    LOG.info("storage: starting loop, DB=%s", DB_PATH)
    # make sure directory exists (main should ensure but be defensive)
    import os

    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await _apply_pragmas(conn)
            # ensure table exists (safe to run multiple times)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    price REAL,
                    qty REAL,
                    side TEXT,
                    inserted_at INTEGER DEFAULT (strftime('%s','now') * 1000)
                );
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);"
            )
            await conn.commit()

            while True:
                try:
                    rows = await _drain_batch(q, BATCH_SIZE, FLUSH_INTERVAL)
                    if rows:
                        await _insert_batch(conn, rows)
                    else:
                        # nothing queued; small sleep so loop isn't hot
                        await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    LOG.info("storage: cancelled - flushing remaining items")
                    # flush remaining items before exit
                    remaining = []
                    while not q.empty():
                        try:
                            item = q.get_nowait()
                            remaining.append(
                                (
                                    item.get("symbol"),
                                    int(item.get("ts") or 0),
                                    float(item.get("price") or 0.0),
                                    float(item.get("qty") or 0.0),
                                    item.get("side"),
                                )
                            )
                        except Exception:
                            break
                    if remaining:
                        await _insert_batch(conn, remaining)
                    raise
                except Exception:
                    LOG.exception("storage: unexpected error, sleeping briefly")
                    await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        LOG.info("storage: task cancelled (outer)")
    except Exception:
        LOG.exception("storage: fatal error (exiting loop)")


def start_storage_task(loop: asyncio.AbstractEventLoop) -> asyncio.Task:
    """
    Create and remember background storage task. Idempotent.
    """
    global _task
    if _task and not _task.done():
        return _task
    _task = loop.create_task(run_storage_loop(), name="storage-writer")
    return _task
