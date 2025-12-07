"""
Async SQLite writer for ticks.

- Exposes set_tick_queue(queue) to accept an asyncio.Queue from main.
- Exposes run_storage(dry_run=False) coroutine that consumes queue and
  writes ticks to SQLite in batches using aiosqlite.
- Uses WAL mode and simple retry on SQLITE_BUSY.
- Also exposes enqueue_tick coroutine for compatibility.
"""

import asyncio
import logging
import math
import time
from typing import Any, Dict, Iterable, List, Optional

try:
    import aiosqlite
except Exception:
    aiosqlite = None  # main will handle if missing

from config.config import config

log = logging.getLogger("price_harvester.storage")


# queue that will be set by main via set_tick_queue
_tick_queue: Optional[asyncio.Queue] = None
_stop_requested = False
_db_conn: Optional["aiosqlite.Connection"] = None  # type: ignore[name-defined]


def set_tick_queue(q: asyncio.Queue) -> None:
    """Main will call this to give us the ticks queue (non-blocking put)."""
    global _tick_queue
    _tick_queue = q
    log.info("storage: tick queue set")


async def enqueue_tick(tick: Dict[str, Any]) -> None:
    """Async enqueue helper if caller prefers coroutine interface."""
    if _tick_queue is None:
        log.debug("enqueue_tick called but no queue configured")
        return
    await _tick_queue.put(tick)


async def _ensure_db() -> "aiosqlite.Connection":
    """Open DB and set pragmas."""
    global _db_conn
    if _db_conn is not None:
        return _db_conn
    if aiosqlite is None:
        raise RuntimeError("aiosqlite is required for async storage")

    conn = await aiosqlite.connect(config.DATABASE_PATH, timeout=5.0)
    # performance pragmas
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous = NORMAL;")
    await conn.execute("PRAGMA foreign_keys = ON;")
    await conn.commit()
    _db_conn = conn
    return conn


def _tick_to_row(tick: Dict[str, Any]):
    """Map normalized tick dict to DB row tuple (symbol, ts, price, qty, side)."""
    # expected keys in normalized tick:
    # symbol (str), ts (int ms), price (float), qty (float), side (str)
    sym = tick.get("symbol") or tick.get("s") or tick.get("symbol_id") or "UNKNOWN"
    ts = tick.get("ts") or tick.get("T") or tick.get("time") or int(time.time() * 1000)
    # ensure ints
    try:
        ts = int(ts)
    except Exception:
        try:
            ts = int(float(ts))
        except Exception:
            ts = int(time.time() * 1000)

    price = tick.get("price") or tick.get("p") or tick.get("price_str") or 0.0
    qty = tick.get("qty") or tick.get("v") or tick.get("volume") or 0.0
    side = tick.get("side") or tick.get("S") or "NA"

    try:
        price = float(price)
    except Exception:
        try:
            price = float(str(price).replace(",", ""))
        except Exception:
            price = 0.0
    try:
        qty = float(qty)
    except Exception:
        try:
            qty = float(str(qty).replace(",", ""))
        except Exception:
            qty = 0.0

    return (sym, ts, price, qty, side)


async def run_storage(dry_run: bool = False, batch_size: int = 200, flush_interval: float = 0.5):
    """
    Consume ticks from the configured queue and write to SQLite in batches.

    - dry_run=True will consume but not write to DB (useful for tests).
    - flush_interval: maximum wait before flushing a partial batch.
    """
    global _stop_requested
    if _tick_queue is None:
        raise RuntimeError("storage: tick queue not configured (call set_tick_queue)")
    log.info("storage: run_storage starting (dry_run=%s)", dry_run)

    conn = None
    if not dry_run:
        conn = await _ensure_db()

    batch: List = []
    last_flush = time.time()
    try:
        while not _stop_requested:
            try:
                # gather up to batch_size items with timeout
                item = await asyncio.wait_for(_tick_queue.get(), timeout=flush_interval)
                batch.append(_tick_to_row(item))
                # mark task done on queue
                try:
                    _tick_queue.task_done()
                except Exception:
                    pass
            except asyncio.TimeoutError:
                # flush on timeout if we have something
                pass

            # flush if batch is big or flush_interval passed
            now = time.time()
            if batch and (len(batch) >= batch_size or now - last_flush >= flush_interval):
                if dry_run:
                    log.debug("storage: dry_run flush %d rows", len(batch))
                    batch.clear()
                    last_flush = now
                    continue
                # try insert with basic retry on SQLITE_BUSY
                inserted = False
                attempt = 0
                max_attempts = 3
                while not inserted and attempt < max_attempts:
                    try:
                        placeholders = ",".join(["(?,?,?,?,?)"] * len(batch))
                        sql = (
                            "INSERT INTO ticks (symbol, ts, price, qty, side) VALUES "
                            + ",".join(["(?,?,?,?,?)"] * len(batch))
                        )
                        # flatten batch rows
                        params = []
                        for r in batch:
                            params.extend(r)
                        async with conn.execute("BEGIN"):
                            await conn.execute_many(
                                "INSERT INTO ticks (symbol, ts, price, qty, side) VALUES (?,?,?,?,?)", batch
                            )
                            await conn.commit()
                        inserted = True
                        log.debug("storage: inserted %d rows", len(batch))
                        batch.clear()
                        last_flush = now
                    except Exception as e:
                        attempt += 1
                        log.warning("storage: insert attempt %d failed: %s", attempt, e)
                        await asyncio.sleep(0.1 * attempt)
                if not inserted:
                    log.error("storage: failed to insert batch after %d attempts; dropping batch", max_attempts)
                    batch.clear()

    except asyncio.CancelledError:
        log.info("storage: cancelled")
    except Exception:
        log.exception("storage: unexpected error in run_storage")
    finally:
        # flush remaining batch if any
        if batch and not dry_run and conn:
            try:
                async with conn.execute("BEGIN"):
                    await conn.execute_many(
                        "INSERT INTO ticks (symbol, ts, price, qty, side) VALUES (?,?,?,?,?)", batch
                    )
                    await conn.commit()
                log.info("storage: flushed last %d rows", len(batch))
            except Exception:
                log.exception("storage: failed to flush final batch")
        # close DB
        if _db_conn:
            try:
                await _db_conn.close()
            except Exception:
                log.exception("storage: error closing DB")
        log.info("storage: stopped")


def stop():
    """Request storage loop to stop (can be called from sync shutdown)."""
    global _stop_requested
    _stop_requested = True
    log.info("storage: stop requested")
