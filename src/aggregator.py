"""
Simple async OHLCV aggregator.

Usage:
    from src.aggregator import OHLCVAggregator
    agg = OHLCVAggregator(db_path="data/price.db", intervals=("1s","1m","5m","1h"))
    await agg.start(tick_queue)   # tick_queue is an asyncio.Queue() of normalized ticks
    ...
    await agg.stop()
"""

import asyncio
import aiosqlite
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, Any, Iterable, Optional, List

# mapping textual interval -> ms
_INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "5m": 5 * 60_000,
    "1h": 60 * 60_000,
}


@dataclass
class _Bucket:
    symbol: str
    interval: str
    ts: int  # start timestamp (ms)
    open: float
    high: float
    low: float
    close: float
    volume: float

    def update(self, price: float, qty: float):
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += qty


def _align_ts(ts_ms: int, interval_ms: int) -> int:
    """
    Align timestamp to interval start in milliseconds.
    Example: ts_ms = 12345, interval_ms = 10000 -> returns 10000
    """
    return (ts_ms // interval_ms) * interval_ms


class OHLCVAggregator:
    """
    OHLCV aggregator.

    - db_path: SQLite file (will write to aggregates table)
    - intervals: iterable of intervals ("1s","1m","5m","1h")
    - flush_interval: how often (seconds) to sweep/flush expired buckets
    """

    def __init__(
        self,
        db_path: str,
        intervals: Iterable[str] = ("1s", "1m", "5m", "1h"),
        flush_interval: float = 0.5,
    ):
        self.db_path = db_path
        self.intervals = [i for i in intervals if i in _INTERVAL_MS]
        if not self.intervals:
            raise ValueError("no valid intervals provided")
        self.flush_interval = flush_interval

        # internal state
        # buckets: interval -> dict[(symbol, aligned_ts_ms)] -> _Bucket
        self._buckets: Dict[str, Dict[Tuple[str, int], _Bucket]] = {
            interval: {} for interval in self.intervals
        }
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._queue: Optional[asyncio.Queue] = None
        self._db_lock = asyncio.Lock()  # serialize DB writes from aggregator
        self._conn: Optional[aiosqlite.Connection] = None

    async def _ensure_db(self):
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._conn.execute("PRAGMA journal_mode=WAL;")
            await self._conn.execute(
                "CREATE TABLE IF NOT EXISTS aggregates ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "symbol TEXT, interval TEXT, ts INTEGER, "
                "open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            await self._conn.commit()

    async def start(self, tick_queue: asyncio.Queue):
        """
        Start aggregator loop. tick_queue should be an asyncio.Queue that receives
        normalized tick dict with keys: symbol, ts (ms), price (float), qty (float)
        """
        if self._running:
            return
        self._queue = tick_queue
        await self._ensure_db()
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        if not self._running:
            return
        self._running = False
        if self._task:
            await self._task
            self._task = None
        # flush remaining buckets to DB
        await self._flush_all()
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _run_loop(self):
        """
        Main loop: consume ticks from queue and periodically flush expired buckets.
        """
        try:
            while self._running:
                try:
                    # wait for either a tick or a timeout to flush
                    tick = await asyncio.wait_for(self._queue.get(), timeout=self.flush_interval)
                    await self._handle_tick(tick)
                except asyncio.TimeoutError:
                    # time to flush expired buckets
                    await self._flush_expired()
                except Exception:
                    # protect loop from single tick parsing errors
                    import traceback
                    traceback.print_exc()
        except asyncio.CancelledError:
            pass
        finally:
            # final flush on exit
            await self._flush_all()

    async def _handle_tick(self, tick: Dict[str, Any]):
        """
        Update buckets with incoming normalized tick.
        tick expected keys: symbol (str), ts (int ms), price (float), qty (float)
        """
        # sanity checks and normalization
        try:
            symbol = str(tick.get("symbol"))
            ts = int(tick.get("ts"))
            price = float(tick.get("price"))
            qty = float(tick.get("qty", 0.0))
        except Exception:
            # ignore malformed tick
            return

        now_ms = int(time.time() * 1000)
        # ignore ticks far in the future
        if ts - now_ms > 60_000:
            # too far in future; ignore
            return

        for interval in self.intervals:
            interval_ms = _INTERVAL_MS[interval]
            aligned = _align_ts(ts, interval_ms)
            key = (symbol, aligned)
            bmap = self._buckets[interval]
            if key in bmap:
                bmap[key].update(price, qty)
            else:
                bmap[key] = _Bucket(
                    symbol=symbol,
                    interval=interval,
                    ts=aligned,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=qty,
                )

    async def _flush_expired(self):
        """
        Flush buckets whose interval window ended before now.
        For example, for 1s bucket aligned at T, if now >= T + interval_ms, we can flush it.
        """
        now_ms = int(time.time() * 1000)
        to_write: List[_Bucket] = []

        for interval in list(self._buckets.keys()):
            interval_ms = _INTERVAL_MS[interval]
            bucket_map = self._buckets[interval]
            expired_keys = []
            for (symbol, ts), bucket in bucket_map.items():
                # bucket end time is ts + interval_ms
                if now_ms >= ts + interval_ms:
                    expired_keys.append((symbol, ts))
                    to_write.append(bucket)
            # remove expired
            for k in expired_keys:
                bucket_map.pop(k, None)

        if to_write:
            await self._write_buckets(to_write)

    async def _flush_all(self):
        """
        Flush everything remaining (called on shutdown)
        """
        all_buckets = []
        for interval in list(self._buckets.keys()):
            bucket_map = self._buckets[interval]
            all_buckets.extend(bucket_map.values())
            bucket_map.clear()
        if all_buckets:
            await self._write_buckets(all_buckets)

    async def _write_buckets(self, buckets: List[_Bucket]):
        """
        Write list of buckets to DB in a single transaction.
        """
        if not buckets:
            return
        await self._ensure_db()
        async with self._db_lock:
            assert self._conn is not None
            cur = await self._conn.cursor()
            try:
                await cur.executemany(
                    "INSERT INTO aggregates (symbol, interval, ts, open, high, low, close, volume) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            b.symbol,
                            b.interval,
                            b.ts,
                            b.open,
                            b.high,
                            b.low,
                            b.close,
                            b.volume,
                        )
                        for b in buckets
                    ],
                )
                await self._conn.commit()
            finally:
                await cur.close()
