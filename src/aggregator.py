import asyncio
import time
import aiosqlite
from typing import Dict, Any, Tuple
from collections import defaultdict
from config.config import config

CREATE_AGG_TABLE = """
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
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    UNIQUE(symbol, interval, ts)
);
"""

class OHLCBucket:
    def __init__(self):
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = 0.0

    def add(self, price: float, qty: float):
        if self.open is None:
            self.open = price
            self.high = price
            self.low = price
            self.close = price
            self.volume = float(qty or 0.0)
        else:
            self.close = price
            if price > self.high:
                self.high = price
            if price < self.low:
                self.low = price
            self.volume += float(qty or 0.0)

    def to_tuple(self, symbol: str, interval: str, ts: int) -> Tuple:
        return (symbol, interval, ts, self.open, self.high, self.low, self.close, self.volume)

class OHLCVAggregator:
    """
    Async OHLCV aggregator that creates aggregates for configured intervals.
    Idempotent-flush protection added to avoid duplicate writes for same (symbol,interval,ts).
    """
    INTERVAL_MS = {
        "1s": 1000,
        "1m": 60 * 1000,
    }

    def __init__(self, db_path: str = None, intervals=("1s","1m"), flush_interval: float = 1.0):
        self.db_path = db_path or config.DATABASE_PATH
        self.intervals = list(intervals)
        self.flush_interval = float(flush_interval)
        # buckets: interval -> (symbol, bucket_ts) -> OHLCBucket
        self.buckets: Dict[str, Dict[tuple, OHLCBucket]] = {iv: {} for iv in self.intervals}
        # keep track of which (symbol, ts) we've already flushed to DB to avoid double-write
        self._flushed_ts: Dict[str, set] = {iv: set() for iv in self.intervals}
        self._task = None
        self._stop = asyncio.Event()

    async def start(self):
        # ensure table exists
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(CREATE_AGG_TABLE)
            await db.commit()
        if self._task is None:
            self._task = asyncio.create_task(self._worker())

    async def stop(self):
        self._stop.set()
        if self._task:
            await self._task
            self._task = None

    def _align_ts(self, ts_ms: int, interval_ms: int) -> int:
        return (ts_ms // interval_ms) * interval_ms

    async def feed(self, tick: Dict[str, Any]):
        """
        Accept one tick. Non-blocking addition to in-memory bucket.
        """
        try:
            symbol = tick.get("symbol")
            ts = tick.get("ts")
            price = tick.get("price")
            qty = tick.get("qty") or 0.0
            if symbol is None or ts is None or price is None:
                return
            ts = int(ts)
            price = float(price)
            qty = float(qty)
        except Exception:
            # malformed tick, ignore silently
            return

        for iv in self.intervals:
            interval_ms = self.INTERVAL_MS.get(iv)
            if not interval_ms:
                continue
            bucket_ts = self._align_ts(ts, interval_ms)
            key = (symbol, bucket_ts)
            bmap = self.buckets[iv]
            if key not in bmap:
                bmap[key] = OHLCBucket()
            bmap[key].add(price, qty)

    async def _flush_interval(self, iv: str, cutoff_ts: int):
        """
        Flush buckets for interval iv where bucket_ts < cutoff_ts.
        Returns list of tuples ready to insert to DB.
        """
        interval_ms = self.INTERVAL_MS[iv]
        to_write = []
        bmap = self.buckets[iv]
        keys = list(bmap.keys())
        for (symbol, bucket_ts) in keys:
            if bucket_ts < cutoff_ts:
                # idempotency check: only flush if not already flushed
                if (symbol, bucket_ts) in self._flushed_ts[iv]:
                    # already flushed earlier, remove from map to free memory
                    bmap.pop((symbol, bucket_ts), None)
                    continue
                bucket = bmap.pop((symbol, bucket_ts))
                to_write.append(bucket.to_tuple(symbol, iv, bucket_ts))
                # mark as flushed so future flush/stop won't duplicate
                self._flushed_ts[iv].add((symbol, bucket_ts))
        return to_write

    async def _worker(self):
        """
        Periodic worker: every flush_interval seconds flush finished buckets to DB.
        A bucket is considered finished when its bucket_ts < current aligned time.
        """
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.flush_interval)
                now_ms = int(time.time() * 1000)
                all_to_write = []
                for iv in self.intervals:
                    interval_ms = self.INTERVAL_MS.get(iv)
                    if not interval_ms:
                        continue
                    cutoff = (now_ms // interval_ms) * interval_ms
                    rows = await self._flush_interval(iv, cutoff)
                    all_to_write.extend(rows)

                if all_to_write:
                    async with aiosqlite.connect(self.db_path) as db:
                        try:
                            await db.executemany(
                                "INSERT OR REPLACE INTO aggregates (symbol, interval, ts, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                all_to_write,
                            )
                            await db.commit()
                        except Exception as e:
                            # best-effort: ignore errors to avoid crashing worker
                            print("aggregator: db write error:", e)
            # On stop: flush everything remaining (but skip those already flushed)
            all_to_write = []
            for iv in self.intervals:
                bmap = self.buckets[iv]
                for (symbol, bucket_ts), bucket in list(bmap.items()):
                    if (symbol, bucket_ts) in self._flushed_ts[iv]:
                        # already flushed earlier
                        bmap.pop((symbol, bucket_ts), None)
                        continue
                    all_to_write.append(bucket.to_tuple(symbol, iv, bucket_ts))
                    bmap.pop((symbol, bucket_ts), None)
                    self._flushed_ts[iv].add((symbol, bucket_ts))
            if all_to_write:
                async with aiosqlite.connect(self.db_path) as db:
                    try:
                        await db.executemany(
                            "INSERT OR REPLACE INTO aggregates (symbol, interval, ts, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            all_to_write,
                        )
                        await db.commit()
                    except Exception as e:
                        print("aggregator final write error:", e)
        except asyncio.CancelledError:
            pass
