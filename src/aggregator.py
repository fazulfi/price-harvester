import asyncio
import sqlite3
import time
import contextlib
from collections import defaultdict
from typing import Dict, List, Iterable


class OHLCVAggregator:
    def __init__(
        self,
        db_path: str,
        intervals: Iterable[str] = ("1s",),
        flush_interval: float = 1.0,
    ):
        self.db_path = db_path
        self.intervals = tuple(intervals)
        self.flush_interval = flush_interval

        self._buckets: Dict[str, Dict[int, Dict]] = defaultdict(dict)
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._running = False

        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS aggregates (
                symbol TEXT,
                interval TEXT,
                ts INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (symbol, interval, ts)
            )
            """
        )
        self._conn.commit()

    # ======================
    # lifecycle
    # ======================
    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self.flush()
        self._conn.close()

    async def _loop(self):
        while self._running:
            await asyncio.sleep(self.flush_interval)
            await self.flush()

    # ======================
    # public API (TEST USES THIS)
    # ======================
    async def feed(self, tick: Dict):
        """
        tick:
        {
          symbol, ts(ms), price, qty
        }
        """
        symbol = tick["symbol"]
        ts_ms = int(tick["ts"])
        price = float(tick["price"])
        qty = float(tick.get("qty", 0.0))

        async with self._lock:
            for interval in self.intervals:
                sec = self._interval_seconds(interval)
                bucket_ts = (ts_ms // 1000 // sec) * sec * 1000

                b = self._buckets[interval].get(bucket_ts)
                if not b:
                    self._buckets[interval][bucket_ts] = {
                        "symbol": symbol,
                        "interval": interval,
                        "ts": bucket_ts,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": qty,
                    }
                else:
                    b["high"] = max(b["high"], price)
                    b["low"] = min(b["low"], price)
                    b["close"] = price
                    b["volume"] += qty

    async def flush(self):
        async with self._lock:
            rows = []
            for interval, data in self._buckets.items():
                rows.extend(data.values())
            self._buckets.clear()

        if not rows:
            return

        sql = """
        INSERT INTO aggregates
        (symbol, interval, ts, open, high, low, close, volume)
        VALUES
        (:symbol, :interval, :ts, :open, :high, :low, :close, :volume)
        ON CONFLICT(symbol, interval, ts)
        DO UPDATE SET
            high=MAX(high, excluded.high),
            low=MIN(low, excluded.low),
            close=excluded.close,
            volume=aggregates.volume + excluded.volume
        """

        self._conn.executemany(sql, rows)
        self._conn.commit()

    # ======================
    def _interval_seconds(self, interval: str) -> int:
        return {
            "1s": 1,
            "1m": 60,
            "5m": 300,
            "1h": 3600,
        }[interval]
