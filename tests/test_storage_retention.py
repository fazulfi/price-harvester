import sqlite3
import time

import pytest

from src.storage import AsyncStorage


@pytest.mark.asyncio
async def test_storage_keeps_only_last_hour(tmp_path):
    db_path = tmp_path / "retention.db"
    storage = AsyncStorage(db_path=str(db_path), batch_size=10, flush_interval=999, retention_ms=3600_000)
    await storage.start()

    now_ms = int(time.time() * 1000)
    old_tick = {
        "symbol": "BTCUSDT",
        "ts": now_ms - 3600_000 - 5_000,
        "price": 100.0,
        "qty": 1.0,
        "side": "Buy",
        "raw": {"k": "old"},
    }
    fresh_tick = {
        "symbol": "ETHUSDT",
        "ts": now_ms - 10_000,
        "price": 200.0,
        "qty": 2.0,
        "side": "Sell",
        "raw": {"k": "fresh"},
    }

    await storage.insert_tick(old_tick)
    await storage.insert_tick(fresh_tick)
    await storage.stop()

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM ticks ORDER BY id")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()

    assert rows == ["ETHUSDT"]
