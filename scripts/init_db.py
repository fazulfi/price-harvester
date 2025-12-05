#!/usr/bin/env python3
"""
Init SQLite DB for price-harvester.
Creates tables: ticks, aggregates, sets pragmas (WAL) for better concurrency.
"""
import os, sys
# tambahkan root project ke sys.path (relatif terhadap lokasi scripts/)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
from pathlib import Path
from config.config import config

DB_PATH = Path(config.DATABASE_PATH)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Schema SQL: ticks + aggregates
schema_sql = r"""
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ts INTEGER NOT NULL,        -- epoch milliseconds
    price REAL NOT NULL,
    qty REAL,
    side TEXT,                  -- 'buy'|'sell'|'unknown'
    raw TEXT,                   -- original payload (optional JSON)
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);

CREATE TABLE IF NOT EXISTS aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,     -- e.g. '1s','1m'
    ts INTEGER NOT NULL,        -- interval start epoch ms
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    UNIQUE(symbol, interval, ts)
);

CREATE INDEX IF NOT EXISTS idx_agg_symbol_interval_ts ON aggregates(symbol, interval, ts);
"""

def init_db(path: str):
    print(f"[init_db] Ensuring DB file at: {path}")
    conn = sqlite3.connect(path, timeout=30)
    try:
        cur = conn.cursor()
        cur.executescript(schema_sql)
        conn.commit()
        print("[init_db] Schema applied. Tables: ticks, aggregates")
        # Show a simple sanity check: number of tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        print("[init_db] Current tables:", [t[0] for t in tables])
    finally:
        conn.close()

if __name__ == "__main__":
    init_db(str(DB_PATH))
