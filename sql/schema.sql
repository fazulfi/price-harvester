-- SQLite schema for price-harvester
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ts INTEGER NOT NULL,
    price REAL NOT NULL,
    qty REAL,
    side TEXT,
    raw TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000)
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
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    UNIQUE(symbol, interval, ts)
);

CREATE INDEX IF NOT EXISTS idx_agg_symbol_interval_ts ON aggregates(symbol, interval, ts);
