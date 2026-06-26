-- init.sql
-- ─────────────────────────────────────────────────────────────────
-- This file runs automatically when the Postgres container starts for the first time.
-- It sets up the TimescaleDB extension and creates our tables.
--
-- TimescaleDB concept:
--   A "hypertable" is a regular Postgres table that TimescaleDB automatically
--   partitions by time behind the scenes. You insert and query it with normal SQL.
--   The benefit: range queries like WHERE time > NOW() - INTERVAL '1 hour'
--   only scan the relevant time partition instead of the whole table.

-- Step 1: Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── TABLE: price_ticks ────────────────────────────────────────────
-- Stores every single price update we receive from Binance via Kafka.
-- This is the raw tick data — one row per price update.
--
-- Why store every tick?
--   Quant firms use tick data to backtest strategies:
--   "If I had bought BTC every time the spread widened to $0.05, what would my returns be?"
--   You need the full history for that.

CREATE TABLE IF NOT EXISTS price_ticks (
    time        TIMESTAMPTZ NOT NULL,         -- when we received this tick
    symbol      TEXT        NOT NULL,         -- e.g. 'BTCUSDT'
    bid         DOUBLE PRECISION NOT NULL,    -- best bid price
    ask         DOUBLE PRECISION NOT NULL,    -- best ask price
    spread      DOUBLE PRECISION NOT NULL,    -- ask - bid
    bid_qty     DOUBLE PRECISION NOT NULL,    -- quantity at best bid
    ask_qty     DOUBLE PRECISION NOT NULL     -- quantity at best ask
);

-- Convert to a TimescaleDB hypertable, partitioned by time
-- chunk_time_interval = '1 day' means each day's data is in its own chunk
SELECT create_hypertable('price_ticks', 'time', if_not_exists => TRUE);

-- Index on symbol for fast per-symbol queries
CREATE INDEX IF NOT EXISTS idx_price_ticks_symbol ON price_ticks (symbol, time DESC);

-- ── TABLE: ohlcv_1min ─────────────────────────────────────────────
-- OHLCV = Open, High, Low, Close, Volume — the standard candlestick format.
-- This is what you see on Zerodha/TradingView charts.
-- We compute 1-minute candles from the raw ticks in the analytics service.
--
-- What each column means:
--   open  = first price in the minute
--   high  = highest price in the minute
--   low   = lowest price in the minute
--   close = last price in the minute
--   volume= total BTC quantity traded in the minute (bid_qty + ask_qty sum)
--   vwap  = Volume Weighted Average Price for that minute

CREATE TABLE IF NOT EXISTS ohlcv_1min (
    time        TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    vwap        DOUBLE PRECISION NOT NULL,
    tick_count  INTEGER          NOT NULL    -- how many ticks went into this candle
);

SELECT create_hypertable('ohlcv_1min', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol ON ohlcv_1min (symbol, time DESC);


