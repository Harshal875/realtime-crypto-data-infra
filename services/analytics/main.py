"""
Week 4 — Analytics Service
────────────────────────────────────────────────────────────────────
What this service does (plain English):
  1. Reads price updates from Kafka topic "market-data-raw"
  2. Writes every tick to Postgres (TimescaleDB) for permanent storage
  3. Indexes every tick in Elasticsearch for fast search later
  4. Accumulates ticks per symbol per minute → computes OHLCV candle
     → writes completed candles to Postgres ohlcv_1min table

Key new concepts:
  asyncpg       : async Python driver for Postgres (faster than psycopg2 on macOS)
  elasticsearch : Python client for Elasticsearch
  VWAP          : Volume Weighted Average Price — sum(price*qty) / sum(qty)
  OHLCV candle  : Open/High/Low/Close/Volume — the candlestick chart format
"""

import json
import time
import asyncio
from datetime import datetime, timezone
from collections import defaultdict

import asyncpg
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaError

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

KAFKA_TOPIC = "market-data-raw"

POSTGRES_DSN = "postgresql://market_user:market_pass@localhost:5432/market_data"

ES_HOST = "http://localhost:9200"
ES_INDEX = "market-ticks"   # Elasticsearch index name (like a table name)

# ── CANDLE ACCUMULATOR ─────────────────────────────────────────────────────────
# We build 1-minute OHLCV candles by collecting ticks and flushing every 60s.
#
# Structure per symbol:
# candle_buckets["BTCUSDT"] = {
#     "open":       82000.0,   ← price of very first tick this minute
#     "high":       82200.0,   ← highest price seen this minute
#     "low":        81900.0,   ← lowest price seen this minute
#     "close":      82100.0,   ← price of most recent tick
#     "vol_sum":    12.5,      ← sum of all quantities this minute
#     "pv_sum":     1026250.0, ← sum of (price * qty) — needed for VWAP
#     "tick_count": 342,       ← how many ticks this minute
#     "minute":     1715000000 ← Unix timestamp of this minute's start (floored to 60s)
# }

candle_buckets: dict = defaultdict(lambda: None)


def floor_to_minute(ts: float) -> int:
    """Floor a Unix timestamp to the start of its minute."""
    return int(ts // 60) * 60


def update_candle(symbol: str, price: float, qty: float, ts: float) -> dict | None:
    """
    Add a tick to the running candle for this symbol.
    Returns a completed candle dict if the minute just rolled over, else None.

    This is how real-time OHLCV candles are built in production systems —
    you accumulate ticks in memory and flush on minute boundaries.
    """
    current_minute = floor_to_minute(ts)
    bucket = candle_buckets[symbol]

    # If this tick belongs to a NEW minute, flush the old candle and start fresh
    if bucket is not None and bucket["minute"] != current_minute:
        completed = {
            "symbol":     symbol,
            "minute":     bucket["minute"],
            "open":       bucket["open"],
            "high":       bucket["high"],
            "low":        bucket["low"],
            "close":      bucket["close"],
            "volume":     bucket["vol_sum"],
            "vwap":       round(bucket["pv_sum"] / bucket["vol_sum"], 6) if bucket["vol_sum"] > 0 else 0,
            "tick_count": bucket["tick_count"],
        }
        # Start a new bucket for the new minute
        candle_buckets[symbol] = {
            "open": price, "high": price, "low": price, "close": price,
            "vol_sum": qty, "pv_sum": price * qty,
            "tick_count": 1, "minute": current_minute,
        }
        return completed

    # Same minute — update the running bucket
    if bucket is None:
        candle_buckets[symbol] = {
            "open": price, "high": price, "low": price, "close": price,
            "vol_sum": qty, "pv_sum": price * qty,
            "tick_count": 1, "minute": current_minute,
        }
    else:
        bucket["high"]       = max(bucket["high"], price)
        bucket["low"]        = min(bucket["low"], price)
        bucket["close"]      = price
        bucket["vol_sum"]   += qty
        bucket["pv_sum"]    += price * qty
        bucket["tick_count"] += 1

    return None   # candle not yet complete


# ── DATABASE HELPERS ───────────────────────────────────────────────────────────

async def write_tick_to_postgres(conn, tick: dict) -> None:
    """Insert one price tick into the price_ticks hypertable."""
    await conn.execute(
        """
        INSERT INTO price_ticks (time, symbol, bid, ask, spread, bid_qty, ask_qty)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        datetime.fromtimestamp(tick["timestamp"], tz=timezone.utc),
        tick["symbol"],
        tick["bid"],
        tick["ask"],
        tick["spread"],
        tick["bid_qty"],
        tick["ask_qty"],
    )


async def write_candle_to_postgres(conn, candle: dict) -> None:
    """Insert a completed OHLCV candle into ohlcv_1min."""
    await conn.execute(
        """
        INSERT INTO ohlcv_1min (time, symbol, open, high, low, close, volume, vwap, tick_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        datetime.fromtimestamp(candle["minute"], tz=timezone.utc),
        candle["symbol"],
        candle["open"],
        candle["high"],
        candle["low"],
        candle["close"],
        candle["volume"],
        candle["vwap"],
        candle["tick_count"],
    )
    print(
        f"[OHLCV] {candle['symbol']} candle @ "
        f"{datetime.fromtimestamp(candle['minute']).strftime('%H:%M')}  "
        f"O={candle['open']:.2f} H={candle['high']:.2f} "
        f"L={candle['low']:.2f} C={candle['close']:.2f}  "
        f"VWAP={candle['vwap']:.2f}  ticks={candle['tick_count']}"
    )


def index_tick_to_elasticsearch(es: Elasticsearch, tick: dict) -> None:
    """
    Index one tick event in Elasticsearch.

    In Elasticsearch terms:
      - "index" = insert a document (like INSERT in SQL)
      - "document" = one JSON record
      - ES_INDEX = which index (like a table) to put it in

    We don't need a strict schema upfront — Elasticsearch auto-detects types.
    """
    doc = {
        "symbol":    tick["symbol"],
        "bid":       tick["bid"],
        "ask":       tick["ask"],
        "spread":    tick["spread"],
        "bid_qty":   tick["bid_qty"],
        "ask_qty":   tick["ask_qty"],
        "@timestamp": datetime.fromtimestamp(tick["timestamp"], tz=timezone.utc).isoformat(),
    }
    es.index(index=ES_INDEX, document=doc)


# ── MAIN CONSUMER LOOP ─────────────────────────────────────────────────────────

async def main() -> None:
    print("[Analytics] Connecting to Postgres...")
    pg_conn = await asyncpg.connect(POSTGRES_DSN)
    print("[Analytics] Postgres connected.")

    print("[Analytics] Connecting to Elasticsearch...")
    es = Elasticsearch(ES_HOST)
    for _ in range(10):
        if es.ping():
            break
        print("[Analytics] Waiting for Elasticsearch...")
        time.sleep(3)
    print("[Analytics] Elasticsearch connected.")

    print("[Analytics] Connecting to Kafka...")
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id":          "analytics-service",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"[Analytics] Listening to Kafka topic '{KAFKA_TOPIC}'...")
    print("-" * 60)

    batch_size  = 50
    batch_count = 0
    # asyncpg uses explicit transactions for batching
    tr = pg_conn.transaction()
    await tr.start()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[Kafka] Error: {msg.error()}")
                continue

            tick = json.loads(msg.value().decode("utf-8"))
            symbol = tick.get("symbol", "")
            mid_price = (tick["bid"] + tick["ask"]) / 2
            qty = (tick["bid_qty"] + tick["ask_qty"]) / 2

            # 1. Write raw tick to Postgres
            await write_tick_to_postgres(pg_conn, tick)

            # 2. Index tick in Elasticsearch
            index_tick_to_elasticsearch(es, tick)

            # 3. Update OHLCV candle accumulator
            completed_candle = update_candle(symbol, mid_price, qty, tick["timestamp"])
            if completed_candle:
                await write_candle_to_postgres(pg_conn, completed_candle)

            # Batch commit every 50 ticks
            batch_count += 1
            if batch_count >= batch_size:
                await tr.commit()
                print(f"[Postgres] Committed batch of {batch_size} ticks")
                tr = pg_conn.transaction()
                await tr.start()
                batch_count = 0

    except KeyboardInterrupt:
        print("\n[Analytics] Shutting down...")
        await tr.commit()
    finally:
        consumer.close()
        await pg_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
