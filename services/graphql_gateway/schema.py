"""
Week 6 — GraphQL API Gateway
──────────────────────────────────────────────────────────────────────
Library: Strawberry (https://strawberry.rocks) — Python-first GraphQL
Server:  FastAPI + Uvicorn

Three GraphQL operation types:

1. QUERY (read data)
   latestPrice(symbol)           → Redis GET  (sub-millisecond)
   priceHistory(symbol, limit)   → Postgres ohlcv_1min table
   searchTicks(symbol, minBid, maxBid, limit) → Elasticsearch

2. MUTATION (write data)
   setAlert(symbol, threshold)   → Redis SET (stored as "alert:<symbol>")

3. SUBSCRIPTION (live stream over WebSocket)
   priceUpdates(symbol)          → subscribes to Redis Pub/Sub channel
                                    "price-updates", filters by symbol,
                                    yields Price objects in real time

How subscriptions work end-to-end:
  cache/main.py      publishes JSON to Redis channel "price-updates"
        ↓
  Redis Pub/Sub      broadcasts to all subscribers
        ↓
  graphql/schema.py  async generator reads from the channel
        ↓
  Strawberry         streams each Price object over WebSocket
        ↓
  Browser / client   receives live price updates without polling

Access the API:
  GraphQL Playground: http://localhost:8000/graphql
  (Strawberry ships with a built-in browser IDE)
"""

import os
import json
import asyncio
import asyncpg
import redis.asyncio as aioredis
from typing import AsyncGenerator, Optional
from datetime import datetime, timezone

import strawberry
from elasticsearch import AsyncElasticsearch

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

POSTGRES_DSN  = os.environ.get("POSTGRES_DSN", "postgresql://market_user:market_pass@localhost:5432/market_data")
REDIS_HOST    = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT    = int(os.environ.get("REDIS_PORT", "6379"))
ES_HOST       = os.environ.get("ES_HOST", "http://localhost:9200")
ES_INDEX      = "market-ticks"
PUBSUB_CHANNEL = "price-updates"

# ── GRAPHQL TYPES ─────────────────────────────────────────────────────────────
# @strawberry.type turns a plain Python dataclass into a GraphQL type.
# Every field becomes a GraphQL field automatically.

@strawberry.type
class Price:
    """Represents the current best bid/ask for a symbol."""
    symbol:  str
    bid:     float
    ask:     float
    spread:  float
    bid_qty: float
    ask_qty: float
    ts:      float   # Unix timestamp


@strawberry.type
class OHLCVCandle:
    """One minute of price data — like one candle on a Zerodha chart."""
    symbol:     str
    time:       str    # ISO format timestamp
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float
    vwap:       float  # Volume Weighted Average Price
    tick_count: int


@strawberry.type
class AlertResult:
    """Response for setAlert mutation."""
    success: bool
    message: str


# ── QUERY RESOLVERS ───────────────────────────────────────────────────────────

@strawberry.type
class Query:

    @strawberry.field
    async def latest_price(self, symbol: str) -> Optional[Price]:
        """
        Get the latest bid/ask for a symbol.
        Reads from Redis (in-memory cache) — sub-millisecond response.

        Example query:
          query {
            latestPrice(symbol: "BTCUSDT") {
              bid
              ask
              spread
            }
          }
        """
        r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        try:
            val = await r.get(f"price:{symbol.upper()}")
            if not val:
                return None
            d = json.loads(val)
            return Price(
                symbol  = d["symbol"],
                bid     = d["bid"],
                ask     = d["ask"],
                spread  = d["spread"],
                bid_qty = d["bid_qty"],
                ask_qty = d["ask_qty"],
                ts      = d["ts"],
            )
        finally:
            await r.aclose()

    @strawberry.field
    async def price_history(
        self,
        symbol: str,
        limit: int = 60,   # default: last 60 candles = last 60 minutes
    ) -> list[OHLCVCandle]:
        """
        Get OHLCV candle history for a symbol from Postgres.
        Returns the most recent `limit` 1-minute candles.

        Example query:
          query {
            priceHistory(symbol: "BTCUSDT", limit: 10) {
              time
              open
              high
              low
              close
              vwap
            }
          }
        """
        conn = await asyncpg.connect(POSTGRES_DSN)
        try:
            rows = await conn.fetch(
                """
                SELECT time, symbol, open, high, low, close, volume, vwap, tick_count
                FROM ohlcv_1min
                WHERE symbol = $1
                ORDER BY time DESC
                LIMIT $2
                """,
                symbol.upper(), limit
            )
            return [
                OHLCVCandle(
                    symbol     = r["symbol"],
                    time       = r["time"].isoformat(),
                    open       = r["open"],
                    high       = r["high"],
                    low        = r["low"],
                    close      = r["close"],
                    volume     = r["volume"],
                    vwap       = r["vwap"],
                    tick_count = r["tick_count"],
                )
                for r in rows
            ]
        finally:
            await conn.close()

    @strawberry.field
    async def search_ticks(
        self,
        symbol:  str,
        min_bid: float = 0,
        max_bid: float = 999999999,
        limit:   int   = 20,
    ) -> list[Price]:
        """
        Search price ticks using Elasticsearch.
        Returns ticks where bid is between min_bid and max_bid.

        This is where Elasticsearch shines — range queries across millions
        of documents in milliseconds.

        Example query:
          query {
            searchTicks(symbol: "BTCUSDT", minBid: 81000, maxBid: 82000, limit: 5) {
              bid
              ask
              spread
              ts
            }
          }
        """
        es = AsyncElasticsearch(ES_HOST)
        try:
            resp = await es.search(
                index=ES_INDEX,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"symbol": symbol.upper()}},
                                {"range": {"bid": {"gte": min_bid, "lte": max_bid}}},
                            ]
                        }
                    },
                    "sort":  [{"@timestamp": {"order": "desc"}}],
                    "size":  limit,
                }
            )
            hits = resp["hits"]["hits"]
            return [
                Price(
                    symbol  = h["_source"]["symbol"],
                    bid     = h["_source"]["bid"],
                    ask     = h["_source"]["ask"],
                    spread  = h["_source"]["spread"],
                    bid_qty = h["_source"].get("bid_qty", 0),
                    ask_qty = h["_source"].get("ask_qty", 0),
                    ts      = 0,
                )
                for h in hits
            ]
        finally:
            await es.close()


# ── MUTATION RESOLVERS ────────────────────────────────────────────────────────

@strawberry.type
class Mutation:

    @strawberry.mutation
    async def set_alert(self, symbol: str, threshold: float) -> AlertResult:
        """
        Store a price alert in Redis.
        When the GraphQL service sees bid >= threshold, it could trigger a notification.
        (Notification logic is out of scope — this shows the mutation pattern.)

        Example mutation:
          mutation {
            setAlert(symbol: "BTCUSDT", threshold: 85000) {
              success
              message
            }
          }
        """
        r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        try:
            key = f"alert:{symbol.upper()}"
            await r.set(key, threshold)
            return AlertResult(
                success = True,
                message = f"Alert set: notify when {symbol.upper()} bid >= ${threshold:,.2f}"
            )
        except Exception as e:
            return AlertResult(success=False, message=str(e))
        finally:
            await r.aclose()


# ── SUBSCRIPTION RESOLVERS ────────────────────────────────────────────────────

@strawberry.type
class Subscription:

    @strawberry.subscription
    async def price_updates(self, symbol: str) -> AsyncGenerator[Price, None]:
        """
        Live price feed — streams updates in real time over WebSocket.

        How it works:
          1. Opens a Redis Pub/Sub connection
          2. Subscribes to the "price-updates" channel
          3. cache/main.py publishes to this channel on every Binance tick
          4. We filter for the requested symbol
          5. Yield each matching Price → Strawberry streams it to the client

        This is what changes "published to 0 subscriber(s)"
        to "published to 1 subscriber(s)" in the cache service output.

        Example subscription (in Strawberry's browser Playground):
          subscription {
            priceUpdates(symbol: "BTCUSDT") {
              bid
              ask
              spread
            }
          }
        """
        r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pubsub = r.pubsub()
        await pubsub.subscribe(PUBSUB_CHANNEL)

        try:
            async for message in pubsub.listen():
                # pubsub.listen() yields control messages ("subscribe" confirmation)
                # and actual messages ("message"). We only care about "message".
                if message["type"] != "message":
                    continue

                data = json.loads(message["data"])

                # Filter: only yield updates for the requested symbol
                if data.get("symbol", "").upper() != symbol.upper():
                    continue

                yield Price(
                    symbol  = data["symbol"],
                    bid     = data["bid"],
                    ask     = data["ask"],
                    spread  = data["spread"],
                    bid_qty = data.get("bid_qty", 0),
                    ask_qty = data.get("ask_qty", 0),
                    ts      = data.get("ts", 0),
                )
        finally:
            await pubsub.unsubscribe(PUBSUB_CHANNEL)
            await r.aclose()
