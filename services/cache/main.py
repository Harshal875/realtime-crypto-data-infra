"""
Week 5 — Cache Service (Redis key-value + Pub/Sub)
────────────────────────────────────────────────────
What this service does:
  1. Reads price updates from Kafka (same topic as analytics service)
  2. For each update:
     a. SET price:<SYMBOL> → stores latest price in Redis (key-value cache)
        Any service can GET this instantly from RAM — ~0.1ms
     b. PUBLISH price-updates → broadcasts to all Redis Pub/Sub subscribers
        The GraphQL service (Week 6) will subscribe here to push live
        price updates to frontend WebSocket connections

Why a separate consumer group ("cache-service")?
  Kafka tracks offsets per consumer GROUP.
  "analytics-service" and "cache-service" are independent groups.
  Both receive EVERY message from the topic independently.
  Neither knows about the other — fully decoupled.
  If the cache service crashes, analytics keeps running and vice versa.

Redis data model:
  Key:   "price:BTCUSDT"
  Value: JSON string  {"bid": 81456.77, "ask": 81456.78, "spread": 0.01,
                       "bid_qty": 0.34, "ask_qty": 1.20, "ts": 1715000000.0}

  TTL:   5 seconds — if no update arrives in 5s, the key expires automatically.
         This prevents stale prices from lingering if the ingestion service dies.

Pub/Sub channel:
  Channel: "price-updates"
  Message: same JSON as above
"""

import os
import json
import redis
from confluent_kafka import Consumer, KafkaError

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

KAFKA_TOPIC    = "market-data-raw"
REDIS_HOST     = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_CHANNEL  = "price-updates"   # Pub/Sub channel name
PRICE_KEY_TTL  = 5                 # seconds before a cached price expires

# ── MAIN ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # Connect to Redis
    # decode_responses=True → Redis returns Python strings instead of bytes
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()   # raises ConnectionError immediately if Redis is down
    print(f"[Cache] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    # Connect to Kafka
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id":          "cache-service",    # separate group from analytics
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"[Cache] Listening to Kafka topic '{KAFKA_TOPIC}'...")
    print("-" * 60)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[Kafka] Error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            symbol = data.get("symbol", "")

            # Build the cache payload
            payload = json.dumps({
                "symbol":  symbol,
                "bid":     data.get("bid", 0),
                "ask":     data.get("ask", 0),
                "spread":  data.get("spread", 0),
                "bid_qty": data.get("bid_qty", 0),
                "ask_qty": data.get("ask_qty", 0),
                "ts":      data.get("timestamp", 0),
            })

            # ── 1. SET: update latest price in key-value cache ───────────
            # Key pattern: "price:BTCUSDT", "price:ETHUSDT", etc.
            # ex=PRICE_KEY_TTL → key expires after 5 seconds automatically
            #   Why TTL? If ingestion dies, we don't want to serve stale data.
            #   5 seconds = if no update arrives, the key disappears.
            cache_key = f"price:{symbol}"
            r.set(cache_key, payload, ex=PRICE_KEY_TTL)

            # ── 2. PUBLISH: broadcast to all Pub/Sub subscribers ─────────
            # PUBLISH is fire-and-forget — Redis delivers to whoever is
            # subscribed RIGHT NOW. If no one is subscribed, message is lost.
            # (This is fine — unlike Kafka, Pub/Sub is not for durability,
            #  it's for real-time push. Durability is Kafka's job.)
            subscriber_count = r.publish(REDIS_CHANNEL, payload)

            print(
                f"[{symbol}]  "
                f"bid=${data.get('bid', 0):,.2f}  "
                f"cached → {cache_key}  |  "
                f"published to {subscriber_count} subscriber(s)"
            )

    except KeyboardInterrupt:
        print("\n[Cache] Shutting down.")
    finally:
        consumer.close()
        r.close()


if __name__ == "__main__":
    main()

