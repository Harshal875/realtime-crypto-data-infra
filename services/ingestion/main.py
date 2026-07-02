"""
Week 2 — Market Data Ingestion Service (with Kafka)
-----------------------------------------------------
What changed from Week 1:
  - We added a Kafka Producer
  - Instead of only printing, every price update is now PUBLISHED to a Kafka topic
  - The print() is kept so you can still see data in terminal

New concepts:
  - Kafka Producer : sends messages to a Kafka topic
  - Topic          : a named channel in Kafka (ours: "market-data-raw")
  - Message key    : we use the symbol (BTCUSDT) as the key
                     Kafka uses this to route same-symbol messages to the same partition,
                     so BTC updates always stay in order relative to each other
  - producer.poll(): non-blocking flush — triggers delivery callbacks without blocking
"""

import os
import asyncio
import json
import ssl
import certifi                          # pip install certifi
import websockets                       # pip install websockets
from confluent_kafka import Producer    # pip install confluent-kafka

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

# These are the 3 trading pairs we want to watch.
# "btcusdt" means: Bitcoin priced in USDT (Tether, a stable coin pegged to USD)
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

# Binance provides a free public WebSocket stream. No API key needed.
# The URL pattern is: wss://stream.binance.com:9443/ws/<symbol>@bookTicker
BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"

# Kafka configuration.
# bootstrap.servers: the address of the Kafka broker.
# Inside Docker, services reach Kafka via "kafka:29092" (internal listener).
# Outside Docker (dev machine), it's "localhost:9092".
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
}

# The topic name where all raw market data will be published.
# Think of this as the mailbox name in the post office.
KAFKA_TOPIC = "market-data-raw"

# ── CORE LOGIC ─────────────────────────────────────────────────────────────────

async def stream_symbol(symbol: str, producer: Producer) -> None:
    """
    Connects to Binance for ONE symbol and listens forever.
    Now also publishes every update to Kafka.
    
    producer is passed in from main() — one shared producer for all symbols.
    """
    url = f"{BINANCE_WS_BASE}/{symbol}@bookTicker"

    print(f"[{symbol.upper()}] Connecting to Binance...")

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    async with websockets.connect(url, ssl=ssl_context) as ws:
        print(f"[{symbol.upper()}] Connected! Listening for updates...")

        async for raw_message in ws:
            data = json.loads(raw_message)

            # Binance bookTicker fields:
            #   "s" = symbol, "b" = best bid price, "B" = best bid qty
            #   "a" = best ask price, "A" = best ask qty
            symbol_name = data.get("s", symbol.upper())
            best_bid    = float(data.get("b", 0))
            best_ask    = float(data.get("a", 0))
            bid_qty     = float(data.get("B", 0))
            ask_qty     = float(data.get("A", 0))
            spread      = round(best_ask - best_bid, 4)

            # ── KAFKA PUBLISH ──────────────────────────────────────────────
            # Build a clean message dict to publish.
            # We add a timestamp so consumers know when we received this.
            import time
            message = {
                "symbol":   symbol_name,
                "bid":      best_bid,
                "bid_qty":  bid_qty,
                "ask":      best_ask,
                "ask_qty":  ask_qty,
                "spread":   spread,
                "timestamp": time.time(),   # Unix timestamp (seconds since 1970)
            }

            producer.produce(
                topic=KAFKA_TOPIC,
                key=symbol_name.encode(),       # key = symbol name
                                                # Kafka routes same key to same partition
                                                # → BTC updates always stay in order
                value=json.dumps(message).encode(),  # value = JSON string as bytes
            )
            # poll(0) = non-blocking flush.
            # It triggers internal delivery callbacks without making us wait.
            # Think of it as: "hey Kafka client, check if anything needs sending."
            producer.poll(0)
            # ──────────────────────────────────────────────────────────────

            # Still print so we can see data flowing
            print(
                f"[{symbol_name}]  "
                f"Bid: ${best_bid:,.2f} ({bid_qty} BTC)  |  "
                f"Ask: ${best_ask:,.2f} ({ask_qty} BTC)  |  "
                f"Spread: ${spread}  → published to Kafka"
            )


async def main() -> None:
    """
    Creates one shared Kafka producer, then starts all 3 symbol streams concurrently.

    Why one shared producer?
    Creating a producer opens a TCP connection to Kafka.
    Opening 3 separate connections (one per symbol) wastes resources.
    One producer can publish messages for all 3 symbols — Kafka handles routing.
    """
    print("Starting Market Data Ingestion Service...")
    print(f"Watching symbols: {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"Publishing to Kafka topic: {KAFKA_TOPIC}")
    print("-" * 60)

    # Create the Kafka producer once — shared across all symbol streams
    producer = Producer(KAFKA_CONFIG)

    # Create one task per symbol, pass the shared producer in
    tasks = [stream_symbol(symbol, producer) for symbol in SYMBOLS]
    await asyncio.gather(*tasks)


# ── ENTRYPOINT ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        # asyncio.run() starts the event loop — the "engine" that powers async code.
        # Everything async happens inside here.
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down. Bye!")



