"""
Week 3 — Order Book Service (gRPC server + Kafka consumer)
──────────────────────────────────────────────────────────
What this service does (plain English):
  1. Runs a Kafka consumer in a background thread
     → reads price updates from the "market-data-raw" topic
     → stores the latest bid/ask/qty per symbol in a dict (in memory)

  2. Runs a gRPC server in the main thread
     → any other service can call GetBestBid("BTCUSDT") and get
        the current best bid price — without hitting Binance directly

Why gRPC instead of a REST API here?
  - Binary encoding (Protobuf) is much faster than JSON for hot paths
  - The .proto file enforces a typed contract — wrong input = compile error
  - Auto-generated client code — callers don't write HTTP request logic
  - Designed for inter-service communication (not browser-facing APIs)

Architecture inside this file:
  Thread 1 (daemon):  KafkaConsumerThread
                        ↓ reads messages
                      updates shared `order_books` dict
                        ↑ protected by a threading.Lock

  Main thread:        gRPC server
                        ← accepts RPC calls from other services
                      reads from `order_books` dict
"""

import json
import time
import threading
import grpc
from concurrent import futures

from confluent_kafka import Consumer, KafkaError

# Import the auto-generated classes from the .proto file
# orderbook_pb2       = the message classes (SymbolRequest, PriceLevel, etc.)
# orderbook_pb2_grpc  = the service base class and stub
import orderbook_pb2
import orderbook_pb2_grpc

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

KAFKA_TOPIC   = "market-data-raw"
GRPC_PORT     = 50051           # standard gRPC port convention
SYMBOLS       = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# ── SHARED STATE ───────────────────────────────────────────────────────────────
# This dict is the "in-memory order book".
# It holds the latest known bid/ask for each symbol.
#
# Structure:
#   order_books = {
#       "BTCUSDT": { "bid": 82135.53, "bid_qty": 4.94, "ask": 82135.54, "ask_qty": 0.24, "ts": 1234567890.0 },
#       "ETHUSDT": { ... },
#       "SOLUSDT": { ... },
#   }
#
# Why in-memory and not a database?
#   Speed. We need sub-millisecond reads for the gRPC calls.
#   The latest price needs to be instantly available.
#   Redis (Week 5) will serve as persistent cache; this dict is the hot layer.

order_books: dict = {s: {} for s in SYMBOLS}

# threading.Lock prevents two threads from writing to order_books at the same time.
# Without it, the Kafka thread and the gRPC thread could corrupt the dict simultaneously.
lock = threading.Lock()

# ── KAFKA CONSUMER THREAD ──────────────────────────────────────────────────────

def kafka_consumer_thread() -> None:
    """
    Runs forever in a background thread.
    Reads messages from Kafka and updates the order_books dict.

    This is a plain function (not async) because it runs in its own thread —
    no need for asyncio here. The gRPC server handles concurrency differently
    (thread pool), so we keep things simple with regular threading.
    """
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id":          "order-book-service",   # unique group for this service
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"[Kafka] Consumer started, listening to '{KAFKA_TOPIC}'...")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[Kafka] Error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
            symbol = data.get("symbol")
            if symbol not in order_books:
                continue

            # Acquire the lock before writing to the shared dict
            # This prevents the gRPC thread from reading a half-written update
            with lock:
                order_books[symbol] = {
                    "bid":     data.get("bid", 0.0),
                    "bid_qty": data.get("bid_qty", 0.0),
                    "ask":     data.get("ask", 0.0),
                    "ask_qty": data.get("ask_qty", 0.0),
                    "ts":      data.get("timestamp", time.time()),
                }

            print(f"[OrderBook] Updated {symbol}: "
                  f"bid={order_books[symbol]['bid']:.2f}  "
                  f"ask={order_books[symbol]['ask']:.2f}")

        except (json.JSONDecodeError, KeyError) as e:
            print(f"[Kafka] Bad message: {e}")


# ── gRPC SERVICE IMPLEMENTATION ────────────────────────────────────────────────

class OrderBookServicer(orderbook_pb2_grpc.OrderBookServiceServicer):
    """
    This class IMPLEMENTS the service defined in orderbook.proto.

    Every method here corresponds to one "rpc" line in the .proto file.
    The method signatures are dictated by the generated code — we just fill them in.

    "context" is gRPC's request context (metadata, deadlines, cancellation).
    We don't use it here but it must be in the signature.
    """

    def _get_book(self, symbol: str) -> dict:
        """Helper: safely read the current order book for a symbol."""
        with lock:
            return dict(order_books.get(symbol.upper(), {}))

    def GetBestBid(self, request, context):
        """Return the current best bid price + quantity for a symbol."""
        book = self._get_book(request.symbol)
        if not book:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No data yet for symbol: {request.symbol}")
            return orderbook_pb2.PriceLevel()

        return orderbook_pb2.PriceLevel(
            symbol   = request.symbol.upper(),
            price    = book["bid"],
            quantity = book["bid_qty"],
        )

    def GetBestAsk(self, request, context):
        """Return the current best ask price + quantity for a symbol."""
        book = self._get_book(request.symbol)
        if not book:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No data yet for symbol: {request.symbol}")
            return orderbook_pb2.PriceLevel()

        return orderbook_pb2.PriceLevel(
            symbol   = request.symbol.upper(),
            price    = book["ask"],
            quantity = book["ask_qty"],
        )

    def GetSpread(self, request, context):
        """Return bid, ask, and spread for a symbol."""
        book = self._get_book(request.symbol)
        if not book:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No data yet for symbol: {request.symbol}")
            return orderbook_pb2.SpreadResponse()

        return orderbook_pb2.SpreadResponse(
            symbol = request.symbol.upper(),
            bid    = book["bid"],
            ask    = book["ask"],
            spread = round(book["ask"] - book["bid"], 4),
        )

    def GetSnapshot(self, request, context):
        """Return full snapshot of the current order book state."""
        book = self._get_book(request.symbol)
        if not book:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No data yet for symbol: {request.symbol}")
            return orderbook_pb2.SnapshotResponse()

        return orderbook_pb2.SnapshotResponse(
            symbol       = request.symbol.upper(),
            best_bid     = book["bid"],
            best_ask     = book["ask"],
            spread       = round(book["ask"] - book["bid"], 4),
            bid_qty      = book["bid_qty"],
            ask_qty      = book["ask_qty"],
            last_updated = book["ts"],
        )


# ── ENTRYPOINT ─────────────────────────────────────────────────────────────────

def serve() -> None:
    # Step 1: Start Kafka consumer in a background daemon thread.
    # daemon=True means this thread dies automatically when the main program exits.
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    # Step 2: Give Kafka a moment to connect and receive first messages
    print("[Server] Waiting 3s for Kafka to populate order books...")
    time.sleep(3)

    # Step 3: Create the gRPC server.
    # ThreadPoolExecutor(10) = up to 10 concurrent RPC calls handled simultaneously.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Register our implementation with the server
    orderbook_pb2_grpc.add_OrderBookServiceServicer_to_server(
        OrderBookServicer(), server
    )

    # Bind to port
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()

    print(f"[Server] gRPC Order Book Service running on port {GRPC_PORT}")
    print("[Server] Ready to accept calls: GetBestBid, GetBestAsk, GetSpread, GetSnapshot")
    print("[Server] Press Ctrl+C to stop.")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[Server] Shutting down.")
        server.stop(grace=2)


if __name__ == "__main__":
    serve()
