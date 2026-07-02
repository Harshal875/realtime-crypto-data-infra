"""
client_test.py — gRPC Client (for testing only)
─────────────────────────────────────────────────
This is your "does the gRPC server work?" checker.

How a gRPC client works:
  1. Create a "channel" — a connection to the server (like opening a socket)
  2. Create a "stub" — the auto-generated client object that has methods matching
     each "rpc" in the .proto file
  3. Call methods on the stub — they behave like normal Python function calls,
     but under the hood they serialize to binary, send over the network,
     get a binary response, and deserialize back to a Python object.

That's the magic of gRPC — calling a function on another machine looks identical
to calling a local function in your code.
"""

import grpc
import orderbook_pb2
import orderbook_pb2_grpc

GRPC_SERVER = "localhost:50051"
SYMBOLS     = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def main():
    # Open a channel to the gRPC server
    # "insecure_channel" = no TLS (fine for local dev, production would use TLS)
    with grpc.insecure_channel(GRPC_SERVER) as channel:

        # Create the stub — your "remote control" for the server
        stub = orderbook_pb2_grpc.OrderBookServiceStub(channel)

        print(f"Connected to gRPC server at {GRPC_SERVER}")
        print("=" * 60)

        for symbol in SYMBOLS:
            req = orderbook_pb2.SymbolRequest(symbol=symbol)

            try:
                # ── Call GetSnapshot (all info in one call) ──────────────
                snap = stub.GetSnapshot(req)
                print(f"\n[{symbol}] Full Snapshot:")
                print(f"  Best Bid : ${snap.best_bid:,.2f}  ({snap.bid_qty} units)")
                print(f"  Best Ask : ${snap.best_ask:,.2f}  ({snap.ask_qty} units)")
                print(f"  Spread   : ${snap.spread}")

                # ── Call GetBestBid separately (shows individual RPC call) ─
                bid = stub.GetBestBid(req)
                print(f"  [GetBestBid RPC] → ${bid.price:,.2f} x {bid.quantity}")

            except grpc.RpcError as e:
                # gRPC errors are typed — e.code() gives you the status code
                # e.g. NOT_FOUND, UNAVAILABLE, DEADLINE_EXCEEDED
                print(f"[{symbol}] RPC error: {e.code()} — {e.details()}")

        print("\n" + "=" * 60)
        print("Done. All gRPC calls completed.")


if __name__ == "__main__":
    main()



