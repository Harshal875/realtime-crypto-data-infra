"""
consumer_test.py — Kafka Consumer (for testing only)
──────────────────────────────────────────────────────
Purpose: verify that messages published by main.py are actually arriving in Kafka.

This is NOT a production service. It's your "did it work?" checker.
In Week 4, the real Analytics Service will replace this with proper processing.

How a Kafka Consumer works:
  1. You tell it: "I want to read from topic X"
  2. You tell it: "I am part of consumer group Y"
  3. Kafka tracks your position (offset) per group
  4. You call poll() in a loop — each poll() gives you the next batch of messages
  5. If you restart, Kafka picks up from where you left off (because of the offset)

Consumer Group:
  If you have 2 consumers in the SAME group, Kafka splits the work between them.
  If you have 2 consumers in DIFFERENT groups, both get ALL messages independently.
  Our test consumer uses group "test-consumer" so it doesn't interfere with
  future services (which will use their own group names).
"""

import json
from confluent_kafka import Consumer, KafkaError

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

KAFKA_TOPIC = "market-data-raw"

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "test-consumer",        # this consumer's group name
    "auto.offset.reset": "latest",      # "latest" = only read NEW messages from now
                                        # "earliest" = read from the very beginning
}

# ── MAIN LOOP ──────────────────────────────────────────────────────────────────

def main():
    consumer = Consumer(consumer_config)

    # Subscribe to the topic
    # A consumer can subscribe to multiple topics at once if needed.
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening to Kafka topic: '{KAFKA_TOPIC}'")
    print("Waiting for messages... (make sure main.py is running in another terminal)")
    print("-" * 60)

    try:
        while True:
            # poll(timeout=1.0) waits up to 1 second for a message.
            # Returns None if no message arrived in that second.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message yet — just keep waiting
                continue

            if msg.error():
                # Kafka signalled an error or end-of-partition
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — not actually an error, just informational
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break

            # We got a real message! Decode it.
            # msg.key()   = the symbol (e.g. b"BTCUSDT")
            # msg.value() = the JSON payload as bytes
            key   = msg.key().decode("utf-8")
            value = json.loads(msg.value().decode("utf-8"))

            print(
                f"[{key}]  "
                f"Bid: ${value['bid']:,.2f}  |  "
                f"Ask: ${value['ask']:,.2f}  |  "
                f"Spread: ${value['spread']}  |  "
                f"Offset: {msg.offset()}"   # ← offset = message number in partition
            )

    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        # Always close the consumer — this commits the final offset to Kafka
        consumer.close()


if __name__ == "__main__":
    main()



