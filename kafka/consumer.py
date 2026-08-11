"""
Kafka consumer (standalone Python consumer — for debugging and validation).

NOTE: Production ingestion into Delta Lake is handled by Spark Structured
Streaming (spark/bronze/ingest_transactions.py). This consumer is useful for:
  - Local testing without Spark
  - Inspecting topic contents
  - Validating producer output
"""
import argparse
import json
import logging
import signal
import sys
from collections import Counter

from confluent_kafka import Consumer, KafkaError, KafkaException

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_TRANSACTIONS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("kafka.consumer")

_running = True


def _handle_signal(sig, frame) -> None:
    global _running
    logger.info("Shutdown signal received, stopping consumer...")
    _running = False


def consume(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    max_messages: int,
    print_messages: bool,
) -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,       # manual commit for safety
            "max.poll.interval.ms": 300_000,
        }
    )
    consumer.subscribe([topic])

    stats: Counter = Counter()
    message_count = 0

    try:
        while _running and (max_messages == 0 or message_count < max_messages):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition %d", msg.partition())
                else:
                    raise KafkaException(msg.error())
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                stats["total"] += 1
                stats[f"status_{record.get('status', 'unknown')}"] += 1
                stats[f"type_{record.get('transaction_type', 'unknown')}"] += 1
                if record.get("_is_late_arrival"):
                    stats["late_arrivals"] += 1
                if record.get("transaction_id") is None:
                    stats["null_transaction_id"] += 1

                if print_messages:
                    print(json.dumps(record, indent=2))

                message_count += 1
                consumer.commit(msg)

                if message_count % 10_000 == 0:
                    logger.info("Consumed %d messages | Stats: %s", message_count, dict(stats))

            except json.JSONDecodeError as e:
                logger.warning("Failed to parse message at offset %d: %s", msg.offset(), e)
                stats["parse_errors"] += 1

    finally:
        consumer.close()
        logger.info("Consumer closed. Final stats: %s", dict(stats))


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and inspect Kafka transactions")
    parser.add_argument("--topic", default=KAFKA_TOPIC_TRANSACTIONS)
    parser.add_argument("--group-id", default=f"{KAFKA_CONSUMER_GROUP}-debug")
    parser.add_argument("--max-messages", type=int, default=0, help="0 = unlimited")
    parser.add_argument("--print-messages", action="store_true")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    consume(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        max_messages=args.max_messages,
        print_messages=args.print_messages,
    )


if __name__ == "__main__":
    main()
