"""
Kafka producer: streams synthetic transactions to the 'transaction-events' topic.

Design decisions:
  - Keyed by account_id to preserve ordering per account across partitions
  - JSON serialization (v1/v2); Avro with Schema Registry is the planned upgrade
  - Configurable throughput (--tps) for load testing
  - Delivery reports logged for observability
"""
import argparse
import json
import logging
import time
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import (
    DEFAULT_ACCOUNT_COUNT,
    DEFAULT_CUSTOMER_COUNT,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_NUM_PARTITIONS,
    KAFKA_TOPIC_TRANSACTIONS,
)
from data_generator.transactions import generate_transactions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("kafka.producer")


def _ensure_topic(bootstrap_servers: str, topic: str, num_partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        admin.create_topics(
            [NewTopic(topic, num_partitions=num_partitions, replication_factor=1)]
        )
        logger.info("Created topic '%s' with %d partitions", topic, num_partitions)
    else:
        logger.info("Topic '%s' already exists", topic)


def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s[%d] @ offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def produce(
    bootstrap_servers: str,
    topic: str,
    records: int,
    tps: int,
    account_count: int,
    customer_count: int,
    schema_version: int,
) -> None:
    _ensure_topic(bootstrap_servers, topic, KAFKA_NUM_PARTITIONS)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",                  # strongest durability guarantee
            "retries": 5,
            "retry.backoff.ms": 500,
            "compression.type": "lz4",
            "linger.ms": 10,               # micro-batching for throughput
            "batch.size": 65536,
        }
    )

    transactions = generate_transactions(records, account_count, customer_count, schema_version)
    interval = 1.0 / tps if tps > 0 else 0

    logger.info(
        "Starting producer → topic=%s | records=%d | tps=%d",
        topic, len(transactions), tps,
    )
    start = time.monotonic()

    for i, tx in enumerate(transactions):
        key = tx.get("account_id", "unknown")
        value = json.dumps(tx, default=str).encode("utf-8")

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            callback=_delivery_report,
        )
        producer.poll(0)  # non-blocking poll to trigger delivery callbacks

        if interval > 0 and i % tps == 0 and i > 0:
            time.sleep(max(0, interval * tps - (time.monotonic() - start) % (interval * tps)))

    producer.flush()
    elapsed = time.monotonic() - start
    logger.info(
        "Produced %d messages in %.2fs (%.0f msg/s)",
        len(transactions), elapsed, len(transactions) / elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream transactions to Kafka")
    parser.add_argument("--topic", default=KAFKA_TOPIC_TRANSACTIONS)
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--tps", type=int, default=500, help="Target messages/second (0=unlimited)")
    parser.add_argument("--accounts", type=int, default=DEFAULT_ACCOUNT_COUNT)
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMER_COUNT)
    parser.add_argument("--schema-version", type=int, default=2, choices=[1, 2, 3])
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    produce(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        records=args.records,
        tps=args.tps,
        account_count=args.accounts,
        customer_count=args.customers,
        schema_version=args.schema_version,
    )


if __name__ == "__main__":
    main()
