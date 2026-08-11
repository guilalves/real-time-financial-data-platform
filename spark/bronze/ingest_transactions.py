"""
Bronze Layer — Kafka → Delta Lake (Structured Streaming)

Responsibilities:
  - Read raw transaction events from Kafka
  - Write append-only to Bronze Delta Lake with minimal transformation
  - Manage exactly-once semantics via Spark checkpoints
  - Preserve full raw payload for auditability
"""
import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from config.settings import (
    BRONZE_PATH,
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRANSACTIONS,
)
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("bronze.ingest_transactions")


def build_bronze_stream(spark, bootstrap_servers: str, topic: str, starting_offsets: str):
    """Read from Kafka and return a streaming DataFrame with bronze schema."""
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", 100_000)  # back-pressure control
        .option("failOnDataLoss", "false")         # handle topic compaction gracefully
        .load()
    )

    # Keep full raw payload + Kafka metadata for complete audit trail
    return raw_stream.select(
        F.col("key").cast(StringType()).alias("kafka_key"),
        F.col("value").cast(StringType()).alias("raw_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.to_date(F.current_timestamp()).alias("ingestion_date"),  # partition key
    )


def write_bronze_stream(stream_df, path: str, checkpoint: str, trigger_interval: str):
    """Write streaming DataFrame to Bronze Delta Lake."""
    return (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")             # handle future Kafka payload changes
        .partitionBy("ingestion_date")
        .trigger(processingTime=trigger_interval)
        .start(path)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze ingestion: Kafka → Delta Lake")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=KAFKA_TOPIC_TRANSACTIONS)
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--trigger-interval", default="1 minute")
    parser.add_argument("--await-termination", action="store_true", default=True)
    args = parser.parse_args()

    spark = get_spark_session("Bronze-Ingest-Transactions")
    logger.info(
        "Starting Bronze ingestion | topic=%s | checkpoint=%s",
        args.topic, CHECKPOINT_PATH,
    )

    stream_df = build_bronze_stream(
        spark, args.bootstrap_servers, args.topic, args.starting_offsets
    )
    query = write_bronze_stream(
        stream_df, BRONZE_PATH, CHECKPOINT_PATH, args.trigger_interval
    )

    logger.info("Stream query started: id=%s", query.id)

    if args.await_termination:
        query.awaitTermination()


if __name__ == "__main__":
    main()
