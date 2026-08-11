"""
Silver Layer — Step 1: Clean & Parse Transactions

Responsibilities:
  - Parse raw JSON payload from Bronze
  - Cast types to their correct representations
  - Enforce non-nullable constraints (route failures to quarantine)
  - Handle schema evolution with mergeSchema
  - Apply watermark for late-arrival tracking
"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import (
    BRONZE_PATH,
    SILVER_QUARANTINE_PATH,
    SILVER_TRANSACTIONS_PATH,
)
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("silver.clean_transactions")

# Canonical schema — v2 (payment_method nullable, v3 fields omitted until evolved)
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("transaction_type", StringType(), nullable=False),
        StructField("amount", DecimalType(18, 2), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("merchant", StringType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("payment_method", StringType(), nullable=True),    # v2
        StructField("device_fingerprint", StringType(), nullable=True), # v3
        StructField("ip_country", StringType(), nullable=True),         # v3
        StructField("_is_late_arrival", StringType(), nullable=True),
    ]
)

CRITICAL_FIELDS = ["transaction_id", "customer_id", "account_id", "amount", "timestamp"]
VALID_CURRENCIES = {"USD", "EUR", "BRL", "GBP", "JPY", "CAD", "AUD"}
VALID_STATUSES = {"completed", "pending", "failed"}
VALID_TX_TYPES = {"purchase", "transfer", "withdrawal", "deposit", "refund"}


def parse_raw_payload(bronze_df: DataFrame) -> DataFrame:
    """Parse JSON string payload into typed columns."""
    return bronze_df.select(
        F.from_json(F.col("raw_payload"), TRANSACTION_SCHEMA).alias("data"),
        F.col("kafka_offset"),
        F.col("kafka_partition"),
        F.col("kafka_timestamp"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
    ).select(
        "data.*",
        "kafka_offset",
        "kafka_partition",
        "kafka_timestamp",
        "ingestion_timestamp",
        "ingestion_date",
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Route records with any critical null to quarantine."""
    null_check = " OR ".join(
        [f"{field} IS NULL" for field in CRITICAL_FIELDS]
    )
    invalid_df = df.filter(null_check).withColumn(
        "quarantine_reason", F.lit("null_critical_field")
    )
    valid_df = df.filter(f"NOT ({null_check})")
    return valid_df, invalid_df


def apply_type_corrections(df: DataFrame) -> DataFrame:
    """Normalize values: trim strings, enforce known enumerations."""
    return (
        df.withColumn("transaction_id", F.trim(F.col("transaction_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("account_id", F.trim(F.col("account_id")))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("transaction_type", F.lower(F.trim(F.col("transaction_type"))))
        # Mark records with unknown enum values (soft correction — doesn't quarantine)
        .withColumn(
            "currency",
            F.when(F.col("currency").isin(list(VALID_CURRENCIES)), F.col("currency"))
            .otherwise(F.lit("UNKNOWN")),
        )
        .withColumn(
            "status",
            F.when(F.col("status").isin(list(VALID_STATUSES)), F.col("status"))
            .otherwise(F.lit("unknown")),
        )
        # Ensure amount is positive
        .withColumn("amount", F.abs(F.col("amount")))
        # Enrich: processing metadata
        .withColumn("processing_date", F.to_date(F.col("ingestion_timestamp")))
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn(
            "is_late_arrival",
            F.col("timestamp") < (F.col("ingestion_timestamp") - F.expr("INTERVAL 30 MINUTES")),
        )
    )


def write_to_silver(df: DataFrame, path: str, partition_col: str = "event_date") -> None:
    """Write cleaned DataFrame to Silver Delta table."""
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # allow new nullable columns without breaking
        .partitionBy(partition_col)
        .save(path)
    )
    logger.info("Written %s rows to Silver: %s", df.count(), path)


def write_quarantine(df: DataFrame, path: str) -> None:
    (
        df.withColumn("quarantined_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)
    )
    count = df.count()
    if count:
        logger.warning("Quarantined %d records → %s", count, path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver cleaning: Bronze → Silver")
    parser.add_argument("--process-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session("Silver-Clean-Transactions")

    bronze_df = (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("ingestion_date") == args.process_date)
    )
    logger.info("Read %d raw Bronze records for date=%s", bronze_df.count(), args.process_date)

    parsed_df = parse_raw_payload(bronze_df)
    valid_df, invalid_df = split_valid_invalid(parsed_df)
    cleaned_df = apply_type_corrections(valid_df)

    write_to_silver(cleaned_df, SILVER_TRANSACTIONS_PATH)
    write_quarantine(invalid_df, SILVER_QUARANTINE_PATH)

    logger.info(
        "Clean step complete | valid=%d | quarantined=%d",
        valid_df.count(), invalid_df.count(),
    )
    spark.stop()


if __name__ == "__main__":
    main()
