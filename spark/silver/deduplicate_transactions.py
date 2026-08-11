"""
Silver Layer — Step 2: Idempotent Deduplication

Engineering challenge: Kafka at-least-once delivery means the same
transaction_id can appear multiple times in Bronze (and Silver staging).

Solution: MERGE INTO on transaction_id — only inserts new records,
updates existing if a newer version arrives (status changes, late fields).

This approach is idempotent: running the job multiple times for the same
date produces the same result.
"""
import argparse
import logging
from datetime import datetime

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import SILVER_TRANSACTIONS_PATH
from spark.utils.delta_utils import table_exists
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("silver.deduplicate_transactions")


def deduplicate_within_batch(df: DataFrame) -> DataFrame:
    """
    Handle duplicates within the incoming batch before the MERGE.

    When multiple rows share the same transaction_id in the same batch,
    keep the one with the latest ingestion_timestamp. This prevents
    ambiguous MERGE conditions.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("transaction_id").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def merge_to_silver(spark, source_df: DataFrame, target_path: str) -> dict:
    """
    MERGE INTO silver.transactions:
      - INSERT when transaction_id does not exist
      - UPDATE when transaction_id exists and source has a newer ingestion_timestamp
        (handles late status updates, schema evolution fields)
    """
    if not table_exists(spark, target_path):
        source_df.write.format("delta").partitionBy("event_date").save(target_path)
        logger.info("Bootstrapped Silver table at %s", target_path)
        return {"bootstrapped": True}

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            "target.transaction_id = source.transaction_id",
        )
        .whenMatchedUpdate(
            condition="source.ingestion_timestamp > target.ingestion_timestamp",
            set={
                "status": "source.status",
                "amount": "source.amount",
                "payment_method": "source.payment_method",
                "device_fingerprint": "source.device_fingerprint",
                "ip_country": "source.ip_country",
                "ingestion_timestamp": "source.ingestion_timestamp",
                "is_late_arrival": "source.is_late_arrival",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = target.history(1).select("operationMetrics").collect()
    metrics = history[0]["operationMetrics"] if history else {}
    logger.info("MERGE metrics: %s", metrics)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Idempotent deduplication: Silver MERGE")
    parser.add_argument("--process-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session("Silver-Deduplicate-Transactions")

    # Read the staged (newly cleaned) records for today
    staging_df = (
        spark.read.format("delta")
        .load(SILVER_TRANSACTIONS_PATH)
        .filter(F.col("processing_date") == args.process_date)
    )

    raw_count = staging_df.count()
    logger.info("Staging records for %s: %d", args.process_date, raw_count)

    deduped_batch = deduplicate_within_batch(staging_df)
    deduped_count = deduped_batch.count()
    logger.info(
        "Within-batch dedup: %d → %d (removed %d duplicates)",
        raw_count, deduped_count, raw_count - deduped_count,
    )

    metrics = merge_to_silver(spark, deduped_batch, SILVER_TRANSACTIONS_PATH)
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    updated = int(metrics.get("numTargetRowsUpdated", 0))
    logger.info(
        "MERGE complete | inserted=%d | updated=%d | cross-batch-duplicates=%d",
        inserted, updated, deduped_count - inserted - updated,
    )

    spark.stop()


if __name__ == "__main__":
    main()
