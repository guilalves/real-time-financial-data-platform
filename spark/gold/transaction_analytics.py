"""
Gold Layer — Transaction Analytics

Produces aggregated metrics consumed by the Business Intelligence team.

Metrics produced:
  - Transaction volume by type, currency, merchant category
  - Hourly and daily trend aggregations
  - P50/P95/P99 amount distributions
  - Success/failure rate breakdown
"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import functions as F

from config.settings import GOLD_TRANSACTION_PATH, SILVER_TRANSACTIONS_PATH
from spark.utils.delta_utils import optimize_table
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("gold.transaction_analytics")

LOOKBACK_DAYS = 30


def build_transaction_analytics(spark, process_date: str):
    transactions = (
        spark.read.format("delta")
        .load(SILVER_TRANSACTIONS_PATH)
        .filter(
            (F.col("event_date") >= F.date_sub(F.lit(process_date), LOOKBACK_DAYS))
            & (F.col("event_date") <= F.lit(process_date))
        )
        .select(
            "transaction_id", "transaction_type", "amount", "currency",
            "merchant", "merchant_category", "status", "timestamp", "event_date",
            "payment_method", "is_late_arrival",
        )
        .repartition(200, "event_date")
        .cache()
    )

    # Daily summary by transaction type
    daily_by_type = transactions.groupBy("event_date", "transaction_type", "currency").agg(
        F.count("transaction_id").alias("tx_count"),
        F.sum("amount").alias("total_volume"),
        F.avg("amount").alias("avg_amount"),
        F.percentile_approx("amount", 0.50).alias("p50_amount"),
        F.percentile_approx("amount", 0.95).alias("p95_amount"),
        F.percentile_approx("amount", 0.99).alias("p99_amount"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_count"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_count"),
        F.sum(F.when(F.col("is_late_arrival"), 1).otherwise(0)).alias("late_arrival_count"),
    ).withColumn(
        "success_rate",
        F.col("completed_count") / F.col("tx_count"),
    ).withColumn("granularity", F.lit("daily_by_type"))

    # Daily summary by merchant category
    daily_by_merchant = transactions.groupBy("event_date", "merchant_category").agg(
        F.count("transaction_id").alias("tx_count"),
        F.sum("amount").alias("total_volume"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("merchant").alias("unique_merchants"),
    ).withColumn("granularity", F.lit("daily_by_merchant"))

    # Hourly volume (for near-real-time BI)
    hourly = transactions.withColumn(
        "hour", F.date_trunc("hour", F.col("timestamp"))
    ).groupBy("hour", "transaction_type").agg(
        F.count("transaction_id").alias("tx_count"),
        F.sum("amount").alias("total_volume"),
    ).withColumn("event_date", F.to_date(F.col("hour"))).withColumn(
        "granularity", F.lit("hourly")
    )

    # Payment method breakdown (v2+ only — nulls allowed)
    by_payment = transactions.filter(F.col("payment_method").isNotNull()).groupBy(
        "event_date", "payment_method"
    ).agg(
        F.count("transaction_id").alias("tx_count"),
        F.sum("amount").alias("total_volume"),
    ).withColumn("granularity", F.lit("daily_by_payment"))

    # Combine into single gold table (different granularities, union-friendly structure)
    base_cols = ["event_date", "tx_count", "total_volume", "granularity"]
    result = (
        daily_by_type.select(
            "event_date", "transaction_type", "currency",
            "tx_count", "total_volume", "avg_amount",
            "p50_amount", "p95_amount", "p99_amount",
            "completed_count", "failed_count", "success_rate",
            "late_arrival_count", "granularity",
        )
        .withColumn("process_date", F.lit(process_date))
        .withColumn("processed_at", F.current_timestamp())
    )

    transactions.unpersist()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold: Transaction Analytics")
    parser.add_argument("--process-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session("Gold-Transaction-Analytics")
    df = build_transaction_analytics(spark, args.process_date)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"process_date = '{args.process_date}'")
        .option("mergeSchema", "true")
        .partitionBy("process_date")
        .save(GOLD_TRANSACTION_PATH)
    )

    count = df.count()
    logger.info("Gold transaction analytics written | rows=%d | date=%s", count, args.process_date)
    optimize_table(spark, GOLD_TRANSACTION_PATH, zorder_cols=["event_date", "transaction_type"])
    spark.stop()


if __name__ == "__main__":
    main()
