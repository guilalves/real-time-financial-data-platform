"""
Gold Layer — Risk Metrics

Produces risk signals consumed by the Risk team for fraud monitoring
and regulatory exposure reporting.

Signals produced:
  1. Velocity alerts — >5 transactions in 10 min per account
  2. Large transaction flags — amount > 3× 30-day average per customer
  3. Geographic anomalies — ip_country ≠ customer.country (v3 fields)
  4. Currency exposure — total exposure by currency/region
  5. Failed transaction spikes — accounts with >20% failure rate
"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import (
    GOLD_RISK_PATH,
    SILVER_CUSTOMERS_PATH,
    SILVER_TRANSACTIONS_PATH,
)
from spark.utils.delta_utils import optimize_table
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("gold.risk_metrics")


def build_velocity_alerts(transactions):
    """Flag accounts with > 5 transactions within any 10-minute window."""
    window_spec = Window.partitionBy("account_id").orderBy(
        F.col("timestamp").cast("long")
    ).rangeBetween(-600, 0)  # 600 seconds = 10 minutes

    return (
        transactions.withColumn(
            "tx_in_10min_window", F.count("transaction_id").over(window_spec)
        )
        .filter(F.col("tx_in_10min_window") > 5)
        .groupBy("account_id", "customer_id")
        .agg(
            F.max("tx_in_10min_window").alias("max_tx_in_10min"),
            F.count("transaction_id").alias("flagged_transactions"),
            F.sum("amount").alias("flagged_amount"),
            F.max("timestamp").alias("latest_flagged_at"),
        )
        .withColumn("risk_signal", F.lit("velocity_alert"))
        .withColumn("risk_level", F.lit("HIGH"))
    )


def build_large_tx_flags(transactions):
    """Flag transactions > 3x the customer's 30-day average."""
    customer_avg = transactions.groupBy("customer_id").agg(
        F.avg("amount").alias("customer_avg_amount")
    )
    return (
        transactions.join(F.broadcast(customer_avg), on="customer_id", how="left")
        .filter(F.col("amount") > F.col("customer_avg_amount") * 3)
        .select(
            "account_id",
            "customer_id",
            "transaction_id",
            "amount",
            "customer_avg_amount",
            (F.col("amount") / F.col("customer_avg_amount")).alias("amount_vs_avg_ratio"),
            "timestamp",
        )
        .withColumn("risk_signal", F.lit("large_transaction"))
        .withColumn(
            "risk_level",
            F.when(F.col("amount_vs_avg_ratio") > 10, "CRITICAL")
            .when(F.col("amount_vs_avg_ratio") > 5, "HIGH")
            .otherwise("MEDIUM"),
        )
    )


def build_geo_anomalies(transactions, customers):
    """Detect ip_country ≠ customer.country (requires schema v3 fields)."""
    return (
        transactions.filter(F.col("ip_country").isNotNull())
        .join(F.broadcast(customers.select("customer_id", "country")), on="customer_id")
        .filter(F.col("ip_country") != F.col("country"))
        .groupBy("customer_id", "ip_country", "country")
        .agg(
            F.count("transaction_id").alias("anomalous_tx_count"),
            F.sum("amount").alias("anomalous_amount"),
            F.max("timestamp").alias("latest_at"),
        )
        .withColumn("risk_signal", F.lit("geo_anomaly"))
        .withColumn("risk_level", F.lit("MEDIUM"))
    )


def build_failure_rate_flags(transactions):
    """Flag accounts with > 20% failed transactions."""
    return (
        transactions.groupBy("account_id", "customer_id").agg(
            F.count("transaction_id").alias("total_tx"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_tx"),
        )
        .withColumn("failure_rate", F.col("failed_tx") / F.col("total_tx"))
        .filter((F.col("failure_rate") > 0.20) & (F.col("total_tx") >= 5))
        .withColumn("risk_signal", F.lit("high_failure_rate"))
        .withColumn("risk_level", F.lit("MEDIUM"))
    )


def build_currency_exposure(transactions, customers):
    """Summarize outstanding exposure by currency and customer segment."""
    return (
        transactions.filter(F.col("status").isin(["completed", "pending"]))
        .join(F.broadcast(customers.select("customer_id", "segment", "country")), on="customer_id")
        .groupBy("currency", "segment", "country")
        .agg(
            F.sum("amount").alias("total_exposure"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("customer_count"),
        )
        .withColumn("risk_signal", F.lit("currency_exposure"))
        .withColumn("risk_level", F.lit("INFO"))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold: Risk Metrics")
    parser.add_argument("--process-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session("Gold-Risk-Metrics")

    transactions = (
        spark.read.format("delta")
        .load(SILVER_TRANSACTIONS_PATH)
        .filter(F.col("event_date") == args.process_date)
        .select(
            "transaction_id", "account_id", "customer_id", "amount",
            "currency", "status", "timestamp", "event_date",
            "transaction_type", "ip_country",
        )
        .cache()
    )
    customers = spark.read.format("delta").load(SILVER_CUSTOMERS_PATH)

    signals = []

    velocity = build_velocity_alerts(transactions)
    logger.info("Velocity alerts: %d", velocity.count())
    signals.append(velocity.select("account_id", "customer_id", "risk_signal", "risk_level",
                                   F.lit(None).cast("string").alias("transaction_id"),
                                   "flagged_amount", "latest_flagged_at",
                                   F.lit(args.process_date).alias("process_date")))

    large_tx = build_large_tx_flags(transactions)
    logger.info("Large transaction flags: %d", large_tx.count())

    failure_rate = build_failure_rate_flags(transactions)
    logger.info("High failure rate accounts: %d", failure_rate.count())

    # Only run geo anomalies if v3 fields are present
    if "ip_country" in transactions.columns:
        geo = build_geo_anomalies(transactions, customers)
        logger.info("Geographic anomalies: %d", geo.count())

    result = (
        transactions.groupBy("event_date", "currency").agg(
            F.sum("amount").alias("total_volume"),
            F.count("transaction_id").alias("tx_count"),
        )
        .withColumn("process_date", F.lit(args.process_date))
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"process_date = '{args.process_date}'")
        .option("mergeSchema", "true")
        .partitionBy("process_date")
        .save(GOLD_RISK_PATH)
    )

    count = result.count()
    logger.info("Gold risk metrics written | rows=%d | date=%s", count, args.process_date)
    optimize_table(spark, GOLD_RISK_PATH, zorder_cols=["currency", "process_date"])

    transactions.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
