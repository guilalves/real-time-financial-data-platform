"""
Gold Layer — Customer Analytics

Produces the gold.customer_analytics table consumed by BI and Analytics teams.

Metrics produced:
  - Transaction counts and volumes by customer
  - Average transaction value and P95 spend
  - RFM scoring (Recency, Frequency, Monetary) for segmentation
  - Active days and customer activity tier
"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import (
    GOLD_CUSTOMER_PATH,
    SILVER_ACCOUNTS_PATH,
    SILVER_CUSTOMERS_PATH,
    SILVER_TRANSACTIONS_PATH,
)
from spark.utils.delta_utils import optimize_table
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("gold.customer_analytics")

LOOKBACK_DAYS = 90


def build_customer_analytics(spark, process_date: str):
    transactions = (
        spark.read.format("delta")
        .load(SILVER_TRANSACTIONS_PATH)
        .filter(
            (F.col("event_date") >= F.date_sub(F.lit(process_date), LOOKBACK_DAYS))
            & (F.col("event_date") <= F.lit(process_date))
            & (F.col("status") != "failed")
        )
        .select(
            "transaction_id", "customer_id", "account_id",
            "amount", "timestamp", "event_date", "transaction_type",
        )
        .repartition(200, "customer_id")
        .cache()
    )

    customers = F.broadcast(
        spark.read.format("delta")
        .load(SILVER_CUSTOMERS_PATH)
        .select("customer_id", "name", "country", "segment", "preferred_currency", "age")
    )

    # Core aggregations
    tx_agg = transactions.groupBy("customer_id").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_spend"),
        F.avg("amount").alias("avg_transaction_value"),
        F.percentile_approx("amount", 0.95).alias("p95_transaction_value"),
        F.max("timestamp").alias("last_transaction_at"),
        F.min("timestamp").alias("first_transaction_at"),
        F.countDistinct("event_date").alias("active_days"),
        F.countDistinct("account_id").alias("accounts_used"),
        F.sum(F.when(F.col("transaction_type") == "purchase", F.col("amount")).otherwise(0))
        .alias("total_purchases"),
        F.sum(F.when(F.col("transaction_type") == "transfer", F.col("amount")).otherwise(0))
        .alias("total_transfers"),
    )

    # RFM — Recency (days since last tx), Frequency (count), Monetary (sum)
    rfm = transactions.groupBy("customer_id").agg(
        F.datediff(F.lit(process_date), F.max("event_date")).alias("recency_days"),
        F.count("transaction_id").alias("frequency"),
        F.sum("amount").alias("monetary"),
    )

    # RFM scoring: 1–5 quintiles (5 = best)
    quintile_window = Window.orderBy("recency_days")
    rfm = rfm.withColumn(
        "recency_score",
        F.ntile(5).over(quintile_window.orderBy(F.col("recency_days").asc())),
    ).withColumn(
        "frequency_score",
        F.ntile(5).over(Window.orderBy(F.col("frequency").asc())),
    ).withColumn(
        "monetary_score",
        F.ntile(5).over(Window.orderBy(F.col("monetary").asc())),
    ).withColumn(
        "rfm_score",
        F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"),
    )

    # Activity tier based on RFM score
    rfm = rfm.withColumn(
        "activity_tier",
        F.when(F.col("rfm_score") >= 12, "champion")
        .when(F.col("rfm_score") >= 9, "loyal")
        .when(F.col("rfm_score") >= 6, "potential")
        .when(F.col("rfm_score") >= 4, "at_risk")
        .otherwise("churned"),
    )

    result = (
        tx_agg.join(customers, on="customer_id", how="left")
        .join(rfm.select("customer_id", "recency_days", "recency_score", "frequency_score",
                         "monetary_score", "rfm_score", "activity_tier"),
              on="customer_id", how="left")
        .withColumn("process_date", F.lit(process_date))
        .withColumn("processed_at", F.current_timestamp())
    )

    transactions.unpersist()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold: Customer Analytics")
    parser.add_argument("--process-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session("Gold-Customer-Analytics")
    df = build_customer_analytics(spark, args.process_date)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"process_date = '{args.process_date}'")
        .option("mergeSchema", "true")
        .partitionBy("process_date")
        .save(GOLD_CUSTOMER_PATH)
    )

    count = df.count()
    logger.info("Gold customer analytics written | rows=%d | date=%s", count, args.process_date)

    optimize_table(spark, GOLD_CUSTOMER_PATH, zorder_cols=["customer_id", "segment"])
    spark.stop()


if __name__ == "__main__":
    main()
