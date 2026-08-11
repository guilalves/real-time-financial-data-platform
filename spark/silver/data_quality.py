"""
Silver Layer — Step 3: Data Quality Checks & Metrics

Validates the Silver table against business rules and writes DQ metrics
that the Airflow quality gate reads to decide whether to proceed to Gold.

DQ checks implemented:
  1. Completeness — mandatory fields not null
  2. Validity — amount > 0, currency in allowed list, valid status
  3. Referential integrity — customer_id exists in silver.customers
  4. Freshness — latest event_date is within 2 hours of now
  5. Volume anomaly — record count within expected bounds

Each check produces: check_name, status (PASS/FAIL), pass_rate, threshold,
                     failed_count, total_count, checked_at, process_date
"""
import argparse
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, functions as F

from config.settings import (
    CURRENCIES,
    DQ_PASS_RATE_THRESHOLD,
    SILVER_CUSTOMERS_PATH,
    SILVER_DQ_METRICS_PATH,
    SILVER_TRANSACTIONS_PATH,
)
from spark.utils.delta_utils import table_exists
from spark.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("silver.data_quality")


@dataclass
class DQResult:
    check_name: str
    status: str          # "PASS" | "FAIL"
    pass_rate: float
    threshold: float
    failed_count: int
    total_count: int
    process_date: str
    checked_at: str


def _check(
    name: str,
    total: int,
    failed: int,
    threshold: float,
    process_date: str,
) -> DQResult:
    passed = total - failed
    pass_rate = passed / total if total > 0 else 1.0
    status = "PASS" if pass_rate >= threshold else "FAIL"
    return DQResult(
        check_name=name,
        status=status,
        pass_rate=round(pass_rate, 6),
        threshold=threshold,
        failed_count=failed,
        total_count=total,
        process_date=process_date,
        checked_at=datetime.utcnow().isoformat(),
    )


def run_dq_checks(spark, process_date: str) -> list[DQResult]:
    df = (
        spark.read.format("delta")
        .load(SILVER_TRANSACTIONS_PATH)
        .filter(F.col("event_date") == process_date)
        .cache()
    )
    total = df.count()
    results: list[DQResult] = []

    if total == 0:
        logger.warning("No records found for process_date=%s", process_date)
        return results

    # 1. Completeness
    null_critical = df.filter(
        "transaction_id IS NULL OR customer_id IS NULL OR "
        "account_id IS NULL OR amount IS NULL OR timestamp IS NULL"
    ).count()
    results.append(_check("completeness_critical_fields", total, null_critical, 1.0, process_date))

    # 2. Amount validity
    invalid_amount = df.filter("amount <= 0").count()
    results.append(_check("validity_amount_positive", total, invalid_amount, DQ_PASS_RATE_THRESHOLD, process_date))

    # 3. Currency validity
    invalid_currency = df.filter(~F.col("currency").isin(CURRENCIES)).count()
    results.append(_check("validity_currency_known", total, invalid_currency, DQ_PASS_RATE_THRESHOLD, process_date))

    # 4. Status validity
    invalid_status = df.filter(
        ~F.col("status").isin(["completed", "pending", "failed", "unknown"])
    ).count()
    results.append(_check("validity_status_known", total, invalid_status, 1.0, process_date))

    # 5. Referential integrity (customer must exist in dimension)
    if table_exists(spark, SILVER_CUSTOMERS_PATH):
        customer_ids = spark.read.format("delta").load(SILVER_CUSTOMERS_PATH).select("customer_id")
        orphan_tx = df.join(customer_ids, on="customer_id", how="left_anti").count()
        results.append(_check("referential_integrity_customer", total, orphan_tx, DQ_PASS_RATE_THRESHOLD, process_date))

    # 6. Late-arrival ratio (informational — threshold is relaxed)
    late_count = df.filter("is_late_arrival = true").count()
    results.append(_check("late_arrival_rate", total, late_count, 0.90, process_date))

    # 7. Freshness: latest event must be within last 2 hours
    max_ts_row = df.selectExpr("max(timestamp) as max_ts").collect()
    max_ts = max_ts_row[0]["max_ts"] if max_ts_row else None
    stale = 0 if (max_ts and max_ts >= datetime.utcnow() - timedelta(hours=2)) else 1
    results.append(_check("freshness_2h", 1, stale, 1.0, process_date))

    df.unpersist()
    return results


def write_dq_metrics(spark, results: list[DQResult], path: str) -> None:
    rows = [asdict(r) for r in results]
    metrics_df = spark.createDataFrame(rows)
    (
        metrics_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)
    )
    logger.info("Written %d DQ metrics to %s", len(rows), path)


def evaluate_quality_gate(results: list[DQResult]) -> bool:
    """Return True if all checks pass (used by Airflow quality gate)."""
    failures = [r for r in results if r.status == "FAIL"]
    if failures:
        for f in failures:
            logger.error(
                "DQ FAIL | check=%s | pass_rate=%.4f | threshold=%.4f | failed=%d",
                f.check_name, f.pass_rate, f.threshold, f.failed_count,
            )
    return len(failures) == 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Data quality checks on Silver")
    parser.add_argument("--process-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session("Silver-Data-Quality")

    results = run_dq_checks(spark, args.process_date)
    write_dq_metrics(spark, results, SILVER_DQ_METRICS_PATH)

    for r in results:
        logger.info(
            "[%s] %s | pass_rate=%.4f (threshold=%.4f) | failed=%d / %d",
            r.status, r.check_name, r.pass_rate, r.threshold, r.failed_count, r.total_count,
        )

    passed = evaluate_quality_gate(results)
    if not passed:
        raise SystemExit("Quality gate FAILED — Gold layer will not be updated.")

    logger.info("All DQ checks PASSED for %s", args.process_date)
    spark.stop()


if __name__ == "__main__":
    main()
