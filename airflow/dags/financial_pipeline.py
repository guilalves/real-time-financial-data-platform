"""
Airflow DAG — Real-Time Financial Data Platform

Orchestrates the daily batch pipeline:
  Bronze freshness check → Silver clean → Silver dedup → Silver DQ
  → [quality gate] → Gold layers (parallel) → success notification

Design decisions:
  - Quality gate (BranchPythonOperator): checks DQ pass_rate in silver.dq_metrics.
    If any check FAILS, the Gold update is skipped and the team is notified.
  - All spark-submit tasks use SparkSubmitOperator for native Spark UI integration.
  - Retry with exponential backoff on all Spark tasks.
  - SLA configured to alert if pipeline takes > 3 hours.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

SPARK_CONN_ID = "spark_default"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-engineering@company.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=3),
}

SPARK_SUBMIT_DEFAULTS = {
    "conn_id": SPARK_CONN_ID,
    "conf": {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    },
    "packages": "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4",
    "driver_memory": "2g",
    "executor_memory": "4g",
    "executor_cores": 2,
    "num_executors": 4,
}


def _check_bronze_freshness(**context) -> None:
    """Fail the task if the latest Bronze partition is more than 2 hours old."""
    from config.settings import BRONZE_PATH
    from spark.utils.spark_session import get_spark_session

    spark = get_spark_session("Airflow-Freshness-Check")
    try:
        latest = (
            spark.read.format("delta")
            .load(BRONZE_PATH)
            .selectExpr("max(ingestion_timestamp) as latest_ts")
            .collect()[0]["latest_ts"]
        )
        if latest is None:
            raise ValueError("Bronze table is empty")
        age_hours = (datetime.utcnow() - latest).total_seconds() / 3600
        if age_hours > 2:
            raise ValueError(f"Bronze data is {age_hours:.1f}h old (threshold: 2h)")
    finally:
        spark.stop()


def _quality_gate(**context) -> str:
    """Read DQ metrics and branch based on pass/fail status."""
    from pyspark.sql import functions as F

    from config.settings import SILVER_DQ_METRICS_PATH
    from spark.utils.spark_session import get_spark_session

    process_date = context["ds"]
    spark = get_spark_session("Airflow-Quality-Gate")
    try:
        metrics = (
            spark.read.format("delta")
            .load(SILVER_DQ_METRICS_PATH)
            .filter(F.col("process_date") == process_date)
            .filter(F.col("status") == "FAIL")
        )
        failed_checks = metrics.count()
    finally:
        spark.stop()

    if failed_checks > 0:
        return "quality_gate_failed"
    return "quality_gate_passed"


def _notify_failure(**context) -> None:
    """Send Slack/email alert on quality gate failure."""
    process_date = context["ds"]
    # In production: use SlackWebhookOperator or EmailOperator
    print(f"[ALERT] Quality gate FAILED for {process_date}. Gold layer NOT updated.")


with DAG(
    dag_id="financial_pipeline",
    description="End-to-end financial data pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["financial", "medallion", "delta-lake"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_bronze_freshness = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=_check_bronze_freshness,
    )

    silver_clean = SparkSubmitOperator(
        task_id="run_silver_clean",
        application="spark/silver/clean_transactions.py",
        application_args=["--process-date", "{{ ds }}"],
        name="silver-clean-{{ ds }}",
        **SPARK_SUBMIT_DEFAULTS,
    )

    silver_dedup = SparkSubmitOperator(
        task_id="run_silver_deduplicate",
        application="spark/silver/deduplicate_transactions.py",
        application_args=["--process-date", "{{ ds }}"],
        name="silver-dedup-{{ ds }}",
        **SPARK_SUBMIT_DEFAULTS,
    )

    silver_dq = SparkSubmitOperator(
        task_id="run_silver_data_quality",
        application="spark/silver/data_quality.py",
        application_args=["--process-date", "{{ ds }}"],
        name="silver-dq-{{ ds }}",
        **SPARK_SUBMIT_DEFAULTS,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
    )

    gate_passed = EmptyOperator(task_id="quality_gate_passed")
    gate_failed = EmptyOperator(task_id="quality_gate_failed")

    notify_failure = PythonOperator(
        task_id="notify_on_failure",
        python_callable=_notify_failure,
        trigger_rule="none_failed",
    )

    gold_customers = SparkSubmitOperator(
        task_id="run_gold_customer_analytics",
        application="spark/gold/customer_analytics.py",
        application_args=["--process-date", "{{ ds }}"],
        name="gold-customers-{{ ds }}",
        **SPARK_SUBMIT_DEFAULTS,
    )

    gold_transactions = SparkSubmitOperator(
        task_id="run_gold_transaction_analytics",
        application="spark/gold/transaction_analytics.py",
        application_args=["--process-date", "{{ ds }}"],
        name="gold-transactions-{{ ds }}",
        **SPARK_SUBMIT_DEFAULTS,
    )

    gold_risk = SparkSubmitOperator(
        task_id="run_gold_risk_metrics",
        application="spark/gold/risk_metrics.py",
        application_args=["--process-date", "{{ ds }}"],
        name="gold-risk-{{ ds }}",
        **SPARK_SUBMIT_DEFAULTS,
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG dependency graph
    start >> check_bronze_freshness >> silver_clean >> silver_dedup >> silver_dq >> quality_gate
    quality_gate >> gate_passed >> [gold_customers, gold_transactions, gold_risk] >> pipeline_complete
    quality_gate >> gate_failed >> notify_failure >> pipeline_complete
