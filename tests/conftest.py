"""
Shared pytest fixtures for unit and integration tests.

Unit tests use a local SparkSession in local[2] mode — no external services needed.
Integration tests require Docker services (set INTEGRATION_TESTS=1 env variable).
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with Delta Lake enabled for all tests."""
    session = (
        SparkSession.builder.appName("TestSuite")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")  # fast in tests
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_transactions(spark):
    """A small synthetic DataFrame replicating the Silver schema."""
    from datetime import datetime

    from pyspark.sql.types import (
        BooleanType,
        DecimalType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("transaction_id", StringType(), nullable=True),
        StructField("account_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("transaction_type", StringType(), nullable=True),
        StructField("amount", DecimalType(18, 2), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("merchant", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("is_late_arrival", BooleanType(), nullable=True),
    ])
    data = [
        ("TXN001", "ACC001", "CUS001", "purchase", 150.00, "USD", "Amazon", "completed",
         datetime(2024, 1, 15, 10, 0, 0), "credit_card", False),
        ("TXN002", "ACC001", "CUS001", "purchase", 25.50, "USD", "Starbucks", "completed",
         datetime(2024, 1, 15, 11, 0, 0), "debit_card", False),
        ("TXN003", "ACC002", "CUS002", "transfer", 500.00, "EUR", None, "completed",
         datetime(2024, 1, 15, 9, 30, 0), "wire_transfer", False),
        ("TXN004", "ACC003", "CUS003", "withdrawal", -50.00, "USD", None, "failed",
         datetime(2024, 1, 15, 8, 0, 0), "debit_card", False),
        # Duplicate of TXN001 (same transaction_id)
        ("TXN001", "ACC001", "CUS001", "purchase", 150.00, "USD", "Amazon", "completed",
         datetime(2024, 1, 15, 10, 0, 0), "credit_card", False),
        # Null transaction_id (DQ failure)
        (None, "ACC004", "CUS004", "purchase", 75.00, "USD", "Target", "completed",
         datetime(2024, 1, 15, 12, 0, 0), "credit_card", False),
        # Null customer_id
        ("TXN005", "ACC005", None, "purchase", 30.00, "USD", "Netflix", "completed",
         datetime(2024, 1, 15, 13, 0, 0), "credit_card", False),
        # Late arrival
        ("TXN006", "ACC006", "CUS006", "purchase", 200.00, "BRL", "Supermarket", "completed",
         datetime(2024, 1, 15, 7, 0, 0), "credit_card", True),
    ]
    return spark.createDataFrame(data, schema)
