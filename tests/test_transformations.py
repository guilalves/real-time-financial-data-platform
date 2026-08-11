"""Unit tests for Silver transformations — cleaning and deduplication logic."""
from decimal import Decimal

from pyspark.sql import functions as F

from spark.silver.clean_transactions import (
    apply_type_corrections,
    split_valid_invalid,
)
from spark.silver.deduplicate_transactions import deduplicate_within_batch


class TestCleanTransactions:
    def test_split_routes_null_transaction_id_to_invalid(self, spark, sample_transactions):
        valid_df, invalid_df = split_valid_invalid(sample_transactions)
        null_txn_in_invalid = invalid_df.filter("transaction_id IS NULL").count()
        assert null_txn_in_invalid == 1

    def test_split_routes_null_customer_id_to_invalid(self, spark, sample_transactions):
        valid_df, invalid_df = split_valid_invalid(sample_transactions)
        null_cus_in_invalid = invalid_df.filter("customer_id IS NULL").count()
        assert null_cus_in_invalid == 1

    def test_valid_records_have_no_critical_nulls(self, spark, sample_transactions):
        valid_df, _ = split_valid_invalid(sample_transactions)
        assert valid_df.filter(
            "transaction_id IS NULL OR customer_id IS NULL OR "
            "account_id IS NULL OR amount IS NULL OR timestamp IS NULL"
        ).count() == 0

    def test_quarantine_reason_column_added(self, spark, sample_transactions):
        _, invalid_df = split_valid_invalid(sample_transactions)
        assert "quarantine_reason" in invalid_df.columns

    def test_currency_normalized_to_uppercase(self, spark, sample_transactions):
        valid_df, _ = split_valid_invalid(sample_transactions)
        lowercased = valid_df.withColumn("currency", F.lower("currency"))
        corrected = apply_type_corrections(lowercased)
        # After apply_type_corrections, all known currencies should be uppercase
        lower_count = corrected.filter(F.col("currency") == F.lower(F.col("currency"))).count()
        assert lower_count == 0

    def test_amount_absolute_value_applied(self, spark, sample_transactions):
        """apply_type_corrections converts negative amounts to positive."""
        valid_df, _ = split_valid_invalid(sample_transactions)
        corrected = apply_type_corrections(valid_df)
        negative = corrected.filter("amount < 0").count()
        assert negative == 0

    def test_unknown_currency_mapped_to_unknown(self, spark, spark_session=None):
        """Currency not in the known list should be mapped to 'UNKNOWN'."""
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
            StructField("transaction_id", StringType()),
            StructField("account_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("transaction_type", StringType()),
            StructField("amount", DecimalType(18, 2)),
            StructField("currency", StringType()),
            StructField("merchant", StringType()),
            StructField("status", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("payment_method", StringType()),
            StructField("is_late_arrival", BooleanType()),
        ])
        data = [("TXN999", "ACC001", "CUS001", "purchase", Decimal("10.00"),
                 "XYZ", "Merchant", "completed",
                 datetime(2024, 1, 15), "credit_card", False)]
        df = spark.createDataFrame(data, schema)
        corrected = apply_type_corrections(df)
        currency = corrected.select("currency").collect()[0]["currency"]
        assert currency == "UNKNOWN"


class TestDeduplication:
    def test_within_batch_dedup_removes_exact_duplicates(self, spark, sample_transactions):
        """TXN001 appears twice in sample_transactions; only one should remain."""
        valid_df, _ = split_valid_invalid(sample_transactions)
        deduped = deduplicate_within_batch(valid_df)
        txn001_count = deduped.filter("transaction_id = 'TXN001'").count()
        assert txn001_count == 1

    def test_within_batch_dedup_preserves_unique_records(self, spark, sample_transactions):
        valid_df, _ = split_valid_invalid(sample_transactions)
        before = valid_df.select("transaction_id").distinct().count()
        deduped = deduplicate_within_batch(valid_df)
        after = deduped.count()
        assert after == before

    def test_dedup_keeps_latest_by_ingestion_timestamp(self, spark):
        """When two rows share a transaction_id, keep the most recent ingestion."""
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
            StructField("transaction_id", StringType()),
            StructField("account_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("transaction_type", StringType()),
            StructField("amount", DecimalType(18, 2)),
            StructField("currency", StringType()),
            StructField("merchant", StringType()),
            StructField("status", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("payment_method", StringType()),
            StructField("is_late_arrival", BooleanType()),
            StructField("ingestion_timestamp", TimestampType()),
        ])
        data = [
            ("TXN_DUP", "ACC001", "CUS001", "purchase", Decimal("100.00"),
             "USD", "M", "pending", datetime(2024, 1, 15), "credit_card", False,
             datetime(2024, 1, 15, 1, 0, 0)),
            ("TXN_DUP", "ACC001", "CUS001", "purchase", Decimal("100.00"),
             "USD", "M", "completed", datetime(2024, 1, 15), "credit_card", False,
             datetime(2024, 1, 15, 2, 0, 0)),  # newer ingestion
        ]
        df = spark.createDataFrame(data, schema)
        deduped = deduplicate_within_batch(df)
        assert deduped.count() == 1
        status = deduped.select("status").collect()[0]["status"]
        assert status == "completed"  # the newer ingestion should win
