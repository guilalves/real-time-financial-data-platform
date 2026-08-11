"""Unit tests for data quality checks."""
import pytest
from pyspark.sql import functions as F

from spark.silver.data_quality import (
    DQResult,
    _check,
    evaluate_quality_gate,
)


class TestDQResultHelper:
    def test_pass_when_above_threshold(self):
        result = _check("test_check", total=1000, failed=5, threshold=0.99, process_date="2024-01-15")
        assert result.status == "PASS"
        assert result.pass_rate == pytest.approx(0.995, abs=1e-4)

    def test_fail_when_below_threshold(self):
        result = _check("test_check", total=1000, failed=20, threshold=0.99, process_date="2024-01-15")
        assert result.status == "FAIL"
        assert result.failed_count == 20

    def test_pass_rate_is_one_when_no_failures(self):
        result = _check("test_check", total=500, failed=0, threshold=1.0, process_date="2024-01-15")
        assert result.status == "PASS"
        assert result.pass_rate == 1.0

    def test_empty_dataset_defaults_to_pass(self):
        result = _check("test_check", total=0, failed=0, threshold=0.99, process_date="2024-01-15")
        assert result.status == "PASS"
        assert result.pass_rate == 1.0


class TestQualityGate:
    def test_all_pass_returns_true(self):
        results = [
            DQResult("check_a", "PASS", 0.999, 0.99, 1, 1000, "2024-01-15", "2024-01-15T00:00:00"),
            DQResult("check_b", "PASS", 1.0, 1.0, 0, 500, "2024-01-15", "2024-01-15T00:00:00"),
        ]
        assert evaluate_quality_gate(results) is True

    def test_any_fail_returns_false(self):
        results = [
            DQResult("check_a", "PASS", 0.999, 0.99, 1, 1000, "2024-01-15", "2024-01-15T00:00:00"),
            DQResult("check_b", "FAIL", 0.95, 0.99, 50, 1000, "2024-01-15", "2024-01-15T00:00:00"),
        ]
        assert evaluate_quality_gate(results) is False

    def test_empty_results_returns_true(self):
        assert evaluate_quality_gate([]) is True


class TestDataQualityWithSpark:
    def test_null_critical_fields_are_identified(self, spark, sample_transactions):
        from pyspark.sql import functions as F

        CRITICAL_FIELDS = ["transaction_id", "customer_id", "account_id", "amount", "timestamp"]
        null_check = " OR ".join([f"{f} IS NULL" for f in CRITICAL_FIELDS])

        null_count = sample_transactions.filter(null_check).count()
        # conftest injects 2 records with nulls (transaction_id=None, customer_id=None)
        assert null_count == 2

    def test_invalid_amounts_identified(self, spark, sample_transactions):
        # TXN004 has amount = -50.00 (should be flagged)
        invalid = sample_transactions.filter("amount <= 0")
        assert invalid.count() == 1

    def test_valid_records_pass_all_checks(self, spark, sample_transactions):
        valid = sample_transactions.filter(
            "transaction_id IS NOT NULL AND customer_id IS NOT NULL AND amount > 0"
        )
        assert valid.count() == 5  # 8 total - 2 nulls - 1 negative amount

    def test_late_arrivals_are_flagged(self, spark, sample_transactions):
        late = sample_transactions.filter("is_late_arrival = true")
        assert late.count() == 1
