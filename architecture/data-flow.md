# Data Flow — Real-Time Financial Data Platform

## Overview

This document describes the data flow across all layers of the platform, including the streaming path, batch processing path, and the decision points for data quality routing.

---

## Ingestion Layer (Streaming)

```
[Data Generator]
      │
      │  Python Faker-based synthetic data
      │  ~100–1000 transactions/sec (configurable)
      │
      ▼
[Kafka Producer]
      │
      │  Topic: transaction-events
      │  Partitions: 12 (keyed by account_id)
      │  Serialization: JSON (v1/v2) → Avro w/ Schema Registry (planned)
      │  Retention: 7 days
      │
      ▼
[Kafka Broker (Confluent)]
      │
      │  Consumer Group: spark-bronze-consumer
      │  Offset managed by Spark checkpoints
      │  Delivery guarantee: at-least-once
      │
      ▼
[Spark Structured Streaming]
      │
      │  Trigger: ProcessingTime("1 minute")
      │  Watermark: 30 minutes on event_time
      │  Output mode: append
      │
      ▼
[Bronze Layer — Delta Lake]
      │
      │  Path: s3a://datalake/bronze/transactions/
      │  Format: Delta (JSON payload stored as raw string + parsed fields)
      │  Partitioned by: ingestion_date
      │  Schema: raw_payload (string), kafka_offset, kafka_partition,
      │           kafka_timestamp, ingestion_timestamp, ingestion_date
```

---

## Processing Layer — Bronze → Silver (Batch)

```
[Bronze Delta Table]
      │
      │  Read with predicate pushdown on ingestion_date
      │
      ▼
[Step 1: Parse & Cast]  (clean_transactions.py)
      │
      │  JSON parsing of raw_payload
      │  Type casting (string → timestamp, decimal, etc.)
      │  Null enforcement on mandatory fields
      │  Schema evolution: mergeSchema handles new nullable columns
      │
      ▼
[Step 2: Deduplication]  (deduplicate_transactions.py)
      │
      │  Window function: row_number() OVER (PARTITION BY transaction_id
      │                                       ORDER BY event_time DESC)
      │  MERGE INTO silver.transactions ON transaction_id
      │  Late arrivals: flagged, not discarded
      │
      ▼
[Step 3: Data Quality]  (data_quality.py)
      │
      │  Completeness checks → quarantine if critical fields null
      │  Validity checks     → quarantine if amount ≤ 0
      │  Referential checks  → flag if customer_id not in dim_customers
      │  Freshness check     → alert if batch > 2h stale
      │
      ├──[PASS]──► silver.transactions  (clean, deduplicated, validated)
      │
      └──[FAIL]──► silver.transactions_quarantine  (reason, original record)
                   silver.dq_metrics               (counts, pass rates)
```

---

## Processing Layer — Silver → Gold (Batch)

```
[silver.transactions] + [silver.customers] + [silver.accounts]
      │
      │  Broadcast join on dimension tables (< 100MB each)
      │  Partition filter: process_date = today
      │
      ▼
[gold.customer_analytics]
      │  Daily active customers, transaction counts, avg spend
      │  Customer segment distribution
      │  Churn risk score (RFM-based)

[gold.transaction_analytics]
      │  Volume by transaction_type, currency, merchant category
      │  Hourly/daily trend aggregations
      │  P50/P95/P99 amount distribution

[gold.risk_metrics]
      │  Velocity checks: >5 transactions in 10 minutes per account
      │  Geographic anomalies: ip_country ≠ customer.country
      │  Large transaction flags: amount > 3× 30-day avg
      │  Exposure by currency/region
```

---

## Schema Evolution Timeline

```
v1 (Initial)                  v2 (Month 2)                  v3 (Month 4)
─────────────────             ─────────────────             ─────────────────
transaction_id                transaction_id                transaction_id
customer_id                   customer_id                   customer_id
account_id                    account_id                    account_id
transaction_type              transaction_type              transaction_type
amount                        amount                        amount
currency                      currency                      currency
merchant                      merchant                      merchant
timestamp                     timestamp                     timestamp
status                        status                        status
                              payment_method (nullable)     payment_method
                                                            device_fingerprint (nullable)
                                                            ip_country (nullable)

Delta Lake mergeSchema = true handles this automatically.
Old records have NULL for new columns — no backfill required.
```

---

## Data Quality Decision Tree

```
For each incoming record:

  transaction_id is NULL?
      YES → quarantine (reason: missing_transaction_id)
      NO  ↓

  customer_id is NULL?
      YES → quarantine (reason: missing_customer_id)
      NO  ↓

  amount ≤ 0?
      YES → quarantine (reason: invalid_amount)
      NO  ↓

  timestamp is NULL?
      YES → quarantine (reason: missing_timestamp)
      NO  ↓

  currency NOT IN allowed_list?
      YES → quarantine (reason: invalid_currency)
      NO  ↓

  is_duplicate? (same transaction_id already in silver)
      YES → skip + increment duplicate_count metric
      NO  ↓

  → WRITE to silver.transactions ✓
```

---

## Orchestration (Airflow)

```
Schedule: @daily (runs at 02:00 UTC)

[check_bronze_freshness]
      │  Fails if latest bronze partition > 2h old
      ▼
[run_silver_clean]
      │  spark-submit clean_transactions.py --date={{ ds }}
      ▼
[run_silver_deduplicate]
      │  spark-submit deduplicate_transactions.py --date={{ ds }}
      ▼
[run_silver_data_quality]
      │  spark-submit data_quality.py --date={{ ds }}
      │
  [quality_gate]  — BranchPythonOperator
      │
      │  pass_rate >= 99%?
      │
      ├── YES ──► [run_gold_customer_analytics]
      │           [run_gold_transaction_analytics]   ← parallel
      │           [run_gold_risk_metrics]
      │           [pipeline_success_notification]
      │
      └── NO ───► [pipeline_failure_notification]   (Slack alert)
                  [halt — do not update Gold]
```

---

## Storage Layout (MinIO / S3)

```
s3a://datalake/
├── bronze/
│   └── transactions/
│       ├── ingestion_date=2024-01-15/
│       │   └── *.parquet  (Delta files)
│       └── _delta_log/
│
├── silver/
│   ├── transactions/
│   ├── transactions_quarantine/
│   ├── customers/
│   ├── accounts/
│   └── dq_metrics/
│
└── gold/
    ├── customer_analytics/
    ├── transaction_analytics/
    └── risk_metrics/
```

---

## Performance Profile

| Stage | Records | Duration (unoptimized) | Duration (optimized) | Optimization Applied |
|---|---|---|---|---|
| Bronze ingest | 500K / batch | — | ~45s | Checkpoint + append mode |
| Silver clean | 500K | 4m 20s | 1m 10s | Predicate pushdown, column pruning |
| Silver dedup | 500K | 8m 42s | 2m 15s | MERGE + broadcast join |
| Gold customer | 500K → 50K | 3m 05s | 55s | ZORDER, repartition |
| Gold risk | 500K → 5K flags | 6m 18s | 1m 40s | Window + filter pushdown |
