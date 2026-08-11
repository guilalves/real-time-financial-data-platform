# Real-Time Financial Data Platform

> **End-to-end data platform designed to process financial transactions using batch and streaming architectures, implementing Medallion Architecture on Delta Lake with real engineering challenges: late data, schema evolution, deduplication, and performance optimization.**

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-black?logo=apache-kafka)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-red?logo=apache-airflow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![CI](https://img.shields.io/github/actions/workflow/status/yourusername/real-time-financial-data-platform/ci.yml?label=CI)

---

## Table of Contents

- [Business Problem](#business-problem)
- [Architecture](#architecture)
- [Medallion Architecture](#medallion-architecture)
- [Streaming Pipeline](#streaming-pipeline)
- [Engineering Challenges](#engineering-challenges)
- [Data Quality](#data-quality)
- [Performance Optimization](#performance-optimization)
- [Data Model](#data-model)
- [Orchestration](#orchestration)
- [Testing](#testing)
- [CI/CD](#cicd)
- [How to Run](#how-to-run)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Future Improvements](#future-improvements)

---

## Business Problem

A financial institution needs to process thousands of transactions per second and make reliable data available for three different consumer teams:

| Team | Need | Latency |
|---|---|---|
| **Analytics** | Aggregated metrics, trends, KPIs | Minutes |
| **Risk** | Fraud signals, anomalies, exposure | Near real-time |
| **Business** | Customer segments, product performance | Daily |

The platform must:
1. Ingest transactions from multiple sources in near real-time
2. Store raw data in an auditable, immutable layer
3. Validate, clean, and deduplicate events
4. Enrich data with customer and product context
5. Produce analytics-ready datasets for downstream consumers

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SYNTHETIC SOURCES                        │
│                                                             │
│   Transactions · Customers · Accounts · Products           │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   APACHE KAFKA                              │
│                                                             │
│   topic: transaction-events   (partitioned by account_id)  │
└─────────────────────────┬───────────────────────────────────┘
                          │  Structured Streaming
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Delta Lake)                  │
│                                                             │
│   Raw transactional events — schema-on-read                 │
│   Append-only · Immutable · Full audit trail                │
└─────────────────────────┬───────────────────────────────────┘
                          │  PySpark Batch
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Delta Lake)                  │
│                                                             │
│   Cleansing · Validation · Deduplication                    │
│   Schema Enforcement · Enrichment · Data Quality            │
└─────────────────────────┬───────────────────────────────────┘
                          │  PySpark Batch
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   GOLD LAYER (Delta Lake)                   │
│                                                             │
│   Customer Analytics · Transaction Analytics                │
│   Risk Metrics · Aggregated KPIs                            │
└──────────────┬──────────────────────┬───────────────────────┘
               │                      │
               ▼                      ▼
        BI / SQL              ML-Ready Datasets
```

**Stack:** Python · PySpark · Apache Kafka · Delta Lake · Apache Airflow · Docker · MinIO (S3-compatible) · GitHub Actions

---

## Medallion Architecture

### Bronze — Raw Ingestion
- Append-only writes from Kafka Structured Streaming
- Full raw payload preserved (no transformations)
- Partitioned by `ingestion_date` for efficient pruning
- Checkpoint-based exactly-once semantics

### Silver — Trusted Data
- Type casting and null enforcement
- Idempotent deduplication via `MERGE INTO` on `transaction_id`
- Late-arrival handling using event-time watermarking (30-minute window)
- Schema evolution support (`mergeSchema = true`)
- Invalid records routed to a **quarantine table** for observability
- Data quality metrics written to `silver.dq_metrics`

### Gold — Analytics-Ready
- Pre-aggregated for specific consumer use cases
- Dimensional model (facts + dimensions)
- Optimized with `OPTIMIZE` + `ZORDER` on frequent filter columns
- Refreshed on schedule via Airflow DAG

---

## Streaming Pipeline

```python
# Structured Streaming: Kafka → Bronze Delta Lake
stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "transaction-events")
    .option("startingOffsets", "earliest")
    .load()
)

# Write with checkpoint for fault tolerance
(
    stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingestion_date")
    .start(BRONZE_PATH)
)
```

**Key design decisions:**
- Topic partitioned by `account_id` to preserve ordering per account
- Consumer group per environment (dev / staging / prod)
- Offset management delegated to Spark checkpoints (not Kafka consumer groups)

---

## Engineering Challenges

### 1. Duplicate Events
Kafka guarantees at-least-once delivery. The same `transaction_id` can appear multiple times in the Bronze layer.

**Solution:** Idempotent `MERGE INTO` in the Silver layer:

```sql
MERGE INTO silver.transactions AS target
USING (SELECT * FROM staging WHERE row_number = 1) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 2. Late-Arriving Events
A transaction with `event_time = T-45min` may arrive at `processing_time = T`. Without watermarking, this corrupts time-based aggregations.

**Solution:** Watermark of 30 minutes on `event_time`. Events outside the watermark are tracked and reported in DQ metrics but not silently ignored — they land in a late-arrival partition for manual review.

### 3. Schema Evolution
The transaction schema evolved mid-project:

| Version | Fields Added |
|---|---|
| v1 | `transaction_id`, `customer_id`, `amount`, `timestamp` |
| v2 | `payment_method` (nullable) |
| v3 | `device_fingerprint`, `ip_country` (nullable) |

**Solution:** Delta Lake `mergeSchema` + explicit nullable contract. Old records have `NULL` for new fields — no backfills needed, no pipeline breaks.

### 4. Data Quality at Scale
Invalid records are not dropped silently. Every quality check is instrumented:

| Check | Rule |
|---|---|
| Completeness | `transaction_id`, `customer_id`, `timestamp` NOT NULL |
| Validity | `amount > 0`, `currency` in known list |
| Referential integrity | `customer_id` must exist in `silver.customers` |
| Freshness | No batch older than 2 hours |

DQ results feed a metrics table queried by Airflow to decide whether to proceed or halt the pipeline.

### 5. Performance Optimization

Optimization applied to the Silver → Gold aggregation step:

| Technique | Before | After |
|---|---|---|
| Broadcast join (customers dim) | 8m 42s | 2m 15s |
| Column pruning | — | 40% less I/O |
| Partition filter pushdown | Full scan | Partition scan only |
| `ZORDER BY (customer_id, timestamp)` | Random I/O | Co-located reads |
| Repartition before heavy aggregations | Skewed tasks | Balanced tasks |

See [`spark/silver/performance_benchmark.py`](spark/silver/performance_benchmark.py) for the before/after comparison with execution plan analysis.

---

## Data Model

### Source Entities

```
customers           accounts              transactions
──────────          ──────────            ──────────────────────
customer_id (PK)    account_id (PK)       transaction_id (PK)
name                customer_id (FK)      account_id (FK)
age                 account_type          customer_id (FK)
country             status                transaction_type
segment             balance               amount
email               currency              currency
created_at          created_at            merchant
                                          timestamp
                                          status
                                          payment_method   ← v2
                                          device_fingerprint ← v3
                                          ip_country         ← v3

products
──────────
product_id (PK)
product_name
category
interest_rate
status
```

### Gold Layer (Dimensional Model)

```
fact_transactions ──────────► dim_customer
      │                       dim_account
      │                       dim_product
      │                       dim_date
      └──────────────────────► dim_merchant
```

---

## Orchestration

Apache Airflow manages the batch pipeline with a DAG that:

1. Triggers Silver processing after Bronze stream checkpoint
2. Runs data quality checks and **halts on failure threshold** (configurable)
3. Builds Gold layer aggregations
4. Sends Slack alert on failure or DQ degradation
5. Retries with exponential backoff

```
financial_pipeline DAG
│
├── check_bronze_freshness
├── run_silver_clean
├── run_silver_deduplicate
├── run_silver_data_quality
│   └── [branch] quality_gate_passed?
│       ├── YES → run_gold_customer_analytics
│       │         run_gold_transaction_analytics
│       │         run_gold_risk_metrics
│       └── NO  → notify_on_failure (halt)
└── pipeline_complete
```

---

## Testing

```bash
# Unit tests (no Spark required)
pytest tests/unit/ -v

# Integration tests (requires Docker services)
pytest tests/integration/ -v --timeout=120

# Coverage report
pytest --cov=spark --cov-report=html
```

| Test Type | Coverage |
|---|---|
| Data quality rules | 100% |
| Deduplication logic | 100% |
| Schema evolution | Key scenarios |
| Transformations | Core business rules |

---

## CI/CD

GitHub Actions runs on every push and pull request:

```
CI Pipeline
├── lint (ruff + black)
├── type-check (mypy)
├── unit-tests (pytest)
└── integration-tests (pytest + Docker services)
```

Merge to `main` also triggers:
- Docker image build and push to GHCR
- DAG validation (Airflow import check)

---

## How to Run

### Prerequisites
- Docker Desktop 4.x+
- Python 3.11+
- 8 GB RAM minimum (Spark + Kafka)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/real-time-financial-data-platform.git
cd real-time-financial-data-platform

# Start infrastructure (Kafka, Spark, Airflow, MinIO)
docker compose -f docker/docker-compose.yml up -d

# Install Python dependencies
pip install -r requirements.txt

# Generate synthetic data
python data_generator/customers.py --records 10000
python data_generator/accounts.py --records 15000
python data_generator/products.py --records 50
python data_generator/transactions.py --records 500000

# Start Kafka producer (streams transactions)
python kafka/producer.py --topic transaction-events --tps 100

# Submit Bronze ingestion job
spark-submit spark/bronze/ingest_transactions.py

# Run Silver processing
spark-submit spark/silver/clean_transactions.py
spark-submit spark/silver/deduplicate_transactions.py
spark-submit spark/silver/data_quality.py

# Build Gold layer
spark-submit spark/gold/customer_analytics.py
spark-submit spark/gold/transaction_analytics.py
spark-submit spark/gold/risk_metrics.py

# Access Airflow UI
open http://localhost:8080   # admin / admin
```

### Service URLs

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| MinIO Console | http://localhost:9001 |
| Spark UI | http://localhost:4040 |

---

## Key Engineering Decisions

**Why Delta Lake over Parquet?**
ACID transactions, time travel, schema enforcement, and efficient `MERGE INTO` for deduplication. Parquet alone cannot provide upsert semantics.

**Why Kafka instead of direct file ingestion?**
Decouples producers from consumers, provides replay capability, and enables multiple independent consumers (Spark Streaming + future ML pipeline) without affecting the source.

**Why Airflow for orchestration?**
Dependency management between Silver and Gold layers, quality-gate branching, retry policies, and observability through the UI. Simpler tools (cron) cannot express the conditional branching on DQ results.

**Why MinIO?**
Provides an S3-compatible API locally without cloud costs, making the entire stack portable to AWS/Azure/GCP by changing a single endpoint URL.

**Why separate quarantine tables?**
Silent data loss is worse than visible data quality failures. Invalid records are preserved, not dropped, enabling retrospective analysis and root-cause investigation.

---

## Project Structure

```
real-time-financial-data-platform/
├── architecture/
│   └── data-flow.md
├── config/
│   └── settings.py
├── data_generator/
│   ├── customers.py
│   ├── accounts.py
│   ├── products.py
│   └── transactions.py
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── spark/
│   ├── utils/
│   │   ├── spark_session.py
│   │   └── delta_utils.py
│   ├── bronze/
│   │   └── ingest_transactions.py
│   ├── silver/
│   │   ├── clean_transactions.py
│   │   ├── deduplicate_transactions.py
│   │   ├── data_quality.py
│   │   └── performance_benchmark.py
│   └── gold/
│       ├── customer_analytics.py
│       ├── transaction_analytics.py
│       └── risk_metrics.py
├── airflow/
│   └── dags/
│       └── financial_pipeline.py
├── sql/
│   ├── customer_metrics.sql
│   └── transaction_metrics.sql
├── tests/
│   ├── conftest.py
│   ├── test_data_quality.py
│   └── test_transformations.py
├── docker/
│   └── docker-compose.yml
├── .github/
│   └── workflows/
│       └── ci.yml
├── requirements.txt
└── .gitignore
```

---

## Future Improvements

- [ ] Apache Iceberg as alternative table format (comparison benchmark)
- [ ] dbt models for Gold layer transformations
- [ ] Great Expectations integration for declarative data contracts
- [ ] Real-time dashboard with Apache Superset
- [ ] ML feature store for fraud detection signals
- [ ] Terraform IaC for cloud deployment (AWS EMR / Azure Databricks)
- [ ] Schema Registry (Confluent) for Avro-based Kafka messages
- [ ] Lineage tracking with OpenLineage / Marquez

---

## License

MIT License — see [LICENSE](LICENSE) for details.

