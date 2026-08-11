"""
Central configuration for the platform.
All environment-specific values are read from environment variables
with sensible local-development defaults.
"""
import os

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATALAKE_BUCKET = os.getenv("DATALAKE_BUCKET", "datalake")

BRONZE_PATH = f"s3a://{DATALAKE_BUCKET}/bronze/transactions"
SILVER_TRANSACTIONS_PATH = f"s3a://{DATALAKE_BUCKET}/silver/transactions"
SILVER_CUSTOMERS_PATH = f"s3a://{DATALAKE_BUCKET}/silver/customers"
SILVER_ACCOUNTS_PATH = f"s3a://{DATALAKE_BUCKET}/silver/accounts"
SILVER_QUARANTINE_PATH = f"s3a://{DATALAKE_BUCKET}/silver/transactions_quarantine"
SILVER_DQ_METRICS_PATH = f"s3a://{DATALAKE_BUCKET}/silver/dq_metrics"
GOLD_CUSTOMER_PATH = f"s3a://{DATALAKE_BUCKET}/gold/customer_analytics"
GOLD_TRANSACTION_PATH = f"s3a://{DATALAKE_BUCKET}/gold/transaction_analytics"
GOLD_RISK_PATH = f"s3a://{DATALAKE_BUCKET}/gold/risk_metrics"
CHECKPOINT_PATH = f"s3a://{DATALAKE_BUCKET}/_checkpoints/bronze_ingest"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRANSACTIONS = "transaction-events"
KAFKA_CONSUMER_GROUP = "spark-bronze-consumer"
KAFKA_NUM_PARTITIONS = 12

# ---------------------------------------------------------------------------
# Data Quality
# ---------------------------------------------------------------------------
DQ_PASS_RATE_THRESHOLD = float(os.getenv("DQ_PASS_RATE_THRESHOLD", "0.99"))
WATERMARK_DURATION = "30 minutes"
LATE_ARRIVAL_WINDOW = "1 hour"

# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
SEED = 42
DEFAULT_CUSTOMER_COUNT = 10_000
DEFAULT_ACCOUNT_COUNT = 15_000
DEFAULT_PRODUCT_COUNT = 50
DEFAULT_TRANSACTION_COUNT = 500_000

DUPLICATE_RATE = 0.02      # 2% duplicates injected by generator
LATE_ARRIVAL_RATE = 0.05   # 5% late events (up to 45 min late)
NULL_INJECTION_RATE = 0.01  # 1% records with injected nulls for DQ testing

CURRENCIES = ["USD", "EUR", "BRL", "GBP", "JPY", "CAD", "AUD"]
TRANSACTION_TYPES = ["purchase", "transfer", "withdrawal", "deposit", "refund"]
ACCOUNT_TYPES = ["checking", "savings", "investment", "credit"]
CUSTOMER_SEGMENTS = ["retail", "premium", "private", "sme", "corporate"]
MERCHANT_CATEGORIES = [
    "grocery", "restaurant", "travel", "entertainment",
    "healthcare", "utilities", "online_retail", "fuel",
]

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_APP_NAME = "RealTimeFinancialPlatform"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
