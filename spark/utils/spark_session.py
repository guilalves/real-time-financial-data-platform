"""Reusable SparkSession factory with Delta Lake and S3/MinIO configuration."""
from pyspark.sql import SparkSession

from config.settings import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    SPARK_APP_NAME,
    SPARK_MASTER,
)


def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """Build and return a configured SparkSession.

    Delta Lake and S3A (MinIO) are configured here so individual jobs
    don't need to repeat boilerplate configuration.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        # Delta Lake extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A / MinIO connectivity
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
