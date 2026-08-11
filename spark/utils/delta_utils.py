"""Delta Lake utility helpers shared across Bronze, Silver, and Gold jobs."""
import logging

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def table_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a Delta table exists at the given path."""
    return DeltaTable.isDeltaTable(spark, path)


def get_delta_table(spark: SparkSession, path: str) -> DeltaTable:
    return DeltaTable.forPath(spark, path)


def optimize_table(spark: SparkSession, path: str, zorder_cols: list[str] | None = None) -> None:
    """Run OPTIMIZE and optionally ZORDER on a Delta table."""
    dt = DeltaTable.forPath(spark, path)
    if zorder_cols:
        cols_str = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols_str})")
        logger.info("OPTIMIZE + ZORDER BY (%s) on %s", cols_str, path)
    else:
        dt.optimize().executeCompaction()
        logger.info("OPTIMIZE (compaction) on %s", path)


def vacuum_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """Remove old files beyond retention window (default: 7 days)."""
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info("VACUUM (retain %dh) on %s", retention_hours, path)


def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_condition: str,
    update_set: dict,
    insert_values: dict | None = None,
) -> dict:
    """Generic MERGE INTO for idempotent upserts. Returns operation metrics."""
    if not table_exists(spark, target_path):
        source_df.write.format("delta").save(target_path)
        logger.info("Created new Delta table at %s", target_path)
        return {"new_table": True}

    target = DeltaTable.forPath(spark, target_path)
    merge_builder = (
        target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
    )
    if insert_values:
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_values)
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()

    history = target.history(1).select("operationMetrics").collect()
    metrics = history[0]["operationMetrics"] if history else {}
    logger.info("MERGE completed: %s", metrics)
    return metrics


def get_table_row_count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def get_latest_partition(spark: SparkSession, path: str, partition_col: str) -> str | None:
    """Return the most recent partition value for incremental processing."""
    df = spark.read.format("delta").load(path)
    row = df.selectExpr(f"max({partition_col}) as latest").collect()
    return row[0]["latest"] if row else None
