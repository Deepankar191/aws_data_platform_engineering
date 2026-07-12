"""Delta Lake IO helpers — idempotent MERGE-upsert + Glue catalog registration.

Every silver/gold writer in this repo goes through :func:`upsert_delta` so that
re-running a job (job retries, backfills, bookmark replays) converges to the same
state instead of duplicating rows (SPEC §3, "idempotent — Delta MERGE for
upserts").
"""

from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_session import get_logger

LOG = get_logger("delta_io")


def register_glue_table(
    spark: SparkSession, database: str, table: str, s3_path: str
) -> None:
    """Register (or refresh) a Delta table in the Glue Data Catalog so Athena sees it.

    Idempotent: ``CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION`` binds the
    catalog entry to the Delta location. Delta keeps the schema in its own
    transaction log, so the catalog entry needs no column list.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {database}.{table} "
        f"USING DELTA LOCATION '{s3_path}'"
    )
    LOG.info("Registered Glue table %s.%s -> %s", database, table, s3_path)


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    s3_path: str,
    merge_keys: List[str],
    database: str,
    table: str,
    partition_cols: Optional[List[str]] = None,
    delete_when: Optional[str] = None,
) -> None:
    """MERGE-upsert ``df`` into the Delta table at ``s3_path`` and register it.

    Parameters
    ----------
    merge_keys:
        Columns forming the match condition (the table's business key).
    partition_cols:
        Physical partitioning applied on first create only.
    delete_when:
        Optional SQL predicate (over the *source* alias ``s``) selecting rows to
        delete on match — used by the CDC profile job to honour Debezium
        tombstones (op='d'). Deletes are applied before update/insert.
    """
    if not merge_keys:
        raise ValueError("merge_keys must be non-empty for an idempotent upsert")

    if DeltaTable.isDeltaTable(spark, s3_path):
        target = DeltaTable.forPath(spark, s3_path)
        cond = " AND ".join(f"t.{k} <=> s.{k}" for k in merge_keys)
        merger = target.alias("t").merge(df.alias("s"), cond)
        if delete_when:
            merger = merger.whenMatchedDelete(condition=delete_when)
        merger.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        LOG.info("MERGE upsert into %s.%s (keys=%s)", database, table, merge_keys)
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(s3_path)
        LOG.info("Created Delta table %s.%s at %s", database, table, s3_path)

    register_glue_table(spark, database, table, s3_path)


def append_delta(
    spark: SparkSession,
    df: DataFrame,
    s3_path: str,
    database: str,
    table: str,
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Append-only write (immutable fact tables, e.g. quarantine ledger)."""
    writer = df.write.format("delta").mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(s3_path)
    register_glue_table(spark, database, table, s3_path)
    LOG.info("Appended %s rows to %s.%s", df.count(), database, table)


def latest_per_key(df: DataFrame, keys: List[str], order_col: str) -> DataFrame:
    """Keep the newest row per ``keys`` ordered by ``order_col`` desc.

    Used for CDC latest-state collapse and source deduplication.
    """
    from pyspark.sql.window import Window

    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
