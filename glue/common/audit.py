"""Audit-block + PII-comment helpers (SPEC §10).

Silver carries ``source_system, batch_id, created_timestamp, updated_timestamp``.
On MERGE we must *preserve* the original ``created_timestamp`` and only bump
``updated_timestamp`` — the helper here writes both to now() on insert, and the
Delta ``whenMatchedUpdateAll`` in :func:`common.delta_io.upsert_delta` overwrites
``updated_timestamp`` on update while callers keep ``created_timestamp`` stable by
coalescing (see the silver jobs).

PII column comments (``PII Level 1|2|3``) are applied post-write via ALTER TABLE
so the tags land in the Glue catalog / Athena, per SPEC §10.
"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from common.spark_session import get_logger

LOG = get_logger("audit")


def with_audit_block(df: DataFrame, source_system: str, batch_id: str) -> DataFrame:
    """Attach the SPEC §10 silver audit block. Timestamps are GST (session TZ)."""
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )


def apply_pii_comments(
    spark: SparkSession, database: str, table: str, pii_levels: Dict[str, int]
) -> None:
    """Tag columns with ``PII Level N`` comments in the catalog (SPEC §10).

    ``pii_levels`` maps column name -> level (1 EID/passport, 2 phone/email/dob/
    address, 3 derived).
    """
    for column, level in pii_levels.items():
        spark.sql(
            f"ALTER TABLE {database}.{table} ALTER COLUMN {column} "
            f"COMMENT 'PII Level {level}'"
        )
    if pii_levels:
        LOG.info("Applied PII comments on %s.%s: %s", database, table, pii_levels)
