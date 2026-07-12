"""AECB bronze -> silver.credit_silver.aecb_credit_report  (SPEC §2 source 1, §5).

Reads the AECB credit-bureau bronze (XML parsed upstream to Parquet, partitioned
by ``ingest_date``) with **Glue job bookmarks** for incremental pickup, cleans /
types / dedupes to one row per credit report, tags PII, adds the audit block, and
MERGE-upserts the Delta silver table. Native identity key: ``emirates_id`` (§6.2).

Job args:  --JOB_NAME --env --batch_id
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common import constants as C
from common.audit import apply_pii_comments, with_audit_block
from common.delta_io import latest_per_key, upsert_delta
from common.identity import normalise_emirates_id
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("aecb_to_silver")
MERGE_KEY = ["aecb_report_ref"]


def transform(df):
    """Clean/type AECB bronze into the silver contract, dedupe to latest report."""
    typed = (
        df.select(
            normalise_emirates_id(F.col("emirates_id")).alias("emirates_id"),
            F.col("report_ref").cast("string").alias("aecb_report_ref"),
            F.col("credit_score").cast("int").alias("aecb_credit_score"),
            F.col("total_outstanding_aed")
            .cast("decimal(18,2)")
            .alias("aecb_total_outstanding_aed"),
            F.to_timestamp(F.col("report_date")).alias("report_timestamp"),
        )
        # Drop rows with no report ref (cannot key / snapshot them).
        .where(F.col("aecb_report_ref").isNotNull())
    )
    # SPEC §8 sanity kept as data, not enforced here: score range is DQ-checked
    # downstream on decision_input. Dedupe to the newest report per ref.
    return latest_per_key(typed, MERGE_KEY, "report_timestamp")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "batch_id"])
    glue_context, spark, job = glue_bootstrap("aecb_to_silver", args)

    bronze_path = C.s3_uri(args["env"], "bronze", "aecb")
    silver_path = C.s3_uri(args["env"], "silver", C.TBL_AECB)

    LOG.info("Reading AECB bronze (bookmarked) from %s", bronze_path)
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [bronze_path], "recurse": True},
        format="parquet",
        transformation_ctx="aecb_bronze_read",  # <- bookmark context
    )
    src = dyf.toDF()

    if src.rdd.isEmpty():
        LOG.info("No new AECB bronze rows this run; committing empty bookmark.")
        job.commit()
        return

    silver = with_audit_block(transform(src), C.SRC_AECB, args["batch_id"])

    upsert_delta(
        spark, silver, silver_path, MERGE_KEY, C.DB_SILVER, C.TBL_AECB
    )
    apply_pii_comments(
        spark, C.DB_SILVER, C.TBL_AECB, {"emirates_id": 1}  # EID -> PII Level 1
    )

    LOG.info("aecb_credit_report upsert complete rows=%s", silver.count())
    job.commit()


if __name__ == "__main__":
    main()
