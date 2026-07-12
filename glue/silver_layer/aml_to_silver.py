"""AML bronze -> silver.credit_silver.aml_screening  (SPEC §2 source 3, §5).

Reads AML/PEP screening bronze (webhook callback -> JSON -> Parquet, partitioned
by ``ingest_date``) with Glue job bookmarks, cleans/types, precomputes the
``name_soundex`` blocking key used by the probabilistic identity scorer (§6.4),
dedupes to the latest screening per case, tags PII, adds audit block, upserts.

Job args:  --JOB_NAME --env --batch_id
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common import constants as C
from common.audit import apply_pii_comments, with_audit_block
from common.delta_io import latest_per_key, upsert_delta
from common.identity import name_soundex
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("aml_to_silver")
MERGE_KEY = ["aml_case_id"]


def transform(df):
    # Bronze AML schema (SPEC §2 source #3, athena/ddl/bronze_layer/aml_raw.sql): the provider posts
    # screening_ref + full_name + date_of_birth + aml_status + is_pep + screened_at via
    # webhook. It matches on full_name + date_of_birth (§6.4), so it carries no application_id.
    typed = df.select(
        F.col("screening_ref").cast("string").alias("aml_case_id"),
        F.trim(F.col("full_name")).alias("full_name"),
        F.to_date(F.col("date_of_birth")).alias("date_of_birth"),
        # SPEC §6.4 blocking key for the fuzzy AML match.
        name_soundex(F.col("full_name")).alias("name_soundex"),
        F.upper(F.trim(F.col("aml_status"))).alias("aml_status"),
        # Boolean flag per SPEC §10 (never 0/1 INT). Bronze already emits a JSON boolean;
        # cast tolerates a string "true"/"false" too.
        F.col("is_pep").cast("boolean").alias("is_pep"),
        F.to_timestamp(F.col("screened_at")).alias("screening_timestamp"),
    ).where(F.col("aml_case_id").isNotNull())
    return latest_per_key(typed, MERGE_KEY, "screening_timestamp")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "batch_id"])
    glue_context, spark, job = glue_bootstrap("aml_to_silver", args)

    bronze_path = C.s3_uri(args["env"], "bronze", "aml")
    silver_path = C.s3_uri(args["env"], "silver", C.TBL_AML)

    LOG.info("Reading AML bronze (bookmarked) from %s", bronze_path)
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [bronze_path], "recurse": True},
        format="parquet",
        transformation_ctx="aml_bronze_read",
    )
    src = dyf.toDF()

    if src.rdd.isEmpty():
        LOG.info("No new AML bronze rows this run; committing empty bookmark.")
        job.commit()
        return

    silver = with_audit_block(transform(src), C.SRC_AML, args["batch_id"])

    upsert_delta(spark, silver, silver_path, MERGE_KEY, C.DB_SILVER, C.TBL_AML)
    apply_pii_comments(
        spark,
        C.DB_SILVER,
        C.TBL_AML,
        {"full_name": 2, "date_of_birth": 2},  # PII Level 2
    )

    LOG.info("aml_screening upsert complete rows=%s", silver.count())
    job.commit()


if __name__ == "__main__":
    main()
