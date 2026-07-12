"""Fraud bronze -> silver.credit_silver.fraud_score  (SPEC §2 source 2, §5).

Reads the fraud-provider bronze (REST poll -> JSON -> Parquet, partitioned by
``ingest_date``) with Glue job bookmarks, cleans/types, normalises the identity
keys (phone E.164 + lowercased email, §6.3), dedupes to the latest score per
application, tags PII, adds audit block, MERGE-upserts silver.

Job args:  --JOB_NAME --env --batch_id
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common import constants as C
from common.audit import apply_pii_comments, with_audit_block
from common.delta_io import latest_per_key, upsert_delta
from common.identity import lower_email, normalise_phone_e164
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("fraud_to_silver")
# One fraud assessment per application scoring event.
MERGE_KEY = ["fraud_assessment_id"]


def transform(df):
    # Bronze fraud schema (SPEC §2 source #2, athena/ddl/bronze_layer/fraud_raw.sql): the provider
    # emits event_id + phone + email + fraud_score + fraud_decision + scored_at. It
    # matches on phone+email (§6), so it carries no application_id.
    typed = df.select(
        F.col("event_id").cast("string").alias("fraud_assessment_id"),
        normalise_phone_e164(F.col("phone")).alias("phone"),
        lower_email(F.col("email")).alias("email"),
        F.col("fraud_score").cast("decimal(5,4)").alias("fraud_score"),
        F.upper(F.trim(F.col("fraud_decision"))).alias("fraud_decision"),
        F.to_timestamp(F.col("scored_at")).alias("scored_timestamp"),
    ).where(F.col("fraud_assessment_id").isNotNull())
    return latest_per_key(typed, MERGE_KEY, "scored_timestamp")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "batch_id"])
    glue_context, spark, job = glue_bootstrap("fraud_to_silver", args)

    bronze_path = C.s3_uri(args["env"], "bronze", "fraud")
    silver_path = C.s3_uri(args["env"], "silver", C.TBL_FRAUD)

    LOG.info("Reading fraud bronze (bookmarked) from %s", bronze_path)
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [bronze_path], "recurse": True},
        format="parquet",
        transformation_ctx="fraud_bronze_read",
    )
    src = dyf.toDF()

    if src.rdd.isEmpty():
        LOG.info("No new fraud bronze rows this run; committing empty bookmark.")
        job.commit()
        return

    silver = with_audit_block(transform(src), C.SRC_FRAUD, args["batch_id"])

    upsert_delta(spark, silver, silver_path, MERGE_KEY, C.DB_SILVER, C.TBL_FRAUD)
    apply_pii_comments(
        spark, C.DB_SILVER, C.TBL_FRAUD, {"phone": 2, "email": 2}  # PII Level 2
    )

    LOG.info("fraud_score upsert complete rows=%s", silver.count())
    job.commit()


if __name__ == "__main__":
    main()
