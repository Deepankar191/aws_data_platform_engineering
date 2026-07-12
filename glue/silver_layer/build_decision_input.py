"""Assemble silver.credit_silver.decision_input — the unified decision record (SPEC §5).

For a batch of credit decisions (scoring events), resolve identity via
``customer_identity_xref`` and fuse the four silver sources into one row per
``decision_id``. The identity xref has already decided *which* AECB / fraud / AML
records belong to each master (their native keys are carried on the xref row), so
this job joins on those native keys rather than re-doing matching.

``dq_pass`` and ``snapshot_s3_uri`` are intentionally left NULL here — they are
set by the DQ scorecard job (§8) and the snapshot job (§7) respectively.

The driving "decisions" feed (scoring events: decision_id, application_id,
internal_customer_uuid, product_code, decision_timestamp, requested/approved
amounts, decision_outcome) lands in ``bronze/decisions/`` and is read with Glue
job bookmarks for incremental pickup.

Job args:  --JOB_NAME --env --batch_id
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common import constants as C
from common.audit import with_audit_block
from common.delta_io import upsert_delta
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("build_decision_input")
MERGE_KEY = ["decision_id"]

# The four "expected inputs" whose presence defines input_completeness_score.
EXPECTED_INPUTS = [
    "aecb_credit_score",   # AECB
    "fraud_score",         # Fraud
    "aml_status",          # AML
    "monthly_income_aed",  # Internal profile
]


def assemble(decisions, xref, aecb, fraud, aml, profile):
    # Only golden (resolved) xref rows participate in the join; decisions whose
    # customer is unresolved still produce a row (master = UNRESOLVED sentinel).
    xref_resolved = xref.where(
        F.col("master_customer_id") != F.lit(C.UNRESOLVED_SENTINEL)
    ).select(
        F.col("internal_customer_uuid"),
        F.col("master_customer_id"),
        F.col("aecb_source_key"),
        F.col("fraud_source_key"),
        F.col("aml_source_key"),
    )

    df = decisions.join(xref_resolved, "internal_customer_uuid", "left")

    # AECB inputs via the xref-selected report ref.
    df = df.join(
        aecb.select(
            F.col("aecb_report_ref").alias("aecb_source_key"),
            "aecb_credit_score",
            "aecb_total_outstanding_aed",
            "aecb_report_ref",
        ),
        "aecb_source_key",
        "left",
    )
    # Fraud inputs via the xref-selected assessment id.
    df = df.join(
        fraud.select(
            F.col("fraud_assessment_id").alias("fraud_source_key"),
            "fraud_score",
            "fraud_decision",
        ),
        "fraud_source_key",
        "left",
    )
    # AML inputs via the xref-selected case id.
    df = df.join(
        aml.select(
            F.col("aml_case_id").alias("aml_source_key"),
            "aml_status",
            "is_pep",
        ),
        "aml_source_key",
        "left",
    )
    # Internal profile inputs join directly on the spine id.
    df = df.join(
        profile.select(
            "internal_customer_uuid", "monthly_income_aed", "kyc_completed"
        ),
        "internal_customer_uuid",
        "left",
    )

    # SPEC §6: nothing dropped — unresolved decisions keep the sentinel master id.
    df = df.withColumn(
        "master_customer_id",
        F.coalesce(F.col("master_customer_id"), F.lit(C.UNRESOLVED_SENTINEL)),
    )

    # input_completeness_score = fraction of the four expected inputs present.
    present = sum(
        F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for c in EXPECTED_INPUTS
    )
    df = df.withColumn(
        "input_completeness_score",
        (present / F.lit(float(len(EXPECTED_INPUTS)))).cast("decimal(5,4)"),
    )

    return df.select(
        F.col("decision_id"),
        F.col("application_id"),
        F.col("master_customer_id"),
        F.col("internal_customer_uuid"),
        F.col("product_code"),
        F.col("decision_timestamp").cast("timestamp").alias("decision_timestamp"),
        F.to_date(F.col("decision_timestamp")).alias("decision_date"),
        # AECB
        F.col("aecb_credit_score").cast("int").alias("aecb_credit_score"),
        F.col("aecb_total_outstanding_aed")
        .cast("decimal(18,2)")
        .alias("aecb_total_outstanding_aed"),
        F.col("aecb_report_ref"),
        # Fraud
        F.col("fraud_score").cast("decimal(5,4)").alias("fraud_score"),
        F.col("fraud_decision"),
        # AML
        F.col("aml_status"),
        F.col("is_pep").cast("boolean").alias("is_pep"),
        # Internal profile
        F.col("monthly_income_aed").cast("decimal(18,2)").alias("monthly_income_aed"),
        F.col("kyc_completed").cast("boolean").alias("kyc_completed"),
        # Decision economics (carried for the §9 portfolio mart)
        F.col("requested_amount_aed").cast("decimal(18,2)").alias("requested_amount_aed"),
        F.col("approved_amount_aed").cast("decimal(18,2)").alias("approved_amount_aed"),
        F.upper(F.trim(F.col("decision_outcome"))).alias("decision_outcome"),
        # Traceability
        F.col("input_completeness_score"),
        F.lit(None).cast("boolean").alias("dq_pass"),          # set by DQ job (§8)
        F.lit(None).cast("string").alias("snapshot_s3_uri"),   # set by snapshot job (§7)
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "batch_id"])
    glue_context, spark, job = glue_bootstrap("build_decision_input", args)
    env = args["env"]

    LOG.info("Reading decisions bronze (bookmarked)")
    decisions = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [C.s3_uri(env, "bronze", "decisions")],
            "recurse": True,
        },
        format="parquet",
        transformation_ctx="decisions_bronze_read",
    ).toDF()

    if decisions.rdd.isEmpty():
        LOG.info("No new decisions this run.")
        job.commit()
        return

    xref = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_IDENTITY_XREF))
    aecb = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_AECB))
    fraud = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_FRAUD))
    aml = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_AML))
    profile = spark.read.format("delta").load(
        C.s3_uri(env, "silver", C.TBL_CUSTOMER_PROFILE)
    )

    decision_input = with_audit_block(
        assemble(decisions, xref, aecb, fraud, aml, profile), "DECISION_ENGINE", args["batch_id"]
    )

    upsert_delta(
        spark,
        decision_input,
        C.s3_uri(env, "silver", C.TBL_DECISION_INPUT),
        MERGE_KEY,
        C.DB_SILVER,
        C.TBL_DECISION_INPUT,
        partition_cols=["decision_date"],
    )

    LOG.info("decision_input upsert complete rows=%s", decision_input.count())
    job.commit()


if __name__ == "__main__":
    main()
