"""Build gold.credit_gold.portfolio_monitoring_daily (SPEC §9).

Aggregates the DQ-passing ``decision_input`` rows to the risk team's daily
portfolio mart at grain:

    one row per snapshot_date × product_code × decision_outcome_band × risk_band

Metrics: decision volume, approval rate, average fraud score, AML-hit rate, PEP
exposure, average AECB score, average requested/approved amount (AED), and the
day's dq_score (joined from ``dq_scorecard_daily``). Money is DECIMAL(18,2); Gold
exposes natural keys only (SPEC §10).

Job args:  --JOB_NAME --env  [--run_date YYYY-MM-DD]
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common import constants as C
from common.delta_io import upsert_delta
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("build_portfolio_monitoring_daily")
MERGE_KEY = ["snapshot_date", "product_code", "decision_outcome_band", "risk_band"]


def risk_band(score_col):
    """FICO-style banding of the AECB credit score (300..900). NULL -> UNKNOWN."""
    return (
        F.when(score_col.isNull(), F.lit("UNKNOWN"))
        .when(score_col < 580, F.lit("POOR"))
        .when(score_col < 670, F.lit("FAIR"))
        .when(score_col < 740, F.lit("GOOD"))
        .when(score_col < 800, F.lit("VERY_GOOD"))
        .otherwise(F.lit("EXCELLENT"))
    )


def build_mart(decision_input, dq_scorecard):
    di = decision_input.where(F.col("dq_pass") == F.lit(True))

    di = (
        di.withColumn("snapshot_date", F.col("decision_date"))
        .withColumn(
            "decision_outcome_band",
            F.coalesce(F.upper(F.col("decision_outcome")), F.lit("UNKNOWN")),
        )
        .withColumn("risk_band", risk_band(F.col("aecb_credit_score")))
    )

    agg = di.groupBy(
        "snapshot_date", "product_code", "decision_outcome_band", "risk_band"
    ).agg(
        F.count(F.lit(1)).cast("int").alias("decision_count"),
        F.sum(
            F.when(F.col("decision_outcome") == F.lit("APPROVED"), 1).otherwise(0)
        ).cast("int").alias("approved_count"),
        F.avg("fraud_score").cast("decimal(5,4)").alias("avg_fraud_score"),
        F.sum(F.when(F.col("aml_status") == F.lit("HIT"), 1).otherwise(0))
        .cast("int")
        .alias("aml_hit_count"),
        F.sum(F.when(F.col("is_pep") == F.lit(True), 1).otherwise(0))
        .cast("int")
        .alias("pep_count"),
        F.avg("aecb_credit_score").cast("decimal(5,2)").alias("avg_aecb_credit_score"),
        F.avg("requested_amount_aed")
        .cast("decimal(18,2)")
        .alias("avg_requested_amount_aed"),
        F.avg("approved_amount_aed")
        .cast("decimal(18,2)")
        .alias("avg_approved_amount_aed"),
        F.sum("requested_amount_aed")
        .cast("decimal(18,2)")
        .alias("total_requested_amount_aed"),
        F.sum("approved_amount_aed")
        .cast("decimal(18,2)")
        .alias("total_approved_amount_aed"),
    )

    # Derived rates (guard divide-by-zero; DECIMAL(5,2) percentages per SPEC §10).
    agg = (
        agg.withColumn(
            "approval_rate_pct",
            (F.col("approved_count") / F.col("decision_count") * 100).cast(
                "decimal(5,2)"
            ),
        )
        .withColumn(
            "aml_hit_rate_pct",
            (F.col("aml_hit_count") / F.col("decision_count") * 100).cast(
                "decimal(5,2)"
            ),
        )
        .withColumn(
            "pep_exposure_pct",
            (F.col("pep_count") / F.col("decision_count") * 100).cast("decimal(5,2)"),
        )
    )

    # Attach the day's overall dq_score from the scorecard mart.
    dq = dq_scorecard.select(
        F.col("scorecard_date").alias("snapshot_date"),
        F.col("dq_score").cast("decimal(5,2)").alias("dq_score"),
    )
    agg = agg.join(dq, "snapshot_date", "left")

    return agg.withColumn("created_timestamp", F.current_timestamp())


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env"])
    optional = (
        getResolvedOptions(sys.argv, ["run_date"]) if "--run_date" in sys.argv else {}
    )
    glue_context, spark, job = glue_bootstrap("build_portfolio_monitoring_daily", args)
    env = args["env"]

    decision_input = spark.read.format("delta").load(
        C.s3_uri(env, "silver", C.TBL_DECISION_INPUT)
    )
    if optional.get("run_date"):
        decision_input = decision_input.where(
            F.col("decision_date") == F.lit(optional["run_date"])
        )

    # dq_scorecard_daily may not exist on a first-ever run; degrade gracefully.
    dq_path = C.s3_uri(env, "gold", C.TBL_DQ_SCORECARD_DAILY)
    try:
        dq_scorecard = spark.read.format("delta").load(dq_path)
    except Exception:  # noqa: BLE001 - table not created yet
        LOG.warning("dq_scorecard_daily not found at %s; dq_score will be NULL", dq_path)
        dq_scorecard = spark.createDataFrame(
            [], "scorecard_date date, dq_score double"
        )

    mart = build_mart(decision_input, dq_scorecard)

    upsert_delta(
        spark,
        mart,
        C.s3_uri(env, "gold", C.TBL_PORTFOLIO_DAILY),
        MERGE_KEY,
        C.DB_GOLD,
        C.TBL_PORTFOLIO_DAILY,
        partition_cols=["snapshot_date"],
    )
    LOG.info("portfolio_monitoring_daily upsert complete rows=%s", mart.count())
    job.commit()


if __name__ == "__main__":
    main()
