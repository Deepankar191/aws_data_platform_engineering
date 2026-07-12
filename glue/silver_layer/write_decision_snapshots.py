"""Write immutable decision snapshots — silver.decision_input_snapshot (SPEC §7).

For every decision that does not yet have a snapshot, freeze the exact inputs the
credit engine saw:

  * a single JSON object per decision at
    ``silver/decision_input_snapshot/decision_date=YYYY-MM-DD/decision_id=<uuid>/snapshot.json``
    containing the resolved ``master_customer_id`` and the **verbatim raw** AECB /
    fraud / AML / profile bronze records, each with its bronze S3 URI and a
    per-record SHA-256;
  * a ``content_sha256`` over the whole snapshot object, stored in the Delta index
    table so tampering is detectable (the Delta row is the queryable index; the S3
    object is the legal record);
  * the resulting ``snapshot_s3_uri`` written back onto ``decision_input``.

S3 Object Lock (COMPLIANCE mode, 7-year retention — SPEC §7 / §11
SNAPSHOT_RETENTION_YEARS) is enforced at the bucket/prefix level by IaC (see
``infra/``) so retention policy is centrally governed and cannot be weakened by
this job. The per-object hook is noted in ``_write_partition`` below.

Job args:  --JOB_NAME --env --batch_id  [--decision_date YYYY-MM-DD]
"""

import sys
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import constants as C
from common.delta_io import upsert_delta
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("write_decision_snapshots")
MERGE_KEY = ["decision_id"]


def _latest_raw(df, key_col, order_col, source_alias):
    """Verbatim raw bronze record per native key (the one silver used = latest).

    Returns: <key_col>, <alias>_bronze_s3_uri, <alias>_record, <alias>_record_sha256.
    """
    raw = df.withColumn("_bronze_s3_uri", F.input_file_name())
    payload_cols = [c for c in df.columns]
    raw = raw.withColumn("_record_json", F.to_json(F.struct(*payload_cols)))

    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc_nulls_last())
    latest = (
        raw.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    return latest.select(
        F.col(key_col),
        F.col("_bronze_s3_uri").alias(f"{source_alias}_bronze_s3_uri"),
        F.col("_record_json").alias(f"{source_alias}_record"),
        F.sha2(F.col("_record_json"), 256).alias(f"{source_alias}_record_sha256"),
    )


def _write_partition(rows):
    """Executor-side: put each snapshot.json to S3 (side-effecting foreachPartition)."""
    import boto3

    s3 = boto3.client("s3")
    for row in rows:
        parsed = urlparse(row["snapshot_s3_uri"])
        bucket, key = parsed.netloc, parsed.path.lstrip("/")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=row["snapshot_json"].encode("utf-8"),
            ContentType="application/json",
            # NOTE: Object Lock retention (COMPLIANCE, 7y) is applied by the
            # bucket's default retention configured in IaC. To pin per-object,
            # add ObjectLockMode="COMPLIANCE" and ObjectLockRetainUntilDate=<now+7y>
            # here — deliberately left to IaC for central governance.
        )


def build_snapshots(decisions, xref, aecb, fraud, aml, profile, env):
    # Resolve each decision's source native keys from the identity xref.
    keys = xref.select(
        "internal_customer_uuid",
        "master_customer_id",
        "aecb_source_key",
        "fraud_source_key",
        "aml_source_key",
    )
    d = decisions.join(keys, "internal_customer_uuid", "left")

    aecb_raw = _latest_raw(aecb, "report_ref", "report_date", "aecb")
    fraud_raw = _latest_raw(fraud, "assessment_id", "scored_at", "fraud")
    aml_raw = _latest_raw(aml, "case_id", "screened_at", "aml")
    profile_raw = _latest_raw(
        profile, "internal_customer_uuid", "__ts_ms", "profile"
    )

    d = (
        d.join(
            aecb_raw, d["aecb_source_key"] == aecb_raw["report_ref"], "left"
        ).drop("report_ref")
        .join(fraud_raw, d["fraud_source_key"] == fraud_raw["assessment_id"], "left")
        .drop("assessment_id")
        .join(aml_raw, d["aml_source_key"] == aml_raw["case_id"], "left")
        .drop("case_id")
        .join(profile_raw, "internal_customer_uuid", "left")
    )

    base = C.s3_uri(env, "silver", C.TBL_DECISION_SNAPSHOT)
    d = d.withColumn(
        "snapshot_s3_uri",
        F.concat(
            F.lit(base),
            F.lit("/decision_date="),
            F.col("decision_date"),
            F.lit("/decision_id="),
            F.col("decision_id"),
            F.lit("/snapshot.json"),
        ),
    ).withColumn("captured_timestamp", F.current_timestamp())

    # The immutable object body: resolved id + verbatim records + provenance.
    snapshot_struct = F.struct(
        F.col("decision_id"),
        F.col("master_customer_id"),
        F.col("captured_timestamp"),
        F.struct(
            F.struct(
                F.col("aecb_bronze_s3_uri").alias("bronze_s3_uri"),
                F.col("aecb_record_sha256").alias("record_sha256"),
                F.col("aecb_record").alias("record"),
            ).alias("aecb"),
            F.struct(
                F.col("fraud_bronze_s3_uri").alias("bronze_s3_uri"),
                F.col("fraud_record_sha256").alias("record_sha256"),
                F.col("fraud_record").alias("record"),
            ).alias("fraud"),
            F.struct(
                F.col("aml_bronze_s3_uri").alias("bronze_s3_uri"),
                F.col("aml_record_sha256").alias("record_sha256"),
                F.col("aml_record").alias("record"),
            ).alias("aml"),
            F.struct(
                F.col("profile_bronze_s3_uri").alias("bronze_s3_uri"),
                F.col("profile_record_sha256").alias("record_sha256"),
                F.col("profile_record").alias("record"),
            ).alias("profile"),
        ).alias("records"),
    )

    d = d.withColumn("snapshot_json", F.to_json(snapshot_struct)).withColumn(
        # content hash over the exact bytes written to S3 (SPEC §7 tamper check).
        "content_sha256",
        F.sha2(F.col("snapshot_json"), 256),
    )
    return d


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "env", "batch_id"]
    )
    optional = getResolvedOptions(sys.argv, ["decision_date"]) if "--decision_date" in sys.argv else {}
    glue_context, spark, job = glue_bootstrap("write_decision_snapshots", args)
    env = args["env"]

    di_path = C.s3_uri(env, "silver", C.TBL_DECISION_INPUT)
    decision_input = spark.read.format("delta").load(di_path)

    # Only decisions without a snapshot yet (idempotent re-runs skip existing).
    todo = decision_input.where(F.col("snapshot_s3_uri").isNull())
    if optional.get("decision_date"):
        todo = todo.where(F.col("decision_date") == F.lit(optional["decision_date"]))

    decisions = todo.select(
        "decision_id",
        "decision_date",
        "internal_customer_uuid",
    )
    if decisions.rdd.isEmpty():
        LOG.info("No decisions require a snapshot.")
        job.commit()
        return

    xref = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_IDENTITY_XREF))
    aecb = spark.read.parquet(C.s3_uri(env, "bronze", "aecb"))
    fraud = spark.read.parquet(C.s3_uri(env, "bronze", "fraud"))
    aml = spark.read.parquet(C.s3_uri(env, "bronze", "aml"))
    profile = spark.read.format("delta").load(
        C.s3_uri(env, "bronze", "customer_profile")
    )
    # Debezium envelope may nest the key + ts; expose flat helpers for _latest_raw.
    if "internal_customer_uuid" not in profile.columns:
        profile = profile.withColumn(
            "internal_customer_uuid", F.col("after.internal_customer_uuid")
        )
    if "__ts_ms" not in profile.columns:
        profile = profile.withColumn(
            "__ts_ms", F.coalesce(F.col("ts_ms"), F.lit(0)).cast("long")
        )

    snapshots = build_snapshots(
        decisions, xref, aecb, fraud, aml, profile, env
    ).cache()

    # 1) Write the immutable S3 objects.
    snapshots.select("snapshot_s3_uri", "snapshot_json").foreachPartition(
        _write_partition
    )
    LOG.info("Wrote %s snapshot objects to S3", snapshots.count())

    # 2) Upsert the queryable Delta index.
    index = snapshots.select(
        "decision_id",
        "decision_date",
        "master_customer_id",
        "snapshot_s3_uri",
        "content_sha256",
        "aecb_bronze_s3_uri",
        "aecb_record_sha256",
        "fraud_bronze_s3_uri",
        "fraud_record_sha256",
        "aml_bronze_s3_uri",
        "aml_record_sha256",
        "profile_bronze_s3_uri",
        "profile_record_sha256",
        "captured_timestamp",
        F.lit(C.SNAPSHOT_RETENTION_YEARS).alias("retention_years"),
        F.lit("SNAPSHOT").alias("source_system"),
        F.lit(args["batch_id"]).alias("batch_id"),
        F.current_timestamp().alias("created_timestamp"),
        F.current_timestamp().alias("updated_timestamp"),
    )
    upsert_delta(
        spark,
        index,
        C.s3_uri(env, "silver", C.TBL_DECISION_SNAPSHOT),
        MERGE_KEY,
        C.DB_SILVER,
        C.TBL_DECISION_SNAPSHOT,
        partition_cols=["decision_date"],
    )

    # 3) Write snapshot_s3_uri back onto decision_input (targeted update only).
    di = DeltaTable.forPath(spark, di_path)
    (
        di.alias("t")
        .merge(
            snapshots.select("decision_id", "snapshot_s3_uri").alias("s"),
            "t.decision_id = s.decision_id",
        )
        .whenMatchedUpdate(
            set={
                "snapshot_s3_uri": "s.snapshot_s3_uri",
                "updated_timestamp": "current_timestamp()",
            }
        )
        .execute()
    )
    LOG.info("Back-filled snapshot_s3_uri on decision_input")
    job.commit()


if __name__ == "__main__":
    main()
