"""Customer-profile CDC bronze -> silver.credit_silver.customer_profile
(SPEC §2 source 4, §3, §5).

The bronze here is the **Debezium CDC** stream (PostgreSQL -> Kafka -> Kafka
Connect S3 sink, written as **Delta**). This job collapses the change stream to
the latest state per ``internal_customer_uuid`` and MERGE-upserts silver,
honouring tombstones (Debezium ``op='d'``) so deletes propagate.

Incremental strategy
--------------------
The CDC source is Delta, so Glue bookmarks (which track S3 file offsets for the
batch parquet sources) do not apply. Instead we read the bronze Delta table and
collapse to latest-per-key; the downstream MERGE is idempotent so re-processing
overlapping windows converges. For large volumes this read can be switched to
Delta **Change Data Feed** (``readChangeFeed`` between two versions) with a
watermark stored in a small control table — noted inline below.

Job args:  --JOB_NAME --env --batch_id  [--cdc_since_ts_ms]
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from common import constants as C
from common.audit import apply_pii_comments, with_audit_block
from common.delta_io import latest_per_key, upsert_delta
from common.identity import lower_email, normalise_emirates_id, normalise_phone_e164
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("customer_profile_to_silver")
MERGE_KEY = ["internal_customer_uuid"]


def _unwrap_debezium(df):
    """Flatten a Debezium envelope (op/ts_ms/after) to a typed profile row.

    ``op`` ∈ {c (create), u (update), r (snapshot read), d (delete)}. For deletes
    the ``after`` payload is null, so the key is taken from ``before``.
    """
    has_envelope = "after" in df.columns and "op" in df.columns
    if not has_envelope:
        # Kafka Connect may already unwrap via ExtractNewRecordState SMT, leaving
        # columns flat plus __op / __deleted metadata.
        op = F.coalesce(F.col("__op"), F.lit("u"))
        ts = F.coalesce(F.col("__source_ts_ms"), F.col("ts_ms"), F.lit(0)).cast("long")
        return df.withColumn("__op", op).withColumn("__ts_ms", ts)

    after = F.col("after")
    before = F.col("before")
    return df.select(
        F.col("op").alias("__op"),
        F.col("ts_ms").cast("long").alias("__ts_ms"),
        F.coalesce(after.getField("internal_customer_uuid"),
                   before.getField("internal_customer_uuid")).alias("internal_customer_uuid"),
        after.getField("emirates_id").alias("emirates_id"),
        after.getField("phone").alias("phone"),
        after.getField("email").alias("email"),
        after.getField("full_name").alias("full_name"),
        after.getField("date_of_birth").alias("date_of_birth"),
        after.getField("monthly_income_aed").alias("monthly_income_aed"),
        after.getField("kyc_completed").alias("kyc_completed"),
        after.getField("updated_at").alias("profile_updated_at"),
    )


def transform(df):
    flat = _unwrap_debezium(df).where(F.col("internal_customer_uuid").isNotNull())
    # Latest change per customer wins (SPEC §7 "latest-per-key via Delta MERGE").
    latest = latest_per_key(flat, MERGE_KEY, "__ts_ms")

    typed = latest.select(
        F.col("internal_customer_uuid").cast("string").alias("internal_customer_uuid"),
        normalise_emirates_id(F.col("emirates_id")).alias("emirates_id"),
        normalise_phone_e164(F.col("phone")).alias("phone"),
        lower_email(F.col("email")).alias("email"),
        F.trim(F.col("full_name")).alias("full_name"),
        F.to_date(F.col("date_of_birth")).alias("date_of_birth"),
        F.col("monthly_income_aed").cast("decimal(18,2)").alias("monthly_income_aed"),
        F.col("kyc_completed").cast("boolean").alias("kyc_completed"),
        F.to_timestamp(F.col("profile_updated_at")).alias("profile_updated_timestamp"),
        # Tombstone marker consumed by the MERGE delete predicate.
        (F.col("__op") == F.lit("d")).alias("_is_delete"),
    )
    return typed


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "batch_id"])
    glue_context, spark, job = glue_bootstrap("customer_profile_to_silver", args)

    bronze_path = C.s3_uri(args["env"], "bronze", "customer_profile")
    silver_path = C.s3_uri(args["env"], "silver", C.TBL_CUSTOMER_PROFILE)

    LOG.info("Reading customer_profile CDC bronze (Delta) from %s", bronze_path)
    src = spark.read.format("delta").load(bronze_path)

    if src.rdd.isEmpty():
        LOG.info("No CDC rows to process this run.")
        job.commit()
        return

    silver = with_audit_block(transform(src), C.SRC_POSTGRES, args["batch_id"])

    # Split the change stream: tombstones (op='d') delete the silver row, all
    # other ops upsert. `_is_delete` is an internal marker and is dropped before
    # the row is persisted, so it never enters the silver schema.
    deletes = silver.where(F.col("_is_delete")).select(MERGE_KEY[0])
    upserts = silver.where(~F.col("_is_delete")).drop("_is_delete")

    upsert_delta(
        spark, upserts, silver_path, MERGE_KEY, C.DB_SILVER, C.TBL_CUSTOMER_PROFILE
    )

    # Apply tombstone deletes explicitly against the Delta target.
    if not deletes.rdd.isEmpty():
        from delta.tables import DeltaTable

        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("t")
            .merge(deletes.alias("s"), "t.internal_customer_uuid = s.internal_customer_uuid")
            .whenMatchedDelete()
            .execute()
        )
        LOG.info("Applied %s CDC tombstone deletes", deletes.count())

    apply_pii_comments(
        spark,
        C.DB_SILVER,
        C.TBL_CUSTOMER_PROFILE,
        {"emirates_id": 1, "phone": 2, "email": 2, "full_name": 2, "date_of_birth": 2},
    )

    LOG.info("customer_profile upsert complete rows=%s", upserts.count())
    job.commit()


if __name__ == "__main__":
    main()
