"""Build silver.credit_silver.customer_identity_xref — the golden record (SPEC §6).

Pipeline (deterministic-first, probabilistic fallback), each step annotated with
the SPEC §6 rule it implements:

  §6.1  Spine  = PostgreSQL internal_customer_uuid seeds one master_customer_id
                 (deterministic UUIDv5, stable/reproducible).
  §6.2  AECB   attached by EXACT normalised emirates_id  -> DETERMINISTIC, 1.00.
  §6.3  Fraud  attached by EXACT phone AND email         -> DETERMINISTIC, 1.00;
               single-key hits fall through to the scorer.
  §6.4  AML    always fuzzy: soundex(name)+dob blocking, then the weighted
               Jaro-Winkler scorer.
  §6 thresholds:  >=0.85 attach; 0.70..0.85 attach + needs_manual_review;
                  <0.70 -> UNRESOLVED record (nothing dropped).
  §6 survivorship: demographics resolved POSTGRES > AECB > FRAUD > AML, newest
                   within a priority.

Grain: one row per master_customer_id (current state) + UNRESOLVED sentinel rows
for source records that could not be attached. Writes match_method,
match_confidence, matched_on, needs_manual_review + audit block.

Job args:  --JOB_NAME --env --batch_id
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import constants as C
from common.audit import apply_pii_comments
from common.delta_io import upsert_delta
from common.identity import (
    classify_match,
    is_attachable,
    master_customer_id_udf,
    match_confidence_udf,
    name_soundex,
    needs_manual_review,
    normalise_name,
)
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("build_customer_identity_xref")
MERGE_KEY = ["master_customer_id", "source_record_key"]


# --------------------------------------------------------------------------- #
# §6.1 — spine
# --------------------------------------------------------------------------- #
def build_spine(profile):
    """Each spine row seeds a deterministic master_customer_id (SPEC §6.1)."""
    return profile.select(
        master_customer_id_udf(F.col("internal_customer_uuid")).alias(
            "master_customer_id"
        ),
        F.col("internal_customer_uuid"),
        F.col("emirates_id"),
        F.col("phone"),
        F.col("email"),
        F.col("full_name"),
        normalise_name(F.col("full_name")).alias("name_norm"),
        name_soundex(F.col("full_name")).alias("name_soundex"),
        F.col("date_of_birth"),
    )


# --------------------------------------------------------------------------- #
# §6.2 — AECB deterministic on emirates_id
# --------------------------------------------------------------------------- #
def resolve_aecb(aecb, spine):
    joined = aecb.where(F.col("emirates_id").isNotNull()).join(
        spine.select("master_customer_id", "emirates_id"), on="emirates_id", how="inner"
    )
    links = joined.select(
        F.col("master_customer_id"),
        F.col("aecb_report_ref").alias("aecb_source_key"),
        F.lit("DETERMINISTIC").alias("aecb_match_method"),
        F.lit(1.0).cast("double").alias("aecb_match_confidence"),
        F.lit(False).alias("aecb_needs_review"),
    )
    # AECB rows that hit no spine master -> unresolved (nothing dropped).
    matched = joined.select("aecb_report_ref").distinct()
    unresolved = aecb.join(matched, on="aecb_report_ref", how="left_anti").select(
        F.lit(C.SRC_AECB).alias("source_system"),
        F.col("aecb_report_ref").alias("source_record_key"),
        F.col("emirates_id"),
        F.lit(None).cast("string").alias("phone"),
        F.lit(None).cast("string").alias("email"),
        F.lit(None).cast("string").alias("full_name"),
        F.lit(None).cast("date").alias("date_of_birth"),
        F.array(F.lit("emirates_id")).alias("attempted_on"),
    )
    return _best_per_master(links, "aecb_source_key", "aecb_match_confidence"), unresolved


# --------------------------------------------------------------------------- #
# §6.3 — Fraud: exact phone+email is deterministic; single-key -> scorer
# --------------------------------------------------------------------------- #
def resolve_fraud(fraud, spine):
    s = spine.select(
        "master_customer_id",
        F.col("phone").alias("s_phone"),
        F.col("email").alias("s_email"),
    )
    # Exact on BOTH keys -> deterministic.
    both = fraud.join(
        s,
        (F.col("phone") == F.col("s_phone")) & (F.col("email") == F.col("s_email")),
        how="inner",
    )
    # Demographic columns get source-unique names (fraud_*) so the survivorship
    # join in assemble_xref has no name collision with the spine's own phone/email.
    det_links = both.select(
        F.col("master_customer_id"),
        F.col("fraud_assessment_id").alias("fraud_source_key"),
        F.col("phone").alias("fraud_phone"),
        F.col("email").alias("fraud_email"),
        F.lit("DETERMINISTIC").alias("fraud_match_method"),
        F.lit(1.0).cast("double").alias("fraud_match_confidence"),
        F.lit(False).alias("fraud_needs_review"),
    )
    det_keys = det_links.select("fraud_source_key").distinct()

    # Remaining fraud rows: block on phone OR email, score probabilistically.
    remaining = fraud.join(det_keys, on="fraud_source_key", how="left_anti")
    cand = remaining.join(
        s,
        (F.col("phone") == F.col("s_phone")) | (F.col("email") == F.col("s_email")),
        how="inner",
    ).withColumn(
        "match_confidence",
        match_confidence_udf(
            F.lit(None), F.lit(None),            # name (fraud has none)
            F.lit(None), F.lit(None),            # dob
            F.col("phone"), F.col("s_phone"),    # phone
            F.col("email"), F.col("s_email"),    # email
            F.lit(None), F.lit(None),            # eid
        ),
    )
    scored = _pick_best(cand, "fraud_assessment_id", "match_confidence")
    prob_links = scored.where(is_attachable(F.col("match_confidence"))).select(
        F.col("master_customer_id"),
        F.col("fraud_assessment_id").alias("fraud_source_key"),
        F.col("phone").alias("fraud_phone"),
        F.col("email").alias("fraud_email"),
        classify_match(F.col("match_confidence")).alias("fraud_match_method"),
        F.col("match_confidence").alias("fraud_match_confidence"),
        needs_manual_review(F.col("match_confidence")).alias("fraud_needs_review"),
    )

    links = _best_per_master(
        det_links.unionByName(prob_links), "fraud_source_key", "fraud_match_confidence"
    )

    # Fraud rows attached nowhere (neither deterministic nor >=0.70) -> unresolved.
    attached_keys = links.select(F.col("fraud_source_key")).distinct()
    unresolved = fraud.join(
        attached_keys.withColumnRenamed("fraud_source_key", "fraud_assessment_id"),
        on="fraud_assessment_id",
        how="left_anti",
    ).select(
        F.lit(C.SRC_FRAUD).alias("source_system"),
        F.col("fraud_assessment_id").alias("source_record_key"),
        F.lit(None).cast("string").alias("emirates_id"),
        F.col("phone"),
        F.col("email"),
        F.lit(None).cast("string").alias("full_name"),
        F.lit(None).cast("date").alias("date_of_birth"),
        F.array(F.lit("phone"), F.lit("email")).alias("attempted_on"),
    )
    return links, unresolved


# --------------------------------------------------------------------------- #
# §6.4 — AML: always probabilistic (soundex+dob blocking -> weighted JW)
# --------------------------------------------------------------------------- #
def resolve_aml(aml, spine):
    s = spine.select(
        "master_customer_id",
        F.col("name_soundex").alias("s_soundex"),
        F.col("name_norm").alias("s_name"),
        F.col("date_of_birth").alias("s_dob"),
    )
    cand = aml.withColumn("aml_name_norm", normalise_name(F.col("full_name"))).join(
        s,
        (F.col("name_soundex") == F.col("s_soundex"))
        | (F.col("date_of_birth") == F.col("s_dob")),
        how="inner",
    ).withColumn(
        "match_confidence",
        match_confidence_udf(
            F.col("aml_name_norm"), F.col("s_name"),        # name (JW)
            F.col("date_of_birth"), F.col("s_dob"),         # dob (exact)
            F.lit(None), F.lit(None),                       # phone
            F.lit(None), F.lit(None),                       # email
            F.lit(None), F.lit(None),                       # eid
        ),
    )
    scored = _pick_best(cand, "aml_case_id", "match_confidence")
    # Source-unique demographic names (aml_*) to avoid collision with the spine.
    links = scored.where(is_attachable(F.col("match_confidence"))).select(
        F.col("master_customer_id"),
        F.col("aml_case_id").alias("aml_source_key"),
        F.col("full_name").alias("aml_full_name"),
        F.col("date_of_birth").alias("aml_dob"),
        classify_match(F.col("match_confidence")).alias("aml_match_method"),
        F.col("match_confidence").alias("aml_match_confidence"),
        needs_manual_review(F.col("match_confidence")).alias("aml_needs_review"),
    )
    links = _best_per_master(links, "aml_source_key", "aml_match_confidence")

    attached = links.select("aml_source_key").distinct()
    unresolved = aml.join(
        attached.withColumnRenamed("aml_source_key", "aml_case_id"),
        on="aml_case_id",
        how="left_anti",
    ).select(
        F.lit(C.SRC_AML).alias("source_system"),
        F.col("aml_case_id").alias("source_record_key"),
        F.lit(None).cast("string").alias("emirates_id"),
        F.lit(None).cast("string").alias("phone"),
        F.lit(None).cast("string").alias("email"),
        F.col("full_name"),
        F.col("date_of_birth"),
        F.array(F.lit("full_name"), F.lit("date_of_birth")).alias("attempted_on"),
    )
    return links, unresolved


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _pick_best(cand, key_col, score_col):
    """Highest-scoring spine candidate per source record (SPEC §6, 'take highest')."""
    w = Window.partitionBy(key_col).orderBy(F.col(score_col).desc_nulls_last())
    return cand.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def _best_per_master(links, key_col, conf_col):
    """If several source records attach to one master, keep the strongest link."""
    w = Window.partitionBy("master_customer_id").orderBy(F.col(conf_col).desc_nulls_last())
    return links.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def assemble_xref(spine, aecb_links, fraud_links, aml_links):
    """Join links onto the spine and apply survivorship (SPEC §6)."""
    x = (
        spine.join(aecb_links, "master_customer_id", "left")
        .join(fraud_links, "master_customer_id", "left")
        .join(aml_links, "master_customer_id", "left")
    )

    # §6 survivorship — coalesce demographics in priority order
    # POSTGRES(spine) > AECB > FRAUD > AML. The spine is highest priority so its
    # value wins; missing fields backfill from the next source down. (AECB is
    # matched ON emirates_id, so it can only agree with the spine's EID.)
    # All join columns are string-form (auto-dedup on master_customer_id) and every
    # demographic column has a source-unique name, so plain F.col resolves cleanly
    # with no self-join ambiguity.
    golden = x.select(
        F.col("master_customer_id"),
        F.col("internal_customer_uuid"),
        F.col("emirates_id"),
        F.coalesce(F.col("phone"), F.col("fraud_phone")).alias("phone"),
        F.coalesce(F.col("email"), F.col("fraud_email")).alias("email"),
        F.coalesce(F.col("full_name"), F.col("aml_full_name")).alias("full_name"),
        F.coalesce(F.col("date_of_birth"), F.col("aml_dob")).alias("date_of_birth"),
        # per-source attach flags + native keys
        F.col("aecb_source_key").isNotNull().alias("aecb_matched"),
        F.col("aecb_source_key"),
        F.col("fraud_source_key").isNotNull().alias("fraud_matched"),
        F.col("fraud_source_key"),
        F.col("aml_source_key").isNotNull().alias("aml_matched"),
        F.col("aml_source_key"),
        # per-source match metadata
        F.col("aecb_match_method"),
        F.col("aecb_match_confidence"),
        F.col("fraud_match_method"),
        F.col("fraud_match_confidence"),
        F.col("aml_match_method"),
        F.col("aml_match_confidence"),
        F.coalesce(F.col("aecb_needs_review"), F.lit(False)).alias("aecb_needs_review"),
        F.coalesce(F.col("fraud_needs_review"), F.lit(False)).alias("fraud_needs_review"),
        F.coalesce(F.col("aml_needs_review"), F.lit(False)).alias("aml_needs_review"),
    )

    # matched_on = every key that fired; spine always contributes the internal uuid.
    # (array_compact is Spark 3.4+, so filter out NULLs with a higher-order filter.)
    matched_on = F.expr(
        "filter(array("
        "'internal_customer_uuid', "
        "CASE WHEN aecb_matched THEN 'emirates_id' END, "
        "CASE WHEN fraud_matched THEN 'phone+email' END, "
        "CASE WHEN aml_matched THEN 'full_name+date_of_birth' END"
        "), x -> x is not null)"
    )

    # Overall confidence = weakest attached link (spine seed = 1.00); array_min
    # skips NULLs. Overall method = PROBABILISTIC if any attached link is fuzzy.
    confidences = F.array(
        F.lit(1.0),  # spine seed
        F.col("aecb_match_confidence"),
        F.col("fraud_match_confidence"),
        F.col("aml_match_confidence"),
    )
    any_prob = (
        (F.col("aecb_match_method") == "PROBABILISTIC")
        | (F.col("fraud_match_method") == "PROBABILISTIC")
        | (F.col("aml_match_method") == "PROBABILISTIC")
    )
    return (
        golden.withColumn("matched_on", matched_on)
        .withColumn("match_confidence", F.array_min(confidences).cast("decimal(5,4)"))
        .withColumn(
            "match_method",
            F.when(F.coalesce(any_prob, F.lit(False)), F.lit("PROBABILISTIC")).otherwise(
                F.lit("DETERMINISTIC")
            ),
        )
        .withColumn(
            "needs_manual_review",
            F.col("aecb_needs_review")
            | F.col("fraud_needs_review")
            | F.col("aml_needs_review"),
        )
        # Resolved golden rows are keyed by the spine; source_system = POSTGRES.
        .withColumn("source_record_key", F.col("internal_customer_uuid"))
        .withColumn("source_system", F.lit(C.SRC_POSTGRES))
    )


def build_unresolved(unresolved_dfs):
    """SPEC §6: source rows below REVIEW_THRESHOLD -> UNRESOLVED record (kept)."""
    union = None
    for df in unresolved_dfs:
        union = df if union is None else union.unionByName(df)
    return union.select(
        F.lit(C.UNRESOLVED_SENTINEL).alias("master_customer_id"),
        F.lit(None).cast("string").alias("internal_customer_uuid"),
        F.col("emirates_id"),
        F.col("phone"),
        F.col("email"),
        F.col("full_name"),
        F.col("date_of_birth"),
        F.lit(False).alias("aecb_matched"),
        F.lit(None).cast("string").alias("aecb_source_key"),
        F.lit(False).alias("fraud_matched"),
        F.lit(None).cast("string").alias("fraud_source_key"),
        F.lit(False).alias("aml_matched"),
        F.lit(None).cast("string").alias("aml_source_key"),
        F.lit(None).cast("string").alias("aecb_match_method"),
        F.lit(None).cast("double").alias("aecb_match_confidence"),
        F.lit(None).cast("string").alias("fraud_match_method"),
        F.lit(None).cast("double").alias("fraud_match_confidence"),
        F.lit(None).cast("string").alias("aml_match_method"),
        F.lit(None).cast("double").alias("aml_match_confidence"),
        F.lit(False).alias("aecb_needs_review"),
        F.lit(False).alias("fraud_needs_review"),
        F.lit(False).alias("aml_needs_review"),
        F.col("attempted_on").alias("matched_on"),
        F.lit(0.0).cast("decimal(5,4)").alias("match_confidence"),
        F.lit("UNRESOLVED").alias("match_method"),
        F.lit(True).alias("needs_manual_review"),
        F.col("source_system"),
        F.col("source_record_key"),
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "batch_id"])
    glue_context, spark, job = glue_bootstrap("build_customer_identity_xref", args)
    env, batch_id = args["env"], args["batch_id"]

    profile = spark.read.format("delta").load(
        C.s3_uri(env, "silver", C.TBL_CUSTOMER_PROFILE)
    )
    aecb = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_AECB))
    fraud = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_FRAUD))
    aml = spark.read.format("delta").load(C.s3_uri(env, "silver", C.TBL_AML))

    spine = build_spine(profile).cache()
    LOG.info("Spine seeded: %s master ids", spine.count())

    aecb_links, aecb_unres = resolve_aecb(aecb, spine)
    fraud_links, fraud_unres = resolve_fraud(fraud, spine)
    aml_links, aml_unres = resolve_aml(aml, spine)

    resolved = assemble_xref(spine, aecb_links, fraud_links, aml_links)
    unresolved = build_unresolved([aecb_unres, fraud_unres, aml_unres])

    # Union carries a per-row source_system already (POSTGRES for golden rows,
    # the true source for unresolved rows) — so add only batch_id + timestamps,
    # never clobbering source_system with a constant.
    combined = resolved.unionByName(unresolved, allowMissingColumns=True)
    xref = (
        combined.withColumn("batch_id", F.lit(batch_id))
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )

    silver_path = C.s3_uri(env, "silver", C.TBL_IDENTITY_XREF)
    upsert_delta(spark, xref, silver_path, MERGE_KEY, C.DB_SILVER, C.TBL_IDENTITY_XREF)
    apply_pii_comments(
        spark,
        C.DB_SILVER,
        C.TBL_IDENTITY_XREF,
        {"emirates_id": 1, "phone": 2, "email": 2, "full_name": 2, "date_of_birth": 2},
    )

    LOG.info(
        "identity xref written: resolved=%s unresolved=%s",
        resolved.count(),
        unresolved.count(),
    )
    job.commit()


if __name__ == "__main__":
    main()
