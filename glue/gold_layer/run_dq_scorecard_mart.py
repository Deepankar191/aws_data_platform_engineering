"""Data-quality scorecard + gating — gold.credit_gold.dq_scorecard_daily (SPEC §8).

For a day's ``decision_input``:

  1. Apply the MUST-PASS row rules (SPEC §8) + decision_id uniqueness.
  2. Quarantine failing rows to ``decision_input_quarantine`` (they never reach
     gold / the risk mart) with the list of rules they failed.
  3. Set ``dq_pass = TRUE`` on the passing rows (targeted MERGE onto decision_input).
  4. Evaluate the WARN rules (completeness, AML PENDING rate) + source freshness.
  5. Compute per-rule pass/fail counts and an overall 0–100 ``dq_score`` and write
     one ``dq_scorecard_daily`` row.
  6. Emit CloudWatch-style metric lines; note where SNS alerts fire on breach.

Job args:  --JOB_NAME --env --run_date YYYY-MM-DD
"""

import json
import sys

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import constants as C
from common.delta_io import append_delta, upsert_delta
from common.dq_rules import MUST_PASS_RULES, UNIQUE_KEYS, WARN_RULES
from common.spark_session import get_logger, glue_bootstrap

LOG = get_logger("run_dq_scorecard")

# Weighting of the composite dq_score (SPEC §8: must-pass is blocking, warn is
# advisory) — must-pass dominates.
MUST_PASS_WEIGHT = 0.8
WARN_WEIGHT = 0.2


def _emit_metric(name: str, value, **dims):
    """CloudWatch-scrapable metric line (a metric filter turns these into metrics)."""
    dim_str = " ".join(f"{k}={v}" for k, v in dims.items())
    LOG.info("METRIC %s value=%s %s", name, value, dim_str)


def evaluate(decision_input):
    """Annotate each row with per-rule pass flags, an overall _dq_pass, and the
    list of failed rule names (_fail_reasons)."""
    di = decision_input

    # Per-row must-pass evaluation: one boolean column per rule.
    for rule in MUST_PASS_RULES:
        di = di.withColumn(f"pass__{rule.name}", rule.predicate())

    # Uniqueness (must-pass) — decision_id is the enforced grain PK. A duplicate
    # decision_id fails. (application_id / master_customer_id are checked for
    # non-null as row rules; their uniqueness is reported below but not gated,
    # since legitimate re-scoring produces multiple decisions per application.)
    from pyspark.sql.window import Window

    w = Window.partitionBy("decision_id")
    di = di.withColumn("pass__decision_id_unique", F.count(F.lit(1)).over(w) == 1)

    pass_cols = [c for c in di.columns if c.startswith("pass__")]
    # all() over the boolean pass columns. A predicate that evaluates to NULL
    # (e.g. a NULL product_code -> isin() is NULL) must FAIL, not slip through —
    # so coalesce the conjunction to FALSE.
    di = di.withColumn(
        "_dq_pass",
        F.coalesce(F.expr(" AND ".join(pass_cols)), F.lit(False))
        if pass_cols
        else F.lit(True),
    )
    # failed rule names per row (for the quarantine audit trail). array_compact is
    # Spark 3.4+, so drop NULLs with a higher-order filter (Glue 4.0 = Spark 3.3).
    fail_reason = F.expr(
        "filter(array("
        + ", ".join(
            f"CASE WHEN NOT {c} THEN '{c[len('pass__'):]}' END" for c in pass_cols
        )
        + "), x -> x is not null)"
    )
    di = di.withColumn("_fail_reasons", fail_reason)
    return di


def _score(totals, warn_results):
    total = totals["total"] or 0
    if total == 0:
        return 100.0
    passed = totals["passed"]
    mp_ratio = passed / total
    warn_met = sum(1 for r in warn_results.values() if r["ok"])
    warn_ratio = warn_met / len(warn_results) if warn_results else 1.0
    score = 100.0 * (MUST_PASS_WEIGHT * mp_ratio + WARN_WEIGHT * warn_ratio)
    return round(score, 2)


def source_freshness(spark, env):
    """Per-source event-time lag vs SLA (SPEC §8 WARN)."""
    checks = {
        C.SRC_AECB: (C.TBL_AECB, "report_timestamp"),
        C.SRC_FRAUD: (C.TBL_FRAUD, "scored_timestamp"),
        C.SRC_AML: (C.TBL_AML, "screening_timestamp"),
        C.SRC_POSTGRES: (C.TBL_CUSTOMER_PROFILE, "profile_updated_timestamp"),
    }
    results = {}
    for source, (table, ts_col) in checks.items():
        try:
            df = spark.read.format("delta").load(C.s3_uri(env, "silver", table))
            lag = df.select(
                (
                    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max(ts_col)))
                    / 3600.0
                ).alias("lag_hours")
            ).collect()[0]["lag_hours"]
        except Exception:  # noqa: BLE001
            lag = None
        sla = C.FRESHNESS_SLA_HOURS[source]
        ok = lag is not None and lag <= sla
        results[source] = {"lag_hours": lag, "sla_hours": sla, "ok": bool(ok)}
        _emit_metric("dq.freshness.lag_hours", lag, source=source, sla=sla, ok=ok)
    return results


def main():
    # run_date is required — the scorecard grain is one row per scorecard_date.
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "env", "run_date"])
    glue_context, spark, job = glue_bootstrap("run_dq_scorecard", args)
    env = args["env"]
    run_date = args["run_date"]

    di_path = C.s3_uri(env, "silver", C.TBL_DECISION_INPUT)
    decision_input = spark.read.format("delta").load(di_path).where(
        F.col("decision_date") == F.lit(run_date)
    )

    if decision_input.rdd.isEmpty():
        LOG.info("No decision_input rows for run_date=%s", run_date)
        job.commit()
        return

    evaluated = evaluate(decision_input).cache()

    # ------ per-rule fail counts + warn ratios in a single pass ------
    fail_exprs = [
        F.sum(F.when(~F.col(f"pass__{r.name}"), 1).otherwise(0)).alias(f"fail__{r.name}")
        for r in MUST_PASS_RULES
    ]
    fail_exprs.append(
        F.sum(F.when(~F.col("pass__decision_id_unique"), 1).otherwise(0)).alias(
            "fail__decision_id_unique"
        )
    )
    warn_exprs = []
    for wr in WARN_RULES:
        warn_exprs.append(
            F.avg(F.when(wr.good(), 1.0).otherwise(0.0)).alias(f"warn__{wr.name}")
        )
    dup_app = (F.count(F.lit(1)) - F.countDistinct("application_id")).alias("dup_application_id")
    dup_master = (F.count(F.lit(1)) - F.countDistinct("master_customer_id")).alias(
        "dup_master_customer_id"
    )

    row = evaluated.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("_dq_pass"), 1).otherwise(0)).alias("passed"),
        *fail_exprs,
        *warn_exprs,
        dup_app,
        dup_master,
    ).collect()[0]

    totals = {"total": row["total"], "passed": row["passed"]}
    quarantined = totals["total"] - totals["passed"]

    fail_counts = {r.name: row[f"fail__{r.name}"] for r in MUST_PASS_RULES}
    fail_counts["decision_id_unique"] = row["fail__decision_id_unique"]

    # WARN results — evaluate observed ratio against threshold.
    warn_results = {}
    for wr in WARN_RULES:
        observed = row[f"warn__{wr.name}"] or 0.0
        if wr.invert:  # `good` marks the bad condition; rate must stay ≤ max_bad_ratio
            ok = observed <= wr.max_bad_ratio
        else:
            ok = observed >= wr.min_good_ratio
        warn_results[wr.name] = {"observed": round(float(observed), 4), "ok": bool(ok)}
        _emit_metric("dq.warn.ratio", round(float(observed), 4), rule=wr.name, ok=ok)
        if not ok:
            # SNS: publish to the `credit-dq-alerts` topic (ARN injected by IaC/
            # orchestration) so on-call is paged on a WARN breach.
            LOG.warning("WARN breach rule=%s observed=%s — SNS alert would fire", wr.name, observed)

    freshness = source_freshness(spark, env)
    for source, res in freshness.items():
        if not res["ok"]:
            LOG.warning("Freshness SLA breach source=%s lag=%s — SNS alert would fire", source, res["lag_hours"])

    # ------ metric lines for must-pass ------
    for name, cnt in fail_counts.items():
        _emit_metric("dq.mustpass.fail_count", cnt, rule=name)
    _emit_metric("dq.rows.total", totals["total"])
    _emit_metric("dq.rows.quarantined", quarantined)

    dq_score = _score(totals, {**warn_results, **freshness})

    # ------ 1) quarantine failures ------
    quarantine_df = (
        evaluated.where(~F.col("_dq_pass"))
        .drop(*[c for c in evaluated.columns if c.startswith("pass__")], "_dq_pass")
        .withColumnRenamed("_fail_reasons", "dq_fail_reasons")
        .withColumn("quarantined_timestamp", F.current_timestamp())
    )
    if quarantined > 0:
        append_delta(
            spark,
            quarantine_df,
            C.s3_uri(env, "silver", C.TBL_DECISION_INPUT_QUARANTINE),
            C.DB_SILVER,
            C.TBL_DECISION_INPUT_QUARANTINE,
            partition_cols=["decision_date"],
        )

    # ------ 2) set dq_pass on decision_input (targeted MERGE) ------
    gate = evaluated.select("decision_id", F.col("_dq_pass").alias("dq_pass"))
    di_tbl = DeltaTable.forPath(spark, di_path)
    (
        di_tbl.alias("t")
        .merge(gate.alias("s"), "t.decision_id = s.decision_id")
        .whenMatchedUpdate(
            set={"dq_pass": "s.dq_pass", "updated_timestamp": "current_timestamp()"}
        )
        .execute()
    )

    # ------ 3) write the scorecard row ------
    scorecard = spark.createDataFrame(
        [
            {
                "scorecard_date": run_date,
                "total_rows": int(totals["total"]),
                "passed_rows": int(totals["passed"]),
                "quarantined_rows": int(quarantined),
                "duplicate_application_id_count": int(row["dup_application_id"]),
                "duplicate_master_customer_id_count": int(row["dup_master_customer_id"]),
                "must_pass_fail_counts_json": json.dumps(fail_counts),
                "warn_results_json": json.dumps(warn_results),
                "freshness_results_json": json.dumps(freshness),
                "dq_score": float(dq_score),
            }
        ]
    ).select(
        F.to_date(F.col("scorecard_date")).alias("scorecard_date"),
        "total_rows",
        "passed_rows",
        "quarantined_rows",
        "duplicate_application_id_count",
        "duplicate_master_customer_id_count",
        "must_pass_fail_counts_json",
        "warn_results_json",
        "freshness_results_json",
        F.col("dq_score").cast("decimal(5,2)").alias("dq_score"),
        F.current_timestamp().alias("created_timestamp"),
    )

    upsert_delta(
        spark,
        scorecard,
        C.s3_uri(env, "gold", C.TBL_DQ_SCORECARD_DAILY),
        ["scorecard_date"],
        C.DB_GOLD,
        C.TBL_DQ_SCORECARD_DAILY,
        partition_cols=["scorecard_date"],
    )

    _emit_metric("dq.score", dq_score, run_date=run_date)
    LOG.info(
        "DQ scorecard complete run_date=%s total=%s passed=%s quarantined=%s dq_score=%s",
        run_date,
        totals["total"],
        totals["passed"],
        quarantined,
        dq_score,
    )
    job.commit()


if __name__ == "__main__":
    main()
