# Glue transformation layer — credit-decision data platform

PySpark + Delta Lake jobs (AWS Glue 4.0 / Spark 3.3) that turn the four raw
sources into the unified `decision_input`, the identity golden record, immutable
audit snapshots, and the risk/DQ gold marts. Everything here follows
[`docs/SPEC.md`](../docs/SPEC.md) — that file wins on any naming/path/threshold
question.

## Layout

Jobs are segregated by the **medallion layer they produce** — the same scheme as
[`athena/ddl/`](../athena/ddl/) (`silver_layer/` → `credit_silver`, `gold_layer/` → `credit_gold`).
There is no `bronze_layer/` here: bronze is *landed* by [`ingestion/`](../ingestion/) (the SFTP/
API/webhook jobs and the Debezium→Kafka→S3 sink), not by Glue.

```
glue/
  common/                     # shared library, shipped as --extra-py-files common.zip
    constants.py              # SPEC §11 constants, DB/table names, enums, weights
    spark_session.py          # Delta-enabled SparkSession + Glue bootstrap + logger
    identity.py               # Spark Column normalisers + UDF wrappers over text_match
    text_match.py             # pure Jaro-Winkler / weighted scorer / UUIDv5 (Spark-free, unit-tested)
    dq_rules.py               # declarative MUST-PASS / WARN rules (SPEC §8)
    delta_io.py               # MERGE-upsert + Glue catalog registration
    audit.py                  # audit block + PII COMMENT tagging (SPEC §10)
  silver_layer/               # jobs that WRITE silver (db: credit_silver)
    aecb_to_silver.py               # bronze -> aecb_credit_report
    fraud_to_silver.py              # bronze -> fraud_score
    aml_to_silver.py                # bronze -> aml_screening
    customer_profile_to_silver.py   # Debezium CDC -> latest-per-key via Delta MERGE (spine)
    build_customer_identity_xref.py # golden record + conflict resolution (SPEC §6)
    build_decision_input.py         # unified decision record (SPEC §5)
    write_decision_snapshots.py     # immutable JSON snapshots + index (SPEC §7)
  gold_layer/                 # jobs that WRITE gold (db: credit_gold) — files end with _mart
    run_dq_scorecard_mart.py             # DQ gating + scorecard (SPEC §8); quarantines to silver
    build_portfolio_monitoring_daily_mart.py   # risk mart (SPEC §9)
dq/soda/                        # Soda Core checks mirroring §8 thresholds
```

## Job DAG (orchestration order)

```
                 ┌── aecb_to_silver ──┐
 bronze ─────────┼── fraud_to_silver ─┤
 (SFTP/API/       ├── aml_to_silver ───┤──► build_customer_identity_xref (§6)
  webhook/CDC)   └── customer_profile_to_silver (CDC)          │
                                                               ▼
                                              build_decision_input (§5)
                                                               │
                                     ┌─────────────────────────┼───────────────────┐
                                     ▼                         ▼                   ▼
                        write_decision_snapshots (§7)   run_dq_scorecard (§8)   (dq_pass set)
                                     │                         │                   │
                                     └── snapshot_s3_uri ──► decision_input ◄──────┘
                                                               │
                                                               ▼
                                            build_portfolio_monitoring_daily (§9)
                                               (reads dq_pass rows + day's dq_score)
```

1. **bronze → silver** (4 jobs, independent, run in parallel). Each cleans/types/
   dedupes/PII-tags its source and MERGE-upserts a Delta silver table.
2. **build_customer_identity_xref** — needs all four silver tables. Produces the
   golden `customer_identity_xref` (deterministic-first, probabilistic fallback,
   survivorship, UNRESOLVED sentinels; nothing dropped).
3. **build_decision_input** — joins the four sources via the xref into one row per
   `decision_id`; leaves `dq_pass` / `snapshot_s3_uri` NULL.
4. **write_decision_snapshots** — freezes the raw inputs per decision to an
   immutable S3 JSON object (Object Lock, 7y), indexes them in Delta, and writes
   `snapshot_s3_uri` back onto `decision_input`.
5. **run_dq_scorecard** — applies MUST-PASS + WARN rules, quarantines failures,
   sets `dq_pass = TRUE` on survivors, writes `dq_scorecard_daily`.
6. **build_portfolio_monitoring_daily** — aggregates the `dq_pass` rows into the
   risk mart, attaching the day's `dq_score`.

Steps 4 and 5 both consume `build_decision_input`'s output and both write back to
`decision_input` on disjoint columns, so they can run concurrently; step 6 runs
after 5 (it needs `dq_pass` + the scorecard).

## Bookmarks + Delta MERGE = incremental & idempotent

- **Batch S3 sources** (AECB SFTP, fraud REST poll, AML webhook drops, the
  decisions feed) are read with **Glue job bookmarks** (`transformation_ctx` on
  `create_dynamic_frame`), so each run only picks up files landed since the last
  successful `job.commit()`.
- **CDC source** (customer_profile via Debezium) has no file bookmark; it is
  collapsed to latest-per-key and MERGE-upserted, honouring tombstones (`op='d'`).
- **Every silver/gold writer** goes through `common.delta_io.upsert_delta`, a
  Delta `MERGE` on the table's business key. Re-processing an overlapping window
  (retry, backfill, bookmark reset) therefore **converges** instead of
  duplicating — the jobs are idempotent by construction.

## Running on Glue

Each job needs these job parameters (plus the standard `--JOB_NAME`):

```
--env dev|pre|prod
--batch_id <run id>
--datalake-formats delta          # enables Glue's Delta support
--job-bookmark-option job-bookmark-enable
--extra-py-files s3://.../common.zip   # the glue/common package
```

`run_dq_scorecard` and `build_portfolio_monitoring_daily` additionally take
`--run_date YYYY-MM-DD` (the scorecard is required; the mart's is optional).

## EMR migration note

The transformation logic is plain PySpark + Delta and runs unchanged on EMR. The
**only** Glue-specific pieces are:

- the `glue_bootstrap()` call (GlueContext + `Job` + bookmarks) in each job's
  `main()`, and
- the bookmarked `create_dynamic_frame.from_options` reads.

To migrate: swap `glue_bootstrap(...)` for `common.spark_session.build_spark_session(...)`
(already provided — Delta + Glue-catalog + GST timezone configured), replace the
bookmarked reads with a watermark-filtered `spark.read.parquet(...)` (e.g. filter
on `ingest_date`), and drop the `job.init()/job.commit()` calls. No transform,
identity, DQ, or Delta-IO code changes — that all lives in `glue/common/` and is
runtime-agnostic.
