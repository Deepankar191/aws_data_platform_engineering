# Orchestration — Airflow

`dags/credit_decision_pipeline_dag.py` orchestrates the **daily batch** assembly of the
credit-decision lakehouse. It is the glue between the ingestion layer, the Glue transform
jobs (`glue/`), the DQ layer (`dq/soda/`), and the Athena catalog. All shapes, paths, and
names come from `docs/SPEC.md` — the DAG references SPEC sections in its docstring and task
comments.

## The two cadences

The platform runs on **two clocks**, and the DAG only owns one of them:

| Cadence | Sources | Mechanism | In this DAG? |
|---|---|---|---|
| **Continuous** | Internal customer profile (source #4) | PostgreSQL → Debezium → MSK → Kafka Connect **S3/Delta sink**, always on | **No.** The stream lands `bronze/customer_profile/` (Delta) 24×7. The daily `bronze_to_silver_customer_profile` job just **Delta-MERGEs** whatever the stream has landed so far — it has **no landing sensor**. |
| **Daily batch** | AECB (SFTP/XML), fraud (REST poll/JSON), AML (webhook/JSON) | Files land under `bronze/<src>/ingest_date=YYYY-MM-DD/` once per day | **Yes.** `S3KeySensor` waits on each day's `_SUCCESS` marker, then the per-source Glue job runs. |

So the spine (customer_profile) is effectively real-time in bronze; the three external
sources are once-a-day. The DAG waits only for the batch three, then fuses all four.

## Task graph

```
wait_aecb_landing  ─▶ bronze_to_silver_aecb  ─┐
wait_fraud_landing ─▶ bronze_to_silver_fraud ─┤
wait_aml_landing   ─▶ bronze_to_silver_aml   ─┼─▶ build_customer_identity_xref  (§6)
                      bronze_to_silver_        │        │
                      customer_profile  ──────┘        ▼
                      (continuous CDC, no sensor)  build_decision_input          (§5)
                                                       │
                                                       ▼
                                                 write_decision_snapshots        (§7, Object-Lock)
                                                       │
                                                       ▼
                                                 run_dq_scorecard                (§8, SNS alert)
                                                       │
                                                       ▼
                                                 build_portfolio_monitoring_daily (§9)
                                                       │
                                                       ▼
                                                 soda_scan  ─▶ athena_refresh
```

- **Fan-out / fan-in:** the four `bronze_to_silver_*` Glue jobs run in parallel and fan into
  identity resolution. Everything after that is a strict linear chain because each step
  consumes the previous step's Delta output.
- **Glue tasks** use `GlueJobOperator` (amazon provider). Job names live in the `GLUE_JOBS`
  dict and **must match** `infra/terraform/glue.tf` (`locals.glue_jobs`) and the script
  folders under `glue/`.
- **soda_scan** runs the Soda Core checks in `dq/soda/` — an independent DQ assertion layer
  that blocks the catalog refresh if a check fails.
- **athena_refresh** self-registers Delta tables / warms manifests; bronze parquet uses
  partition projection so no `MSCK` is normally needed.

## Schedule

`schedule="30 5 * * *"` — **05:30 GST daily**. This sits after the upstream landings
(AECB batch ~02:00, AML callbacks ~05:00, fraud poll ~06:15 — the fraud sensor absorbs the
small overlap with a 2h timeout). `catchup=False`, `max_active_runs=1` so a slow day never
overlaps the next, and back-fills are run explicitly, not auto-triggered.

## Incremental / bookmark story (SPEC §3)

- **Batch sources (AECB / fraud / AML):** the Glue jobs run with **job bookmarks enabled**
  (`infra/terraform/glue.tf`). Each run only reads bronze partitions/objects it has not
  processed before, keyed off `ingest_date` + the bookmark state. Re-running a date is safe
  because the silver writes are **idempotent Delta MERGEs** on the natural key.
- **CDC source (customer_profile):** no bookmark — the silver job does a **Delta MERGE** of
  the CDC change set (insert/update/delete by `internal_customer_uuid`, ordered by
  `cdc_lsn` / `cdc_source_ts_ms`) so the spine reflects current state.
- **Downstream (xref, decision_input, snapshots, marts):** each reads the silver Delta tables
  by `ingest_date` / `decision_date` window and MERGEs its own output, so a re-run of a day
  overwrites that day deterministically and never double-counts.

## Env / deploy

Each env (dev/pre/prod) is a **separate AWS account**, so Glue job names are not
env-suffixed — the account is the isolation boundary. The DAG reads env-specific values
(`credit_env`, `credit_lakehouse_bucket`, `aws_region`) from **Airflow Variables**, and AWS
auth from the `aws_default` connection. Deploy the same DAG file to each env's Airflow; the
Variables differ.

## Companion scripts (referenced, owned elsewhere)

- `glue/` — the PySpark job scripts, one folder per stage (owned by the transform layer).
- `dq/soda/` — Soda Core `configuration.yml` + `checks/` (owned by the DQ layer).
- `orchestration/airflow/scripts/refresh_catalog.py` — catalog warm/MSCK helper.
