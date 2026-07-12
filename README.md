# Credit Decision Data Platform

A unified decision-input data pipeline for a UAE financial organisation launching three credit
products (**personal finance**, **BNPL**, **credit-card alternative**) in Q2 2025.

It fuses four conflicting data sources into one auditable decision record per credit application,
resolves customer identity across them, snapshots every input immutably for **UAE Central Bank**
compliance, scores data quality, and serves a portfolio-monitoring mart to the risk team.

> **Read [`docs/SPEC.md`](docs/SPEC.md) first** — it is the single source of truth for S3 paths,
> table names, the identity-resolution contract, thresholds, and naming conventions. Every component
> in this repo conforms to it.

---

## Architecture at a glance

```
                 ┌───────────────────────── SOURCES ─────────────────────────┐
  AECB (SFTP/XML) ──▶ aecb_sftp_ingest ─────────────┐
  Fraud (REST/JSON) ─▶ fraud_api_ingest ────────────┤
  AML (webhook/JSON) ▶ aml_webhook (Lambda) ────────┤──▶  S3  bronze/  (Parquet/Delta)
  PostgreSQL ─▶ Debezium CDC ─▶ Kafka/MSK ─▶ S3 sink ┘
                                                          │
                                          Glue (PySpark + Delta, MPP)  │ bookmarks + Delta MERGE
                                                          ▼
                             S3 silver/  ── bronze→silver per source
                                          ── customer_identity_xref  (conflict resolution)
                                          ── decision_input          (unified record)
                                          ── decision_input_snapshot (immutable, S3 Object Lock)
                                                          │
                                          Glue DQ scorecard  ──▶ SNS alerts
                                                          ▼
                             S3 gold/    ── portfolio_monitoring_daily
                                          ── dq_scorecard_daily
                                                          │
                                     AWS Athena  ──▶  Metabase (risk dashboards)
```

Full diagrams (data flow, medallion, identity resolution, AWS deployment, audit sequence) are in
[`docs/architecture/architecture.md`](docs/architecture/architecture.md).

---

## Repository layout

| Path | What it contains |
|---|---|
| `docs/SPEC.md` | **Single source of truth** — contracts, paths, thresholds, naming |
| `docs/architecture/` | **Deliverable 1** — architecture diagrams (mermaid + diagrams-as-code PNG) |
| `docs/execution-plan/` | **Deliverable 3** — 30/60/90-day execution plan (`.md` + `.pdf`) |
| `docs/data-model/` | Data model — layer model, ER diagram, grain/PK catalog, identity model, PII matrix |
| `tests/` | pytest suite — identity algorithm, sample-data linkage, DDL-convention checks (no Spark needed) |
| `ingestion/debezium/` | PostgreSQL CDC: Debezium source + S3 sink connector configs, local docker-compose |
| `ingestion/aecb_sftp/` | AECB batch SFTP XML ingestion → bronze |
| `ingestion/fraud_api/` | Fraud provider REST poller → bronze |
| `ingestion/aml_webhook/` | AML/PEP webhook handler (Lambda + SAM) → bronze |
| `glue/common/` | Shared PySpark utils: identity matching, DQ rules, Delta IO, constants |
| `glue/silver_layer/` | Jobs that **produce silver** — bronze→silver conform (per source), identity resolution/conflict logic, `decision_input`, immutable snapshots |
| `glue/gold_layer/` | Jobs that **produce gold** — portfolio monitoring mart + two-tier DQ scorecard |
| `dq/soda/` | Soda Core check definitions mirroring the DQ thresholds |
| `athena/ddl/` | Glue-catalog / Delta table DDLs |
| `athena/views/` | Analyst + regulator views and sample queries |
| `orchestration/airflow/` | The end-to-end pipeline DAG |
| `infra/terraform/` | AWS IaC: S3, RDS, MSK, Glue, Athena, IAM, SNS, Object-Lock bucket |
| `sample_data/` | Cross-referenced sample records (with an intentional conflict) to trace end-to-end |

---

## The four sources & how they are matched

| Source | Transport | Format | Native key | Bronze prefix |
|---|---|---|---|---|
| UAE Credit Bureau (AECB) | Batch SFTP | XML | `emirates_id` | `bronze/aecb/` |
| Fraud detection | REST API | JSON | `phone` + `email` | `bronze/fraud/` |
| AML / PEP screening | Webhook | JSON | `full_name` + `date_of_birth` | `bronze/aml/` |
| Internal customer profile | PostgreSQL CDC (Debezium→Kafka) | JSON | `internal_customer_uuid` | `bronze/customer_profile/` |

PostgreSQL is the **identity spine**. The other three are resolved onto it by
`glue/silver_layer/build_customer_identity_xref.py` — deterministic keys first, probabilistic
fallback (thresholds `MATCH=0.85`, `REVIEW=0.70`), survivorship on conflict, and **nothing is dropped**
(unmatched rows are kept as `UNRESOLVED`). See `docs/SPEC.md` §6.

---

## Key design decisions

- **Delta Lake on S3** for silver/gold — ACID MERGE upserts give idempotent, incremental processing.
- **Incremental processing** = Glue **job bookmarks** on batch/API sources + **Delta MERGE** on CDC.
- **Glue now, EMR later** — the same PySpark runs on both; only the SparkSession bootstrap differs
  (`glue/common/spark_session.py`). Migration path documented in `glue/README.md`.
- **Immutable audit** — every decision's inputs are frozen to an **S3 Object-Lock (compliance-mode,
  7-year)** object; a `content_sha256` in Delta makes tampering detectable. (`docs/SPEC.md` §7).
- **Two-tier DQ** — must-pass rules quarantine bad rows before gold; warn rules alert via SNS.
- **Query & BI** — Athena over the Glue catalog; Metabase for the risk team.
- **Scale** — sized for 10K decisions/day at launch → 100K/day within 12 months (see execution plan).

---

## Setup

One consolidated dependency list lives at the repo root:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt        # ingestion + tests + doc/PDF/diagram tooling
```

Heavy runtime-provided layers (Spark on Glue, Airflow on MWAA) are commented in that file —
uncomment to run them locally. Per-component `requirements.txt` files remain for packaging a
single unit (a Lambda zip, a Glue bundle). System deps: Graphviz `dot` (diagrams), optional `pandoc`.

## Running it locally / deploying

- **CDC path locally:** `ingestion/debezium/docker-compose.yml` spins up Postgres + Kafka + Connect.
- **Glue jobs locally:** run any `glue/**/*.py` with a local Delta-enabled Spark (see `glue/README.md`);
  the same scripts deploy as Glue 4.0 jobs via `infra/terraform/glue.tf`.
- **Infra:** `cd infra/terraform && terraform init && terraform plan -var env=dev` (see its README).
- **Orchestration:** the Airflow DAG in `orchestration/airflow/dags/` wires the whole thing daily.

## Deliverables

1. **Architecture diagram** — `docs/architecture/` (mermaid diagrams + rendered PNGs, plus
   `architecture.pdf` — a single PDF of the full architecture doc with every diagram embedded,
   built by `docs/architecture/build_pdf.py`).
2. **Code repo** — this repository.
3. **Execution plan (PDF)** — `docs/execution-plan/execution_plan.pdf`
   (built by `docs/execution-plan/build_pdf.py`).
