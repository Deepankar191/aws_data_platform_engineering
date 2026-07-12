# Athena DDL — Glue Catalog table definitions

One `.sql` file per table, organised by **medallion layer**. These are the **catalog
contracts** Athena uses to query the lakehouse. They map 1:1 to the tables in `docs/SPEC.md` §3–§9.

## Layout

```
athena/ddl/
├── bronze_layer/   raw landing tables (Parquet + partition projection; Delta for the CDC sink)
│   ├── aecb_raw.sql            fraud_raw.sql            aml_raw.sql
│   ├── decisions_raw.sql       (driver feed, SPEC §2.1)
│   └── customer_profile_raw.sql  (Kafka-Connect Delta CDC sink)
├── silver_layer/   conformed + identity + decision + audit (Delta)
│   ├── aecb_credit_report.sql  fraud_score.sql          aml_screening.sql
│   ├── customer_profile.sql    customer_identity_xref.sql
│   └── decision_input.sql      decision_input_snapshot.sql   decision_input_quarantine.sql
└── gold_layer/     marts (Delta) — file names end with _mart
    └── portfolio_monitoring_daily_mart.sql   dq_scorecard_daily_mart.sql
```

The subdirectory maps directly to the Glue catalog database: `bronze_layer/` → `credit_bronze`,
`silver_layer/` → `credit_silver`, `gold_layer/` → `credit_gold` (the DB is also written in each
`CREATE`). The `.sql` **file** names in `gold_layer/` end with `_mart`; the **table** names inside
them are unchanged (`portfolio_monitoring_daily`, `dq_scorecard_daily`) — those are the catalog contract.

## Delta-on-Athena approach

Silver and Gold tables are **Delta Lake** tables (SPEC §3). Athena engine **v3** reads
Delta Lake natively when the table is registered in the Glue Data Catalog with
`TBLPROPERTIES ('table_type'='DELTA')` and a `LOCATION` pointing at the Delta table root.

> **Schema of record.** For silver/gold tables the **Glue job is the source of truth** — Delta
> is schema-on-write and the jobs self-register the catalog. Each silver/gold DDL here is kept
> in lockstep with its writing job (noted in the file header, e.g. `fraud_score.sql` ↔
> `glue/silver_layer/fraud_to_silver.py`). Bronze DDLs match the ingestion layer. If a DDL
> and its Glue job ever disagree, the Glue job wins and the DDL is the bug.

In production these Delta tables are **created by the Glue jobs** (via the Delta connector,
which writes the `_delta_log/` and registers/updates the Glue catalog entry), or by a Glue
crawler configured for Delta. The DDL in this folder is the **reviewable, human-readable
equivalent** of that catalog entry:

- Athena's native Delta reader takes the schema from `_delta_log/`, so the explicit column
  list below is primarily a **documentation + review artefact** (column names, types,
  `COMMENT`s, and PII tags per SPEC §10). It must stay in lockstep with the Glue job output
  schema and the `meta.yaml` catalog contract.
- A reviewer can run any of these statements verbatim against Athena v3 to (re)register the
  table if the catalog entry is ever lost.

For **Bronze** Parquet tables (`bronze_layer/aecb_raw`, `bronze_layer/fraud_raw`,
`bronze_layer/aml_raw`, `bronze_layer/decisions_raw`) we use a classic Hive external table with
**partition projection** on `ingest_date` (no `MSCK REPAIR` needed). `bronze_layer/customer_profile_raw` is the Kafka-Connect
**Delta** CDC sink, so it uses the Delta form.

## `${ENV}` substitution

`LOCATION` uses `s3://wio-credit-decision-${ENV}/...` (SPEC §3). `${ENV} ∈ {dev,pre,prod}`.
The deploy step (Terraform `null_resource`/CI) substitutes `${ENV}` before the statement is
run in the target account. Do not commit a hardcoded env into the `LOCATION`.

## Databases (SPEC §4)

| Database        | Tables |
|-----------------|--------|
| `credit_bronze` | `aecb_raw`, `fraud_raw`, `aml_raw`, `decisions_raw`, `customer_profile_raw` |
| `credit_silver` | `aecb_credit_report`, `fraud_score`, `aml_screening`, `customer_profile`, `customer_identity_xref`, `decision_input`, `decision_input_snapshot`, `decision_input_quarantine` |
| `credit_gold`   | `portfolio_monitoring_daily`, `dq_scorecard_daily` |

## PII levels (SPEC §10)

`PII Level 1` = Emirates ID / passport · `PII Level 2` = phone / email / DOB / name / address
· `PII Level 3` = derived / aggregated. Tagged inline in each column `COMMENT`.
