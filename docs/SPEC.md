# Credit Decision Data Platform — Engineering Spec (Single Source of Truth)

> Every component in this repo (ingestion, Glue, Athena, orchestration, IaC) MUST follow the
> naming, path, and contract conventions defined here. If a component diverges, this file wins.

---

## 1. Business context
1. Credit Decision Data Architecture & Implementation
A Financial Organisation is launching 3 credit products (personal finance, BNPL, credit card alternative) in Q2 2025. You must design and implement a unified decision-input data pipeline.

Context:

Data sources arrive in conflicting formats:

UAE Credit Bureau (AECB) — batch SFTP delivery, XML format, customer matched on Emirates ID
Fraud detection provider — REST API, JSON, real-time scoring, customer matched on phone + email
AML/PEP screening — webhook callbacks, customer matched on name + DOB
Internal customer profile — PostgreSQL, customer matched on internal UUID
Requirements:

Design a medallion architecture (bronze/silver/gold) handling these 4 sources
Implement conflict resolution logic for customer key matching across sources
Build decision traceability — capture immutable snapshots of all inputs per credit decision
Create a data quality scorecard with must-pass rules and alert thresholds
Design a portfolio monitoring mart for risk team dashboards
Provide a 30/60/90 day execution plan distinguishing launch-critical vs post-launch work
Constraints:

Use SQL + Python (any framework)
Assume AWS infrastructure (S3, RDS, Glue/Airflow)
Must support 10K decisions/day at launch, 100K within 12 months
Full audit trail required for UAE Central Bank compliance
Deliverables must include working SQL/Python code samples for key components.
A UAE financial organisation is launching 3 credit products in Q2 2025:

| Product code | Product |
|---|---|
| `PERSONAL_FINANCE` | Personal finance loan |
| `BNPL` | Buy-now-pay-later |
| `CARD_ALT` | Credit-card alternative |

The platform assembles a **unified decision-input record** per credit application by fusing 4 sources,
resolves the customer identity across them, snapshots every input immutably for audit (UAE Central
Bank compliance), scores data quality, and serves a portfolio-monitoring mart to the risk team.

Scale target: **10K decisions/day at launch → 100K/day within 12 months.**

---

## 2. The four sources

| # | Source | Transport | Format | Native match key | Bronze landing |
|---|---|---|---|---|---|
| 1 | UAE Credit Bureau (AECB) | Batch SFTP | XML | `emirates_id` | `bronze/aecb/` |
| 2 | Fraud detection provider | REST API (poll) | JSON | `phone` + `email` | `bronze/fraud/` |
| 3 | AML / PEP screening | Webhook callback | JSON | `full_name` + `date_of_birth` | `bronze/aml/` |
| 4 | Internal customer profile | PostgreSQL (Debezium CDC → Kafka) | JSON (CDC) | `internal_customer_uuid` | `bronze/customer_profile/` |

PostgreSQL is the **identity spine**: `internal_customer_uuid` is the canonical id. The other three
sources are joined onto it through the identity-resolution logic in §6.

### 2.1 The decision (scoring-event) driver feed

The four sources above are the **decision inputs**. What triggers a row in `decision_input` is a
**scoring event** emitted by the credit decision engine — one per application scored. This driver feed
is not one of the four external sources; it is internal, and it is what the pipeline is *about*.

| Field | Notes |
|---|---|
| `decision_id` | UUID, one per scoring event — the grain of `decision_input` |
| `application_id` | The credit application being scored |
| `internal_customer_uuid` | Spine id — how the decision is tied to the resolved customer |
| `product_code` | `PERSONAL_FINANCE` \| `BNPL` \| `CARD_ALT` |
| `decision_timestamp` | When scored (GST) |
| `requested_amount_aed` / `approved_amount_aed` | `DECIMAL(18,2)`; feed the §9 mart |
| `decision_outcome` | `APPROVED` \| `DECLINED` \| `REFERRED`; drives `decision_outcome_band` |

Transport: decision engine → Kinesis Firehose / Kafka → Parquet in `bronze/decisions/`. Bronze table
`credit_bronze.decisions_raw` (`athena/ddl/bronze_layer/decisions_raw.sql`); sample in `sample_data/decisions/`.
`glue/silver_layer/build_decision_input.py` reads it (bookmarked) and fuses the 4 sources onto it
via `customer_identity_xref` (§6).

---

## 3. S3 layout (medallion)

Bucket: `s3://wio-credit-decision-${ENV}` where `ENV ∈ {dev, pre, prod}`.

```
s3://wio-credit-decision-${ENV}/
  bronze/                         # raw, append-only, source schema preserved
    aecb/ingest_date=YYYY-MM-DD/*.parquet
    fraud/ingest_date=YYYY-MM-DD/*.parquet
    aml/ingest_date=YYYY-MM-DD/*.parquet
    decisions/ingest_date=YYYY-MM-DD/*.parquet   # driver: scoring events (§2.1)
    customer_profile/            # Kafka Connect S3 sink output (CDC), Delta
  silver/                         # cleaned, typed, deduped, PII-tagged  (Delta)
    aecb_credit_report/
    fraud_score/
    aml_screening/
    customer_profile/
    customer_identity_xref/      # golden-record cross-reference (§6)
    decision_input/              # unified per-application decision input (§5)
    decision_input_snapshot/     # immutable audit snapshots (§7)
  gold/                           # marts, aggregates  (Delta)
    portfolio_monitoring_daily/
    dq_scorecard_daily/
  _checkpoints/                   # Structured Streaming / job checkpoints
  _athena_results/                # Athena query output location
```

- **Bronze** is Parquet (or Delta for the CDC sink). Partitioned by `ingest_date`.
- **Silver + Gold** are **Delta Lake** tables in S3, registered in the Glue Data Catalog, queried by Athena.
- Incremental processing: **Glue job bookmarks** for the batch (SFTP/API) sources; Delta MERGE for CDC.
  Later migration path to EMR is documented but the same PySpark code runs on both.

---

## 4. Glue Data Catalog databases

| Database | Purpose |
|---|---|
| `credit_bronze` | Raw landing tables |
| `credit_silver` | Cleaned/conformed + identity xref + decision inputs + snapshots |
| `credit_gold` | Portfolio + DQ marts |

Table names are `snake_case`, singular entity where it is a dimension, per the Wio data-modeling
standard. Athena queries these via the Glue catalog.

---

## 5. `decision_input` — the unified decision record (silver)

One row per **credit decision** (a scoring event for an application). Grain: `one row per decision_id`.

Key columns (full DDL in `athena/ddl/`):

```
decision_id                STRING NOT NULL   -- UUID, one per scoring event
application_id             STRING NOT NULL   -- the credit application
master_customer_id         STRING NOT NULL   -- golden record id from identity xref (§6)
internal_customer_uuid     STRING            -- PostgreSQL spine id
product_code               STRING            -- PERSONAL_FINANCE | BNPL | CARD_ALT
decision_timestamp         TIMESTAMP NOT NULL -- GST (UTC+4)
-- AECB inputs
aecb_credit_score          INT
aecb_total_outstanding_aed DECIMAL(18,2)
aecb_report_ref            STRING
-- Fraud inputs
fraud_score                DECIMAL(5,4)      -- 0.0000-1.0000
fraud_decision             STRING            -- APPROVE | REVIEW | DECLINE
-- AML inputs
aml_status                 STRING            -- CLEAR | HIT | PENDING
is_pep                     BOOLEAN
-- Internal profile inputs
monthly_income_aed         DECIMAL(18,2)
kyc_completed              BOOLEAN
-- traceability
input_completeness_score   DECIMAL(5,4)      -- fraction of expected inputs present
dq_pass                    BOOLEAN           -- all must-pass DQ rules green
snapshot_s3_uri            STRING            -- pointer to immutable snapshot (§7)
source_system              STRING
batch_id                   STRING
created_timestamp          TIMESTAMP NOT NULL
updated_timestamp          TIMESTAMP NOT NULL
```

---

## 6. Identity resolution / conflict resolution (silver `customer_identity_xref`)

Produces the **golden record**: maps every source's native key to one `master_customer_id`.

Grain: `one row per master_customer_id` (current state). History kept in an SCD2 sidecar.

Matching strategy (deterministic-first, then probabilistic fallback):

1. **Spine** = PostgreSQL `internal_customer_uuid`. Each spine row seeds one `master_customer_id`
   (a deterministic UUIDv5 of the internal uuid, so it is stable and reproducible).
2. **AECB** joins on normalised `emirates_id` (strip spaces/dashes) — deterministic. Emirates ID
   is also stored on the internal profile, so this is an exact join when present.
3. **Fraud** joins on normalised `phone` (E.164) **AND** lowercased `email` — deterministic when both
   match; if only one matches, it is a *candidate* resolved by the probabilistic scorer.
4. **AML** joins on `soundex(full_name)` + `date_of_birth` — fuzzy by construction; always goes through
   the probabilistic scorer.

**Conflict resolution rules** when a source row matches >1 spine candidate (or none):

| Situation | Rule |
|---|---|
| Exact deterministic match on a strong key (EID, internal uuid) | Accept, `match_confidence = 1.00`, `match_method = DETERMINISTIC` |
| Multiple candidates | Score each candidate (weighted Jaro-Winkler on name, exact on dob/phone/email/eid); take the **highest** score above `MATCH_THRESHOLD = 0.85`; record `match_confidence` |
| Best score below `0.85` but above `REVIEW_THRESHOLD = 0.70` | Attach but flag `needs_manual_review = TRUE` |
| Best score below `0.70` | Do **not** attach; create an **unresolved** record so no source data is silently dropped |
| Conflicting attribute values across sources for a matched customer | Apply **survivorship**: source priority `POSTGRES > AECB > FRAUD > AML` for demographics; most-recent-timestamp wins within the same priority |

Every match decision is written to `customer_identity_xref` with: `match_method`, `match_confidence`,
`matched_on` (which keys fired), `needs_manual_review`, and full audit timestamps. **Nothing is dropped.**

---

## 7. Decision traceability — immutable snapshots (silver `decision_input_snapshot`)

For every `decision_id`, the exact bytes of all inputs used are frozen so a regulator can reconstruct
precisely what the credit engine saw.

- On each decision, the pipeline writes a **single immutable JSON object** to
  `s3://…/silver/decision_input_snapshot/decision_date=YYYY-MM-DD/decision_id=<uuid>/snapshot.json`
  containing: the resolved `master_customer_id`, and the **raw** AECB / fraud / AML / profile records
  (verbatim, plus their bronze S3 URIs and record hashes).
- The object is written with an **S3 Object Lock (compliance mode) retention** (see IaC) so it cannot
  be altered or deleted for the regulatory retention period (7 years).
- A `content_sha256` of the snapshot is stored in the Delta table `decision_input_snapshot` so any
  tampering is detectable. The Delta row is the queryable index; the S3 object is the legal record.

---

## 8. Data quality scorecard (gold `dq_scorecard_daily`)

Two tiers of checks run on `decision_input` before it is marked `dq_pass = TRUE`:

- **MUST-PASS (blocking)** — a failure quarantines the row (it does not reach gold / the risk mart):
  - `decision_id`, `application_id`, `master_customer_id` non-null & unique.
  - `fraud_score` between 0 and 1; `aecb_credit_score` between 300 and 900 when present.
  - `product_code` in the allowed enum.
  - `decision_timestamp` not in the future, not older than 48h at load.
  - identity match: `master_customer_id` resolved (not the unresolved sentinel).
- **WARN (non-blocking, alert)** — recorded, dashboarded, alert if threshold breached:
  - input completeness (`input_completeness_score`) ≥ 0.75 for ≥ 95% of rows.
  - AML `PENDING` rate ≤ 5%.
  - source freshness within SLA (AECB < 24h, fraud < 1h, AML < 6h, profile CDC < 15m).

Alert thresholds live in `dq/soda/` (Soda Core checks) and are mirrored by the Glue DQ job which
writes per-day pass/fail counts and a 0–100 `dq_score` to `dq_scorecard_daily`.

---

## 9. Portfolio monitoring mart (gold `portfolio_monitoring_daily`)

Grain: `one row per snapshot_date × product_code × decision_outcome_band × risk_band`.

Serves the risk team's Metabase dashboards. Columns include decision volumes, approval rate,
average fraud score, AML-hit rate, PEP exposure, average AECB score, average requested/approved
amount in AED, and the day's `dq_score`. Money is always `DECIMAL(18,2)`; no `FLOAT`.

---

## 10. Naming & typing rules (from Wio data-modeling standards)

- `snake_case` everywhere; English; singular columns.
- Temporal: `*_timestamp` → `TIMESTAMP` (GST), `*_date` → `DATE`.
- Boolean: `is_*` / `has_*` / `*_completed`. Never `0/1` INT flags.
- Money: `{amount}_{iso_ccy}` → `DECIMAL(18,2)`. Never `FLOAT`/`DOUBLE`.
- Score/probability: `DECIMAL(5,4)` for 0–1, `DECIMAL(5,2)` for 0–100.
- PII tagging in Delta column `COMMENT` using `PII Level 1|2|3` (EID/passport = L1, phone/email/dob/address = L2, derived = L3).
- Silver carries audit block: `source_system`, `batch_id`, `created_timestamp`, `updated_timestamp`.
- Gold exposes natural keys only — no surrogate `_sk`, no `is_current`/`effective_*`.

---

## 11. Constants (used across code)

```
MATCH_THRESHOLD       = 0.85
REVIEW_THRESHOLD      = 0.70
SURVIVORSHIP_PRIORITY = ["POSTGRES", "AECB", "FRAUD", "AML"]
UNRESOLVED_SENTINEL   = "UNRESOLVED"
SNAPSHOT_RETENTION_YEARS = 7
TZ                    = "Asia/Dubai"   # GST, UTC+4
```
