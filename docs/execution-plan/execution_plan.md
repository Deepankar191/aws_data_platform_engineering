# Credit Decision Data Platform — 30 / 60 / 90-Day Execution Plan

**Owner:** Data Engineering Lead
**Audience:** Data Engineering, Platform/DevOps, Risk, Compliance, Product
**Status:** Plan of record for Q2 2025 go-live
**Companion documents:** [`../SPEC.md`](../SPEC.md) (contract) · [`../architecture/architecture.md`](../architecture/architecture.md) (design)

---

## 1. Executive summary

We are delivering the platform specified in [`SPEC.md`](../SPEC.md): a medallion lakehouse on AWS that
fuses four sources into a unified, immutable, audit-grade credit `decision_input` for three products
(`PERSONAL_FINANCE`, `BNPL`, `CARD_ALT`), resolves customer identity across sources, snapshots every
input immutably for UAE Central Bank audit, scores data quality, and serves a portfolio-monitoring mart
to the risk team.

This plan splits work into three 30-day tranches with a hard line between **LAUNCH-CRITICAL** (everything
required to go live at **10K decisions/day in Q2 2025**) and **POST-LAUNCH** (hardening, then scale to
**100K/day**, EMR migration, ML scoring, advanced MDM).

- **Day 0–30 — Foundation & launch-critical.** Landing zone, medallion S3 with Object Lock, RDS +
  Debezium CDC, MSK, all four ingestion paths, Bronze→Silver Glue jobs, deterministic identity resolution
  (v1), `decision_input`, immutable snapshots, must-pass DQ, Athena, a minimal risk mart, Airflow
  orchestration. **Outcome: a thin but complete, compliant end-to-end slice that can go live.**
- **Day 31–60 — Hardening.** Probabilistic matching, full DQ scorecard + SNS alerting, portfolio mart +
  Metabase dashboards, backfill/replay, observability, DR, and a formal security + UAE-CB audit-trail
  review. **Outcome: production-grade reliability and a passed compliance review.**
- **Day 61–90 — Scale & optimise.** 100K/day load test, EMR migration evaluation, cost optimisation,
  partition/compaction tuning (`OPTIMIZE`/`Z-ORDER`), advanced MDM + stewardship queue, ML feature
  groundwork, and the post-launch roadmap. **Outcome: proven headroom to 100K/day and a costed roadmap.**

### Goals

| # | Goal | Tied to |
|---|---|---|
| G1 | Go live at 10K decisions/day in Q2 2025 with all four sources fused | SPEC §1, §2 |
| G2 | Every decision immutably snapshotted, 7-yr WORM, tamper-evident | SPEC §7 |
| G3 | No source data silently dropped (unresolved records materialised) | SPEC §6 |
| G4 | Must-pass DQ enforced before any row reaches the risk mart | SPEC §8 |
| G5 | Proven, costed path to 100K/day within 12 months | SPEC §1, §3 |
| G6 | Pass UAE Central Bank audit-trail + data-residency review | SPEC §7, §10 |

### Success metrics (measured continuously post-launch)

| Metric | Launch target | Scale target |
|---|---|---|
| Decisions processed/day | 10,000 | 100,000 |
| End-to-end freshness (application → decision_input) | ≤ 60 min | ≤ 30 min |
| Snapshot coverage (decisions with an Object-Lock snapshot) | 100% | 100% |
| Must-pass DQ enforcement (bad rows quarantined, not served) | 100% | 100% |
| Identity resolution rate (attached, not UNRESOLVED) | ≥ 97% | ≥ 99% |
| `needs_manual_review` backlog worked within SLA | ≤ 2 business days | ≤ 1 business day |
| Source-freshness SLA adherence (AECB<24h, fraud<1h, AML<6h, CDC<15m) | ≥ 99% | ≥ 99.9% |
| Pipeline cost per 1K decisions | baseline established | ≥ 40% below launch baseline |

---

## 2. Day 0–30 — Foundation & launch-critical

> **Theme:** stand up a thin, complete, compliant end-to-end slice. Everything in this tranche is
> **LAUNCH-CRITICAL** — no item here is optional for go-live.

| ID | Workstream | Deliverable | SPEC ref | Launch-critical |
|---|---|---|---|---|
| F1 | Landing zone | AWS Organizations, `dev/pre/prod` accounts, **me-central-1** region pinned, IAM baseline, KMS keys, VPC + private subnets + NAT, CloudTrail on | §10, compliance | ✅ |
| F2 | Medallion S3 | Bucket `wio-credit-decision-${ENV}` with `bronze/silver/gold/_checkpoints/_athena_results`; SSE-KMS; lifecycle stubs | §3 | ✅ |
| F3 | Object Lock bucket | Separate snapshot bucket, **Object Lock compliance mode**, default 7-yr retention, versioning | §7 | ✅ |
| F4 | Glue catalog | Databases `credit_bronze`, `credit_silver`, `credit_gold` | §4 | ✅ |
| F5 | RDS + CDC | RDS PostgreSQL (identity spine), logical replication on, **Debezium** source connector | §2, §6 | ✅ |
| F6 | MSK + sink | MSK cluster, topic `cdc.public.customer_profile`, **MSK Connect S3 sink** → Delta `bronze/customer_profile/` | §2, §3 | ✅ |
| F7 | Ingestion — AECB | Glue batch job: SFTP pull → parse XML → Parquet `bronze/aecb/ingest_date=…`; **job bookmark** | §2, §3 | ✅ |
| F8 | Ingestion — Fraud | Scheduled Glue poll job: REST GET → JSON → Parquet `bronze/fraud/…`; bookmark | §2 | ✅ |
| F9 | Ingestion — AML | API Gateway + Lambda webhook receiver → Parquet `bronze/aml/…` | §2 | ✅ |
| F10 | Bronze→Silver | Glue PySpark jobs → `aecb_credit_report`, `fraud_score`, `aml_screening`, `customer_profile` (typed, deduped, **PII-tagged**, audit block); CDC via **Delta MERGE** | §3, §5, §10 | ✅ |
| F11 | Identity resolution v1 | **Deterministic-only**: spine seeding `UUIDv5(internal_customer_uuid)`, EID join, phone+email join; UNRESOLVED sentinel for the rest; write `customer_identity_xref` | §6, §11 | ✅ |
| F12 | `decision_input` | Assemble unified row per `decision_id`; Delta MERGE keyed on `decision_id`; populate traceability columns | §5 | ✅ |
| F13 | Immutable snapshots | Per-decision JSON (raw records + bronze URIs + hashes) → Object-Lock bucket; `content_sha256` → `decision_input_snapshot` Delta index | §7 | ✅ |
| F14 | Must-pass DQ | Blocking checks (nulls/uniqueness, score ranges, enum, timestamp window, identity resolved); **quarantine** on fail; set `dq_pass` | §8 | ✅ |
| F15 | Athena | Workgroup, `_athena_results` output, catalog registration; smoke queries | §3, §4 | ✅ |
| F16 | Minimal risk mart | `portfolio_monitoring_daily` (core columns only — volumes, approval rate, avg scores) so risk has day-1 visibility | §9 | ✅ |
| F17 | Orchestration | **Amazon MWAA (Airflow)** DAG: ingest → Silver → identity → decision_input → snapshot → DQ → mart; retries, SLAs | §3 | ✅ |
| F18 | DDL + repo hygiene | `athena/ddl/` DDLs matching SPEC §5/§9 typing rules; naming-standard lint in CI | §5, §9, §10 | ✅ |

**Exit criteria for Day 30 (go/no-go gate):**

- An application flows end-to-end and produces a `decision_input` row **with** a matching Object-Lock
  snapshot and a verifiable `content_sha256`.
- Must-pass DQ demonstrably quarantines a deliberately bad row (it does not reach the mart).
- Deterministic identity resolution attaches AECB (EID) and Fraud (phone+email); AML + partial matches
  land as `UNRESOLVED` (nothing dropped).
- Athena returns the minimal risk mart; risk team can see day-1 volumes.
- Everything runs on schedule from MWAA in `pre`, in **me-central-1**.

---

## 3. Day 31–60 — Hardening

> **Theme:** turn the launch slice into a production-grade, observable, recoverable, compliance-reviewed
> system. Mostly **LAUNCH-CRITICAL for a *robust* launch**; a few items are explicitly POST-LAUNCH.

| ID | Workstream | Deliverable | SPEC ref | Class |
|---|---|---|---|---|
| H1 | Probabilistic matching | Scorer (weighted Jaro-Winkler on name, exact on dob/phone/email/eid); thresholds `MATCH=0.85`, `REVIEW=0.70`; `needs_manual_review` flag; **survivorship** `POSTGRES>AECB>FRAUD>AML` | §6, §11 | Launch-critical |
| H2 | Full DQ scorecard | Add WARN tier (completeness ≥0.75 for ≥95%, AML PENDING ≤5%, source-freshness SLAs); write `dq_scorecard_daily` with 0–100 `dq_score`; Soda Core checks in `dq/soda/` | §8 | Launch-critical |
| H3 | Alerting | **SNS** topics wired to DQ must-pass failures, WARN-threshold breaches, freshness-SLA misses, pipeline failures; routed to on-call | §8 | Launch-critical |
| H4 | Portfolio mart (full) | Complete `portfolio_monitoring_daily` grain `snapshot_date × product_code × outcome_band × risk_band` — approval rate, avg fraud, AML-hit rate, PEP exposure, avg AECB, requested/approved AED, day `dq_score` | §9 | Launch-critical |
| H5 | Metabase | Metabase on ECS Fargate; risk dashboards on the mart (portfolio + DQ scorecard); access controls | §9 | Launch-critical |
| H6 | Backfill / replay | Idempotent reprocessing by partition; bookmark reset runbook; Delta MERGE re-runs proven safe; snapshot write-once respected | §3, §7 | Launch-critical |
| H7 | Observability | CloudWatch dashboards + metrics (per-source volumes, freshness, resolution rate, DQ pass rate, job durations); structured logs; run-book links on alarms | §8 | Launch-critical |
| H8 | Disaster recovery | Cross-region/versioned backups (excl. Object-Lock which is inherently durable), RTO/RPO defined + tested restore of catalog + Silver | compliance | Launch-critical |
| H9 | Security review | Least-privilege IAM audit, KMS key policy review, secrets in Secrets Manager, network review, PII-tag coverage check | §10, security | Launch-critical |
| H10 | UAE CB audit-trail validation | Formal test: reconstruct N random past decisions from Object-Lock snapshots; prove immutability + tamper detection; document retention (7 yr) + residency | §7, §10 | Launch-critical |
| H11 | Stewardship queue (basic) | Surface `needs_manual_review` rows to a simple queue/Athena view for manual resolution | §6 | POST-LAUNCH (basic in this tranche) |
| H12 | Load smoke | 25–50K/day synthetic burst to find early bottlenecks ahead of the full 90-day load test | §1 | Launch-critical |

**Exit criteria for Day 60:**

- Probabilistic matching lifts resolution rate to ≥ 97%; review-band rows are flagged and queued.
- Full DQ scorecard + SNS alerting live; a breached WARN threshold actually pages on-call.
- Risk team is using Metabase dashboards off the full portfolio mart.
- **Security review and UAE-CB audit-trail validation both signed off** (compliance gate).
- DR restore tested; backfill/replay runbook exercised once.

---

## 4. Day 61–90 — Scale & optimise

> **Theme:** prove and cost the path to 100K/day, and lay POST-LAUNCH groundwork. Almost everything here
> is **POST-LAUNCH** — it is not required for the 10K/day go-live but de-risks the 12-month scale target.

| ID | Workstream | Deliverable | SPEC ref | Class |
|---|---|---|---|---|
| S1 | 100K/day load test | Synthetic 10× load; measure freshness, job durations, MSK lag, cost; find + fix bottlenecks | §1, §3 | POST-LAUNCH |
| S2 | EMR migration eval | Run the identity + mart jobs on EMR / EMR-Serverless (same PySpark); compare cost + runtime vs Glue; recommend hybrid split | §3 | POST-LAUNCH |
| S3 | Cost optimisation | S3 lifecycle (cold bronze → IA/Glacier), Athena partition projection + result reuse, Glue DPU right-sizing, reserved capacity where justified | §3 | POST-LAUNCH |
| S4 | Compaction / layout | Scheduled Delta `OPTIMIZE` + `Z-ORDER` on `master_customer_id` / `decision_id`; add `product_code` sub-partition on hot marts; vacuum policy (retain > snapshot needs) | §3 | POST-LAUNCH |
| S5 | Advanced MDM | Full stewardship UI/queue, merge/split of golden records, SCD2 history surfacing, survivorship overrides with audit | §6 | POST-LAUNCH |
| S6 | ML groundwork | Feature tables off Silver/Gold for fraud + credit models; leakage-safe `feature_as_of < label_as_of`; offline eval; **no scoring in the credit decision path until model-risk sign-off** | §5, §8 | POST-LAUNCH |
| S7 | Post-launch roadmap | Costed 12-month roadmap: 100K/day cutover plan, EMR hybrid, ML rollout, additional products/sources | §1 | POST-LAUNCH |
| S8 | Scale hardening | MSK partition/connector-task scaling, probabilistic-scorer bucketing/salting, DQ parallelised per partition | §3, §6, §8 | POST-LAUNCH |

**Exit criteria for Day 90:**

- 100K/day sustained in load test within freshness SLA; bottlenecks documented + addressed.
- EMR-vs-Glue recommendation delivered with numbers; hybrid split decided.
- `OPTIMIZE`/`Z-ORDER` + lifecycle cut cost-per-1K-decisions measurably below the launch baseline.
- Post-launch roadmap approved by Data Eng + Risk + Product.

---

## 5. Milestone table

| Milestone | Target day | Gate | Owner |
|---|---|---|---|
| M0 — Landing zone + medallion S3 + Object Lock live in `dev` | Day 10 | Infra review | Platform/DevOps |
| M1 — All four ingestion paths landing to Bronze | Day 18 | Data-in-Bronze check | Data Eng |
| M2 — Bronze→Silver + deterministic identity + `decision_input` | Day 24 | Silver conformance | Data Eng |
| M3 — Immutable snapshots + must-pass DQ + minimal mart + Athena | Day 30 | **Go/No-Go for pre** | Data Eng Lead |
| M4 — Probabilistic matching + full DQ scorecard + alerting | Day 45 | DQ sign-off | Data Eng |
| M5 — Portfolio mart + Metabase + DR + backfill | Day 54 | Risk UAT | Risk + Data Eng |
| M6 — Security + UAE-CB audit-trail review passed | Day 60 | **Compliance go-live gate** | Compliance + Security |
| **GO-LIVE (10K/day, Q2 2025)** | Day 60–65 | Exec sign-off | Data Eng Lead + Product |
| M7 — 100K/day load test passed | Day 78 | Perf review | Data Eng + Platform |
| M8 — EMR eval + cost optimisation + roadmap | Day 90 | Roadmap approval | Data Eng Lead |

---

## 6. Ownership (RACI)

**Roles:** DEL = Data Eng Lead · DE = Data Engineer(s) · PLT = Platform/DevOps · RISK = Risk team ·
COMP = Compliance · SEC = Security · PM = Product.

| Workstream | R | A | C | I |
|---|---|---|---|---|
| Landing zone / networking / IAM / KMS (F1) | PLT | DEL | SEC | RISK |
| Medallion S3 + Object Lock (F2, F3) | PLT | DEL | COMP | RISK |
| RDS + Debezium + MSK (F5, F6) | DE | DEL | PLT | — |
| Ingestion paths (F7–F9) | DE | DEL | PLT | — |
| Bronze→Silver + typing/PII (F10) | DE | DEL | COMP | RISK |
| Identity resolution v1 + probabilistic (F11, H1) | DE | DEL | RISK | COMP |
| `decision_input` + snapshots (F12, F13) | DE | DEL | COMP | RISK |
| DQ (must-pass + scorecard + alerts) (F14, H2, H3) | DE | DEL | RISK | PM |
| Athena + marts + Metabase (F15, F16, H4, H5) | DE | DEL | RISK | PM |
| Orchestration (F17) | DE | DEL | PLT | — |
| Observability + DR (H7, H8) | PLT | DEL | DE | RISK |
| Security review (H9) | SEC | DEL | PLT | COMP |
| UAE-CB audit-trail validation (H10) | COMP | DEL | SEC | RISK |
| Load test / EMR / cost / compaction (S1–S4) | DE | DEL | PLT | PM |
| Advanced MDM + stewardship (S5) | DE | DEL | RISK | COMP |
| ML groundwork (S6) | DE | DEL | RISK | COMP |
| Roadmap (S7) | DEL | PM | RISK | SEC |

---

## 7. Risks & mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Object Lock misconfigured → snapshots mutable/deletable (regulatory breach) | Low | Critical | Compliance-mode + default retention set at bucket creation (F3); H10 validation explicitly tries and fails to delete/alter |
| R2 | Identity resolution drops or mis-merges customers | Med | High | UNRESOLVED sentinel guarantees no drops (F11); review band + stewardship (H1, H11); survivorship rules from §6; audit every match decision |
| R3 | Debezium/MSK CDC lag or gaps → stale spine | Med | High | CDC-freshness SLA <15m alarmed (H3); MSK partition/task scaling (S8); replay from WAL/offset runbook (H6) |
| R4 | AECB SFTP delays/format drift break batch ingest | Med | Med | Bookmarks make reruns safe (F7); freshness alarm <24h; schema-drift guard + quarantine on Bronze parse |
| R5 | Glue cost/runtime doesn't scale to 100K/day | Med | High | Early load smoke at 25–50K (H12); full 100K test (S1); EMR hybrid path pre-designed (SPEC §3, arch §6) |
| R6 | Data residency / PII exposure | Low | Critical | Region pinned to me-central-1 (F1); SSE-KMS; PII tagging (F10); security review (H9) |
| R7 | Metabase/Athena exposes PII to risk users | Low | High | Gold exposes natural keys only, no raw PII (SPEC §10); L1/L2 columns not surfaced in marts; access controls (H5) |
| R8 | DQ false-positives quarantine good rows / alert fatigue | Med | Med | Two-tier design — only must-pass blocks (§8); WARN tuned against real distributions in H2; thresholds in `dq/soda/` versioned |
| R9 | Scope creep pulls POST-LAUNCH work into launch window | Med | Med | Hard launch-critical vs post-launch split in this plan; M3/M6 gates enforce it |
| R10 | Snapshot volume/cost at 100K/day | Med | Med | Snapshots are compact JSON; S3 lifecycle + Glacier for cold, but Object-Lock retention respected (S3, S4) |

---

## 8. Dependencies

**External / upstream:**

- AECB SFTP credentials, endpoint, and XML schema/sample files (blocks F7).
- Fraud provider REST API credentials + rate limits (blocks F8).
- AML provider webhook contract + signing secret (blocks F9).
- PostgreSQL logical-replication enablement + Debezium user grants (blocks F5, F6).
- UAE Central Bank retention + residency requirements confirmed in writing (blocks H10 sign-off).

**Internal / platform:**

- AWS Organizations + me-central-1 access provisioned (blocks F1 → everything).
- KMS keys + IAM roles approved by Security (blocks F1, F2, F3).
- MWAA environment provisioned (blocks F17 orchestration of everything).
- Metabase hosting decision (ECS Fargate) + risk-team accounts (blocks H5).

**Sequencing:** F1 → F2/F3/F4 → (F5,F6 CDC ∥ F7–F9 batch/API) → F10 → F11 → F12 → F13 → F14 → F15/F16 →
F17 wires it. Hardening (H*) layers on the launch slice; Scale (S*) follows go-live.

---

## 9. Compliance (UAE Central Bank)

Compliance is a **go-live gate (M6)**, not an afterthought. The platform is designed to satisfy the UAE
Central Bank on five fronts:

| Requirement | How the platform meets it | SPEC / plan ref |
|---|---|---|
| **Full audit trail of every decision** | Immutable per-decision JSON snapshot capturing the resolved `master_customer_id` and **verbatim raw** AECB/fraud/AML/profile records with Bronze URIs + record hashes — exact reconstruction of what the engine saw | §7 · F13 · arch §5 |
| **7-year immutable retention** | Snapshot bucket in **S3 Object Lock compliance mode**, default retention `SNAPSHOT_RETENTION_YEARS = 7`; neither operators nor root can alter/delete | §7 · F3 · R1 |
| **Tamper evidence** | `content_sha256` of each snapshot stored in the queryable `decision_input_snapshot` Delta index; re-hash-and-compare proves integrity | §7 · F13 · H10 |
| **PII handling** | PII tagged in Delta column comments (`PII Level 1|2|3`); L1/L2 never exposed in Gold marts; SSE-KMS at rest; least-privilege IAM; no raw PII in Metabase | §10 · F10 · H9 · R7 |
| **Data residency** | Entire stack pinned to **AWS UAE region `me-central-1`**; no cross-region replication of regulated data (DR uses in-region/versioned strategy that respects residency) | F1 · H8 · R6 |

**Audit-trail validation (H10)** is an explicit, evidenced exercise before go-live: pick N random historical
decisions, reconstruct each from its Object-Lock snapshot, prove the object cannot be modified or deleted,
and demonstrate that a tampered byte is caught by the `content_sha256` check. The result is documented and
countersigned by Compliance and Security as the M6 gate.

**Identity auditability** — every match decision in `customer_identity_xref` records `match_method`,
`match_confidence`, `matched_on`, `needs_manual_review`, and audit timestamps (§6), so a regulator can also
audit *how* a customer's records were fused, not just the inputs.

---

## 10. Launch-critical vs post-launch — one-glance summary

| LAUNCH-CRITICAL (Q2 2025, 10K/day) | POST-LAUNCH (→ 100K/day, 12 mo) |
|---|---|
| Landing zone, medallion S3, Object Lock, catalog | 100K/day load-tested scale-out |
| All 4 ingestion paths + CDC | EMR migration (hybrid Glue/EMR) |
| Bronze→Silver, PII tagging, audit block | Cost optimisation + S3 lifecycle |
| **Deterministic** identity resolution + UNRESOLVED | `OPTIMIZE`/`Z-ORDER` + partition tuning |
| `decision_input` + immutable snapshots | Advanced MDM + full stewardship UI |
| Must-pass DQ (blocking) | Full ML fraud/credit scoring in-path |
| Full DQ scorecard + SNS alerting | Additional products/sources |
| Portfolio mart + Metabase | — |
| Probabilistic matching + survivorship | — |
| Observability, DR, backfill/replay | — |
| Security + UAE-CB audit-trail sign-off | — |

> The dividing principle: **anything required for a correct, compliant, observable 10K/day launch is
> launch-critical; anything that only matters for 10× scale, cost, or ML sophistication is post-launch.**
