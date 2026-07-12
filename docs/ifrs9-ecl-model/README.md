# IFRS 9 ECL Provisioning — data model (Part 3, optional extension)

A dimensional risk-data model that computes **Expected Credit Loss (ECL = PD × LGD × EAD)** under
IFRS 9, with 3-stage classification, **point-in-time (PiT) vs through-the-cycle (TTC)** parameter
versioning, forward-looking scenario weighting, and a full audit trail — and it **actually runs**.

> **1-page design explanation:** [`design_explanation.md`](./design_explanation.md) (also as
> `design_explanation.pdf`). Read that first for the *why*.

## What's here

| Path | What it is |
|---|---|
| [`schema/schema.sql`](./schema/schema.sql) | The full model — dimensions, versioned risk parameters (PiT/TTC), exposure/staging/ECL facts, and the two audit tables. ANSI SQL (runs in DuckDB; deploys as Delta in a `credit_risk` catalog). |
| [`schema/sample_data.sql`](./schema/sample_data.sql) | Deterministic seed: 7 exposures engineered to hit every stage + SICR trigger, PD term structures for a TTC set and base/upside/downside PiT sets, LGD/CCF, and scenario weights. |
| [`queries/01_stage_classification.sql`](./queries/01_stage_classification.sql) | **Stage classification logic** — SICR + default triggers → Stage 1/2/3, with the reason. |
| [`queries/02_provision_calculation.sql`](./queries/02_provision_calculation.sql) | **Provision calculation** — EAD × PD × LGD, per-scenario against its PiT set, probability-weighted, stage-appropriate. |
| [`queries/03_stage_transition_audit.sql`](./queries/03_stage_transition_audit.sql) | Period-over-period **stage-migration audit**. |
| [`queries/04_portfolio_ecl_summary.sql`](./queries/04_portfolio_ecl_summary.sql) | Portfolio provision + coverage **by stage**. |
| [`run_demo.py`](./run_demo.py) | Loads schema+seed, runs all four queries, materialises the fact/audit tables (incl. a real SHA-256 reproducibility hash), prints everything. |

## Run it

```bash
pip install duckdb
python3 docs/ifrs9-ecl-model/run_demo.py
```

## Verified output (from `run_demo.py`)

Stage classification and the resulting provisions produce the textbook IFRS 9 coverage gradient:

| Stage | Meaning | Exposures | EAD (AED) | Provision (AED) | Coverage |
|---|---|---|---|---|---|
| 1 | Performing (12-month ECL) | 1 | 120,000 | 610 | 0.51% |
| 2 | Underperforming — SICR (lifetime ECL) | 4 | 159,600 | 12,247 | 7.67% |
| 3 | Impaired / defaulted (lifetime ECL) | 2 | 92,750 | 44,816 | 48.32% |
| **Total** | | **7** | **372,350** | **57,673** | **15.49%** |

Each Stage-2 exposure demonstrates a different SICR trigger (PD deterioration, 30-DPD backstop,
forbearance, watchlist); Stage 3 covers both 90-DPD default and unlikely-to-pay. Downside-scenario ECL
exceeds base for every exposure (the PiT downside set carries higher PDs), and every provision row is
backed by an `audit_provision_calculation` entry with its model version, PiT parameter-set ids, scenario
weights, and a SHA-256 of the inputs.

## How it connects to Parts 1–2

Each `dim_exposure` carries the Part 1 `master_customer_id` (the golden record from identity resolution),
so provisioning runs on the same customer spine. The audit design mirrors Part 1's immutable decision
snapshots — reproducibility and tamper-evidence over the same platform.
