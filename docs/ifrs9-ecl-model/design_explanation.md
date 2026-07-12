# IFRS 9 ECL Provisioning — Design Choices (1-page)

**Part 3 (optional extension)** of the Credit Decision Data Platform. A dimensional risk-data model
that computes Expected Credit Loss (**ECL = PD × LGD × EAD**, discounted, scenario-weighted) with
3-stage classification, point-in-time vs through-the-cycle parameter versioning, and a full audit
trail. It is **runnable** — `run_demo.py` executes the schema, seed, and every sample query in DuckDB.

**Dimensional model & grain.** Slow attributes live in dimensions (`dim_exposure`, `dim_rating_grade`,
`dim_stage`, `dim_scenario`); time-varying facts are captured at a **monthly grain**
(`fact_exposure_snapshot` = one row per exposure per reporting date). Everything downstream keys off
`(reporting_date, exposure_id)`, and each exposure carries the Part 1 `master_customer_id`, so the
provisioning model plugs straight onto the golden record from the decision platform.

**PiT vs TTC parameter versioning (the centrepiece).** Risk parameters are **immutable, effective-dated
versions** in `risk_parameter_set`, split by `parameter_basis`: **PiT** sets are macro-conditioned (tied
to a `dim_scenario` and reporting period — used for the forward-looking IFRS 9 ECL) while **TTC** sets are
long-run averages (scenario-independent — used for Basel/benchmarking and stable SICR thresholds). A set
is bitemporal: `effective_from/to_date` is business-time validity; `created_timestamp` is system time.
The PD term structure (`pd_term_structure`, cumulative PD by grade × horizon), LGD (`lgd_parameter`,
downturn) and EAD CCFs (`ccf_parameter`) all hang off a `parameter_set_id`, so switching a scenario or
re-versioning a model is a new set, never an in-place edit — and every provision records exactly which
versions produced it.

**Staging logic (IFRS 9 5.5).** `fact_staging_assessment` records the stage **and why**:
Stage 3 on the default definition (90-DPD backstop **or** unlikely-to-pay); Stage 2 on SICR —
relative lifetime-PD deterioration ≥ 2.0×, 30-DPD backstop, forbearance, or watchlist; Stage 1 otherwise.
The trigger is stored (`sicr_trigger`) so an examiner sees the driver, not just the outcome
(`queries/01_stage_classification.sql`).

**ECL computation.** `EAD = drawn + CCF × undrawn`; PD is 12-month (Stage 1) or lifetime (Stage 2),
and a defaulted exposure carries **PD = 100%** (Stage 3); LGD is the downturn rate for the collateral
segment; losses are discounted at the EIR to the expected time of loss. Each of the base/upside/downside
scenarios is computed against **its own PiT set** and then **probability-weighted**
(`scenario_weight`) into the reported provision (`queries/02_provision_calculation.sql`). On the sample
book this yields the expected staging gradient — Stage 1 ≈ 0.5%, Stage 2 ≈ 7.7%, Stage 3 ≈ 48% coverage.

**Audit trail & reproducibility.** `audit_stage_transition` logs every period-over-period migration
(direction, trigger, DPD/PD at transition) — the reconciliation regulators require.
`audit_provision_calculation` freezes, per provision, the model version, the exact PiT `parameter_set_id`s,
the scenario weights, the inputs, and a **SHA-256 of those inputs** — the same tamper-evident,
fully-reproducible philosophy as Part 1's immutable decision snapshots, so any provision can be
re-derived and defended years later.

**Deliberate simplifications (demo scope).** Lifetime ECL uses a cumulative-PD-to-horizon shortcut
rather than a full period-by-period marginal-PD survival curve; discounting uses the expected-time-of-loss
midpoint rather than per-period discounting; LGD is held constant across scenarios (PD carries the macro
sensitivity). All three are the first things to deepen for production, and none change the schema — they
are richer parameter tables and a marginal-loss calculation over the same grain.
