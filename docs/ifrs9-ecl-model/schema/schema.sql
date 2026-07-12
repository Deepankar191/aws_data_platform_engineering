-- =============================================================================
-- IFRS 9 Expected Credit Loss (ECL) provisioning — data model
-- -----------------------------------------------------------------------------
-- Part 3 (optional extension) of the Credit Decision Data Platform.
-- A dimensional risk-data model that computes ECL = PD x LGD x EAD under IFRS 9,
-- with 3-stage classification, point-in-time (PiT) vs through-the-cycle (TTC)
-- parameter versioning, forward-looking scenario weighting, and a full audit trail
-- for stage transitions and provision calculations.
--
-- Written in ANSI SQL that runs as-is in DuckDB (see run_demo.py). In the platform
-- it deploys as Delta tables in a `credit_risk` catalog (gold layer), fed monthly
-- from `decision_input` / core-banking positions. Conventions follow the repo's
-- data-modeling standard: snake_case; money DECIMAL(18,2); rates/probabilities
-- DECIMAL(9,8) in [0,1]; *_date DATE, *_timestamp TIMESTAMP (GST); is_/has_ booleans.
--
-- Grain notes are on each table. Money is AED unless suffixed otherwise.
-- =============================================================================

-- ------------------------------------------------------------------ DIMENSIONS

-- IFRS 9 stage lookup. Stage drives whether ECL is 12-month or lifetime.
CREATE TABLE dim_stage (
    stage_id            INTEGER      NOT NULL PRIMARY KEY,   -- 1, 2, 3
    stage_name          VARCHAR      NOT NULL,               -- PERFORMING / UNDERPERFORMING / IMPAIRED
    ecl_horizon         VARCHAR      NOT NULL,               -- 12M or LIFETIME
    interest_basis      VARCHAR      NOT NULL,               -- GROSS or NET (Stage 3 accrues on net carrying)
    description         VARCHAR
);

-- Internal rating master scale (ordinal). Lower grade_order = better credit.
CREATE TABLE dim_rating_grade (
    rating_grade        VARCHAR      NOT NULL PRIMARY KEY,   -- e.g. R1..R10, D
    grade_order         INTEGER      NOT NULL,               -- 1 (best) .. 11 (default)
    is_default_grade    BOOLEAN      NOT NULL DEFAULT FALSE,
    description         VARCHAR
);

-- Credit exposures (facilities). One row per facility, SCD-1 attributes here;
-- time-varying balances live in fact_exposure_snapshot.
CREATE TABLE dim_exposure (
    exposure_id             VARCHAR      NOT NULL PRIMARY KEY,   -- facility id
    master_customer_id      VARCHAR      NOT NULL,               -- links to Part 1 golden record (SPEC §6)
    product_type            VARCHAR      NOT NULL,               -- PERSONAL_FINANCE / BNPL / CARD_ALT / ...
    is_revolving            BOOLEAN      NOT NULL,               -- revolving (card/BNPL) vs term
    currency_code           VARCHAR      NOT NULL DEFAULT 'AED',
    origination_date        DATE         NOT NULL,
    maturity_date           DATE,                                -- NULL for open-ended revolving
    origination_rating_grade VARCHAR     NOT NULL,               -- grade at origination (SICR baseline)
    origination_lifetime_pd DECIMAL(9,8) NOT NULL,               -- lifetime PD at origination (SICR baseline)
    is_purchased_credit_impaired BOOLEAN NOT NULL DEFAULT FALSE, -- POCI treatment
    collateral_type         VARCHAR,                             -- drives LGD segment; NULL = unsecured
    FOREIGN KEY (origination_rating_grade) REFERENCES dim_rating_grade(rating_grade)
);

-- Forward-looking macroeconomic scenarios (probability-weighted per IFRS 9 5.5.17).
CREATE TABLE dim_scenario (
    scenario_id         VARCHAR      NOT NULL PRIMARY KEY,   -- BASE / UPSIDE / DOWNSIDE
    scenario_name       VARCHAR      NOT NULL,
    description         VARCHAR
);

-- ---------------------------------------------------- RISK PARAMETERS (VERSIONED)
-- The PiT-vs-TTC + versioning centrepiece. A parameter *set* is an immutable,
-- effective-dated version. PiT sets are macro-conditioned (tied to a scenario and
-- reporting period, used for IFRS 9 ECL); TTC sets are long-run averages
-- (scenario-independent, used for Basel/benchmarking and SICR relative thresholds).
-- Bitemporal: effective_from/to = business validity; created_timestamp = system time.
CREATE TABLE risk_parameter_set (
    parameter_set_id    VARCHAR      NOT NULL PRIMARY KEY,
    parameter_basis     VARCHAR      NOT NULL,               -- PIT or TTC
    macro_scenario_id   VARCHAR,                             -- set for PIT, NULL for TTC
    model_version       VARCHAR      NOT NULL,               -- e.g. pd_model v2.3
    effective_from_date DATE         NOT NULL,               -- business-time validity start
    effective_to_date   DATE,                                -- NULL = current
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    approved_by         VARCHAR      NOT NULL,               -- model-governance sign-off
    approved_timestamp  TIMESTAMP    NOT NULL,
    created_timestamp   TIMESTAMP    NOT NULL,               -- system-time (audit)
    CHECK (parameter_basis IN ('PIT','TTC')),
    CHECK ((parameter_basis = 'PIT' AND macro_scenario_id IS NOT NULL)
        OR (parameter_basis = 'TTC' AND macro_scenario_id IS NULL)),
    FOREIGN KEY (macro_scenario_id) REFERENCES dim_scenario(scenario_id)
);

-- PD term structure per parameter set: cumulative PD by rating grade and horizon.
-- Stage 1 reads horizon_months = 12; Stage 2/3 read the horizon nearest remaining life.
CREATE TABLE pd_term_structure (
    parameter_set_id    VARCHAR      NOT NULL,
    rating_grade        VARCHAR      NOT NULL,
    horizon_months      INTEGER      NOT NULL,               -- 12, 24, 36, ... up to max tenor
    marginal_pd         DECIMAL(9,8) NOT NULL,               -- PD in the horizon interval
    cumulative_pd       DECIMAL(9,8) NOT NULL,               -- cumulative PD to horizon (used for ECL)
    PRIMARY KEY (parameter_set_id, rating_grade, horizon_months),
    FOREIGN KEY (parameter_set_id) REFERENCES risk_parameter_set(parameter_set_id),
    FOREIGN KEY (rating_grade) REFERENCES dim_rating_grade(rating_grade),
    CHECK (cumulative_pd BETWEEN 0 AND 1)
);

-- LGD per parameter set and collateral segment (downturn LGD for IFRS 9).
CREATE TABLE lgd_parameter (
    parameter_set_id    VARCHAR      NOT NULL,
    collateral_segment  VARCHAR      NOT NULL,               -- UNSECURED / RESIDENTIAL_RE / VEHICLE / ...
    lgd_rate            DECIMAL(9,8) NOT NULL,               -- best-estimate LGD
    downturn_lgd_rate   DECIMAL(9,8) NOT NULL,               -- downturn-adjusted LGD (used in ECL)
    PRIMARY KEY (parameter_set_id, collateral_segment),
    FOREIGN KEY (parameter_set_id) REFERENCES risk_parameter_set(parameter_set_id),
    CHECK (downturn_lgd_rate BETWEEN 0 AND 1)
);

-- EAD credit-conversion factors for undrawn commitments, per parameter set/product.
CREATE TABLE ccf_parameter (
    parameter_set_id        VARCHAR      NOT NULL,
    product_type            VARCHAR      NOT NULL,
    credit_conversion_factor DECIMAL(9,8) NOT NULL,          -- share of undrawn expected to be drawn at default
    PRIMARY KEY (parameter_set_id, product_type),
    FOREIGN KEY (parameter_set_id) REFERENCES risk_parameter_set(parameter_set_id),
    CHECK (credit_conversion_factor BETWEEN 0 AND 1)
);

-- Scenario probability weights, versioned by reporting date (macro committee sets these).
CREATE TABLE scenario_weight (
    reporting_date      DATE         NOT NULL,
    scenario_id         VARCHAR      NOT NULL,
    probability_weight  DECIMAL(9,8) NOT NULL,               -- weights across scenarios sum to 1 per date
    PRIMARY KEY (reporting_date, scenario_id),
    FOREIGN KEY (scenario_id) REFERENCES dim_scenario(scenario_id),
    CHECK (probability_weight BETWEEN 0 AND 1)
);

-- --------------------------------------------------------------------- FACTS

-- Monthly exposure position. Grain: one row per exposure per reporting_date.
-- Supplies EAD inputs (drawn + CCF x undrawn), DPD, current grade, EIR for discount.
CREATE TABLE fact_exposure_snapshot (
    reporting_date          DATE         NOT NULL,
    exposure_id             VARCHAR      NOT NULL,
    drawn_balance_aed       DECIMAL(18,2) NOT NULL,          -- on-balance-sheet exposure
    undrawn_commitment_aed  DECIMAL(18,2) NOT NULL DEFAULT 0,-- off-balance limit available
    days_past_due           INTEGER      NOT NULL DEFAULT 0,
    current_rating_grade    VARCHAR      NOT NULL,
    current_lifetime_pd     DECIMAL(9,8) NOT NULL,           -- PiT lifetime PD now (SICR comparison)
    effective_interest_rate DECIMAL(9,8) NOT NULL,           -- EIR for discounting ECL
    remaining_maturity_months INTEGER    NOT NULL,
    collateral_value_aed    DECIMAL(18,2) NOT NULL DEFAULT 0,
    is_forborne             BOOLEAN      NOT NULL DEFAULT FALSE,
    is_watchlist            BOOLEAN      NOT NULL DEFAULT FALSE,
    is_unlikely_to_pay      BOOLEAN      NOT NULL DEFAULT FALSE, -- qualitative default trigger
    source_system           VARCHAR      NOT NULL DEFAULT 'CORE_BANKING',
    created_timestamp       TIMESTAMP    NOT NULL,
    PRIMARY KEY (reporting_date, exposure_id),
    FOREIGN KEY (exposure_id) REFERENCES dim_exposure(exposure_id),
    FOREIGN KEY (current_rating_grade) REFERENCES dim_rating_grade(rating_grade)
);

-- Staging assessment result. Grain: one row per exposure per reporting_date.
-- Records WHY a stage was assigned (SICR trigger), for transparency and audit.
CREATE TABLE fact_staging_assessment (
    reporting_date          DATE         NOT NULL,
    exposure_id             VARCHAR      NOT NULL,
    origination_lifetime_pd DECIMAL(9,8) NOT NULL,           -- SICR baseline
    current_lifetime_pd     DECIMAL(9,8) NOT NULL,
    pd_ratio                DECIMAL(12,6) NOT NULL,          -- current / origination (relative SICR)
    days_past_due           INTEGER      NOT NULL,
    is_sicr                 BOOLEAN      NOT NULL,           -- significant increase in credit risk
    sicr_trigger            VARCHAR,                         -- PD_DETERIORATION / DPD_30_BACKSTOP / FORBEARANCE / WATCHLIST / QUALITATIVE / NONE
    is_default              BOOLEAN      NOT NULL,           -- 90 DPD backstop or unlikely-to-pay
    assigned_stage          INTEGER      NOT NULL,           -- 1 / 2 / 3
    staging_model_version   VARCHAR      NOT NULL,
    assessed_timestamp      TIMESTAMP    NOT NULL,
    PRIMARY KEY (reporting_date, exposure_id),
    FOREIGN KEY (assigned_stage) REFERENCES dim_stage(stage_id)
);

-- Per-scenario ECL component. Grain: exposure x reporting_date x scenario.
-- Stores every input used, so any provision number is fully reproducible.
CREATE TABLE fact_ecl_scenario (
    reporting_date      DATE         NOT NULL,
    exposure_id         VARCHAR      NOT NULL,
    scenario_id         VARCHAR      NOT NULL,
    parameter_set_id    VARCHAR      NOT NULL,               -- the PiT set used (audit)
    assigned_stage      INTEGER      NOT NULL,
    ead_amount_aed      DECIMAL(18,2) NOT NULL,              -- drawn + CCF x undrawn
    pd_used             DECIMAL(9,8) NOT NULL,               -- 12m (Stage 1) or lifetime (Stage 2/3)
    lgd_used            DECIMAL(9,8) NOT NULL,
    discount_factor     DECIMAL(9,8) NOT NULL,               -- 1/(1+EIR)^t
    ecl_amount_aed      DECIMAL(18,2) NOT NULL,              -- EAD x PD x LGD x discount
    PRIMARY KEY (reporting_date, exposure_id, scenario_id),
    FOREIGN KEY (scenario_id) REFERENCES dim_scenario(scenario_id),
    FOREIGN KEY (parameter_set_id) REFERENCES risk_parameter_set(parameter_set_id)
);

-- Reported provision. Grain: one row per exposure per reporting_date.
-- Probability-weighted ECL across scenarios = the number that hits the P&L.
CREATE TABLE fact_ecl_provision (
    reporting_date          DATE         NOT NULL,
    exposure_id             VARCHAR      NOT NULL,
    assigned_stage          INTEGER      NOT NULL,
    ead_amount_aed          DECIMAL(18,2) NOT NULL,
    ecl_12m_aed             DECIMAL(18,2) NOT NULL,          -- always computed (Stage 1 basis / disclosure)
    ecl_lifetime_aed        DECIMAL(18,2) NOT NULL,          -- always computed (Stage 2/3 basis / disclosure)
    reported_ecl_aed        DECIMAL(18,2) NOT NULL,          -- stage-appropriate, scenario-weighted provision
    coverage_ratio          DECIMAL(9,8) NOT NULL,           -- reported_ecl / EAD
    calculation_id          VARCHAR      NOT NULL,           -- FK to the audit row
    created_timestamp       TIMESTAMP    NOT NULL,
    PRIMARY KEY (reporting_date, exposure_id),
    FOREIGN KEY (assigned_stage) REFERENCES dim_stage(stage_id)
);

-- --------------------------------------------------------------------- AUDIT

-- Stage-transition log. One row per exposure per reporting_date when the stage
-- changes vs the prior reporting date (regulators track migrations & cures).
CREATE TABLE audit_stage_transition (
    transition_id           VARCHAR      NOT NULL PRIMARY KEY,
    exposure_id             VARCHAR      NOT NULL,
    reporting_date          DATE         NOT NULL,
    prior_reporting_date    DATE,                            -- NULL on first observation
    prior_stage             INTEGER,                         -- NULL on first observation
    new_stage               INTEGER      NOT NULL,
    transition_direction    VARCHAR      NOT NULL,           -- DETERIORATION / IMPROVEMENT / CURE / INITIAL / UNCHANGED
    trigger_reason          VARCHAR      NOT NULL,           -- mirrors sicr_trigger / default reason
    dpd_at_transition       INTEGER      NOT NULL,
    pd_ratio_at_transition  DECIMAL(12,6) NOT NULL,
    changed_by              VARCHAR      NOT NULL,           -- staging job / analyst override
    created_timestamp       TIMESTAMP    NOT NULL,
    FOREIGN KEY (exposure_id) REFERENCES dim_exposure(exposure_id)
);

-- Provision-calculation audit. One row per exposure per reporting_date.
-- Captures the exact model version, parameter-set versions, scenario weights and
-- inputs used, plus a reproducibility hash — mirrors Part 1's immutable-snapshot
-- philosophy so any provision can be re-derived years later for an audit.
CREATE TABLE audit_provision_calculation (
    calculation_id          VARCHAR      NOT NULL PRIMARY KEY,
    exposure_id             VARCHAR      NOT NULL,
    reporting_date          DATE         NOT NULL,
    assigned_stage          INTEGER      NOT NULL,
    ecl_model_version       VARCHAR      NOT NULL,
    parameter_set_ids       VARCHAR      NOT NULL,           -- JSON array of PiT set ids used (per scenario)
    scenario_weights_json   VARCHAR      NOT NULL,           -- JSON {scenario: weight} used
    ead_amount_aed          DECIMAL(18,2) NOT NULL,
    reported_ecl_aed        DECIMAL(18,2) NOT NULL,
    input_sha256            VARCHAR      NOT NULL,           -- hash of the calc inputs (tamper-evidence)
    calculated_by           VARCHAR      NOT NULL,
    calculated_timestamp    TIMESTAMP    NOT NULL,
    FOREIGN KEY (exposure_id) REFERENCES dim_exposure(exposure_id)
);
