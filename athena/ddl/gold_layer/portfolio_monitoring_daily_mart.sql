-- =============================================================================
-- credit_gold.portfolio_monitoring_daily  (SPEC §9 — risk portfolio mart)
-- -----------------------------------------------------------------------------
-- Daily portfolio KPIs served to the risk team's Metabase dashboards. Delta table
-- read by Athena v3. Built from decision_input rows where dq_pass = TRUE (SPEC §8).
-- Grain: one row per snapshot_date x product_code x decision_outcome_band x risk_band
-- (SPEC §9). Gold exposes natural keys / bands only — no surrogate _sk, no is_current
-- (SPEC §10). Money always DECIMAL(18,2); rates DECIMAL(5,2) pct; fraud DECIMAL(5,4).
-- Timestamps GST (UTC+4).
--
-- Band derivations (computed in the gold job):
--   decision_outcome_band = fraud_decision (APPROVE | REVIEW | DECLINE)
--   risk_band = LOW  when aecb_credit_score >= 720
--               MEDIUM when 620 <= aecb_credit_score < 720
--               HIGH  when aecb_credit_score < 620 or score missing
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_gold.portfolio_monitoring_daily (
    snapshot_date              DATE          COMMENT 'Business day of the decisions in this slice, GST. Part of grain (SPEC §9)',
    product_code               STRING        COMMENT 'Credit product. Values: PERSONAL_FINANCE, BNPL, CARD_ALT. Part of grain',
    decision_outcome_band      STRING        COMMENT 'Outcome band from fraud_decision. Values: APPROVE, REVIEW, DECLINE. Part of grain',
    risk_band                  STRING        COMMENT 'Risk band from AECB score. Values: LOW, MEDIUM, HIGH. Part of grain (see header for cutoffs)',
    -- volumes ------------------------------------------------------------------
    decision_count             INT           COMMENT 'Total decisions in this slice',
    approved_count             INT           COMMENT 'Decisions with fraud_decision = APPROVE in this slice',
    approval_rate_pct          DECIMAL(5,2)  COMMENT 'approved_count / decision_count * 100 (SPEC §9)',
    -- fraud --------------------------------------------------------------------
    avg_fraud_score            DECIMAL(5,4)  COMMENT 'Mean fraud_score across the slice, 0.0000-1.0000 (SPEC §9)',
    -- aml ----------------------------------------------------------------------
    aml_hit_count              INT           COMMENT 'Decisions with aml_status = HIT in this slice',
    aml_hit_rate_pct           DECIMAL(5,2)  COMMENT 'aml_hit_count / decision_count * 100 (SPEC §9)',
    -- pep ----------------------------------------------------------------------
    pep_count                  INT           COMMENT 'Decisions where is_pep = TRUE in this slice',
    pep_exposure_pct           DECIMAL(5,2)  COMMENT 'pep_count / decision_count * 100 — PEP exposure (SPEC §9)',
    -- aecb ---------------------------------------------------------------------
    avg_aecb_credit_score      DECIMAL(5,2)  COMMENT 'Mean AECB credit score across the slice (300-900) (SPEC §9)',
    -- amounts (money DECIMAL(18,2), SPEC §9/§10) -------------------------------
    avg_requested_amount_aed   DECIMAL(18,2) COMMENT 'Mean requested amount in the slice, AED (SPEC §9)',
    avg_approved_amount_aed    DECIMAL(18,2) COMMENT 'Mean approved amount in the slice, AED. Declines count as 0 (SPEC §9)',
    total_requested_amount_aed DECIMAL(18,2) COMMENT 'Sum of requested amount in the slice, AED',
    total_approved_amount_aed  DECIMAL(18,2) COMMENT 'Sum of approved amount in the slice, AED',
    -- data quality context (SPEC §8/§9) ---------------------------------------
    dq_score                   DECIMAL(5,2)  COMMENT 'The day''s 0-100 dq_score from dq_scorecard_daily, denormalised for the dashboard (SPEC §9)',
    -- lineage ------------------------------------------------------------------
    created_timestamp          TIMESTAMP     COMMENT 'When this mart row was written, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/gold/portfolio_monitoring_daily/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
