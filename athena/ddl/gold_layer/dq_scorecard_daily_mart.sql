-- =============================================================================
-- credit_gold.dq_scorecard_daily  (SPEC §8 — data quality scorecard)
-- -----------------------------------------------------------------------------
-- Per-day data-quality scorecard over decision_input. Delta table read by Athena v3.
-- Written by the Glue DQ job (mirrors the Soda Core checks in dq/soda/). Per-check
-- detail is stored as JSON columns (must-pass fail counts, WARN results, freshness
-- results) so the check set can evolve without a schema migration; headline counts
-- and the 0-100 dq_score are first-class columns. Grain: one row per scorecard_date.
-- Gold natural keys only (SPEC §10). Timestamps GST (UTC+4).
-- Schema matches glue/gold_layer/run_dq_scorecard_mart.py (the Delta writer / catalog owner).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_gold.dq_scorecard_daily (
    scorecard_date                     DATE          COMMENT 'Business day the checks were run for, GST. Natural key / grain (SPEC §8)',
    total_rows                         INT           COMMENT 'decision_input rows evaluated for this day (passed + quarantined)',
    passed_rows                        INT           COMMENT 'Rows that passed all must-pass checks (dq_pass = TRUE, reach gold)',
    quarantined_rows                   INT           COMMENT 'Rows routed to decision_input_quarantine for the day (SPEC §8)',
    duplicate_application_id_count     BIGINT        COMMENT 'Count of duplicate application_id values observed (reported, not gated — re-scoring is legitimate)',
    duplicate_master_customer_id_count BIGINT        COMMENT 'Count of duplicate master_customer_id values observed (reported, not gated)',
    must_pass_fail_counts_json         STRING        COMMENT 'JSON object: per must-pass rule name -> failing-row count for the day (SPEC §8)',
    warn_results_json                  STRING        COMMENT 'JSON object: per WARN rule name -> pass rate / value; SNS alert on breach (SPEC §8)',
    freshness_results_json             STRING        COMMENT 'JSON object: per-source freshness at load (AECB/fraud/AML hours, profile CDC minutes) vs SLA (SPEC §8)',
    dq_score                           DECIMAL(5,2)  COMMENT '0-100 composite data-quality score for the day (SPEC §8). Denormalised into portfolio_monitoring_daily',
    created_timestamp                  TIMESTAMP     COMMENT 'When this scorecard row was written, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/gold/dq_scorecard_daily/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
