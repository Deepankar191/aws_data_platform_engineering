-- =============================================================================
-- credit_gold.v_dq_scorecard_trend
-- -----------------------------------------------------------------------------
-- dq_score over time with must-pass failure counts (SPEC §8). Feeds the DQ trend
-- tile on the risk dashboard and the day-over-day regression alert. Adds a 7-day
-- moving average of dq_score and the day-over-day delta so a slow drift is visible,
-- not just hard failures. Ordered newest first.
-- =============================================================================
CREATE OR REPLACE VIEW credit_gold.v_dq_scorecard_trend AS
SELECT
    s.snapshot_date,
    s.dq_score,
    s.dq_pass,
    s.must_pass_check_count,
    s.must_pass_failed_count,
    s.quarantined_row_count,
    s.total_decision_count,
    s.warn_failed_count,
    s.completeness_pass_rate_pct,
    s.aml_pending_rate_pct,
    s.aecb_freshness_hours,
    s.fraud_freshness_hours,
    s.aml_freshness_hours,
    s.profile_cdc_freshness_minutes,
    round(avg(s.dq_score) OVER (
        ORDER BY s.snapshot_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)         AS dq_score_7d_avg,
    round(s.dq_score - lag(s.dq_score) OVER (
        ORDER BY s.snapshot_date), 2)                         AS dq_score_dod_delta
FROM credit_gold.dq_scorecard_daily s
ORDER BY s.snapshot_date DESC;
