-- =============================================================================
-- Sample query 3 — STAGE-TRANSITION AUDIT TRAIL
-- -----------------------------------------------------------------------------
-- Period-over-period stage migrations (the reconciliation regulators require:
-- Stage 1<->2<->3 movements, and cures back to Stage 1). Emits the first
-- observation (INITIAL) and every subsequent stage change, with direction, the
-- DPD and PD ratio at transition, and the trigger — feeding audit_stage_transition.
-- =============================================================================
WITH staged AS (
    SELECT
        s.reporting_date, s.exposure_id, s.days_past_due,
        ROUND(s.current_lifetime_pd / NULLIF(e.origination_lifetime_pd, 0), 4) AS pd_ratio,
        CASE
            WHEN (s.days_past_due >= 90 OR s.is_unlikely_to_pay) THEN 3
            WHEN (s.current_lifetime_pd / NULLIF(e.origination_lifetime_pd, 0) >= 2.0
                  OR s.days_past_due >= 30 OR s.is_forborne OR s.is_watchlist) THEN 2
            ELSE 1
        END AS assigned_stage,
        CASE
            WHEN (s.days_past_due >= 90 OR s.is_unlikely_to_pay) THEN 'DEFAULT'
            WHEN s.current_lifetime_pd / NULLIF(e.origination_lifetime_pd, 0) >= 2.0 THEN 'PD_DETERIORATION'
            WHEN s.days_past_due >= 30 THEN 'DPD_30_BACKSTOP'
            WHEN s.is_forborne THEN 'FORBEARANCE'
            WHEN s.is_watchlist THEN 'WATCHLIST'
            ELSE 'NONE'
        END AS trigger_reason
    FROM fact_exposure_snapshot s
    JOIN dim_exposure e USING (exposure_id)
),
seq AS (
    SELECT *,
        LAG(assigned_stage)  OVER (PARTITION BY exposure_id ORDER BY reporting_date) AS prior_stage,
        LAG(reporting_date)  OVER (PARTITION BY exposure_id ORDER BY reporting_date) AS prior_reporting_date
    FROM staged
)
SELECT
    exposure_id,
    prior_reporting_date,
    reporting_date,
    prior_stage,
    assigned_stage AS new_stage,
    CASE
        WHEN prior_stage IS NULL                                THEN 'INITIAL'
        WHEN assigned_stage > prior_stage                       THEN 'DETERIORATION'
        WHEN assigned_stage < prior_stage AND assigned_stage = 1 THEN 'CURE'
        WHEN assigned_stage < prior_stage                       THEN 'IMPROVEMENT'
        ELSE 'UNCHANGED'
    END AS transition_direction,
    trigger_reason,
    days_past_due AS dpd_at_transition,
    pd_ratio      AS pd_ratio_at_transition
FROM seq
WHERE prior_stage IS NULL OR assigned_stage <> prior_stage
ORDER BY reporting_date, exposure_id;
