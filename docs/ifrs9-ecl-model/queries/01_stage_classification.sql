-- =============================================================================
-- Sample query 1 — IFRS 9 STAGE CLASSIFICATION LOGIC
-- -----------------------------------------------------------------------------
-- Classifies every exposure at a reporting date into Stage 1 / 2 / 3 and records
-- WHY (the SICR / default trigger). Rules (IFRS 9 5.5):
--   * DEFAULT  (Stage 3): 90+ days past due backstop OR unlikely-to-pay (qualitative).
--   * SICR     (Stage 2): significant increase in credit risk since origination —
--       relative lifetime-PD deterioration >= 2.0x, OR 30+ DPD backstop,
--       OR forbearance, OR watchlist.
--   * Stage 1: everything else (12-month ECL basis).
-- Trigger precedence: default > PD deterioration > 30DPD > forbearance > watchlist.
-- Output shape matches fact_staging_assessment (so it can materialise it directly).
-- =============================================================================
WITH assessed AS (
    SELECT
        s.reporting_date,
        s.exposure_id,
        e.origination_lifetime_pd,
        s.current_lifetime_pd,
        ROUND(s.current_lifetime_pd / NULLIF(e.origination_lifetime_pd, 0), 6) AS pd_ratio,
        s.days_past_due,
        s.is_forborne,
        s.is_watchlist,
        s.is_unlikely_to_pay,
        (s.days_past_due >= 90 OR s.is_unlikely_to_pay)                       AS is_default,
        (s.current_lifetime_pd / NULLIF(e.origination_lifetime_pd, 0) >= 2.0
            OR s.days_past_due >= 30
            OR s.is_forborne
            OR s.is_watchlist)                                               AS is_sicr
    FROM fact_exposure_snapshot s
    JOIN dim_exposure e USING (exposure_id)
)
SELECT
    reporting_date,
    exposure_id,
    origination_lifetime_pd,
    current_lifetime_pd,
    pd_ratio,
    days_past_due,
    is_sicr,
    CASE
        WHEN is_default AND days_past_due >= 90 THEN 'DEFAULT_90DPD'
        WHEN is_default                         THEN 'UNLIKELY_TO_PAY'
        WHEN pd_ratio >= 2.0                     THEN 'PD_DETERIORATION'
        WHEN days_past_due >= 30                 THEN 'DPD_30_BACKSTOP'
        WHEN is_forborne                         THEN 'FORBEARANCE'
        WHEN is_watchlist                        THEN 'WATCHLIST'
        ELSE 'NONE'
    END                                                                     AS sicr_trigger,
    is_default,
    CASE WHEN is_default THEN 3 WHEN is_sicr THEN 2 ELSE 1 END              AS assigned_stage,
    'ifrs9_staging v1.2'                                                    AS staging_model_version,
    CAST('2025-06-30 02:00:00' AS TIMESTAMP)                               AS assessed_timestamp
FROM assessed
ORDER BY reporting_date, exposure_id;
