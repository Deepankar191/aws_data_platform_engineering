-- =============================================================================
-- Sample query 4 — PORTFOLIO ECL SUMMARY BY STAGE
-- -----------------------------------------------------------------------------
-- The headline IFRS 9 disclosure view: exposure, provision, and coverage ratio
-- by stage. Reads fact_ecl_provision (materialised from query 2 by run_demo.py).
-- =============================================================================
SELECT
    COALESCE(CAST(p.assigned_stage AS VARCHAR), 'TOTAL')  AS stage,
    st.stage_name,
    st.ecl_horizon,
    COUNT(*)                                              AS exposure_count,
    ROUND(SUM(p.ead_amount_aed), 2)                       AS total_ead_aed,
    ROUND(SUM(p.reported_ecl_aed), 2)                     AS total_provision_aed,
    ROUND(SUM(p.reported_ecl_aed) / NULLIF(SUM(p.ead_amount_aed), 0), 6) AS portfolio_coverage_ratio
FROM fact_ecl_provision p
JOIN dim_stage st ON st.stage_id = p.assigned_stage
WHERE p.reporting_date = DATE '2025-06-30'
-- one row per stage + a grand-total row
GROUP BY GROUPING SETS ((p.assigned_stage, st.stage_name, st.ecl_horizon), ())
ORDER BY p.assigned_stage NULLS LAST;
