-- =============================================================================
-- Sample query 2 — IFRS 9 ECL PROVISION CALCULATION
-- -----------------------------------------------------------------------------
--   ECL = EAD x PD x LGD x discount, probability-weighted across macro scenarios.
--   * EAD  = drawn balance + CCF x undrawn commitment (CCF from the PiT set).
--   * PD   = 12-month cumulative PD (Stage 1) or lifetime cumulative PD (Stage 2);
--            a defaulted exposure (Stage 3) carries PD = 100%.
--   * LGD  = downturn LGD for the exposure's collateral segment.
--   * disc = 1 / (1 + EIR)^t, t = expected time to loss (mid-horizon).
--   Each scenario uses ITS OWN current PiT parameter set (base/upside/downside),
--   selected by effective date; results are weighted by the period's scenario
--   probabilities. Computed for the current reporting date (2025-06-30).
-- =============================================================================
WITH stage AS (
    SELECT s.reporting_date, s.exposure_id,
        CASE
            WHEN (s.days_past_due >= 90 OR s.is_unlikely_to_pay) THEN 3
            WHEN (s.current_lifetime_pd / NULLIF(e.origination_lifetime_pd, 0) >= 2.0
                  OR s.days_past_due >= 30 OR s.is_forborne OR s.is_watchlist) THEN 2
            ELSE 1
        END AS assigned_stage
    FROM fact_exposure_snapshot s
    JOIN dim_exposure e USING (exposure_id)
),
base AS (
    SELECT
        s.reporting_date, s.exposure_id, e.product_type, e.collateral_type,
        s.current_rating_grade, s.remaining_maturity_months, s.effective_interest_rate,
        s.drawn_balance_aed, s.undrawn_commitment_aed, st.assigned_stage,
        -- lifetime horizon = smallest term-structure horizon covering remaining life
        COALESCE(
            (SELECT MIN(h.horizon_months) FROM (SELECT DISTINCT horizon_months FROM pd_term_structure) h
              WHERE h.horizon_months >= s.remaining_maturity_months),
            (SELECT MAX(horizon_months) FROM pd_term_structure)) AS lifetime_horizon
    FROM fact_exposure_snapshot s
    JOIN dim_exposure e USING (exposure_id)
    JOIN stage st USING (reporting_date, exposure_id)
    WHERE s.reporting_date = DATE '2025-06-30'
),
-- fan out to (exposure x scenario), attaching that scenario's current PiT set + weight
scn AS (
    SELECT b.*, w.scenario_id, w.probability_weight, p.parameter_set_id
    FROM base b
    JOIN scenario_weight w ON w.reporting_date = b.reporting_date
    JOIN risk_parameter_set p
      ON p.parameter_basis = 'PIT' AND p.macro_scenario_id = w.scenario_id AND p.is_current
     AND b.reporting_date >= p.effective_from_date
     AND (p.effective_to_date IS NULL OR b.reporting_date <= p.effective_to_date)
),
ecl AS (
    SELECT
        scn.reporting_date, scn.exposure_id, scn.assigned_stage, scn.scenario_id,
        scn.probability_weight, scn.parameter_set_id,
        ROUND(scn.drawn_balance_aed + ccf.credit_conversion_factor * scn.undrawn_commitment_aed, 2) AS ead_amount_aed,
        CASE
            WHEN scn.assigned_stage = 3 THEN 1.0
            WHEN scn.assigned_stage = 1 THEN pd12.cumulative_pd
            ELSE pdl.cumulative_pd
        END AS pd_used,
        lgd.downturn_lgd_rate AS lgd_used,
        POWER(1 + scn.effective_interest_rate,
              -1 * (CASE WHEN scn.assigned_stage = 1 THEN 0.5 ELSE (scn.lifetime_horizon / 12.0) / 2 END)) AS discount_factor
    FROM scn
    JOIN ccf_parameter ccf ON ccf.parameter_set_id = scn.parameter_set_id AND ccf.product_type = scn.product_type
    JOIN lgd_parameter lgd ON lgd.parameter_set_id = scn.parameter_set_id AND lgd.collateral_segment = scn.collateral_type
    JOIN pd_term_structure pd12 ON pd12.parameter_set_id = scn.parameter_set_id
         AND pd12.rating_grade = scn.current_rating_grade AND pd12.horizon_months = 12
    JOIN pd_term_structure pdl  ON pdl.parameter_set_id = scn.parameter_set_id
         AND pdl.rating_grade = scn.current_rating_grade AND pdl.horizon_months = scn.lifetime_horizon
),
scored AS (
    SELECT *, ROUND(ead_amount_aed * pd_used * lgd_used * discount_factor, 2) AS ecl_amount_aed
    FROM ecl
)
SELECT
    reporting_date,
    exposure_id,
    assigned_stage,
    MAX(ead_amount_aed)                                                   AS ead_amount_aed,
    ROUND(MAX(CASE WHEN scenario_id = 'BASE'     THEN ecl_amount_aed END), 2) AS ecl_base_aed,
    ROUND(MAX(CASE WHEN scenario_id = 'UPSIDE'   THEN ecl_amount_aed END), 2) AS ecl_upside_aed,
    ROUND(MAX(CASE WHEN scenario_id = 'DOWNSIDE' THEN ecl_amount_aed END), 2) AS ecl_downside_aed,
    ROUND(SUM(probability_weight * ecl_amount_aed), 2)                    AS reported_ecl_aed,   -- probability-weighted provision
    ROUND(SUM(probability_weight * ecl_amount_aed) / NULLIF(MAX(ead_amount_aed), 0), 6) AS coverage_ratio
FROM scored
GROUP BY reporting_date, exposure_id, assigned_stage
ORDER BY assigned_stage, exposure_id;
