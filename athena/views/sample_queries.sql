-- =============================================================================
-- Sample analytical queries — credit decision platform
-- -----------------------------------------------------------------------------
-- Realistic questions the risk / data-steward / compliance teams ask. Run in the
-- Athena workgroup `credit-decision-<env>` (infra/terraform/athena.tf). All times GST.
-- =============================================================================

-- 1) Risk: last 30 days approval-rate and fraud trend by product, from the mart.
--    (Uses portfolio_monitoring_daily; rolls the outcome/risk bands back up.)
SELECT
    snapshot_date,
    product_code,
    sum(decision_count)                                          AS decisions,
    round(100.0 * sum(approved_count)
          / nullif(sum(decision_count), 0), 2)                  AS approval_rate_pct,
    round(sum(avg_fraud_score * decision_count)
          / nullif(sum(decision_count), 0), 4)                  AS avg_fraud_score,
    round(100.0 * sum(aml_hit_count)
          / nullif(sum(decision_count), 0), 2)                  AS aml_hit_rate_pct,
    max(dq_score)                                               AS dq_score
FROM credit_gold.portfolio_monitoring_daily
WHERE snapshot_date >= date_add('day', -30, current_date)
GROUP BY snapshot_date, product_code
ORDER BY snapshot_date DESC, product_code;


-- 2) Risk: high-risk PEP exposure yesterday — where is PEP concentration highest?
SELECT
    product_code,
    risk_band,
    sum(decision_count)          AS decisions,
    sum(pep_count)               AS pep_decisions,
    round(100.0 * sum(pep_count)
          / nullif(sum(decision_count), 0), 2) AS pep_exposure_pct,
    sum(total_approved_amount_aed) AS approved_amount_aed
FROM credit_gold.portfolio_monitoring_daily
WHERE snapshot_date = date_add('day', -1, current_date)
GROUP BY product_code, risk_band
HAVING sum(pep_count) > 0
ORDER BY pep_exposure_pct DESC;


-- 3) Stewardship: how big is the identity queue right now, and why?
SELECT
    steward_queue_reason,
    match_method,
    count(*)                    AS records,
    round(avg(match_confidence), 4) AS avg_confidence
FROM credit_silver.v_unresolved_identities
GROUP BY steward_queue_reason, match_method
ORDER BY records DESC;


-- 4) Compliance: reconstruct exactly what the engine saw for one decision, and
--    confirm the immutable snapshot is present and under Object Lock (SPEC §7).
SELECT
    decision_id,
    product_code,
    decision_timestamp,
    snapshot_s3_uri,
    content_sha256,
    object_lock_mode,
    object_lock_retain_until_timestamp,
    snapshot_missing
FROM credit_silver.v_decision_audit_trail
WHERE decision_id = '00000000-0000-0000-0000-000000000000';   -- <-- replace with the decision_id


-- 5) DQ: days in the last quarter that failed a must-pass check or breached a WARN.
SELECT
    snapshot_date,
    dq_score,
    dq_score_dod_delta,
    must_pass_failed_count,
    quarantined_row_count,
    warn_failed_count
FROM credit_gold.v_dq_scorecard_trend
WHERE snapshot_date >= date_add('day', -90, current_date)
  AND (must_pass_failed_count > 0 OR warn_failed_count > 0)
ORDER BY snapshot_date DESC;


-- 6) Ops: source-freshness SLA breaches over the last 7 days (SPEC §8 WARN SLAs).
SELECT
    snapshot_date,
    aecb_freshness_hours,          -- SLA < 24h
    fraud_freshness_hours,         -- SLA < 1h
    aml_freshness_hours,           -- SLA < 6h
    profile_cdc_freshness_minutes  -- SLA < 15m
FROM credit_gold.dq_scorecard_daily
WHERE snapshot_date >= date_add('day', -7, current_date)
  AND (aecb_freshness_hours >= 24
       OR fraud_freshness_hours >= 1
       OR aml_freshness_hours >= 6
       OR profile_cdc_freshness_minutes >= 15)
ORDER BY snapshot_date DESC;
