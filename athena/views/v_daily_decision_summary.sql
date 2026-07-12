-- =============================================================================
-- credit_gold.v_daily_decision_summary
-- -----------------------------------------------------------------------------
-- Analyst-facing rollup: decisions per day x product with approval rate, average
-- fraud score, and AML hit rate. Reads dq-passed decision_input directly (SPEC §5/§8)
-- so it is available even before the portfolio mart materialises for the day.
-- Approval is defined as fraud_decision = 'APPROVE' (SPEC §9 outcome band).
-- All timestamps are GST (UTC+4); the daily bucket is date(decision_timestamp).
-- =============================================================================
CREATE OR REPLACE VIEW credit_gold.v_daily_decision_summary AS
SELECT
    date(di.decision_timestamp)                              AS decision_date,
    di.product_code,
    count(*)                                                 AS decision_count,
    count_if(di.fraud_decision = 'APPROVE')                  AS approved_count,
    round(100.0 * count_if(di.fraud_decision = 'APPROVE')
          / nullif(count(*), 0), 2)                          AS approval_rate_pct,
    round(avg(di.fraud_score), 4)                            AS avg_fraud_score,
    count_if(di.aml_status = 'HIT')                          AS aml_hit_count,
    round(100.0 * count_if(di.aml_status = 'HIT')
          / nullif(count(*), 0), 2)                          AS aml_hit_rate_pct,
    count_if(di.aml_status = 'PENDING')                      AS aml_pending_count,
    count_if(di.is_pep)                                      AS pep_count,
    round(avg(CAST(di.aecb_credit_score AS DOUBLE)), 2)      AS avg_aecb_credit_score
FROM credit_silver.decision_input di
WHERE di.dq_pass = true
GROUP BY
    date(di.decision_timestamp),
    di.product_code;
