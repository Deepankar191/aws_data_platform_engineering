-- =============================================================================
-- credit_silver.fraud_score  (SPEC §3 silver, source #2 conformed)
-- -----------------------------------------------------------------------------
-- Cleaned, typed, deduped fraud scoring events. Delta table read by Athena v3.
-- One row per fraud_assessment_id (provider event id; latest per phone+email).
-- fraud_score DECIMAL(5,4) in 0.0000-1.0000. Match keys are stored already-normalised
-- (phone E.164, email lowercased) for §6. Carries silver audit block. Timestamps GST.
-- Schema matches glue/silver_layer/fraud_to_silver.py (the Delta writer / catalog owner).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.fraud_score (
    fraud_assessment_id  STRING        COMMENT 'Fraud provider event id. Natural key / grain of this table (bronze event_id)',
    phone                STRING        COMMENT 'PII Level 2: phone normalised to E.164 — deterministic match key (SPEC §6.3)',
    email                STRING        COMMENT 'PII Level 2: email lowercased/trimmed — deterministic match key (SPEC §6.3)',
    fraud_score          DECIMAL(5,4)  COMMENT 'Fraud probability 0.0000-1.0000 (DQ must-pass range 0-1, SPEC §8)',
    fraud_decision       STRING        COMMENT 'Provider recommendation. Values: APPROVE, REVIEW, DECLINE',
    scored_timestamp     TIMESTAMP     COMMENT 'Provider scoring time, GST',
    source_system        STRING        COMMENT 'Audit: originating system. Constant "FRAUD"',
    batch_id             STRING        COMMENT 'Audit: Glue run/batch id that produced this silver row',
    created_timestamp    TIMESTAMP     COMMENT 'Audit: when this silver row was first written, GST',
    updated_timestamp    TIMESTAMP     COMMENT 'Audit: when this silver row was last updated, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/fraud_score/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
