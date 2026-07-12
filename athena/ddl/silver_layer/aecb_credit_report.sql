-- =============================================================================
-- credit_silver.aecb_credit_report  (SPEC §3 silver, source #1 conformed)
-- -----------------------------------------------------------------------------
-- Cleaned, typed, deduped AECB credit reports. Delta table read by Athena v3.
-- One row per aecb_report_ref (latest report per emirates_id). emirates_id is stored
-- already-normalised (dashes/spaces stripped) and drives deterministic identity
-- resolution in §6.2. Money -> DECIMAL(18,2); score INT (300-900). Silver audit block.
-- Timestamps GST (UTC+4). AECB matches on emirates_id, so it carries no name/dob.
-- Schema matches glue/silver_layer/aecb_to_silver.py (the Delta writer / catalog owner).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.aecb_credit_report (
    emirates_id                STRING        COMMENT 'PII Level 1: Emirates ID, spaces/dashes stripped — deterministic match key (SPEC §6.2)',
    aecb_report_ref            STRING        COMMENT 'AECB report reference. Natural key / grain of this table',
    aecb_credit_score          INT           COMMENT 'AECB credit score. Valid range 300-900 (DQ must-pass, SPEC §8)',
    aecb_total_outstanding_aed DECIMAL(18,2) COMMENT 'Total outstanding balance across facilities, AED',
    report_timestamp           TIMESTAMP     COMMENT 'When the AECB report was generated, GST',
    source_system             STRING        COMMENT 'Audit: originating system. Constant "AECB"',
    batch_id                  STRING        COMMENT 'Audit: Glue run/batch id that produced this silver row',
    created_timestamp         TIMESTAMP     COMMENT 'Audit: when this silver row was first written, GST',
    updated_timestamp         TIMESTAMP     COMMENT 'Audit: when this silver row was last updated, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/aecb_credit_report/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
