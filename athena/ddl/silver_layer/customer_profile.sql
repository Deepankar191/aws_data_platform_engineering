-- =============================================================================
-- credit_silver.customer_profile  (SPEC §3 silver, source #4 conformed — the SPINE)
-- -----------------------------------------------------------------------------
-- Cleaned, typed, current-state internal customer profile from PostgreSQL CDC.
-- The identity spine (SPEC §6.1): internal_customer_uuid is canonical. Delta MERGE
-- of the CDC stream keeps one current row per internal_customer_uuid. Delta table
-- read by Athena v3. Money -> DECIMAL(18,2). Carries silver audit block. GST (UTC+4).
-- Match keys (emirates_id, phone, email) are stored already-normalised in place.
-- Schema matches glue/silver_layer/customer_profile_to_silver.py (the Delta writer).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.customer_profile (
    internal_customer_uuid   STRING        COMMENT 'Canonical spine id (SPEC §6.1). Natural key / grain of this table',
    emirates_id              STRING        COMMENT 'PII Level 1: Emirates ID, spaces/dashes stripped — deterministic match key vs AECB (SPEC §6.2)',
    phone                    STRING        COMMENT 'PII Level 2: phone normalised to E.164 — deterministic match key vs fraud (SPEC §6.3)',
    email                    STRING        COMMENT 'PII Level 2: email lowercased/trimmed — deterministic match key vs fraud (SPEC §6.3)',
    full_name                STRING        COMMENT 'PII Level 2: customer full name',
    date_of_birth            DATE          COMMENT 'PII Level 2: date of birth — match key with name soundex (SPEC §6.4)',
    monthly_income_aed       DECIMAL(18,2) COMMENT 'Declared monthly income, AED. Feeds decision_input.monthly_income_aed',
    kyc_completed            BOOLEAN       COMMENT 'True when KYC is complete for this customer',
    profile_updated_timestamp TIMESTAMP    COMMENT 'Source row updated_at from PostgreSQL, GST — survivorship recency (SPEC §6)',
    source_system            STRING        COMMENT 'Audit: originating system. Constant "POSTGRES"',
    batch_id                 STRING        COMMENT 'Audit: Glue/stream micro-batch id that produced this silver row',
    created_timestamp        TIMESTAMP     COMMENT 'Audit: when this silver row was first written, GST',
    updated_timestamp        TIMESTAMP     COMMENT 'Audit: when this silver row was last updated, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/customer_profile/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
