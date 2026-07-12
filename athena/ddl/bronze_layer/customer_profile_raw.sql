-- =============================================================================
-- credit_bronze.customer_profile_raw  (SPEC §2 source #4, §3 bronze/customer_profile/)
-- -----------------------------------------------------------------------------
-- Raw internal customer profile CDC. Source: PostgreSQL -> Debezium -> Kafka ->
-- Kafka Connect S3/Delta sink. The identity SPINE (SPEC §6.1): internal_customer_uuid
-- is the canonical id. This is a DELTA table (CDC sink writes Delta, SPEC §3),
-- so Athena v3 reads it natively via table_type=DELTA. Continuous cadence (not batch).
-- Schema mirrors the Debezium envelope flattened by the sink connector.
-- Timestamps GST (UTC+4).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_bronze.customer_profile_raw (
    internal_customer_uuid   STRING  COMMENT 'Canonical spine id (SPEC §6.1). PostgreSQL primary key',
    emirates_id              STRING  COMMENT 'PII Level 1: Emirates ID as stored in PostgreSQL',
    full_name                STRING  COMMENT 'PII Level 2: customer full name',
    date_of_birth            STRING  COMMENT 'PII Level 2: date of birth, raw string; typed to DATE in silver',
    phone                    STRING  COMMENT 'PII Level 2: phone in E.164',
    email                    STRING  COMMENT 'PII Level 2: email address',
    monthly_income_aed       STRING  COMMENT 'Raw monthly income string; typed to DECIMAL(18,2) in silver',
    kyc_completed            BOOLEAN COMMENT 'True when KYC is complete for this customer',
    address                  STRING  COMMENT 'PII Level 2: residential address free text',
    created_at               STRING  COMMENT 'Source row created_at (TIMESTAMPTZ), raw ISO string (GST)',
    updated_at               STRING  COMMENT 'Source row updated_at (TIMESTAMPTZ), raw ISO string (GST)',
    -- Debezium CDC envelope metadata (flattened by the sink) --------------------
    cdc_op                   STRING  COMMENT 'Debezium op: c=create, u=update, d=delete, r=snapshot read',
    cdc_source_ts_ms         BIGINT  COMMENT 'Debezium source.ts_ms — commit time in the source DB (epoch ms)',
    cdc_lsn                  BIGINT  COMMENT 'PostgreSQL log sequence number for ordering CDC events',
    ingested_timestamp       TIMESTAMP COMMENT 'When the sink wrote this bronze record, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/bronze/customer_profile/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
