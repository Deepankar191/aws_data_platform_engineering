-- =============================================================================
-- credit_bronze.fraud_raw  (SPEC §2 source #2, §3 bronze/fraud/)
-- -----------------------------------------------------------------------------
-- Raw fraud-provider scoring events. Source: REST API poll, JSON, landed to
-- Parquet by ingestion/fraud_api/. Append-only, source schema preserved.
-- Native match key: phone (E.164) + email (PII Level 2). Partitioned by ingest_date.
-- Partition projection enabled. Timestamps are GST (UTC+4).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_bronze.fraud_raw (
    event_id             STRING  COMMENT 'Fraud provider event id, e.g. FRD-20250401-0001. Unique per scoring event',
    phone                STRING  COMMENT 'PII Level 2: phone in E.164, raw as received. Native match key (with email)',
    email                STRING  COMMENT 'PII Level 2: email, raw as received. Native match key (with phone)',
    fraud_score          STRING  COMMENT 'Raw fraud score string 0.0000-1.0000; typed to DECIMAL(5,4) in silver',
    fraud_decision       STRING  COMMENT 'Provider recommendation. Values: APPROVE, REVIEW, DECLINE',
    scored_at            STRING  COMMENT 'Provider scoring timestamp, raw ISO string (GST)',
    source_file_s3_uri   STRING  COMMENT 'S3 URI of the source JSON payload this record was parsed from',
    ingested_timestamp   TIMESTAMP COMMENT 'When ingestion wrote this bronze record, GST'
)
PARTITIONED BY (
    ingest_date          STRING  COMMENT 'Landing date partition YYYY-MM-DD (SPEC §3)'
)
STORED AS PARQUET
LOCATION 's3://wio-credit-decision-${ENV}/bronze/fraud/'
TBLPROPERTIES (
    'parquet.compression'          = 'SNAPPY',
    'projection.enabled'           = 'true',
    'projection.ingest_date.type'  = 'date',
    'projection.ingest_date.format'= 'yyyy-MM-dd',
    'projection.ingest_date.range' = '2025-01-01,NOW',
    'projection.ingest_date.interval'      = '1',
    'projection.ingest_date.interval.unit' = 'DAYS',
    'storage.location.template'    = 's3://wio-credit-decision-${ENV}/bronze/fraud/ingest_date=${ingest_date}',
    'classification'               = 'parquet'
);
