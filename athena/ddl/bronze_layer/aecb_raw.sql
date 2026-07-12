-- =============================================================================
-- credit_bronze.aecb_raw  (SPEC §2 source #1, §3 bronze/aecb/)
-- -----------------------------------------------------------------------------
-- Raw AECB (Al Etihad Credit Bureau) credit reports. Source: batch SFTP, XML,
-- parsed to Parquet by ingestion/aecb_sftp/. Append-only, source schema preserved.
-- Native match key: emirates_id (PII Level 1). Partitioned by ingest_date.
-- Partition projection is enabled so no MSCK REPAIR / crawler is required.
-- Timestamps are GST (UTC+4, Asia/Dubai) as received from AECB.
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_bronze.aecb_raw (
    batch_id                  STRING  COMMENT 'AECB batch header BatchId, e.g. AECB-BATCH-20250401-0001',
    generated_timestamp       STRING  COMMENT 'Batch header GeneratedTimestamp, GST. Kept as raw string in bronze',
    report_ref                STRING  COMMENT 'AECB report reference (ReportRef), unique per credit report',
    report_date               STRING  COMMENT 'AECB report date (ReportDate), raw ISO string; typed to DATE in silver',
    emirates_id               STRING  COMMENT 'PII Level 1: Emirates ID as printed (with dashes). Native match key',
    full_name                 STRING  COMMENT 'PII Level 2: subject full name as reported by AECB',
    date_of_birth             STRING  COMMENT 'PII Level 2: subject date of birth, raw ISO string',
    credit_score              INT     COMMENT 'AECB credit score, expected range 300-900',
    total_outstanding_aed     STRING  COMMENT 'Raw TotalOutstandingAED string; typed to DECIMAL(18,2) in silver',
    active_loans              INT     COMMENT 'Count of active loans on the credit report',
    environment               STRING  COMMENT 'Source environment tag from batch header (PROD/UAT)',
    source_file_s3_uri        STRING  COMMENT 'S3 URI of the source XML file this record was parsed from',
    ingested_timestamp        TIMESTAMP COMMENT 'When ingestion wrote this bronze record, GST'
)
PARTITIONED BY (
    ingest_date               STRING  COMMENT 'Landing date partition YYYY-MM-DD (SPEC §3)'
)
STORED AS PARQUET
LOCATION 's3://wio-credit-decision-${ENV}/bronze/aecb/'
TBLPROPERTIES (
    'parquet.compression'          = 'SNAPPY',
    'projection.enabled'           = 'true',
    'projection.ingest_date.type'  = 'date',
    'projection.ingest_date.format'= 'yyyy-MM-dd',
    'projection.ingest_date.range' = '2025-01-01,NOW',
    'projection.ingest_date.interval'      = '1',
    'projection.ingest_date.interval.unit' = 'DAYS',
    'storage.location.template'    = 's3://wio-credit-decision-${ENV}/bronze/aecb/ingest_date=${ingest_date}',
    'classification'               = 'parquet'
);
