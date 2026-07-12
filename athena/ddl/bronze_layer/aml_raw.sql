-- =============================================================================
-- credit_bronze.aml_raw  (SPEC §2 source #3, §3 bronze/aml/)
-- -----------------------------------------------------------------------------
-- Raw AML / PEP screening callbacks. Source: webhook callback, JSON, landed to
-- Parquet by ingestion/aml_webhook/. Append-only, source schema preserved.
-- Native match key: full_name + date_of_birth (PII Level 2) -> fuzzy resolution.
-- Partitioned by ingest_date. Partition projection enabled. Timestamps GST (UTC+4).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_bronze.aml_raw (
    screening_ref        STRING  COMMENT 'AML screening reference, e.g. AML-20250401-0001. Unique per screening',
    full_name            STRING  COMMENT 'PII Level 2: screened subject full name. Native match key (with dob)',
    date_of_birth        STRING  COMMENT 'PII Level 2: subject date of birth, raw ISO string. Native match key',
    aml_status           STRING  COMMENT 'Screening outcome. Values: CLEAR, HIT, PENDING',
    is_pep               BOOLEAN COMMENT 'True if subject flagged as a Politically Exposed Person',
    screened_at          STRING  COMMENT 'Provider screening timestamp, raw ISO string (GST)',
    source_file_s3_uri   STRING  COMMENT 'S3 URI of the source JSON callback this record was parsed from',
    ingested_timestamp   TIMESTAMP COMMENT 'When ingestion wrote this bronze record, GST'
)
PARTITIONED BY (
    ingest_date          STRING  COMMENT 'Landing date partition YYYY-MM-DD (SPEC §3)'
)
STORED AS PARQUET
LOCATION 's3://wio-credit-decision-${ENV}/bronze/aml/'
TBLPROPERTIES (
    'parquet.compression'          = 'SNAPPY',
    'projection.enabled'           = 'true',
    'projection.ingest_date.type'  = 'date',
    'projection.ingest_date.format'= 'yyyy-MM-dd',
    'projection.ingest_date.range' = '2025-01-01,NOW',
    'projection.ingest_date.interval'      = '1',
    'projection.ingest_date.interval.unit' = 'DAYS',
    'storage.location.template'    = 's3://wio-credit-decision-${ENV}/bronze/aml/ingest_date=${ingest_date}',
    'classification'               = 'parquet'
);
