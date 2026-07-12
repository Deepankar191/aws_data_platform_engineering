-- =============================================================================
-- credit_bronze.decisions_raw  (SPEC §2.1 — the driving decision/scoring-event feed)
-- -----------------------------------------------------------------------------
-- Raw credit-decision (scoring) events emitted by the credit decision engine, one
-- per application scored. This is the DRIVER of the whole gold layer: build_decision_input
-- reads it and fuses the 4 conformed sources onto each decision via customer_identity_xref.
-- Source: decision engine -> Kinesis Firehose / Kafka -> Parquet in bronze/decisions/.
-- Append-only, source schema preserved. Native join key onto the spine: internal_customer_uuid.
-- Partitioned by ingest_date, partition projection enabled. Timestamps are GST (UTC+4).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_bronze.decisions_raw (
    decision_id            STRING  COMMENT 'UUID, one per scoring event. Unique. Grain of decision_input (SPEC §5)',
    application_id         STRING  COMMENT 'The credit application id, e.g. APP-2025-000001',
    internal_customer_uuid STRING  COMMENT 'PostgreSQL spine id (SPEC §6.1). Join key onto customer_identity_xref',
    product_code           STRING  COMMENT 'Credit product. Values: PERSONAL_FINANCE, BNPL, CARD_ALT',
    decision_timestamp     STRING  COMMENT 'When the decision was scored, raw ISO string (GST). Typed to TIMESTAMP in silver',
    requested_amount_aed   STRING  COMMENT 'Requested amount, raw string; typed to DECIMAL(18,2) in silver',
    approved_amount_aed    STRING  COMMENT 'Approved amount, raw string (0 for declines); typed to DECIMAL(18,2) in silver',
    decision_outcome       STRING  COMMENT 'Engine outcome. Values: APPROVED, DECLINED, REFERRED. Drives decision_outcome_band (SPEC §9)',
    source_file_s3_uri     STRING  COMMENT 'S3 URI of the source payload this record was parsed from',
    ingested_timestamp     TIMESTAMP COMMENT 'When ingestion wrote this bronze record, GST'
)
PARTITIONED BY (
    ingest_date            STRING  COMMENT 'Landing date partition YYYY-MM-DD (SPEC §3)'
)
STORED AS PARQUET
LOCATION 's3://wio-credit-decision-${ENV}/bronze/decisions/'
TBLPROPERTIES (
    'parquet.compression'          = 'SNAPPY',
    'projection.enabled'           = 'true',
    'projection.ingest_date.type'  = 'date',
    'projection.ingest_date.format'= 'yyyy-MM-dd',
    'projection.ingest_date.range' = '2025-01-01,NOW',
    'projection.ingest_date.interval'      = '1',
    'projection.ingest_date.interval.unit' = 'DAYS',
    'storage.location.template'    = 's3://wio-credit-decision-${ENV}/bronze/decisions/ingest_date=${ingest_date}',
    'classification'               = 'parquet'
);
