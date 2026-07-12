-- =============================================================================
-- credit_silver.decision_input_snapshot  (SPEC §7 — immutable audit index)
-- -----------------------------------------------------------------------------
-- The queryable index over the immutable snapshot objects. For every decision_id
-- the pipeline freezes the exact input bytes as a single JSON object in the
-- Object-Lock (COMPLIANCE, 7y) bucket (SPEC §7, infra/terraform/s3.tf). This Delta
-- table stores the pointer + content hash so tampering is detectable; the S3 object
-- is the legal record, this row is the queryable index.
-- Grain: one row per decision_id. Partitioned physically by decision_date in S3.
-- Delta table read by Athena v3. Timestamps GST (UTC+4).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.decision_input_snapshot (
    decision_id                    STRING     COMMENT 'Decision this snapshot belongs to. Natural key / grain (SPEC §7)',
    decision_date                  DATE       COMMENT 'Decision date (GST). Physical partition of the snapshot object path',
    master_customer_id             STRING     COMMENT 'Resolved golden-record id captured in the frozen snapshot (SPEC §6/§7)',
    snapshot_s3_uri                STRING     COMMENT 'Full s3:// URI of the immutable snapshot.json in the Object-Lock bucket (SPEC §7)',
    content_sha256                 STRING     COMMENT 'SHA-256 of the entire snapshot object. Tamper-evidence for the legal record (SPEC §7)',
    -- verbatim source pointers + per-record hashes (SPEC §7) -------------------
    aecb_bronze_s3_uri             STRING     COMMENT 'Bronze S3 URI of the raw AECB record frozen in the snapshot. Null if none',
    aecb_record_sha256             STRING     COMMENT 'SHA-256 of the verbatim raw AECB record. Null if none',
    fraud_bronze_s3_uri            STRING     COMMENT 'Bronze S3 URI of the raw fraud record frozen in the snapshot. Null if none',
    fraud_record_sha256            STRING     COMMENT 'SHA-256 of the verbatim raw fraud record. Null if none',
    aml_bronze_s3_uri              STRING     COMMENT 'Bronze S3 URI of the raw AML record frozen in the snapshot. Null if none',
    aml_record_sha256              STRING     COMMENT 'SHA-256 of the verbatim raw AML record. Null if none',
    profile_bronze_s3_uri          STRING     COMMENT 'Bronze S3 URI of the raw customer-profile record frozen in the snapshot. Null if none',
    profile_record_sha256          STRING     COMMENT 'SHA-256 of the verbatim raw customer-profile record. Null if none',
    -- object-lock / retention provenance (SPEC §7, infra s3.tf) ---------------
    captured_timestamp             TIMESTAMP  COMMENT 'When the input bytes were frozen into the snapshot object, GST (SPEC §7)',
    retention_years                INT        COMMENT 'Object-Lock COMPLIANCE retention applied to the snapshot object. Constant 7 (SPEC §7/§11); enforced by infra/terraform/s3.tf',
    source_system                  STRING     COMMENT 'Audit: originating system. Constant "SNAPSHOT"',
    batch_id                       STRING     COMMENT 'Audit: Glue run/batch id that wrote this snapshot index row',
    created_timestamp              TIMESTAMP  COMMENT 'Audit: when the snapshot was frozen and indexed, GST. NOT NULL',
    updated_timestamp              TIMESTAMP  COMMENT 'Audit: last update to this index row, GST (index only — the object never changes)'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/decision_input_snapshot/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
