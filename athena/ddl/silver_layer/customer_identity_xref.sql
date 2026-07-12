-- =============================================================================
-- credit_silver.customer_identity_xref  (SPEC §6 — the golden record)
-- -----------------------------------------------------------------------------
-- Maps every source's native key onto one master_customer_id. Delta table read by
-- Athena v3. Grain: one row per master_customer_id (current state; SCD2 history is
-- kept in a sidecar, not exposed here). master_customer_id is a deterministic
-- UUIDv5 of internal_customer_uuid for spine-seeded rows (stable/reproducible),
-- or the UNRESOLVED_SENTINEL ("UNRESOLVED", SPEC §11) when a source row scores below
-- REVIEW_THRESHOLD and cannot attach — nothing is ever dropped (SPEC §6).
-- Carries per-source and overall match provenance. matched_on is an ARRAY of the keys
-- that fired. Timestamps GST (UTC+4). Demographics are survivorship-coalesced
-- (POSTGRES>AECB>FRAUD>AML, SPEC §6). *_source_key are the linked native keys that
-- build_decision_input joins on (aecb_report_ref / fraud event_id / aml screening_ref).
-- Schema matches glue/silver_layer/build_customer_identity_xref.py (the Delta writer).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.customer_identity_xref (
    master_customer_id        STRING        COMMENT 'Golden-record id. UUIDv5 of internal_customer_uuid, or "UNRESOLVED" sentinel (SPEC §6/§11). Natural key / grain',
    internal_customer_uuid    STRING        COMMENT 'PostgreSQL spine id (SPEC §6.1). Null on an unresolved non-spine record',
    -- survivorship-resolved demographics --------------------------------------
    emirates_id               STRING        COMMENT 'PII Level 1: survivorship-resolved Emirates ID (SPEC §6.2)',
    phone                     STRING        COMMENT 'PII Level 2: survivorship-resolved E.164 phone (SPEC §6.3)',
    email                     STRING        COMMENT 'PII Level 2: survivorship-resolved email (SPEC §6.3)',
    full_name                 STRING        COMMENT 'PII Level 2: survivorship-resolved full name (priority POSTGRES>AECB>FRAUD>AML, SPEC §6)',
    date_of_birth             DATE          COMMENT 'PII Level 2: survivorship-resolved date of birth',
    -- per-source attach flags + linked native keys (join keys for decision_input) --
    aecb_matched              BOOLEAN       COMMENT 'True if an AECB report resolved to this master',
    aecb_source_key           STRING        COMMENT 'Linked AECB report_ref, if matched. Null otherwise',
    fraud_matched             BOOLEAN       COMMENT 'True if a fraud event resolved to this master',
    fraud_source_key          STRING        COMMENT 'Linked fraud event_id, if matched. Null otherwise',
    aml_matched               BOOLEAN       COMMENT 'True if an AML screening resolved to this master',
    aml_source_key            STRING        COMMENT 'Linked AML screening_ref, if matched. Null otherwise',
    -- per-source match provenance (SPEC §6) -----------------------------------
    aecb_match_method         STRING        COMMENT 'AECB link method. Values: DETERMINISTIC, PROBABILISTIC. Null if not matched',
    aecb_match_confidence     DECIMAL(5,4)  COMMENT 'AECB link confidence 0.0000-1.0000. Null if not matched',
    fraud_match_method        STRING        COMMENT 'Fraud link method. Values: DETERMINISTIC, PROBABILISTIC. Null if not matched',
    fraud_match_confidence    DECIMAL(5,4)  COMMENT 'Fraud link confidence 0.0000-1.0000. Null if not matched',
    aml_match_method          STRING        COMMENT 'AML link method (always PROBABILISTIC when matched). Null if not matched',
    aml_match_confidence      DECIMAL(5,4)  COMMENT 'AML link confidence 0.0000-1.0000. Null if not matched',
    aecb_needs_review         BOOLEAN       COMMENT 'True if the AECB link scored in [0.70,0.85) — stewardship queue (SPEC §6)',
    fraud_needs_review        BOOLEAN       COMMENT 'True if the fraud link scored in [0.70,0.85) — stewardship queue (SPEC §6)',
    aml_needs_review          BOOLEAN       COMMENT 'True if the AML link scored in [0.70,0.85) — stewardship queue (SPEC §6)',
    -- overall provenance (SPEC §6 mandatory) ----------------------------------
    matched_on                ARRAY<STRING> COMMENT 'Keys that fired, e.g. ["internal_customer_uuid","emirates_id","phone+email"] (SPEC §6)',
    match_confidence          DECIMAL(5,4)  COMMENT 'Overall confidence = weakest attached link (spine seed = 1.0000) (SPEC §6)',
    match_method              STRING        COMMENT 'Overall method: PROBABILISTIC if any attached link is fuzzy, else DETERMINISTIC (SPEC §6)',
    needs_manual_review       BOOLEAN       COMMENT 'True if any per-source link needs review — attached but flagged for the stewardship queue (SPEC §6)',
    source_record_key         STRING        COMMENT 'The spine internal_customer_uuid for resolved rows; the source native key for UNRESOLVED rows',
    source_system             STRING        COMMENT 'Audit: originating system of this xref row',
    batch_id                  STRING        COMMENT 'Audit: Glue run/batch id that produced this xref row',
    created_timestamp         TIMESTAMP     COMMENT 'Audit: when this xref row was first written, GST',
    updated_timestamp         TIMESTAMP     COMMENT 'Audit: when this xref row was last updated, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/customer_identity_xref/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
