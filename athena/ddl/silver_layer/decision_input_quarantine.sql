-- =============================================================================
-- credit_silver.decision_input_quarantine  (SPEC §8 — must-pass failure sink)
-- -----------------------------------------------------------------------------
-- Rows that failed one or more MUST-PASS DQ rules (SPEC §8) and are therefore held
-- back from gold / the risk mart. Same shape as decision_input plus the failure
-- provenance, so a steward can see exactly why a decision was quarantined and either
-- fix upstream data or re-drive it. Nothing is silently dropped (SPEC §6/§8).
-- Grain: one row per decision_id (latest quarantine attempt). Delta table read by
-- Athena v3. Timestamps GST (UTC+4).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.decision_input_quarantine (
    -- mirror of decision_input -------------------------------------------------
    decision_id                STRING        COMMENT 'UUID, one per scoring event. Grain of this table',
    application_id             STRING        COMMENT 'The credit application id',
    master_customer_id         STRING        COMMENT 'Golden-record id, or UNRESOLVED sentinel when identity did not resolve (SPEC §6/§8)',
    internal_customer_uuid     STRING        COMMENT 'PostgreSQL spine id (SPEC §6.1). Nullable',
    product_code               STRING        COMMENT 'Credit product. Values: PERSONAL_FINANCE, BNPL, CARD_ALT (may be invalid — that is a quarantine reason)',
    decision_timestamp         TIMESTAMP     COMMENT 'When the decision was scored, GST',
    decision_date              DATE          COMMENT 'Date of decision_timestamp (GST); physical partition of this table',
    aecb_credit_score          INT           COMMENT 'AECB credit score (may be out of 300-900 range — a quarantine reason)',
    aecb_total_outstanding_aed DECIMAL(18,2) COMMENT 'AECB total outstanding balance, AED',
    aecb_report_ref            STRING        COMMENT 'AECB report reference used for this decision',
    fraud_score                DECIMAL(5,4)  COMMENT 'Fraud probability (may be out of 0-1 range — a quarantine reason)',
    fraud_decision             STRING        COMMENT 'Fraud recommendation. Values: APPROVE, REVIEW, DECLINE',
    aml_status                 STRING        COMMENT 'AML screening outcome. Values: CLEAR, HIT, PENDING',
    is_pep                     BOOLEAN       COMMENT 'True if the customer is a Politically Exposed Person',
    monthly_income_aed         DECIMAL(18,2) COMMENT 'Declared monthly income, AED',
    kyc_completed              BOOLEAN       COMMENT 'True when KYC is complete',
    requested_amount_aed       DECIMAL(18,2) COMMENT 'Requested amount, AED',
    approved_amount_aed        DECIMAL(18,2) COMMENT 'Approved amount, AED',
    decision_outcome           STRING        COMMENT 'Engine outcome. Values: APPROVED, DECLINED, REFERRED',
    input_completeness_score   DECIMAL(5,4)  COMMENT 'Fraction of expected inputs present 0.0000-1.0000',
    dq_pass                    BOOLEAN       COMMENT 'Always FALSE/NULL in this table (row is quarantined)',
    snapshot_s3_uri            STRING        COMMENT 'Pointer to the immutable snapshot (still written for quarantined rows, SPEC §7)',
    -- quarantine provenance (SPEC §8) -----------------------------------------
    dq_fail_reasons            ARRAY<STRING> COMMENT 'Names of the must-pass checks that failed, e.g. ["fraud_score_range","identity_resolved"] (SPEC §8)',
    quarantined_timestamp      TIMESTAMP     COMMENT 'When the row was routed to quarantine, GST',
    -- silver audit block -------------------------------------------------------
    source_system              STRING        COMMENT 'Audit: originating system. Constant "DECISION_ENGINE"',
    batch_id                   STRING        COMMENT 'Audit: Glue run/batch id that produced this row',
    created_timestamp          TIMESTAMP     COMMENT 'Audit: when this row was first written, GST',
    updated_timestamp          TIMESTAMP     COMMENT 'Audit: when this row was last updated, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/decision_input_quarantine/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
