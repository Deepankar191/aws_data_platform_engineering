-- =============================================================================
-- credit_silver.decision_input  (SPEC §5 — the unified decision record)
-- -----------------------------------------------------------------------------
-- One row per credit decision (a scoring event for an application).
-- Grain: one row per decision_id. Delta table read by Athena v3.
-- Fuses the 4 conformed sources onto the golden record (master_customer_id from
-- customer_identity_xref, SPEC §6) and points at the immutable snapshot (SPEC §7).
-- Only rows with dq_pass = TRUE (all must-pass DQ green, SPEC §8) reach gold; failures
-- are routed to decision_input_quarantine. Money DECIMAL(18,2); score DECIMAL(5,4).
-- Carries silver audit block. Timestamps GST (UTC+4).
--
-- The first block below is the SPEC §5 key-column contract, column-for-column.
-- The "application economics" block extends it with the requested/approved amounts
-- that the SPEC §9 portfolio mart aggregates ("full DDL in athena/ddl/", SPEC §5).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.decision_input (
    -- SPEC §5 contract ---------------------------------------------------------
    decision_id                STRING        COMMENT 'UUID, one per scoring event. NOT NULL, unique. Grain (SPEC §5, DQ must-pass §8)',
    application_id             STRING        COMMENT 'The credit application id. NOT NULL (SPEC §5, DQ must-pass §8)',
    master_customer_id         STRING        COMMENT 'Golden-record id from identity xref (SPEC §6). NOT NULL and must not be UNRESOLVED sentinel (DQ must-pass §8)',
    internal_customer_uuid     STRING        COMMENT 'PostgreSQL spine id (SPEC §6.1). Nullable if resolved via non-spine source',
    product_code               STRING        COMMENT 'Credit product. Values: PERSONAL_FINANCE, BNPL, CARD_ALT (DQ must-pass enum §8)',
    decision_timestamp         TIMESTAMP     COMMENT 'When the decision was scored, GST. NOT NULL; not future, not older than 48h at load (DQ must-pass §8)',
    decision_date              DATE          COMMENT 'Date of decision_timestamp (GST). Snapshot/partition grain; feeds portfolio_monitoring_daily.snapshot_date (SPEC §9)',
    -- AECB inputs --------------------------------------------------------------
    aecb_credit_score          INT           COMMENT 'AECB credit score. When present must be 300-900 (DQ must-pass §8). Null if no AECB report',
    aecb_total_outstanding_aed DECIMAL(18,2) COMMENT 'AECB total outstanding balance, AED. Null if no AECB report',
    aecb_report_ref            STRING        COMMENT 'AECB report reference used for this decision (traceability). Null if no AECB report',
    -- Fraud inputs -------------------------------------------------------------
    fraud_score                DECIMAL(5,4)  COMMENT 'Fraud probability 0.0000-1.0000. Must be between 0 and 1 (DQ must-pass §8). Null if no fraud score',
    fraud_decision             STRING        COMMENT 'Fraud recommendation. Values: APPROVE, REVIEW, DECLINE',
    -- AML inputs ---------------------------------------------------------------
    aml_status                 STRING        COMMENT 'AML screening outcome. Values: CLEAR, HIT, PENDING',
    is_pep                     BOOLEAN       COMMENT 'True if the customer is a Politically Exposed Person',
    -- Internal profile inputs --------------------------------------------------
    monthly_income_aed         DECIMAL(18,2) COMMENT 'Declared monthly income, AED, from the internal profile',
    kyc_completed              BOOLEAN       COMMENT 'True when KYC is complete for the customer',
    -- application economics (extends §5; feeds the §9 portfolio mart) ----------
    requested_amount_aed       DECIMAL(18,2) COMMENT 'Amount the customer requested on the application, AED. Feeds portfolio avg_requested_amount_aed (SPEC §9)',
    approved_amount_aed        DECIMAL(18,2) COMMENT 'Amount approved by the decision, AED. 0 for declines. Feeds portfolio avg_approved_amount_aed (SPEC §9)',
    decision_outcome           STRING        COMMENT 'Final decision from the credit engine. Values: APPROVED, DECLINED, REFERRED. Drives decision_outcome_band in the §9 mart',
    -- traceability -------------------------------------------------------------
    input_completeness_score   DECIMAL(5,4)  COMMENT 'Fraction of expected inputs present 0.0000-1.0000. WARN if < 0.75 for >5% of rows (SPEC §8)',
    dq_pass                    BOOLEAN       COMMENT 'True when all must-pass DQ rules are green (SPEC §8). Only TRUE rows reach gold',
    snapshot_s3_uri            STRING        COMMENT 'Pointer to the immutable audit snapshot object in the locked bucket (SPEC §7)',
    -- silver audit block -------------------------------------------------------
    source_system              STRING        COMMENT 'Audit: originating system. Constant "DECISION_ENGINE"',
    batch_id                   STRING        COMMENT 'Audit: Glue run/batch id that produced this row',
    created_timestamp          TIMESTAMP     COMMENT 'Audit: when this row was first written, GST. NOT NULL',
    updated_timestamp          TIMESTAMP     COMMENT 'Audit: when this row was last updated, GST. NOT NULL'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/decision_input/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
