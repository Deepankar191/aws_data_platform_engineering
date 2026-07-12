-- =============================================================================
-- credit_silver.v_decision_audit_trail
-- -----------------------------------------------------------------------------
-- The regulator lookup (SPEC §7). For any decision_id it exposes the full audit
-- chain: the decision inputs the engine saw, joined to the immutable snapshot index
-- (snapshot_s3_uri + content_sha256 + per-source bronze URIs and record hashes and
-- the Object-Lock retain-until). A reviewer fetches the snapshot object at
-- snapshot_s3_uri and verifies its SHA-256 equals content_sha256 to prove the record
-- has not been tampered with. LEFT JOIN so a decision missing its snapshot is visible
-- (a compliance red flag) rather than silently filtered out.
--
-- Usage:
--   SELECT * FROM credit_silver.v_decision_audit_trail
--   WHERE decision_id = '<uuid>';
-- =============================================================================
CREATE OR REPLACE VIEW credit_silver.v_decision_audit_trail AS
SELECT
    di.decision_id,
    di.application_id,
    di.master_customer_id,
    di.internal_customer_uuid,
    di.product_code,
    di.decision_timestamp,
    -- inputs the engine saw ----------------------------------------------------
    di.aecb_credit_score,
    di.aecb_total_outstanding_aed,
    di.aecb_report_ref,
    di.fraud_score,
    di.fraud_decision,
    di.aml_status,
    di.is_pep,
    di.monthly_income_aed,
    di.kyc_completed,
    di.requested_amount_aed,
    di.approved_amount_aed,
    di.input_completeness_score,
    di.dq_pass,
    -- immutable snapshot linkage (SPEC §7) ------------------------------------
    di.snapshot_s3_uri,
    snap.content_sha256,
    snap.object_lock_mode,
    snap.object_lock_retain_until_timestamp,
    snap.aecb_bronze_s3_uri,
    snap.aecb_record_sha256,
    snap.fraud_bronze_s3_uri,
    snap.fraud_record_sha256,
    snap.aml_bronze_s3_uri,
    snap.aml_record_sha256,
    snap.profile_bronze_s3_uri,
    snap.profile_record_sha256,
    (snap.decision_id IS NULL)                        AS snapshot_missing,
    di.created_timestamp                              AS decision_created_timestamp,
    snap.created_timestamp                            AS snapshot_created_timestamp
FROM credit_silver.decision_input di
LEFT JOIN credit_silver.decision_input_snapshot snap
    ON di.decision_id = snap.decision_id;
