-- =============================================================================
-- credit_silver.v_unresolved_identities
-- -----------------------------------------------------------------------------
-- The data-stewardship queue (SPEC §6). Surfaces every identity-xref row that needs
-- a human: either it never resolved (master_customer_id = UNRESOLVED sentinel /
-- is_unresolved) so no source data is silently dropped, or it attached only on a
-- borderline probabilistic score (needs_manual_review = TRUE, best score in
-- [0.70, 0.85)). Ordered worst-confidence first so stewards work the riskiest first.
-- UNRESOLVED_SENTINEL = 'UNRESOLVED' (SPEC §11).
-- =============================================================================
CREATE OR REPLACE VIEW credit_silver.v_unresolved_identities AS
SELECT
    x.master_customer_id,
    CASE
        WHEN x.is_unresolved OR x.master_customer_id = 'UNRESOLVED' THEN 'UNRESOLVED'
        WHEN x.needs_manual_review                                  THEN 'NEEDS_MANUAL_REVIEW'
    END                                                         AS steward_queue_reason,
    x.match_method,
    x.match_confidence,
    x.matched_on,
    x.needs_manual_review,
    x.is_unresolved,
    x.internal_customer_uuid,
    x.emirates_id_normalised,
    x.phone_normalised,
    x.email_normalised,
    x.full_name,
    x.date_of_birth,
    -- which source rows are dangling on this record ---------------------------
    x.aecb_report_ref,
    x.fraud_event_id,
    x.aml_screening_ref,
    x.survivorship_source,
    x.updated_timestamp
FROM credit_silver.customer_identity_xref x
WHERE x.is_unresolved = true
   OR x.master_customer_id = 'UNRESOLVED'
   OR x.needs_manual_review = true
ORDER BY x.match_confidence ASC, x.updated_timestamp DESC;
