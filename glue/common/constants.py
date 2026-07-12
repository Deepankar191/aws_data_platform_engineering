"""Platform-wide constants — SPEC §11 (single source of truth).

Nothing environment-specific or secret lives here. The S3 bucket is derived at
runtime from the ``--env`` job argument (SPEC §3); databases and table names are
fixed by SPEC §4.
"""

import uuid

# --------------------------------------------------------------------------- #
# SPEC §11 — Constants used across code
# --------------------------------------------------------------------------- #
MATCH_THRESHOLD = 0.85
REVIEW_THRESHOLD = 0.70
SURVIVORSHIP_PRIORITY = ["POSTGRES", "AECB", "FRAUD", "AML"]
UNRESOLVED_SENTINEL = "UNRESOLVED"
SNAPSHOT_RETENTION_YEARS = 7
TZ = "Asia/Dubai"  # GST, UTC+4

# --------------------------------------------------------------------------- #
# SPEC §4 — Glue Data Catalog databases
# --------------------------------------------------------------------------- #
DB_BRONZE = "credit_bronze"
DB_SILVER = "credit_silver"
DB_GOLD = "credit_gold"

# --------------------------------------------------------------------------- #
# SPEC §5 / §6 / §7 / §8 / §9 — silver & gold table names
# --------------------------------------------------------------------------- #
TBL_AECB = "aecb_credit_report"
TBL_FRAUD = "fraud_score"
TBL_AML = "aml_screening"
TBL_CUSTOMER_PROFILE = "customer_profile"
TBL_IDENTITY_XREF = "customer_identity_xref"
TBL_DECISION_INPUT = "decision_input"
TBL_DECISION_INPUT_QUARANTINE = "decision_input_quarantine"
TBL_DECISION_SNAPSHOT = "decision_input_snapshot"
TBL_PORTFOLIO_DAILY = "portfolio_monitoring_daily"
TBL_DQ_SCORECARD_DAILY = "dq_scorecard_daily"

# --------------------------------------------------------------------------- #
# Source-system identifiers (audit block + survivorship priority keys)
# --------------------------------------------------------------------------- #
SRC_POSTGRES = "POSTGRES"
SRC_AECB = "AECB"
SRC_FRAUD = "FRAUD"
SRC_AML = "AML"

# --------------------------------------------------------------------------- #
# SPEC §1 / §5 — controlled enums
# --------------------------------------------------------------------------- #
PRODUCT_CODES = ["PERSONAL_FINANCE", "BNPL", "CARD_ALT"]
FRAUD_DECISIONS = ["APPROVE", "REVIEW", "DECLINE"]
AML_STATUSES = ["CLEAR", "HIT", "PENDING"]
DECISION_OUTCOMES = ["APPROVED", "DECLINED", "REVIEW"]

# --------------------------------------------------------------------------- #
# SPEC §6 — probabilistic scorer weights.
# Weighted contribution to match_confidence; the scorer renormalises over the
# fields that are actually populated on *both* sides so confidence stays in
# [0, 1] even when some attributes are missing (SPEC §6, conflict resolution).
# --------------------------------------------------------------------------- #
MATCH_WEIGHTS = {
    "name": 0.40,   # Jaro-Winkler similarity on full_name
    "dob": 0.20,    # exact match on date_of_birth
    "phone": 0.15,  # exact match on E.164 phone
    "email": 0.15,  # exact match on lowercased email
    "eid": 0.10,    # exact match on normalised emirates_id
}
# Jaro-Winkler score at/above which a name pair is treated as an exact name hit
# for the purpose of the strong-key bonus (kept < 1.0 to tolerate typos).
NAME_STRONG_MATCH = 0.92

# --------------------------------------------------------------------------- #
# Deterministic master_customer_id generation (SPEC §6.1).
# master_customer_id = UUIDv5(namespace, internal_customer_uuid) so the same
# spine id always maps to the same master id — stable and reproducible.
# --------------------------------------------------------------------------- #
MASTER_ID_NAMESPACE = uuid.UUID("6f9a1e2c-7b3d-5a4e-9c8f-0d1b2a3c4d5e")

# --------------------------------------------------------------------------- #
# SPEC §8 — WARN-tier source freshness SLAs (max acceptable lag).
# Consumed by the DQ scorecard job; not blocking.
# --------------------------------------------------------------------------- #
FRESHNESS_SLA_HOURS = {
    SRC_AECB: 24.0,
    SRC_FRAUD: 1.0,
    SRC_AML: 6.0,
    SRC_POSTGRES: 0.25,  # profile CDC < 15m
}

# --------------------------------------------------------------------------- #
# SPEC §8 — decision recency bound (must-pass): not older than 48h at load.
# --------------------------------------------------------------------------- #
DECISION_MAX_AGE_HOURS = 48.0

# --------------------------------------------------------------------------- #
# SPEC §8 — WARN thresholds
# --------------------------------------------------------------------------- #
COMPLETENESS_MIN = 0.75           # per-row input_completeness_score floor
COMPLETENESS_MIN_ROW_RATIO = 0.95  # ≥95% of rows must clear the floor
AML_PENDING_MAX_RATIO = 0.05       # ≤5% of rows may be AML PENDING


def s3_bucket(env: str) -> str:
    """SPEC §3 — ``s3://wio-credit-decision-${ENV}`` where ENV ∈ {dev,pre,prod}."""
    env = env.lower().strip()
    if env not in ("dev", "pre", "prod"):
        raise ValueError(f"env must be one of dev|pre|prod, got {env!r}")
    return f"wio-credit-decision-{env}"


def s3_uri(env: str, *parts: str) -> str:
    """Join a path under the medallion bucket, e.g. ``s3_uri('dev','silver','decision_input')``."""
    key = "/".join(p.strip("/") for p in parts if p)
    return f"s3://{s3_bucket(env)}/{key}"
