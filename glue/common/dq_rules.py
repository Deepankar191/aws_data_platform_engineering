"""Declarative data-quality rule definitions — SPEC §8.

Two tiers:

* :data:`MUST_PASS_RULES` — blocking, **row-level**. Each rule's ``predicate``
  returns a boolean Spark ``Column`` that is TRUE when the row *passes*. A row
  that fails any must-pass rule is quarantined (SPEC §8) and never reaches gold.
* :data:`UNIQUE_KEYS` — must-pass **uniqueness** (table-level), handled with a
  window in the DQ job because it cannot be expressed as a pure row predicate.
* :data:`WARN_RULES` — non-blocking **aggregate** rules. Each carries a per-row
  ``good`` predicate and an aggregate threshold; the DQ job computes the ratio
  and raises a WARN (SNS alert) if the threshold is breached.

Keeping these here (not inline in the job) means the Glue DQ job and any future
reconciliation tooling share exactly one definition of "good", and the Soda
checks in ``dq/soda/`` mirror the same thresholds.
"""

from dataclasses import dataclass
from typing import Callable, List

from pyspark.sql import functions as F
from pyspark.sql.column import Column

from common.constants import (
    AML_PENDING_MAX_RATIO,
    COMPLETENESS_MIN,
    COMPLETENESS_MIN_ROW_RATIO,
    DECISION_MAX_AGE_HOURS,
    PRODUCT_CODES,
    UNRESOLVED_SENTINEL,
)


@dataclass(frozen=True)
class RowRule:
    """A row-level must-pass rule. ``predicate()`` -> TRUE means the row passes."""

    name: str
    severity: str  # "MUST_PASS"
    description: str
    predicate: Callable[[], Column]


@dataclass(frozen=True)
class WarnRule:
    """Aggregate WARN rule.

    ``good`` is a per-row predicate (TRUE = row is fine). ``min_good_ratio`` is
    the floor for ``mean(good)``; if the observed ratio drops below it the rule
    is breached. Set ``invert=True`` for "max rate" style rules (e.g. AML PENDING
    ≤ 5%): there ``good`` marks the *bad* condition and the observed rate must
    stay **at or below** ``max_bad_ratio``.
    """

    name: str
    description: str
    good: Callable[[], Column]
    min_good_ratio: float = 0.0
    max_bad_ratio: float = 1.0
    invert: bool = False


# --------------------------------------------------------------------------- #
# MUST-PASS (blocking) — SPEC §8
# --------------------------------------------------------------------------- #

MUST_PASS_RULES: List[RowRule] = [
    RowRule(
        name="decision_id_not_null",
        severity="MUST_PASS",
        description="decision_id must be present.",
        predicate=lambda: F.col("decision_id").isNotNull()
        & (F.trim(F.col("decision_id")) != ""),
    ),
    RowRule(
        name="application_id_not_null",
        severity="MUST_PASS",
        description="application_id must be present.",
        predicate=lambda: F.col("application_id").isNotNull()
        & (F.trim(F.col("application_id")) != ""),
    ),
    RowRule(
        name="master_customer_id_not_null",
        severity="MUST_PASS",
        description="master_customer_id must be present.",
        predicate=lambda: F.col("master_customer_id").isNotNull()
        & (F.trim(F.col("master_customer_id")) != ""),
    ),
    RowRule(
        name="master_customer_id_resolved",
        severity="MUST_PASS",
        description="Identity must be resolved (not the UNRESOLVED sentinel).",
        predicate=lambda: F.col("master_customer_id") != F.lit(UNRESOLVED_SENTINEL),
    ),
    RowRule(
        name="fraud_score_in_range",
        severity="MUST_PASS",
        description="fraud_score must be between 0 and 1 (inclusive) when present.",
        # NULL fraud_score is tolerated here (completeness is a WARN concern);
        # a present value must be in [0, 1].
        predicate=lambda: F.col("fraud_score").isNull()
        | ((F.col("fraud_score") >= 0) & (F.col("fraud_score") <= 1)),
    ),
    RowRule(
        name="aecb_credit_score_in_range",
        severity="MUST_PASS",
        description="aecb_credit_score must be 300..900 when present.",
        predicate=lambda: F.col("aecb_credit_score").isNull()
        | ((F.col("aecb_credit_score") >= 300) & (F.col("aecb_credit_score") <= 900)),
    ),
    RowRule(
        name="product_code_in_enum",
        severity="MUST_PASS",
        description="product_code must be in the allowed enum.",
        predicate=lambda: F.col("product_code").isin(PRODUCT_CODES),
    ),
    RowRule(
        name="decision_timestamp_not_future",
        severity="MUST_PASS",
        description="decision_timestamp must not be in the future.",
        predicate=lambda: F.col("decision_timestamp") <= F.current_timestamp(),
    ),
    RowRule(
        name="decision_timestamp_not_stale",
        severity="MUST_PASS",
        description=f"decision_timestamp must not be older than {DECISION_MAX_AGE_HOURS}h at load.",
        predicate=lambda: F.col("decision_timestamp")
        >= (F.current_timestamp() - F.expr(f"INTERVAL {int(DECISION_MAX_AGE_HOURS)} HOURS")),
    ),
]

# Must-pass uniqueness — handled with a window (count over key) in the DQ job.
UNIQUE_KEYS: List[str] = ["decision_id", "application_id", "master_customer_id"]


# --------------------------------------------------------------------------- #
# WARN (non-blocking, alert) — SPEC §8
# --------------------------------------------------------------------------- #

WARN_RULES: List[WarnRule] = [
    WarnRule(
        name="input_completeness_floor",
        description=(
            f"≥{int(COMPLETENESS_MIN_ROW_RATIO * 100)}% of rows must have "
            f"input_completeness_score ≥ {COMPLETENESS_MIN}."
        ),
        good=lambda: F.col("input_completeness_score") >= F.lit(COMPLETENESS_MIN),
        min_good_ratio=COMPLETENESS_MIN_ROW_RATIO,
    ),
    WarnRule(
        name="aml_pending_rate",
        description=f"AML PENDING rate must be ≤ {int(AML_PENDING_MAX_RATIO * 100)}%.",
        # invert: `good` marks the *bad* condition (PENDING); rate must stay low.
        good=lambda: F.col("aml_status") == F.lit("PENDING"),
        max_bad_ratio=AML_PENDING_MAX_RATIO,
        invert=True,
    ),
]

# Note: source-freshness WARN rules (AECB<24h, fraud<1h, AML<6h, profile CDC<15m)
# are evaluated in the DQ job against per-source silver load timestamps using
# constants.FRESHNESS_SLA_HOURS — they are not row predicates on decision_input.
