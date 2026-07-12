"""Identity-resolution helpers — SPEC §6.

Pure functions returning Spark ``Column`` expressions (normalisers) plus two
UDFs (Jaro-Winkler weighted scorer, deterministic master id) so the same logic
is reusable from the xref builder and any ad-hoc reconciliation job.

Design notes
------------
* Normalisers are expressed as native Spark functions where possible (cheap,
  pushdown-friendly). Only the Jaro-Winkler scorer and the UUIDv5 generation are
  Python UDFs — Spark has no native Jaro-Winkler.
* The weighted scorer renormalises over populated fields (SPEC §6) so a pair
  that only shares, say, name+dob is still scored on a 0..1 scale.
"""

from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import StringType, DoubleType

from common.constants import (
    MATCH_THRESHOLD,
    REVIEW_THRESHOLD,
    UNRESOLVED_SENTINEL,
)

# Pure (Spark-free) algorithmic core, unit-tested in tests/test_text_match.py.
# Re-exported here (with the historical underscore names) so the UDF wrappers and
# any importer of identity.* keep working unchanged.
from common.text_match import (  # noqa: F401
    jaro as _jaro,
    jaro_winkler,
    weighted_match_confidence as _weighted_match_confidence,
    master_id as _master_id,
)

# --------------------------------------------------------------------------- #
# Field normalisers (SPEC §6.2–§6.4)
# --------------------------------------------------------------------------- #


def normalise_emirates_id(col: Column) -> Column:
    """Strip spaces/dashes from Emirates ID (SPEC §6.2). Blank → NULL."""
    cleaned = F.regexp_replace(F.trim(col), r"[\s\-]", "")
    return F.when((cleaned == "") | cleaned.isNull(), F.lit(None)).otherwise(cleaned)


def normalise_phone_e164(col: Column) -> Column:
    """Best-effort E.164 normalisation for UAE-centric numbers (SPEC §6.3).

    Keeps a leading ``+`` and digits only; a bare ``0`` national prefix on a
    UAE number is rewritten to ``+971``. Non-normalisable input → NULL.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9+]", "")
    # Local UAE format 05XXXXXXXX -> +9715XXXXXXXX
    local_uae = F.regexp_replace(digits, r"^0", "+971")
    # 9715XXXXXXXX (missing +) -> +9715...
    add_plus = F.when(
        local_uae.rlike(r"^971"), F.concat(F.lit("+"), local_uae)
    ).otherwise(local_uae)
    e164 = F.when(add_plus.rlike(r"^\+[0-9]{8,15}$"), add_plus).otherwise(F.lit(None))
    return e164


def lower_email(col: Column) -> Column:
    """Lowercase + trim email (SPEC §6.3). Blank → NULL."""
    cleaned = F.lower(F.trim(col))
    return F.when((cleaned == "") | cleaned.isNull(), F.lit(None)).otherwise(cleaned)


def name_soundex(col: Column) -> Column:
    """Soundex of full_name for AML blocking (SPEC §6.4). Uses native Spark soundex."""
    return F.soundex(F.trim(col))


def normalise_name(col: Column) -> Column:
    """Casefold + collapse whitespace for name comparison (feeds the scorer)."""
    collapsed = F.regexp_replace(F.trim(col), r"\s+", " ")
    return F.when(collapsed == "", F.lit(None)).otherwise(F.lower(collapsed))


# --------------------------------------------------------------------------- #
# UDF wrappers over the pure text-match core (common/text_match.py)
# --------------------------------------------------------------------------- #

# Registered UDFs (import these into jobs). Spark has no native Jaro-Winkler, and
# UUIDv5 generation is Python — so these two stay UDFs; the logic they wrap is the
# unit-tested pure code in common/text_match.py.
match_confidence_udf = F.udf(_weighted_match_confidence, DoubleType())
master_customer_id_udf = F.udf(_master_id, StringType())


# --------------------------------------------------------------------------- #
# Threshold logic (SPEC §6 / §11)
# --------------------------------------------------------------------------- #


def classify_match(confidence_col: Column) -> Column:
    """Map a confidence score to a ``match_method`` per SPEC §6 thresholds.

    * ``>= MATCH_THRESHOLD (0.85)``  -> PROBABILISTIC (attach)
    * ``>= REVIEW_THRESHOLD (0.70)`` -> PROBABILISTIC (attach, needs review)
    * ``< REVIEW_THRESHOLD``         -> UNRESOLVED (do not attach)
    """
    return (
        F.when(confidence_col >= F.lit(MATCH_THRESHOLD), F.lit("PROBABILISTIC"))
        .when(confidence_col >= F.lit(REVIEW_THRESHOLD), F.lit("PROBABILISTIC"))
        .otherwise(F.lit("UNRESOLVED"))
    )


def needs_manual_review(confidence_col: Column) -> Column:
    """TRUE when 0.70 <= confidence < 0.85 (SPEC §6, review band)."""
    return (confidence_col >= F.lit(REVIEW_THRESHOLD)) & (
        confidence_col < F.lit(MATCH_THRESHOLD)
    )


def is_attachable(confidence_col: Column) -> Column:
    """TRUE when confidence clears the review floor (attach vs. leave unresolved)."""
    return confidence_col >= F.lit(REVIEW_THRESHOLD)
