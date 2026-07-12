"""Unit tests for the pure identity-matching core (glue/common/text_match.py, SPEC §6).

These run WITHOUT Spark or AWS — the whole point of factoring the algorithm out of
the Spark UDFs. They pin the Jaro-Winkler implementation to the canonical literature
values, exercise the weighted multi-attribute scorer, and prove master-id determinism.
"""

import pytest

from common.text_match import jaro, jaro_winkler, weighted_match_confidence, master_id
from common.constants import MATCH_THRESHOLD, REVIEW_THRESHOLD, UNRESOLVED_SENTINEL


# --------------------------------------------------------------------------- #
# Jaro / Jaro-Winkler — canonical reference values
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "a,b,expected",
    [
        ("MARTHA", "MARHTA", 0.9611),   # classic JW reference pair
        ("DWAYNE", "DUANE", 0.8400),
        ("DIXON", "DICKSONX", 0.8133),
        ("abc", "abc", 1.0000),
    ],
)
def test_jaro_winkler_reference_values(a, b, expected):
    assert jaro_winkler(a, b) == pytest.approx(expected, abs=1e-4)


def test_jaro_winkler_bounds_and_empty():
    assert jaro_winkler("", "x") == 0.0
    assert jaro_winkler("x", "") == 0.0
    assert jaro_winkler(None, "x") == 0.0
    assert 0.0 <= jaro("Sheikh", "Shaikh") <= 1.0


def test_jaro_winkler_symmetric():
    assert jaro_winkler("Sheikh", "Shaikh") == pytest.approx(jaro_winkler("Shaikh", "Sheikh"))


def test_jaro_winkler_prefix_boost():
    # JW >= Jaro because the shared prefix boosts similarity.
    assert jaro_winkler("Rajesh", "Rajenh") >= jaro("Rajesh", "Rajenh")


# --------------------------------------------------------------------------- #
# Weighted multi-attribute confidence (SPEC §6)
# --------------------------------------------------------------------------- #

def test_confidence_all_exact_is_one():
    c = weighted_match_confidence(
        "Ahmed Al Mansoori", "Ahmed Al Mansoori",
        "1988-03-14", "1988-03-14",
        "+971501234567", "+971501234567",
        "a@x.ae", "a@x.ae",
        "784-1988-1234567-1", "784-1988-1234567-1",
    )
    assert c == pytest.approx(1.0, abs=1e-4)


def test_confidence_no_shared_evidence_is_zero():
    # Nothing populated on both sides -> renormalised denominator is 0 -> 0.0.
    assert weighted_match_confidence(*([None] * 10)) == 0.0


def test_confidence_single_strong_exact_key_is_one():
    # Only EID present on both sides and it matches -> full confidence on that evidence.
    c = weighted_match_confidence(None, None, None, None, None, None, None, None,
                                  "784-1", "784-1")
    assert c == pytest.approx(1.0)


def test_confidence_name_dob_fuzzy_surname_still_attaches():
    # SPEC §6.4 AML path: 'Omar Sheikh' vs 'Omar Shaikh', same DOB -> should clear
    # the attach threshold even though the surname differs.
    c = weighted_match_confidence(
        "Omar Sheikh", "Omar Shaikh", "1983-12-25", "1983-12-25",
        None, None, None, None, None, None,
    )
    assert c >= MATCH_THRESHOLD


def test_confidence_conflicting_dob_lowers_score():
    exact = weighted_match_confidence("Sara Abdullah", "Sara Abdullah",
                                      "1995-01-09", "1995-01-09",
                                      None, None, None, None, None, None)
    conflict = weighted_match_confidence("Sara Abdullah", "Sara Abdullah",
                                         "1995-01-09", "1970-01-01",
                                         None, None, None, None, None, None)
    assert conflict < exact


def test_thresholds_are_ordered():
    assert 0.0 < REVIEW_THRESHOLD < MATCH_THRESHOLD <= 1.0


# --------------------------------------------------------------------------- #
# Deterministic master id (SPEC §6.1)
# --------------------------------------------------------------------------- #

def test_master_id_is_deterministic():
    uid = "11111111-1111-4111-8111-111111111111"
    assert master_id(uid) == master_id(uid)


def test_master_id_distinct_for_distinct_input():
    assert master_id("11111111-1111-4111-8111-111111111111") != master_id(
        "22222222-2222-4222-8222-222222222222"
    )


def test_master_id_none_is_unresolved_sentinel():
    assert master_id(None) == UNRESOLVED_SENTINEL
    assert master_id("") == UNRESOLVED_SENTINEL


def test_master_id_is_uuid_shaped():
    mid = master_id("33333333-3333-4333-8333-333333333333")
    assert len(mid) == 36 and mid.count("-") == 4
