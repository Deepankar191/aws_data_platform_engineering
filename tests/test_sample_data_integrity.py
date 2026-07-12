"""Integrity tests over sample_data/ — proves the sample records actually cross-reference
across the four sources so the identity-resolution logic (SPEC §6) has real linkage to
resolve, and that the two INTENTIONAL conflicts exist (phone mismatch + fuzzy surname).

Pure stdlib + text_match; no Spark/AWS.
"""

import json
import re

import pytest

from conftest import repo_path
from common.text_match import jaro_winkler
from common.constants import MATCH_THRESHOLD

PRODUCTS = {"PERSONAL_FINANCE", "BNPL", "CARD_ALT"}
OUTCOMES = {"APPROVED", "DECLINED", "REFERRED"}
AML_STATUSES = {"CLEAR", "HIT", "PENDING"}


def _norm_eid(v):
    return re.sub(r"[\s\-]", "", v) if v else v


# --- loaders --------------------------------------------------------------- #

def _load_seed():
    """Parse the Postgres seed VALUES into dicts keyed by internal_customer_uuid."""
    sql = open(repo_path("sample_data", "postgres", "customer_profile_seed.sql")).read()
    row_re = re.compile(
        r"\('([0-9a-fA-F-]{36})',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)'"
    )
    rows = {}
    for uid, eid, name, dob, phone, email in row_re.findall(sql):
        rows[uid] = {
            "uuid": uid, "emirates_id": eid, "full_name": name,
            "date_of_birth": dob, "phone": phone, "email": email,
        }
    return rows


def _load_json(*parts):
    return json.load(open(repo_path("sample_data", *parts)))


SEED = _load_seed()
FRAUD = _load_json("fraud", "fraud_events_20250401.json")
AML = _load_json("aml", "aml_callbacks_20250401.json")
DECISIONS = _load_json("decisions", "decisions_20250401.json")
AECB_XML = open(repo_path("sample_data", "aecb", "aecb_report_batch_20250401.xml")).read()


# --- spine ------------------------------------------------------------------ #

def test_seed_has_seven_unique_customers():
    assert len(SEED) == 7
    assert len({r["uuid"] for r in SEED.values()}) == 7


# --- AECB deterministic linkage on emirates_id (SPEC §6.2) ------------------ #

def test_every_seed_emirates_id_present_in_aecb():
    aecb_eids = {_norm_eid(e) for e in re.findall(r"784-\d{4}-\d{7}-\d", AECB_XML)}
    for r in SEED.values():
        assert _norm_eid(r["emirates_id"]) in aecb_eids, r["emirates_id"]


# --- Fraud deterministic linkage on phone+email + the phone CONFLICT (§6.3) - #

def test_every_seed_email_present_in_fraud():
    fraud_emails = {e["email"].lower() for e in FRAUD}
    for r in SEED.values():
        assert r["email"].lower() in fraud_emails


def test_exactly_one_intentional_phone_conflict():
    """SPEC §6.3: for one customer the fraud phone differs (email still matches),
    forcing a single-key candidate through the probabilistic scorer."""
    fraud_by_email = {e["email"].lower(): e for e in FRAUD}
    conflicts = [
        r for r in SEED.values()
        if r["email"].lower() in fraud_by_email
        and fraud_by_email[r["email"].lower()]["phone"] != r["phone"]
    ]
    assert len(conflicts) == 1
    assert conflicts[0]["full_name"] == "Rajesh Kumar"


# --- AML fuzzy linkage on name+dob + the surname VARIANT (SPEC §6.4) -------- #

def test_aml_names_link_by_dob_and_are_similar():
    aml_by_dob = {a["date_of_birth"]: a for a in AML}
    fuzzy = 0
    for r in SEED.values():
        a = aml_by_dob.get(r["date_of_birth"])
        assert a is not None, f"no AML row for dob {r['date_of_birth']}"
        # Same person -> names must at least be highly similar (soundex/JW territory).
        assert jaro_winkler(r["full_name"].lower(), a["full_name"].lower()) >= MATCH_THRESHOLD
        if r["full_name"] != a["full_name"]:
            fuzzy += 1
    # Exactly one intentional surname variant (Sheikh vs Shaikh).
    assert fuzzy == 1


def test_aml_statuses_are_valid_enum():
    assert {a["aml_status"] for a in AML} <= AML_STATUSES
    # sample deliberately includes a PEP and an AML HIT for downstream DQ coverage
    assert any(a["is_pep"] for a in AML)
    assert any(a["aml_status"] == "HIT" for a in AML)


# --- Decisions driver feed (SPEC §2.1) -------------------------------------- #

def test_every_decision_references_a_seeded_customer():
    for d in DECISIONS:
        assert d["internal_customer_uuid"] in SEED, d["decision_id"]


def test_decision_enums_valid_and_ids_unique():
    ids = [d["decision_id"] for d in DECISIONS]
    assert len(ids) == len(set(ids)), "decision_id must be unique (grain)"
    for d in DECISIONS:
        assert d["product_code"] in PRODUCTS
        assert d["decision_outcome"] in OUTCOMES
        assert float(d["requested_amount_aed"]) >= 0
        assert float(d["approved_amount_aed"]) >= 0


def test_declines_have_zero_approved_amount():
    for d in DECISIONS:
        if d["decision_outcome"] in {"DECLINED", "REFERRED"}:
            assert float(d["approved_amount_aed"]) == 0.0
