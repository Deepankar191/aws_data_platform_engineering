"""Static checks over the Athena DDLs — enforce the Wio data-modeling conventions
(snake_case, money = DECIMAL(18,2) never FLOAT/DOUBLE, temporal suffixes, decision_input
carries the SPEC §5 contract) so a drift is caught in CI, not in production.

Pure stdlib; no Spark/AWS.
"""

import glob
import os
import re

import pytest

from conftest import repo_path

DDL_DIR = repo_path("athena", "ddl")
# DDLs are organised into bronze/ silver/ gold/ subdirectories.
DDL_FILES = sorted(glob.glob(os.path.join(DDL_DIR, "**", "*.sql"), recursive=True))
DECISION_INPUT_SQL = os.path.join(DDL_DIR, "silver_layer", "decision_input.sql")
# Bronze raw tables preserve the source schema (money/timestamps land as STRING and
# are typed in silver), so the strict money/temporal type checks apply to silver/gold only.
SILVER_GOLD_FILES = [p for p in DDL_FILES if not os.path.basename(p).endswith("_raw.sql")]

COL_RE = re.compile(r"^\s{4}([a-z_][a-z0-9_]*)\s+([A-Z][A-Z0-9(),<>]*)\s+COMMENT", re.M)


def _columns(path):
    """(name, type) for each declared column line."""
    return [(m.group(1), m.group(2).strip()) for m in COL_RE.finditer(open(path).read())]


def test_ddl_dir_not_empty():
    assert DDL_FILES, "no DDL files found"


@pytest.mark.parametrize("path", DDL_FILES, ids=[os.path.basename(p) for p in DDL_FILES])
def test_ddl_wellformed(path):
    sql = open(path).read()
    assert sql.count("CREATE EXTERNAL TABLE") == 1, "one CREATE per file"
    assert re.search(r"^\);", sql, re.M), "must close with );"
    assert _columns(path), "at least one column parsed"


@pytest.mark.parametrize("path", DDL_FILES, ids=[os.path.basename(p) for p in DDL_FILES])
def test_columns_snake_case(path):
    for name, _ in _columns(path):
        assert re.fullmatch(r"[a-z][a-z0-9_]*", name), f"{name} not snake_case in {path}"


@pytest.mark.parametrize("path", DDL_FILES, ids=[os.path.basename(p) for p in DDL_FILES])
def test_no_float_or_double(path):
    """Money/measures must never be FLOAT/DOUBLE (data-modeling standard §4)."""
    for name, typ in _columns(path):
        assert not typ.startswith(("FLOAT", "DOUBLE")), f"{name} is {typ} in {path}"


@pytest.mark.parametrize("path", SILVER_GOLD_FILES, ids=[os.path.basename(p) for p in SILVER_GOLD_FILES])
def test_money_columns_are_decimal_18_2(path):
    """Any *_aed column is a monetary value -> DECIMAL(18,2) in silver/gold."""
    for name, typ in _columns(path):
        if name.endswith("_aed"):
            assert typ.replace(" ", "") == "DECIMAL(18,2)", f"{name} is {typ} in {path}"


@pytest.mark.parametrize("path", SILVER_GOLD_FILES, ids=[os.path.basename(p) for p in SILVER_GOLD_FILES])
def test_temporal_suffix_types(path):
    """*_timestamp -> TIMESTAMP, *_date -> DATE (data-modeling standard §3.3)."""
    for name, typ in _columns(path):
        if name.endswith("_timestamp"):
            assert typ == "TIMESTAMP", f"{name} is {typ} in {path}"
        if name.endswith("_date"):
            assert typ == "DATE", f"{name} is {typ} in {path}"


def test_decision_input_carries_spec_5_contract():
    cols = {n for n, _ in _columns(DECISION_INPUT_SQL)}
    required = {
        "decision_id", "application_id", "master_customer_id", "internal_customer_uuid",
        "product_code", "decision_timestamp", "aecb_credit_score",
        "aecb_total_outstanding_aed", "fraud_score", "fraud_decision", "aml_status",
        "is_pep", "monthly_income_aed", "kyc_completed", "input_completeness_score",
        "dq_pass", "snapshot_s3_uri", "source_system", "batch_id",
        "created_timestamp", "updated_timestamp",
    }
    assert required <= cols, f"decision_input missing {required - cols}"


def test_score_columns_are_decimal():
    """fraud_score / *_score in 0-1 must be DECIMAL(5,4) not a float."""
    fs = dict(_columns(DECISION_INPUT_SQL))
    assert fs["fraud_score"].replace(" ", "") == "DECIMAL(5,4)"
    assert fs["input_completeness_score"].replace(" ", "") == "DECIMAL(5,4)"
