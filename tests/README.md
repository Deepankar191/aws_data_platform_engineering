# Tests

Fast, dependency-light checks that pin the load-bearing logic and guard the contracts.
The default suite runs on **pytest + stdlib only — no Spark, no AWS** — so it runs in CI
and locally in well under a second.

## Run

```bash
pip install -r tests/requirements.txt
pytest tests/ -q
```

## What's covered

| File | Pins | Runs without Spark? |
|---|---|---|
| `test_text_match.py` | The pure identity-matching core (`glue/common/text_match.py`, SPEC §6): Jaro-Winkler against canonical reference values (MARTHA/MARHTA=0.9611, DWAYNE/DUANE=0.84, DIXON/DICKSONX=0.8133), the weighted multi-attribute scorer, deterministic `master_customer_id`. | ✅ |
| `test_sample_data_integrity.py` | `sample_data/` actually cross-references across all four sources so identity resolution has real linkage — and the two **intentional conflicts** exist: the fraud phone mismatch (SPEC §6.3 single-key path) and the `Sheikh`/`Shaikh` surname variant (SPEC §6.4 fuzzy path). Also validates the `decisions` driver feed (§2.1) enums and keys. | ✅ |
| `test_ddl_consistency.py` | Every Athena DDL obeys the Wio data-modeling standard: snake_case columns, money = `DECIMAL(18,2)` (never `FLOAT`/`DOUBLE`), `*_timestamp`→`TIMESTAMP` / `*_date`→`DATE` on silver/gold, and `decision_input` carries the SPEC §5 contract column-for-column. | ✅ |

## Why the algorithm is a separate module

`glue/common/identity.py` imports pyspark, so it can't be unit-tested without a Spark
runtime. The *algorithmic core* (Jaro-Winkler, the weighted scorer, the UUIDv5 master-id)
is therefore factored into the Spark-free `glue/common/text_match.py`, and `identity.py`
wraps those functions as Spark UDFs. That keeps the correctness-critical logic testable in
isolation — this suite proves it — while the Spark layer stays a thin adapter.

## Spark-backed transformation tests (optional)

The end-to-end Glue transforms (bronze→silver, identity xref, DQ gating, marts) run on
Spark + Delta. To exercise them locally, uncomment `pyspark`/`delta-spark` in
`requirements.txt`; tests that need Spark **skip automatically** when it isn't importable,
so the default run stays green everywhere. In CI they run on the Glue 4.0 / Spark 3.3 image.
