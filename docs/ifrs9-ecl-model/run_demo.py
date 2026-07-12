#!/usr/bin/env python3
"""
Runnable IFRS 9 ECL demo (DuckDB).

Loads schema/schema.sql + schema/sample_data.sql, runs the four sample queries in
queries/, materialises the fact and audit tables (with a real SHA-256
reproducibility hash per provision), and prints every result — so the model is
demonstrably correct, not just described.

    pip install duckdb
    python3 run_demo.py
"""
from __future__ import annotations

import hashlib
import json
import os

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
REPORTING_DATE = "2025-06-30"
ECL_MODEL_VERSION = "ecl_engine v3.1"


def sql(*parts: str) -> str:
    with open(os.path.join(HERE, *parts), encoding="utf-8") as fh:
        return fh.read()


def show(con, title: str, query: str) -> list:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")
    rel = con.sql(query)
    print(rel)
    return rel.fetchall()


def main() -> int:
    con = duckdb.connect()  # in-memory

    # 1) schema + seed --------------------------------------------------------
    con.execute(sql("schema", "schema.sql"))
    con.execute(sql("schema", "sample_data.sql"))
    print("Loaded schema + seed: "
          f"{con.sql('SELECT COUNT(*) FROM dim_exposure').fetchone()[0]} exposures, "
          f"{con.sql('SELECT COUNT(*) FROM fact_exposure_snapshot').fetchone()[0]} snapshots, "
          f"{con.sql('SELECT COUNT(*) FROM risk_parameter_set').fetchone()[0]} parameter sets "
          "(PiT base/upside/downside + TTC).")

    # 2) stage classification -> materialise fact_staging_assessment ----------
    show(con, "Query 1 — STAGE CLASSIFICATION (SICR / default triggers)",
         sql("queries", "01_stage_classification.sql").rstrip().rstrip(";"))
    con.execute("INSERT INTO fact_staging_assessment "
                + sql("queries", "01_stage_classification.sql").rstrip().rstrip(";"))

    # 3) provision calculation (display) --------------------------------------
    show(con, "Query 2 — ECL PROVISION (EAD x PD x LGD, scenario-weighted, by stage)",
         sql("queries", "02_provision_calculation.sql").rstrip().rstrip(";"))

    # 4) materialise fact_ecl_provision (12m + lifetime + reported, weighted) --
    con.execute(f"""
        INSERT INTO fact_ecl_provision
        WITH stage AS (
            SELECT s.reporting_date, s.exposure_id,
                CASE WHEN (s.days_past_due>=90 OR s.is_unlikely_to_pay) THEN 3
                     WHEN (s.current_lifetime_pd/NULLIF(e.origination_lifetime_pd,0)>=2.0
                           OR s.days_past_due>=30 OR s.is_forborne OR s.is_watchlist) THEN 2
                     ELSE 1 END AS assigned_stage
            FROM fact_exposure_snapshot s JOIN dim_exposure e USING (exposure_id)),
        base AS (
            SELECT s.reporting_date, s.exposure_id, e.product_type, e.collateral_type,
                s.current_rating_grade, s.remaining_maturity_months, s.effective_interest_rate,
                s.drawn_balance_aed, s.undrawn_commitment_aed, st.assigned_stage,
                COALESCE((SELECT MIN(h.horizon_months) FROM (SELECT DISTINCT horizon_months FROM pd_term_structure) h
                          WHERE h.horizon_months >= s.remaining_maturity_months),
                         (SELECT MAX(horizon_months) FROM pd_term_structure)) AS lifetime_horizon
            FROM fact_exposure_snapshot s JOIN dim_exposure e USING (exposure_id)
            JOIN stage st USING (reporting_date, exposure_id)
            WHERE s.reporting_date = DATE '{REPORTING_DATE}'),
        scn AS (
            SELECT b.*, w.scenario_id, w.probability_weight, p.parameter_set_id
            FROM base b JOIN scenario_weight w ON w.reporting_date=b.reporting_date
            JOIN risk_parameter_set p ON p.parameter_basis='PIT' AND p.macro_scenario_id=w.scenario_id
                 AND p.is_current AND b.reporting_date>=p.effective_from_date
                 AND (p.effective_to_date IS NULL OR b.reporting_date<=p.effective_to_date)),
        duo AS (
            SELECT scn.reporting_date, scn.exposure_id, scn.assigned_stage, scn.probability_weight,
                ROUND(scn.drawn_balance_aed + ccf.credit_conversion_factor*scn.undrawn_commitment_aed,2) AS ead,
                ROUND((scn.drawn_balance_aed + ccf.credit_conversion_factor*scn.undrawn_commitment_aed)
                      * (CASE WHEN scn.assigned_stage=3 THEN 1.0 ELSE pd12.cumulative_pd END)
                      * lgd.downturn_lgd_rate
                      * POWER(1+scn.effective_interest_rate,-0.5),2) AS ecl12,
                ROUND((scn.drawn_balance_aed + ccf.credit_conversion_factor*scn.undrawn_commitment_aed)
                      * (CASE WHEN scn.assigned_stage=3 THEN 1.0 ELSE pdl.cumulative_pd END)
                      * lgd.downturn_lgd_rate
                      * POWER(1+scn.effective_interest_rate,-1*(scn.lifetime_horizon/12.0)/2),2) AS ecll
            FROM scn
            JOIN ccf_parameter ccf ON ccf.parameter_set_id=scn.parameter_set_id AND ccf.product_type=scn.product_type
            JOIN lgd_parameter lgd ON lgd.parameter_set_id=scn.parameter_set_id AND lgd.collateral_segment=scn.collateral_type
            JOIN pd_term_structure pd12 ON pd12.parameter_set_id=scn.parameter_set_id
                 AND pd12.rating_grade=scn.current_rating_grade AND pd12.horizon_months=12
            JOIN pd_term_structure pdl ON pdl.parameter_set_id=scn.parameter_set_id
                 AND pdl.rating_grade=scn.current_rating_grade AND pdl.horizon_months=scn.lifetime_horizon)
        SELECT reporting_date, exposure_id, assigned_stage,
            MAX(ead) AS ead_amount_aed,
            ROUND(SUM(probability_weight*ecl12),2) AS ecl_12m_aed,
            ROUND(SUM(probability_weight*ecll),2) AS ecl_lifetime_aed,
            ROUND(SUM(probability_weight*CASE WHEN assigned_stage=1 THEN ecl12 ELSE ecll END),2) AS reported_ecl_aed,
            ROUND(SUM(probability_weight*CASE WHEN assigned_stage=1 THEN ecl12 ELSE ecll END)/NULLIF(MAX(ead),0),6) AS coverage_ratio,
            'CALC-' || exposure_id || '-' || strftime(reporting_date,'%Y%m%d') AS calculation_id,
            CAST('{REPORTING_DATE} 03:00:00' AS TIMESTAMP) AS created_timestamp
        FROM duo GROUP BY reporting_date, exposure_id, assigned_stage
    """)

    # 5) stage-transition audit (display) + materialise ----------------------
    show(con, "Query 3 — STAGE-TRANSITION AUDIT (period-over-period migrations)",
         sql("queries", "03_stage_transition_audit.sql").rstrip().rstrip(";"))
    con.execute(f"""
        INSERT INTO audit_stage_transition
        SELECT 'TR-' || exposure_id || '-' || strftime(reporting_date,'%Y%m%d') AS transition_id,
               exposure_id, reporting_date, prior_reporting_date, prior_stage, new_stage,
               transition_direction, trigger_reason, dpd_at_transition, pd_ratio_at_transition,
               'ifrs9_staging v1.2' AS changed_by,
               CAST('{REPORTING_DATE} 03:05:00' AS TIMESTAMP) AS created_timestamp
        FROM ({sql("queries", "03_stage_transition_audit.sql").rstrip().rstrip(";")})
    """)

    # 6) provision-calculation audit with a real SHA-256 reproducibility hash --
    weights = {r[0]: float(r[1]) for r in con.sql(
        f"SELECT scenario_id, probability_weight FROM scenario_weight WHERE reporting_date=DATE '{REPORTING_DATE}'").fetchall()}
    pit_sets = [r[0] for r in con.sql(
        "SELECT parameter_set_id FROM risk_parameter_set WHERE parameter_basis='PIT' AND is_current ORDER BY 1").fetchall()]
    provisions = con.sql(f"""
        SELECT calculation_id, exposure_id, reporting_date, assigned_stage, ead_amount_aed, reported_ecl_aed
        FROM fact_ecl_provision WHERE reporting_date=DATE '{REPORTING_DATE}' ORDER BY exposure_id""").fetchall()
    for calc_id, exp, rd, stage, ead, ecl in provisions:
        canonical = json.dumps({"exposure": exp, "date": str(rd), "stage": stage,
                                "ead": float(ead), "ecl": float(ecl), "sets": pit_sets,
                                "weights": weights, "model": ECL_MODEL_VERSION}, sort_keys=True)
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        con.execute("INSERT INTO audit_provision_calculation VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?)",
                    [calc_id, exp, rd, stage, ECL_MODEL_VERSION, json.dumps(pit_sets),
                     json.dumps(weights), float(ead), float(ecl), digest,
                     "ecl_engine", f"{REPORTING_DATE} 03:00:00"])

    # 7) portfolio summary + audit evidence ----------------------------------
    show(con, "Query 4 — PORTFOLIO ECL SUMMARY BY STAGE",
         sql("queries", "04_portfolio_ecl_summary.sql").rstrip().rstrip(";"))

    show(con, "AUDIT — provision calculations are reproducible (model, param-set versions, weights, SHA-256)",
         """SELECT exposure_id, assigned_stage, reported_ecl_aed, ecl_model_version,
                   substr(input_sha256,1,16) || '...' AS input_sha256
            FROM audit_provision_calculation ORDER BY exposure_id""")

    total = con.sql(f"SELECT ROUND(SUM(reported_ecl_aed),2), ROUND(SUM(ead_amount_aed),2) "
                    f"FROM fact_ecl_provision WHERE reporting_date=DATE '{REPORTING_DATE}'").fetchone()
    print(f"\nPortfolio provision at {REPORTING_DATE}: AED {total[0]:,.2f} ECL on "
          f"AED {total[1]:,.2f} EAD  ({100*total[0]/total[1]:.2f}% coverage).\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
