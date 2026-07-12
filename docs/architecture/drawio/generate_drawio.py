#!/usr/bin/env python3
"""
Generate an editable draw.io (diagrams.net) version of every architecture diagram.

Emits ONE multi-page file — docs/architecture/drawio/architecture.drawio — with five
pages (tabs), one per diagram in architecture.md:

    1. End-to-end data flow
    2. Medallion layers (Bronze / Silver / Gold)
    3. Identity resolution flow (SPEC §6)
    4. AWS deployment view
    5. Decision traceability — audit-snapshot sequence (SPEC §7)

Every shape and connector is a native mxGraph cell, so anyone can open the file at
https://app.diagrams.net (or the VS Code "Draw.io Integration" extension) and freely
move, relabel, restyle, add, or delete elements. No dependencies — pure stdlib.

Usage:
    python3 generate_drawio.py           # writes architecture.drawio next to this script
"""
from __future__ import annotations

import os
import xml.dom.minidom as minidom

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "architecture.drawio")

# --------------------------------------------------------------------------- #
# Styles (edit freely in draw.io afterwards)
# --------------------------------------------------------------------------- #
BOX = "rounded=1;whiteSpace=wrap;html=1;arcSize=8;"
STYLE = {
    "source":  BOX + "fillColor=#e8eaed;strokeColor=#5f6368;",
    "ingest":  BOX + "fillColor=#e1f0fa;strokeColor=#2e86c1;",
    "cdc":     BOX + "fillColor=#fde2c4;strokeColor=#d67c1c;",
    "bronze":  BOX + "fillColor=#f6e7c9;strokeColor=#b9770e;",
    "silver":  BOX + "fillColor=#e1ecf4;strokeColor=#2e6da4;",
    "gold":    BOX + "fillColor=#d4efdf;strokeColor=#1e8449;",
    "serve":   BOX + "fillColor=#eaf2f8;strokeColor=#2874a6;",
    "audit":   BOX + "fillColor=#f9d0d0;strokeColor=#c0392b;",
    "good":    BOX + "fillColor=#d4efdf;strokeColor=#1e8449;",
    "warn":    BOX + "fillColor=#fcf3cf;strokeColor=#b7950b;",
    "bad":     BOX + "fillColor=#f5b7b1;strokeColor=#c0392b;",
    "aws":     BOX + "fillColor=#fef6e7;strokeColor=#e67e22;",
    "net":     BOX + "fillColor=#eef7ff;strokeColor=#2e86c1;",
    "plain":   BOX + "fillColor=#ffffff;strokeColor=#333333;",
    "decision": "rhombus;whiteSpace=wrap;html=1;fillColor=#fdebd0;strokeColor=#b9770e;",
    "store":   "shape=cylinder3;whiteSpace=wrap;html=1;boundedLbl=1;backgroundOutline=1;"
               "fillColor=#e8eaed;strokeColor=#5f6368;",
    "queue":   "shape=hexagon;whiteSpace=wrap;html=1;fillColor=#fde2c4;strokeColor=#d67c1c;",
    "queuep":  "shape=hexagon;whiteSpace=wrap;html=1;fillColor=#eef7ff;strokeColor=#2e86c1;",
    "part":    BOX + "fillColor=#dae8fc;strokeColor=#6c8ebf;",
}
GROUP = "rounded=1;html=1;verticalAlign=top;fontStyle=1;fontSize=12;arcSize=4;"
GROUPC = {
    "bronze": GROUP + "fillColor=#fbf3e3;strokeColor=#b9770e;",
    "silver": GROUP + "fillColor=#f1f6fb;strokeColor=#2e6da4;",
    "gold":   GROUP + "fillColor=#eafaf1;strokeColor=#1e8449;",
    "band":   GROUP + "fillColor=#f7f9fb;strokeColor=#aab7c4;",
    "aws":    GROUP + "fillColor=#fffaf2;strokeColor=#e67e22;",
    "net":    GROUP + "fillColor=#f3f9ff;strokeColor=#2e86c1;",
    "ext":    GROUP + "fillColor=#f4f6f7;strokeColor=#7f8c8d;",
}

E_SOLID = "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;endArrow=block;endFill=1;strokeColor=#42556b;"
E_CDC = "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;dashed=1;endArrow=block;strokeColor=#d67c1c;"
E_AUDIT = "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;strokeWidth=3;endArrow=block;strokeColor=#c0392b;"
E_DOT = "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;dashed=1;endArrow=open;strokeColor=#7f8c8d;"
E_MSG = "html=1;endArrow=block;endFill=1;strokeColor=#42556b;"
E_RET = "html=1;endArrow=open;dashed=1;strokeColor=#7f8c8d;"
E_LIFE = "html=1;endArrow=none;dashed=1;strokeColor=#9aa7b4;"


def esc(s: str) -> str:
    s = (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
          .replace('"', "&quot;"))
    return s.replace("\n", "&#10;")


class Page:
    """Accumulates mxCells for one diagram page."""

    def __init__(self, name: str):
        self.name = name
        self.cells: list[str] = []
        self._n = 0

    def _id(self, prefix="c") -> str:
        self._n += 1
        return f"{prefix}{self._n}"

    def node(self, nid, label, x, y, w, h, style_key):
        self.cells.append(
            f'<mxCell id="{nid}" value="{esc(label)}" style="{STYLE[style_key]}" '
            f'vertex="1" parent="1"><mxGeometry x="{x}" y="{y}" width="{w}" '
            f'height="{h}" as="geometry"/></mxCell>')

    def group(self, gid, label, x, y, w, h, style_key="band"):
        self.cells.append(
            f'<mxCell id="{gid}" value="{esc(label)}" style="{GROUPC[style_key]}" '
            f'vertex="1" parent="1"><mxGeometry x="{x}" y="{y}" width="{w}" '
            f'height="{h}" as="geometry"/></mxCell>')

    def edge(self, src, dst, label="", style=E_SOLID):
        eid = self._id("e")
        self.cells.append(
            f'<mxCell id="{eid}" value="{esc(label)}" style="{style}" edge="1" '
            f'parent="1" source="{src}" target="{dst}">'
            f'<mxGeometry relative="1" as="geometry"/></mxCell>')

    def free_edge(self, x1, y1, x2, y2, label="", style=E_MSG):
        eid = self._id("e")
        self.cells.append(
            f'<mxCell id="{eid}" value="{esc(label)}" style="{style}" edge="1" parent="1">'
            f'<mxGeometry relative="1" as="geometry">'
            f'<mxPoint x="{x1}" y="{y1}" as="sourcePoint"/>'
            f'<mxPoint x="{x2}" y="{y2}" as="targetPoint"/></mxGeometry></mxCell>')

    def xml(self) -> str:
        body = "".join(self.cells)
        return (f'<diagram id="{esc(self.name)}" name="{esc(self.name)}">'
                f'<mxGraphModel dx="1400" dy="900" grid="1" gridSize="10" guides="1" '
                f'tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" '
                f'pageWidth="1600" pageHeight="1200" math="0" shadow="0"><root>'
                f'<mxCell id="0"/><mxCell id="1" parent="0"/>{body}</root></mxGraphModel></diagram>')


# --------------------------------------------------------------------------- #
# Diagram 1 — End-to-end data flow
# --------------------------------------------------------------------------- #
def diagram1() -> Page:
    p = Page("1. End-to-end data flow")
    CX, W, H = 210, 180, 60

    def col(c):
        return 40 + c * CX

    def row(r):
        return 40 + r * 100

    # bands (drawn behind)
    p.group("g_src", "① Four sources (§2)", 20, 20, 5 * CX - 10, 90, "band")
    p.group("g_ing", "② Ingestion mechanisms", 20, 120, 5 * CX - 10, 90, "band")
    p.group("g_brz", "③ Bronze — credit_bronze (§3)", 20, 220, 4 * CX - 30, 90, "bronze")
    p.group("g_slv", "④ Silver — credit_silver (Delta)", 20, 320, 4 * CX - 30, 490, "silver")
    p.group("g_gold", "⑥ Gold — credit_gold", 20, 920, 3 * CX - 30, 90, "gold")

    # sources
    p.node("AECB", "UAE Credit Bureau (AECB)\nSFTP · XML\nkey: emirates_id", col(0), row(0), W, H, "source")
    p.node("FRAUD", "Fraud provider\nREST poll · JSON\nkey: phone + email", col(1), row(0), W, H, "source")
    p.node("AML", "AML / PEP screening\nWebhook · JSON\nkey: full_name + dob", col(2), row(0), W, H, "source")
    p.node("PG", "Internal customer profile\nPostgreSQL / RDS\nspine: internal_customer_uuid", col(4), row(0), W, H, "store")
    # ingestion
    p.node("SFTPJOB", "Glue batch job\nSFTP → parse XML → Parquet", col(0), row(1), W, H, "ingest")
    p.node("POLLJOB", "Glue poll job\nREST GET → JSON → Parquet", col(1), row(1), W, H, "ingest")
    p.node("WEBHOOK", "API Gateway + Lambda\nwebhook → JSON → Parquet", col(2), row(1), W, H, "ingest")
    p.node("DBZ", "Debezium PostgreSQL\nsource connector", col(4), row(1), W, H, "cdc")
    # bronze
    p.node("B_AECB", "bronze/aecb (Parquet)", col(0), row(2), W, H, "bronze")
    p.node("B_FRAUD", "bronze/fraud (Parquet)", col(1), row(2), W, H, "bronze")
    p.node("B_AML", "bronze/aml (Parquet)", col(2), row(2), W, H, "bronze")
    p.node("MSK", "MSK / Kafka\ncdc.public.customer_profile", col(4), row(2), W, H, "queue")
    # cdc continues + silver
    p.node("S_AECB", "aecb_credit_report", col(0), row(3), W, H, "silver")
    p.node("S_FRAUD", "fraud_score", col(1), row(3), W, H, "silver")
    p.node("S_AML", "aml_screening", col(2), row(3), W, H, "silver")
    p.node("S3SINK", "MSK Connect\nS3 sink (Delta)", col(4), row(3), W, H, "cdc")
    p.node("B_CP", "bronze/customer_profile\n(Delta, CDC)", col(4), row(4), W, H, "cdc")
    p.node("S_CP", "customer_profile (spine)", col(4), row(5), W, H, "silver")
    # assembly
    p.node("IDR", "Identity resolution (§6)\ndeterministic → probabilistic", col(1), row(4), W, H, "decision")
    p.node("XREF", "customer_identity_xref\ngolden record", col(1), row(5), W, H, "silver")
    p.node("DI", "decision_input\none row per decision_id (§5)", col(1), row(6), W, H, "silver")
    p.node("SNAP", "decision_input_snapshot\nimmutable index (§7)", col(0), row(7), W, H, "audit")
    p.node("OBJLOCK", "S3 Object Lock bucket\ncompliance · 7 yr (§7)", col(2), row(7), W, H, "audit")
    # dq gate
    p.node("MUSTPASS", "MUST-PASS rules (§8)\nblocking", col(3), row(6), W, H, "decision")
    p.node("QUAR", "quarantine", col(3), row(7), W, H, "warn")
    # gold + serve
    p.node("PORT", "portfolio_monitoring_daily (§9)", col(0), row(9), W, H, "gold")
    p.node("DQSC", "dq_scorecard_daily (§8)", col(1), row(9), W, H, "gold")
    p.node("ATHENA", "Amazon Athena\n(Glue catalog)", col(3), row(9), W, H, "serve")
    p.node("META", "Metabase — risk dashboards", col(4), row(9), W, H, "serve")

    # edges — batch/API/webhook
    for a, b, l in [("AECB", "SFTPJOB", "SFTP"), ("SFTPJOB", "B_AECB", ""),
                    ("FRAUD", "POLLJOB", "poll"), ("POLLJOB", "B_FRAUD", ""),
                    ("AML", "WEBHOOK", "push"), ("WEBHOOK", "B_AML", "")]:
        p.edge(a, b, l)
    # cdc (distinct dotted)
    for a, b, l in [("PG", "DBZ", "WAL"), ("DBZ", "MSK", "produce"),
                    ("MSK", "S3SINK", "consume"), ("S3SINK", "B_CP", "Delta write"),
                    ("B_CP", "S_CP", "MERGE")]:
        p.edge(a, b, l, E_CDC)
    # bronze -> silver
    for a, b in [("B_AECB", "S_AECB"), ("B_FRAUD", "S_FRAUD"), ("B_AML", "S_AML")]:
        p.edge(a, b, "Glue + bookmark")
    # identity + assembly
    for a in ("S_CP", "S_AECB", "S_FRAUD", "S_AML"):
        p.edge(a, "IDR", "spine" if a == "S_CP" else "")
    p.edge("IDR", "XREF")
    p.edge("XREF", "DI")
    for a in ("S_AECB", "S_FRAUD", "S_AML", "S_CP"):
        p.edge(a, "DI")
    p.edge("DI", "SNAP")
    p.edge("SNAP", "OBJLOCK", "write raw JSON · WORM", E_AUDIT)
    # dq
    p.edge("DI", "MUSTPASS")
    p.edge("MUSTPASS", "QUAR", "fail")
    p.edge("MUSTPASS", "PORT", "pass → dq_pass=TRUE")
    p.edge("DI", "DQSC")
    # serve
    p.edge("PORT", "ATHENA")
    p.edge("DQSC", "ATHENA")
    p.edge("ATHENA", "META")
    return p


# --------------------------------------------------------------------------- #
# Diagram 2 — Medallion layers (LR, three lanes)
# --------------------------------------------------------------------------- #
def diagram2() -> Page:
    p = Page("2. Medallion layers")
    W, H = 220, 50
    lane_w = 300
    bx, sx, gx = 40, 40 + lane_w + 60, 40 + 2 * (lane_w + 60)

    p.group("gB", "Bronze — credit_bronze\nraw · append-only", bx - 15, 20, lane_w, 460, "bronze")
    p.group("gS", "Silver — credit_silver\ncleaned · typed · PII-tagged · Delta", sx - 15, 20, lane_w, 640, "silver")
    p.group("gG", "Gold — credit_gold\nmarts · natural keys · Delta", gx - 15, 20, lane_w, 300, "gold")

    def y(i):
        return 60 + i * 80

    b = {"b1": "aecb\nParquet · part. ingest_date", "b2": "fraud\nParquet · part. ingest_date",
         "b3": "aml\nParquet · part. ingest_date", "b4": "customer_profile\nDelta · CDC S3 sink"}
    s = {"s1": "aecb_credit_report", "s2": "fraud_score", "s3": "aml_screening",
         "s4": "customer_profile", "s5": "customer_identity_xref\ngolden record (§6)",
         "s6": "decision_input\n1 row / decision_id (§5)", "s7": "decision_input_snapshot\nimmutable index (§7)"}
    g = {"g1": "portfolio_monitoring_daily (§9)\nsnapshot × product × outcome × risk",
         "g2": "dq_scorecard_daily (§8)\nper-day pass/fail + dq_score"}
    for i, (k, v) in enumerate(b.items()):
        p.node(k, v, bx, y(i), W, H, "bronze")
    for i, (k, v) in enumerate(s.items()):
        p.node(k, v, sx, y(i), W, H, "silver")
    for i, (k, v) in enumerate(g.items()):
        p.node(k, v, gx, y(i), W, H, "gold")

    for a, bb in [("b1", "s1"), ("b2", "s2"), ("b3", "s3"), ("b4", "s4"),
                  ("s1", "s5"), ("s2", "s5"), ("s3", "s5"), ("s4", "s5"),
                  ("s5", "s6"), ("s1", "s6"), ("s2", "s6"), ("s3", "s6"), ("s4", "s6"),
                  ("s6", "s7"), ("s6", "g1"), ("s6", "g2")]:
        p.edge(a, bb)
    return p


# --------------------------------------------------------------------------- #
# Diagram 3 — Identity resolution flow (TB)
# --------------------------------------------------------------------------- #
def diagram3() -> Page:
    p = Page("3. Identity resolution flow")
    W, H = 230, 60

    def n(nid, label, x, y, sk, w=W, h=H):
        p.node(nid, label, x, y, w, h, sk)

    n("START", "Source row to resolve\n(AECB / Fraud / AML)", 320, 20, "plain")
    n("SPINE", "Spine = PostgreSQL internal_customer_uuid\nmaster_customer_id = UUIDv5(internal_uuid)\nstable & reproducible", 620, 20, "silver", 300, 70)
    n("DET", "Deterministic match?", 360, 130, "decision", 150, 80)
    n("ACCEPT", "Accept\nmatch_confidence = 1.00\nmatch_method = DETERMINISTIC", 40, 250, "good")
    n("PROB", "Probabilistic scorer\nweighted Jaro-Winkler on name\n+ exact on dob/phone/email/eid", 360, 250, "silver")
    n("MULTI", "Candidates?", 400, 370, "decision", 150, 80)
    n("TAKEBEST", "Take highest-scoring candidate\nrecord match_confidence", 40, 490, "good")
    n("REVIEW", "Attach BUT flag\nneeds_manual_review = TRUE", 340, 490, "warn")
    n("UNRES", "Create UNRESOLVED record\nmaster_customer_id = UNRESOLVED\n(no source data dropped)", 640, 490, "bad")
    n("SURV", "Conflicting attributes\nacross sources?", 340, 610, "decision", 170, 80)
    n("SURVRULE", "Survivorship:\nPOSTGRES > AECB > FRAUD > AML\nmost-recent-timestamp wins in a tie", 40, 610, "silver")
    n("WRITE", "Write to customer_identity_xref\nmatch_method · match_confidence · matched_on\nneeds_manual_review · audit timestamps", 320, 730, "silver", 300, 70)
    n("XREFOUT", "customer_identity_xref\ngolden record, SCD2 history", 340, 840, "gold")

    p.edge("START", "DET")
    p.edge("SPINE", "DET", "seeds", E_DOT)
    p.edge("DET", "ACCEPT", "EID exact / phone+email exact")
    p.edge("DET", "PROB", "partial / fuzzy · AML soundex+dob")
    p.edge("PROB", "MULTI")
    p.edge("MULTI", "TAKEBEST", "best ≥ 0.85 (MATCH)")
    p.edge("MULTI", "REVIEW", "0.70 ≤ best < 0.85 (REVIEW)")
    p.edge("MULTI", "UNRES", "best < 0.70")
    p.edge("ACCEPT", "SURV")
    p.edge("TAKEBEST", "SURV")
    p.edge("REVIEW", "SURV")
    p.edge("SURV", "SURVRULE", "yes")
    p.edge("SURV", "WRITE", "no")
    p.edge("SURVRULE", "WRITE")
    p.edge("UNRES", "WRITE")
    p.edge("WRITE", "XREFOUT")
    return p


# --------------------------------------------------------------------------- #
# Diagram 4 — AWS deployment view
# --------------------------------------------------------------------------- #
def diagram4() -> Page:
    p = Page("4. AWS deployment view")
    W, H = 190, 56

    p.group("gEXT", "External", 20, 20, 220, 320, "ext")
    p.group("gAWS", "AWS — me-central-1 (data residency)", 280, 20, 1120, 820, "aws")
    p.group("gNET", "VPC (private subnets · NAT egress)", 300, 60, 700, 470, "net")
    p.group("gCMP", "Compute", 320, 360, 420, 150, "net")
    p.group("gS3", "Amazon S3", 1030, 60, 350, 200, "aws")

    p.node("AECBEXT", "AECB SFTP endpoint", 40, 60, W, H, "source")
    p.node("FRAUDEXT", "Fraud REST API", 40, 150, W, H, "source")
    p.node("AMLEXT", "AML provider (webhook)", 40, 240, W, H, "source")

    p.node("APIGW", "API Gateway\n(AML webhook)", 320, 100, W, H, "net")
    p.node("LAMBDA", "Lambda\nwebhook receiver", 320, 180, W, H, "net")
    p.node("RDS", "RDS PostgreSQL\nidentity spine", 540, 100, W, H, "store")
    p.node("DBZ", "MSK Connect\nDebezium source", 540, 180, W, H, "cdc")
    p.node("MSK", "Amazon MSK (Kafka)", 540, 260, W, H, "queue")
    p.node("S3SINK", "MSK Connect\nS3 sink", 760, 180, W, H, "cdc")
    p.node("META", "Metabase on ECS Fargate", 760, 260, W, H, "serve")
    p.node("GLUE", "AWS Glue jobs\n(PySpark, bookmarks) → EMR", 340, 400, W, H, "ingest")
    p.node("GLUECAT", "Glue Data Catalog\ncredit_bronze/silver/gold", 540, 400, W, H, "ingest")

    p.node("S3DATA", "s3://wio-credit-decision-ENV\nbronze / silver / gold", 1050, 100, W, H, "bronze")
    p.node("S3LOCK", "Snapshot bucket\nObject Lock · 7-yr WORM (§7)", 1050, 180, W, H, "audit")

    p.node("ATHENA", "Amazon Athena\n(serverless SQL)", 1050, 300, W, H, "serve")
    p.node("SNS", "Amazon SNS\nDQ + pipeline alerts (§8)", 1050, 380, W, H, "serve")
    p.node("IAM", "IAM\nleast-privilege roles", 320, 560, W, H, "plain")
    p.node("KMS", "AWS KMS\nSSE-KMS encryption", 540, 560, W, H, "plain")
    p.node("AIRFLOW", "Amazon MWAA (Airflow)\norchestration", 760, 400, W, H, "serve")
    p.node("CW", "CloudWatch\nlogs · metrics · alarms", 1050, 460, W, H, "plain")

    p.edge("AECBEXT", "GLUE", "SFTP")
    p.edge("FRAUDEXT", "GLUE", "HTTPS poll")
    p.edge("AMLEXT", "APIGW", "HTTPS webhook")
    p.edge("APIGW", "LAMBDA")
    p.edge("LAMBDA", "S3DATA")
    p.edge("RDS", "DBZ", "WAL", E_CDC)
    p.edge("DBZ", "MSK", "", E_CDC)
    p.edge("MSK", "S3SINK", "", E_CDC)
    p.edge("S3SINK", "S3DATA", "", E_CDC)
    p.edge("GLUE", "S3DATA")
    p.edge("GLUE", "GLUECAT")
    p.edge("GLUE", "S3LOCK", "immutable JSON", E_AUDIT)
    p.edge("GLUECAT", "ATHENA")
    p.edge("S3DATA", "ATHENA")
    p.edge("ATHENA", "META")
    p.edge("GLUE", "SNS")
    p.edge("AIRFLOW", "GLUE")
    p.edge("AIRFLOW", "DBZ")
    p.edge("IAM", "GLUE", "governs", E_DOT)
    p.edge("KMS", "RDS", "encrypts", E_DOT)
    p.edge("KMS", "S3DATA", "encrypts", E_DOT)
    p.edge("GLUE", "CW")
    p.edge("LAMBDA", "CW")
    return p


# --------------------------------------------------------------------------- #
# Diagram 5 — Decision-traceability sequence
# --------------------------------------------------------------------------- #
def diagram5() -> Page:
    p = Page("5. Audit-snapshot sequence")
    parts = [
        ("ORCH", "MWAA (Airflow)"),
        ("GLUE", "Glue decision-assembly job"),
        ("XREF", "customer_identity_xref"),
        ("SRC", "Silver source tables\n(aecb/fraud/aml/profile)"),
        ("LOCK", "S3 Object-Lock bucket\n(compliance, 7 yr)"),
        ("DELTA", "decision_input_snapshot\n(Delta index)"),
        ("DI", "decision_input (Delta)"),
    ]
    PW, PH, GAPX = 180, 46, 200
    top, bottom = 20, 900
    cx = {}
    for i, (pid, label) in enumerate(parts):
        x = 40 + i * GAPX
        cx[pid] = x + PW / 2
        p.node(pid, label, x, top, PW, PH, "part")
        # lifeline
        p.free_edge(cx[pid], top + PH, cx[pid], bottom, "", E_LIFE)

    msgs = [
        ("ORCH", "GLUE", "1: trigger decision assembly (batch_id)", E_MSG),
        ("GLUE", "XREF", "2: resolve master_customer_id", E_MSG),
        ("XREF", "GLUE", "3: master_customer_id (+ match metadata)", E_RET),
        ("GLUE", "SRC", "4: fetch RAW records (verbatim + bronze URIs)", E_MSG),
        ("SRC", "GLUE", "5: raw records + record hashes", E_RET),
        ("GLUE", "GLUE", "6: assemble snapshot JSON", E_MSG),
        ("GLUE", "GLUE", "7: compute content_sha256(snapshot.json)", E_MSG),
        ("GLUE", "LOCK", "8: PUT snapshot.json · retain-until now+7yr", E_MSG),
        ("LOCK", "GLUE", "9: versionId + ETag (WORM)", E_RET),
        ("GLUE", "DELTA", "10: write {decision_id, snapshot_s3_uri, content_sha256}", E_MSG),
        ("GLUE", "DI", "11: write decision_input row (snapshot_s3_uri)", E_MSG),
        ("ORCH", "LOCK", "12: GET snapshot.json  [audit / tamper check]", E_MSG),
        ("LOCK", "ORCH", "13: bytes", E_RET),
        ("ORCH", "DELTA", "14: read stored content_sha256", E_MSG),
        ("ORCH", "ORCH", "15: sha256(bytes) == stored? integrity / tamper", E_MSG),
    ]
    y = top + PH + 40
    step = 46
    for src, dst, label, style in msgs:
        if src == dst:  # self message
            x = cx[src]
            p.free_edge(x, y, x + 120, y, "", style)
            p.free_edge(x + 120, y, x + 120, y + 18, "", style)
            p.free_edge(x + 120, y + 18, x, y + 18, label, E_RET)
            y += step + 18
        else:
            p.free_edge(cx[src], y, cx[dst], y, label, style)
            y += step
    # audit region note
    p.node("NOTE1", "The S3 object is the LEGAL record.\nThe Delta row is the queryable INDEX.",
            cx["LOCK"] - 90, bottom - 60, 260, 44, "warn")
    return p


def main() -> int:
    pages = [diagram1(), diagram2(), diagram3(), diagram4(), diagram5()]
    xml = ('<mxfile host="app.diagrams.net" type="device" '
           'agent="generate_drawio.py">' + "".join(pg.xml() for pg in pages) + "</mxfile>")
    pretty = minidom.parseString(xml).toprettyxml(indent="  ")
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write(pretty)
    print(f"wrote {OUT} ({os.path.getsize(OUT):,} bytes, {len(pages)} pages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
