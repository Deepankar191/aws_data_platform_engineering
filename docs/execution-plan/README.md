# Execution Plan — docs/execution-plan/

The 30/60/90-day execution plan for the Credit Decision Data Platform (see [`../SPEC.md`](../SPEC.md)).

## Contents

| File | What it is |
|---|---|
| [`execution_plan.md`](./execution_plan.md) | The plan (source of truth). 30/60/90-day tranches, an explicit **launch-critical vs post-launch** split, milestone table, RACI, risks & mitigations, dependencies, and a UAE Central Bank compliance section. |
| [`build_pdf.py`](./build_pdf.py) | Converts `execution_plan.md` → `execution_plan.pdf`. Tries pandoc → reportlab → markdown+weasyprint, uses whichever is available, and **actually produces** the PDF. |
| [`requirements.txt`](./requirements.txt) | Python deps for `build_pdf.py` (`reportlab`). |
| `execution_plan.pdf` | Generated PDF (checked in). |

## Regenerating the PDF

```bash
python3 build_pdf.py
```

The script picks the first working method:

1. **pandoc** — `pandoc execution_plan.md -o execution_plan.pdf` (if `pandoc` is on PATH).
2. **reportlab** — a self-contained Markdown→PDF renderer. If `reportlab` isn't importable, the
   script creates a local `.venv`, `pip install`s reportlab into it, and re-execs itself. **No system
   dependency** — this is the default path and the one that produced the checked-in PDF.
3. **markdown → HTML → weasyprint/pdfkit** — if those libraries are present.

It prints which method succeeded and verifies the PDF is non-empty before exiting.

> **In this environment:** pandoc was not available, so the PDF was generated via **reportlab**
> (auto-installed into a local `.venv`). Result: a 12-page A4 PDF with styled headings, tables, and
> page footers.

## What the plan covers

- **Executive summary**, goals (G1–G6), and success metrics (launch vs scale targets).
- **Day 0–30 — Foundation & launch-critical:** landing zone, medallion S3 + Object Lock, RDS + Debezium
  CDC, MSK, the four ingestion paths, Bronze→Silver Glue jobs, deterministic identity resolution v1,
  `decision_input`, immutable snapshots, must-pass DQ, Athena, a minimal risk mart, Airflow (MWAA)
  orchestration. Every item marked launch-critical with a Day-30 go/no-go gate.
- **Day 31–60 — Hardening:** probabilistic matching + survivorship, full DQ scorecard + SNS alerting,
  portfolio mart + Metabase, backfill/replay, observability, DR, and a **security + UAE-CB audit-trail
  review gate**.
- **Day 61–90 — Scale & optimise:** 100K/day load test, EMR migration evaluation, cost optimisation,
  `OPTIMIZE`/`Z-ORDER` compaction tuning, advanced MDM + stewardship, ML groundwork, and the
  post-launch roadmap.
- **Milestone table, RACI ownership, risks & mitigations, dependencies**, and an explicit **compliance
  section** (full audit trail, 7-year immutable retention, tamper evidence, PII handling, data residency
  in AWS `me-central-1`).

All sections reference the relevant [`SPEC.md`](../SPEC.md) sections inline.
