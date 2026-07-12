# Part 2 — Trade-offs & Production Readiness Analysis

The written analysis deliverable for Part 2, grounded in the Part 1 implementation in this repo.

| File | What it is |
|---|---|
| [`trade_offs_and_readiness_analysis.md`](./trade_offs_and_readiness_analysis.md) | The analysis (source). Five sections: conflict-resolution trade-offs, data-quality strategy, compliance & auditability, Sharia-compliance considerations, and what was cut & why. |
| `trade_offs_and_readiness_analysis.pdf` | **The deliverable** — 9-page PDF rendered from the markdown. |
| [`build_pdf.py`](./build_pdf.py) | Renders the markdown → PDF via reportlab (same builder as the other deliverables). Regenerate with `python3 build_pdf.py`. |
| [`requirements.txt`](./requirements.txt) | `reportlab` (only dep). |

Every section references the concrete Part 1 implementation — `docs/SPEC.md` sections, the Glue jobs
under `glue/silver_layer/` and `glue/gold_layer/`, `glue/common/dq_rules.py`, `glue/common/text_match.py`,
`infra/terraform/s3.tf` (Object Lock), and the Athena audit views.
