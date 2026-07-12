# Architecture — docs/architecture/

Architecture deliverable for the Credit Decision Data Platform (see [`../SPEC.md`](../SPEC.md)).

## Contents

| File | What it is |
|---|---|
| [`architecture.md`](./architecture.md) | The architecture document. Five embedded **Mermaid** diagrams + prose: end-to-end data flow, medallion layers, identity-resolution flow, AWS deployment view, and the decision-traceability/audit-snapshot sequence. Also covers incremental processing (Glue bookmarks + Delta MERGE), the Glue→EMR migration path, and how the design meets the 10K→100K/day scale target. |
| `architecture.pdf` | **Single PDF of the whole architecture doc** with every diagram embedded (the 5 Mermaid diagrams + the AWS deployment render) — the diagram deliverable in one file, analogous to `execution_plan.pdf`. |
| [`build_pdf.py`](./build_pdf.py) | Renders `architecture.md` → `architecture.pdf` via reportlab, swapping each `mermaid` code block for its PNG in `rendered/`. Falls back to the Mermaid source in a code box if a PNG is missing, so it always produces a PDF. |
| [`generate_diagram.py`](./generate_diagram.py) | Renders the **AWS deployment view** to `aws_deployment.png` using the `diagrams` (mingrammer) library. Degrades gracefully (prints instructions, exits 0) if `diagrams` or Graphviz are missing. |
| [`requirements.txt`](./requirements.txt) | Python deps for `generate_diagram.py` (`diagrams`) and `build_pdf.py` (`reportlab`). `generate_diagram.py` also needs the Graphviz **system** binary `dot`. |
| `aws_deployment.png` | Generated PNG of the AWS deployment view (checked in). |
| `rendered/` | PNG exports of the five Mermaid diagrams, meaningfully named in document order (see mapping below), consumed by `build_pdf.py`. |
| [`drawio/`](./drawio/) | **Editable** draw.io version of all five diagrams (one multi-page `architecture.drawio` + `generate_drawio.py`). Open in [diagrams.net](https://app.diagrams.net) or the VS Code Draw.io extension to change anything. |

## Viewing the Mermaid diagrams

The five diagrams in `architecture.md` are **Mermaid** and render automatically on:

- GitHub / GitLab (native Mermaid support in Markdown preview),
- VS Code with the "Markdown Preview Mermaid Support" extension,
- Obsidian, Typora, and most modern Markdown viewers.

No tooling is required just to read them.

## Regenerating the artifacts

### AWS deployment PNG (`aws_deployment.png`)

Requires the Graphviz system binary plus the `diagrams` package:

```bash
# system dep
brew install graphviz            # macOS
# sudo apt-get install graphviz  # Debian/Ubuntu

# python dep
pip install -r requirements.txt

python3 generate_diagram.py      # writes aws_deployment.png
```

If Graphviz or `diagrams` is missing, the script prints install instructions and
exits 0 — it never crashes.

### Mermaid diagrams to PNG (optional)

The Mermaid source already renders on GitHub, so this is only for offline/PDF use.
Using `@mermaid-js/mermaid-cli` (requires Node + a headless Chromium):

```bash
npx @mermaid-js/mermaid-cli -i architecture.md -o rendered/diagram.png -e png
```

`mermaid-cli` emits one **numbered** PNG per diagram (`diagram-1.png` … `diagram-5.png`),
which we rename to the meaningful names in the mapping table below (that's the order
`build_pdf.py` expects). If a headless Chromium can't be provisioned in your environment,
skip it — the embedded Mermaid is the source of truth and renders in the browser.

### Rendered PNG mapping (`rendered/`)

| Mermaid block (order) | File in `rendered/` | Diagram |
|---|---|---|
| 1 | `1-end-to-end-data-flow.png` | End-to-end data flow |
| 2 | `2-medallion-layers.png` | Medallion layers (Bronze/Silver/Gold) |
| 3 | `3-identity-resolution-flow.png` | Identity resolution flow (SPEC §6) |
| 4 | `4-aws-deployment-view.png` | AWS deployment view |
| 5 | `5-decision-audit-snapshot-sequence.png` | Decision traceability / audit-snapshot sequence (SPEC §7) |

## Diagram inventory (all in `architecture.md`)

1. **End-to-end data flow** — 4 sources → ingestion → Bronze → Silver → identity resolution → `decision_input` + immutable snapshot → DQ gate → Gold marts → Athena → Metabase. The CDC path (Debezium → MSK → S3 sink → Delta Bronze) is drawn distinctly from batch/API/webhook.
2. **Medallion layers** — Bronze/Silver/Gold table inventory from SPEC §3–§4.
3. **Identity-resolution flow** — deterministic-first, probabilistic fallback, thresholds, survivorship, unresolved handling (SPEC §6).
4. **AWS deployment view** — S3, RDS, MSK/MSK Connect, Glue, Athena, SNS, Object-Lock snapshot bucket, IAM/KMS, Metabase on ECS, MWAA. (Also rendered to `aws_deployment.png`.)
5. **Decision-traceability / audit-snapshot sequence** — how each immutable snapshot is produced and how tamper detection works (SPEC §7).
