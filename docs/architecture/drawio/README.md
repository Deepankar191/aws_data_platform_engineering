# Editable draw.io diagrams

`architecture.drawio` is an **editable** version of every architecture diagram, so anyone can
change, update, or extend them — no need to touch Mermaid or re-render PNGs.

It's a single multi-page file (five tabs), one page per diagram in
[`../architecture.md`](../architecture.md):

| Tab | Diagram | Shapes / connectors |
|---|---|---|
| 1 | End-to-end data flow | 34 / 33 |
| 2 | Medallion layers (Bronze / Silver / Gold) | 16 / 16 |
| 3 | Identity resolution flow (SPEC §6) | 13 / 16 |
| 4 | AWS deployment view | 25 / 23 |
| 5 | Decision traceability — audit-snapshot sequence (SPEC §7) | 8 / 28 |

## How to open / edit

- **Web:** open [https://app.diagrams.net](https://app.diagrams.net) → *Open Existing Diagram* →
  select `architecture.drawio`. (Diagrams.net can open from your device, GitHub, Drive, etc.)
- **VS Code:** install the **“Draw.io Integration” (hediet.vscode-drawio)** extension, then just
  open `architecture.drawio` in the editor — it renders and edits inline.
- **Desktop:** the draw.io desktop app opens it directly.

Every box, label, arrow, and colour is a native shape — move, relabel, restyle, add, or delete
freely. Use the page tabs at the bottom to switch diagrams.

## Regenerating

The file is produced by [`generate_drawio.py`](./generate_drawio.py) (pure stdlib, no deps):

```bash
python3 generate_drawio.py     # (re)writes architecture.drawio
```

Edit the node/edge specs in that script to change the generated baseline, or just edit the
`.drawio` directly in draw.io — both are valid ways to maintain it. The styles (colours per
layer, edge styles for CDC/audit/normal) are defined at the top of the script.

> **Note on the layer “bands”.** The coloured group rectangles (Bronze/Silver/Gold, VPC, etc.)
> are visual backgrounds drawn behind the shapes, not geometric parents — moving a band does not
> move the shapes inside it. In draw.io you can rubber-band select a band + its shapes and
> *Edit → Group* them if you want them to move together.

## Relationship to the other diagram artefacts

- `../architecture.md` — the **Mermaid source** (renders on GitHub); the source of truth for content.
- `../rendered/*.png` — static PNG exports of the Mermaid diagrams (used by the PDF).
- `../aws_deployment.png` — the diagrams-as-code render of the AWS view.
- **this folder** — the **editable** draw.io version for hands-on changes.
