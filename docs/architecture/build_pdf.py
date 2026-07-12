#!/usr/bin/env python3
"""
Build architecture.pdf from architecture.md — the architecture-diagram deliverable,
analogous to docs/execution-plan/build_pdf.py.

Same reportlab Markdown->PDF renderer (headings, tables, lists, code, quotes), with one
addition: each fenced ```mermaid block in the Markdown is replaced, in document order, by
its pre-rendered PNG in docs/architecture/rendered/ (see DIAGRAM_PNGS below) so the PDF
shows the actual diagrams rather than Mermaid source. The AWS deployment section
additionally embeds the diagrams-as-code render (aws_deployment.png).

Prereqisite: the diagram PNGs must already exist (run `generate_diagram.py` for the AWS
PNG, and render the Mermaid blocks via `@mermaid-js/mermaid-cli` — see README.md). If a
PNG is missing the script falls back to printing the Mermaid source in a code box, so it
still produces a PDF.

The script ACTUALLY PRODUCES the PDF and verifies it is non-empty. Usage:
    python3 build_pdf.py
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MD_PATH = os.path.join(HERE, "architecture.md")
PDF_PATH = os.path.join(HERE, "architecture.pdf")
RENDERED_DIR = os.path.join(HERE, "rendered")
AWS_PNG = os.path.join(HERE, "aws_deployment.png")

# The Mermaid diagrams in architecture.md, in document order. Index i (1-based) is the
# i-th ```mermaid block; the value is its rendered PNG under rendered/.
DIAGRAM_PNGS = {
    1: "1-end-to-end-data-flow.png",
    2: "2-medallion-layers.png",
    3: "3-identity-resolution-flow.png",
    4: "4-aws-deployment-view.png",
    5: "5-decision-audit-snapshot-sequence.png",
}
DIAGRAM_TITLES = {
    1: "End-to-end data flow",
    2: "Medallion layers (Bronze / Silver / Gold)",
    3: "Identity resolution flow",
    4: "AWS deployment view",
    5: "Decision traceability — audit-snapshot sequence",
}

# Mermaid block index (1-based, in document order) -> extra image to append after it.
EXTRA_AFTER_BLOCK = {4: AWS_PNG}  # AWS deployment view gets the diagrams-as-code render too


def _ensure_reportlab() -> bool:
    try:
        import reportlab  # noqa: F401
        return True
    except ImportError:
        pass
    if os.environ.get("_BUILD_PDF_REEXEC") == "1":
        print("[reportlab] still not importable after venv re-exec — giving up.")
        return False
    venv_dir = os.path.join(HERE, ".venv")
    venv_py = os.path.join(venv_dir, "bin", "python")
    if not os.path.isfile(venv_py):
        print("[reportlab] not importable — creating local venv and installing...")
        subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
    subprocess.run([venv_py, "-m", "pip", "install", "--quiet", "--upgrade",
                    "pip", "reportlab"], check=True)
    os.environ["_BUILD_PDF_REEXEC"] = "1"
    os.execv(venv_py, [venv_py, os.path.abspath(__file__)])
    return True  # unreachable


def _nonempty(path: str) -> bool:
    return os.path.isfile(path) and os.path.getsize(path) > 0


def build() -> bool:
    if not _ensure_reportlab():
        return False

    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import (
        HRFlowable, Image, ListFlowable, ListItem, Paragraph,
        SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    CONTENT_W = 17.0 * cm
    MAX_IMG_H = 22.0 * cm

    styles = getSampleStyleSheet()
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=9.5,
                          leading=13.5, spaceAfter=6, alignment=TA_LEFT)
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=19, leading=23,
                        textColor=colors.HexColor("#1a3d6b"), spaceBefore=6, spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=14, leading=18,
                        textColor=colors.HexColor("#1a5276"), spaceBefore=14, spaceAfter=6)
    h3 = ParagraphStyle("h3", parent=styles["Heading3"], fontSize=11.5, leading=15,
                        textColor=colors.HexColor("#21618c"), spaceBefore=10, spaceAfter=4)
    meta = ParagraphStyle("meta", parent=body, fontSize=8.5,
                          textColor=colors.HexColor("#555555"))
    code = ParagraphStyle("code", parent=body, fontName="Courier", fontSize=7.5,
                          leading=9.5, backColor=colors.HexColor("#f4f4f4"),
                          leftIndent=6, spaceBefore=4, spaceAfter=6)
    caption = ParagraphStyle("caption", parent=body, fontSize=8, alignment=TA_CENTER,
                             textColor=colors.HexColor("#666666"), spaceBefore=2, spaceAfter=10)

    def inline(text: str) -> str:
        text = (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"`([^`]+?)`", r'<font face="Courier">\1</font>', text)
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<u>\1</u>", text)
        text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)
        return text

    def split_row(line: str):
        return [c.strip() for c in line.strip().strip("|").split("|")]

    def image_flowable(path: str):
        ir = ImageReader(path)
        iw, ih = ir.getSize()
        ratio = min(CONTENT_W / iw, MAX_IMG_H / ih)
        img = Image(path, width=iw * ratio, height=ih * ratio)
        img.hAlign = "CENTER"
        return img

    with open(MD_PATH, encoding="utf-8") as fh:
        md = fh.read()

    story = []
    lines = md.splitlines()
    i, n = 0, len(lines)
    in_code = False
    fence_lang = ""
    code_buf: list[str] = []
    mermaid_idx = 0

    while i < n:
        line = lines[i]

        if line.strip().startswith("```"):
            if in_code:  # closing fence
                if fence_lang == "mermaid":
                    mermaid_idx += 1
                    png = os.path.join(RENDERED_DIR,
                                       DIAGRAM_PNGS.get(mermaid_idx, f"diagram-{mermaid_idx}.png"))
                    if _nonempty(png):
                        story.append(image_flowable(png))
                        title = DIAGRAM_TITLES.get(mermaid_idx, "")
                        story.append(Paragraph(
                            f"Figure {mermaid_idx} — {title}" if title else f"Figure {mermaid_idx}",
                            caption))
                    else:  # graceful fallback: show the source
                        print(f"[warn] {png} missing — embedding Mermaid source instead.")
                        story.append(Paragraph("<br/>".join(
                            (c.replace("&", "&amp;").replace("<", "&lt;")
                               .replace(">", "&gt;").replace(" ", "&nbsp;"))
                            for c in code_buf), code))
                    extra = EXTRA_AFTER_BLOCK.get(mermaid_idx)
                    if extra and _nonempty(extra):
                        story.append(image_flowable(extra))
                        story.append(Paragraph(
                            "AWS deployment — diagrams-as-code render", caption))
                else:  # ordinary code block
                    story.append(Paragraph("<br/>".join(
                        (c.replace("&", "&amp;").replace("<", "&lt;")
                           .replace(">", "&gt;").replace(" ", "&nbsp;"))
                        for c in code_buf), code))
                code_buf, in_code, fence_lang = [], False, ""
            else:  # opening fence
                in_code = True
                fence_lang = line.strip()[3:].strip().lower()
            i += 1
            continue
        if in_code:
            code_buf.append(line)
            i += 1
            continue

        if line.strip().startswith("|") and i + 1 < n and re.match(
                r"^\s*\|[\s:|-]+\|\s*$", lines[i + 1]):
            header = split_row(line)
            rows = [header]
            i += 2
            while i < n and lines[i].strip().startswith("|"):
                rows.append(split_row(lines[i]))
                i += 1
            ncol = len(header)
            data = []
            for r_idx, r in enumerate(rows):
                r = (r + [""] * ncol)[:ncol]
                sty = ParagraphStyle(f"cell{r_idx}", parent=body, fontSize=8, leading=10.5,
                                     textColor=colors.white if r_idx == 0 else colors.black)
                data.append([Paragraph(inline(c), sty) for c in r])
            tbl = Table(data, repeatRows=1, hAlign="LEFT",
                        colWidths=[(17.0 / ncol) * cm] * ncol)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [colors.white, colors.HexColor("#eef3f8")]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b0b8c1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]))
            story.append(Spacer(1, 4))
            story.append(tbl)
            story.append(Spacer(1, 6))
            continue

        if line.startswith("# "):
            story.append(Paragraph(inline(line[2:].strip()), h1))
            story.append(HRFlowable(width="100%", thickness=1,
                                    color=colors.HexColor("#1a3d6b"), spaceAfter=8))
        elif line.startswith("## "):
            story.append(Paragraph(inline(line[3:].strip()), h2))
        elif line.startswith("### "):
            story.append(Paragraph(inline(line[4:].strip()), h3))
        elif line.strip() in ("---", "***", "___"):
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=colors.HexColor("#cccccc"),
                                    spaceBefore=6, spaceAfter=6))
        elif line.strip().startswith(">"):
            q = ParagraphStyle("quote", parent=body, leftIndent=10,
                               textColor=colors.HexColor("#444444"),
                               backColor=colors.HexColor("#f7f9fb"),
                               spaceBefore=4, spaceAfter=6)
            story.append(Paragraph(inline(line.strip().lstrip(">").strip()), q))
        elif re.match(r"^\s*[-*] ", line):
            items = []
            while i < n and re.match(r"^\s*[-*] ", lines[i]):
                items.append(ListItem(
                    Paragraph(inline(re.sub(r"^\s*[-*] ", "", lines[i])), body),
                    leftIndent=12))
                i += 1
            story.append(ListFlowable(items, bulletType="bullet", start="•", leftIndent=10))
            continue
        elif line.strip() == "":
            story.append(Spacer(1, 3))
        else:
            style = meta if line.startswith("**") and line.count("**") >= 2 and \
                len(line) < 120 else body
            story.append(Paragraph(inline(line.strip()), style))
        i += 1

    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.HexColor("#888888"))
        canvas.drawString(2 * cm, 1.0 * cm,
                          "Credit Decision Data Platform — Architecture")
        canvas.drawRightString(19 * cm, 1.0 * cm, f"Page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(
        PDF_PATH, pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=1.8 * cm, bottomMargin=1.6 * cm,
        title="Credit Decision Data Platform — Architecture",
        author="Data Engineering",
    )
    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return _nonempty(PDF_PATH)


def main() -> int:
    if not os.path.isfile(MD_PATH):
        print(f"ERROR: source not found: {MD_PATH}")
        return 1
    try:
        if build():
            print("=" * 60)
            print(f"SUCCESS: {PDF_PATH} ({os.path.getsize(PDF_PATH):,} bytes)")
            print("=" * 60)
            return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[reportlab] errored: {exc}")
    print("ERROR: could not produce the architecture PDF.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
