#!/usr/bin/env python3
"""
Build trade_offs_and_readiness_analysis.pdf from trade_offs_and_readiness_analysis.md — Part 2 deliverable
(Trade-offs & Production Readiness Analysis), rendered with the same reportlab
Markdown->PDF renderer as docs/execution-plan/build_pdf.py (headings, tables,
lists, quotes, code, styled footer).

Self-contained: installs reportlab into a local venv if it isn't importable, then
re-execs. Produces the PDF and verifies it is non-empty. Usage:
    python3 build_pdf.py
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MD_PATH = os.path.join(HERE, "trade_offs_and_readiness_analysis.md")
PDF_PATH = os.path.join(HERE, "trade_offs_and_readiness_analysis.pdf")
FOOTER = "Credit Decision Data Platform — Part 2: Trade-offs & Readiness Analysis"


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
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        HRFlowable, ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer,
        Table, TableStyle,
    )

    styles = getSampleStyleSheet()
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=9.5, leading=13.5,
                          spaceAfter=6, alignment=TA_LEFT)
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=19, leading=23,
                        textColor=colors.HexColor("#1a3d6b"), spaceBefore=6, spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=14, leading=18,
                        textColor=colors.HexColor("#1a5276"), spaceBefore=14, spaceAfter=6)
    h3 = ParagraphStyle("h3", parent=styles["Heading3"], fontSize=11.5, leading=15,
                        textColor=colors.HexColor("#21618c"), spaceBefore=10, spaceAfter=4)
    meta = ParagraphStyle("meta", parent=body, fontSize=8.5, textColor=colors.HexColor("#555555"))
    code = ParagraphStyle("code", parent=body, fontName="Courier", fontSize=8, leading=10,
                          backColor=colors.HexColor("#f4f4f4"), leftIndent=6, spaceBefore=4, spaceAfter=6)

    def inline(text: str) -> str:
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"`([^`]+?)`", r'<font face="Courier">\1</font>', text)
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<u>\1</u>", text)
        text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)
        return text

    def split_row(line: str):
        return [c.strip() for c in line.strip().strip("|").split("|")]

    with open(MD_PATH, encoding="utf-8") as fh:
        md = fh.read()

    story, lines = [], md.splitlines()
    i, n, in_code, code_buf = 0, len(md.splitlines()), False, []
    while i < n:
        line = lines[i]
        if line.strip().startswith("```"):
            if in_code:
                story.append(Paragraph("<br/>".join(
                    (c.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(" ", "&nbsp;"))
                    for c in code_buf), code))
                code_buf, in_code = [], False
            else:
                in_code = True
            i += 1; continue
        if in_code:
            code_buf.append(line); i += 1; continue
        if line.strip().startswith("|") and i + 1 < n and re.match(r"^\s*\|[\s:|-]+\|\s*$", lines[i + 1]):
            header = split_row(line); rows = [header]; i += 2
            while i < n and lines[i].strip().startswith("|"):
                rows.append(split_row(lines[i])); i += 1
            ncol = len(header); data = []
            for r_idx, r in enumerate(rows):
                r = (r + [""] * ncol)[:ncol]
                sty = ParagraphStyle(f"cell{r_idx}", parent=body, fontSize=8, leading=10.5,
                                     textColor=colors.white if r_idx == 0 else colors.black)
                data.append([Paragraph(inline(c), sty) for c in r])
            tbl = Table(data, repeatRows=1, hAlign="LEFT", colWidths=[(17.0 / ncol) * cm] * ncol)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#eef3f8")]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b0b8c1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]))
            story += [Spacer(1, 4), tbl, Spacer(1, 6)]; continue
        if line.startswith("# "):
            story.append(Paragraph(inline(line[2:].strip()), h1))
            story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#1a3d6b"), spaceAfter=8))
        elif line.startswith("## "):
            story.append(Paragraph(inline(line[3:].strip()), h2))
        elif line.startswith("### "):
            story.append(Paragraph(inline(line[4:].strip()), h3))
        elif line.strip() in ("---", "***", "___"):
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc"),
                                    spaceBefore=6, spaceAfter=6))
        elif line.strip().startswith(">"):
            q = ParagraphStyle("quote", parent=body, leftIndent=10, textColor=colors.HexColor("#444444"),
                               backColor=colors.HexColor("#f7f9fb"), spaceBefore=4, spaceAfter=6)
            story.append(Paragraph(inline(line.strip().lstrip(">").strip()), q))
        elif re.match(r"^\s*[-*] ", line):
            items = []
            while i < n and re.match(r"^\s*[-*] ", lines[i]):
                items.append(ListItem(Paragraph(inline(re.sub(r"^\s*[-*] ", "", lines[i])), body), leftIndent=12))
                i += 1
            story.append(ListFlowable(items, bulletType="bullet", start="•", leftIndent=10)); continue
        elif line.strip() == "":
            story.append(Spacer(1, 3))
        else:
            style = meta if line.startswith("**") and line.count("**") >= 2 and len(line) < 120 else body
            story.append(Paragraph(inline(line.strip()), style))
        i += 1

    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5); canvas.setFillColor(colors.HexColor("#888888"))
        canvas.drawString(2 * cm, 1.0 * cm, FOOTER)
        canvas.drawRightString(19 * cm, 1.0 * cm, f"Page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(PDF_PATH, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                            topMargin=1.8 * cm, bottomMargin=1.6 * cm, title=FOOTER, author="Deepankar")
    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return _nonempty(PDF_PATH)


def main() -> int:
    if not os.path.isfile(MD_PATH):
        print(f"ERROR: source not found: {MD_PATH}"); return 1
    try:
        if build():
            print("=" * 60)
            print(f"SUCCESS: {PDF_PATH} ({os.path.getsize(PDF_PATH):,} bytes)")
            print("=" * 60)
            return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[reportlab] errored: {exc}")
    print("ERROR: could not produce the analysis PDF.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
