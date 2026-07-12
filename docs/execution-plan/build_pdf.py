#!/usr/bin/env python3
"""
Build execution_plan.pdf from execution_plan.md.

Tries, in order, whichever method is available in this environment:
  1. pandoc  execution_plan.md -o execution_plan.pdf   (if `pandoc` on PATH)
  2. reportlab renderer (a self-contained Markdown->PDF; pip-installs reportlab
     into a local venv if it isn't importable)
  3. markdown -> HTML -> weasyprint/pdfkit  (if available)

The script ACTUALLY PRODUCES the PDF and verifies it is non-empty before exiting.
It prints which method succeeded.

Usage:
    python3 build_pdf.py
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MD_PATH = os.path.join(HERE, "execution_plan.md")
PDF_PATH = os.path.join(HERE, "execution_plan.pdf")


# --------------------------------------------------------------------------- #
# Method 1 — pandoc
# --------------------------------------------------------------------------- #
def try_pandoc() -> bool:
    if shutil.which("pandoc") is None:
        print("[pandoc] not found on PATH — skipping.")
        return False
    print("[pandoc] found — rendering...")
    cmd = ["pandoc", MD_PATH, "-o", PDF_PATH]
    try:
        subprocess.run(cmd, check=True, cwd=HERE)
    except subprocess.CalledProcessError as exc:
        # A very common failure is a missing LaTeX engine. Retry with wkhtmltopdf.
        print(f"[pandoc] default engine failed ({exc}); retrying via wkhtmltopdf...")
        if shutil.which("wkhtmltopdf"):
            try:
                subprocess.run(
                    ["pandoc", MD_PATH, "-t", "html5", "--pdf-engine=wkhtmltopdf",
                     "-o", PDF_PATH],
                    check=True, cwd=HERE,
                )
            except subprocess.CalledProcessError as exc2:
                print(f"[pandoc] also failed via wkhtmltopdf ({exc2}) — skipping.")
                return False
        else:
            return False
    return _nonempty(PDF_PATH)


# --------------------------------------------------------------------------- #
# Method 2 — reportlab (self-contained)
# --------------------------------------------------------------------------- #
def _ensure_reportlab():
    """Import reportlab; if missing, create a local venv and install it there,
    then re-exec this script using that venv's python."""
    try:
        import reportlab  # noqa: F401
        return True
    except ImportError:
        pass

    # If we've already re-exec'd once and reportlab is still missing, give up on
    # this method rather than loop forever.
    if os.environ.get("_BUILD_PDF_REEXEC") == "1":
        print("[reportlab] still not importable after venv re-exec — skipping.")
        return False

    venv_dir = os.path.join(HERE, ".venv")
    venv_py = os.path.join(venv_dir, "bin", "python")
    if not os.path.isfile(venv_py):
        print("[reportlab] not importable — creating local venv and installing...")
        subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
    # Install into the venv (idempotent — pip is a no-op if already present).
    subprocess.run([venv_py, "-m", "pip", "install", "--quiet",
                    "--upgrade", "pip", "reportlab"], check=True)
    # Re-exec under the venv interpreter. Use an env flag (not a realpath compare)
    # to break the loop, because on macOS the venv python realpath collapses to
    # the same framework binary as the system python.
    os.environ["_BUILD_PDF_REEXEC"] = "1"
    os.execv(venv_py, [venv_py, os.path.abspath(__file__)])
    return True  # unreachable (execv replaces the process)


def try_reportlab() -> bool:
    if not _ensure_reportlab():
        return False
    try:
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.platypus import (
            HRFlowable, ListFlowable, ListItem, PageBreak, Paragraph,
            SimpleDocTemplate, Spacer, Table, TableStyle,
        )
    except ImportError as exc:
        print(f"[reportlab] import failed after install: {exc} — skipping.")
        return False

    print("[reportlab] rendering Markdown -> PDF...")

    with open(MD_PATH, encoding="utf-8") as fh:
        md = fh.read()

    styles = getSampleStyleSheet()
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=9.5,
                          leading=13.5, spaceAfter=6, alignment=TA_LEFT)
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=19, leading=23,
                        textColor=colors.HexColor("#1a3d6b"), spaceBefore=6,
                        spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=14, leading=18,
                        textColor=colors.HexColor("#1a5276"), spaceBefore=14,
                        spaceAfter=6)
    h3 = ParagraphStyle("h3", parent=styles["Heading3"], fontSize=11.5, leading=15,
                        textColor=colors.HexColor("#21618c"), spaceBefore=10,
                        spaceAfter=4)
    meta = ParagraphStyle("meta", parent=body, fontSize=8.5,
                          textColor=colors.HexColor("#555555"))
    code = ParagraphStyle("code", parent=body, fontName="Courier", fontSize=8,
                          leading=10, backColor=colors.HexColor("#f4f4f4"),
                          leftIndent=6, spaceBefore=4, spaceAfter=6)

    def inline(text: str) -> str:
        """Convert a subset of inline Markdown to ReportLab mini-HTML."""
        # escape XML
        text = (text.replace("&", "&amp;").replace("<", "&lt;")
                    .replace(">", "&gt;"))
        # bold
        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        # inline code
        text = re.sub(r"`([^`]+?)`", r'<font face="Courier">\1</font>', text)
        # links [text](url) -> text
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<u>\1</u>", text)
        # emphasis
        text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)
        return text

    def split_row(line: str):
        cells = line.strip().strip("|").split("|")
        return [c.strip() for c in cells]

    story = []
    lines = md.splitlines()
    i = 0
    n = len(lines)
    in_code = False
    code_buf: list[str] = []

    while i < n:
        line = lines[i]

        # fenced code blocks
        if line.strip().startswith("```"):
            if in_code:
                story.append(Paragraph("<br/>".join(
                    (c.replace("&", "&amp;").replace("<", "&lt;")
                       .replace(">", "&gt;").replace(" ", "&nbsp;"))
                    for c in code_buf), code))
                code_buf = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue
        if in_code:
            code_buf.append(line)
            i += 1
            continue

        # tables (a header line followed by a |---| separator)
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
                sty = ParagraphStyle(
                    f"cell{r_idx}", parent=body, fontSize=8,
                    leading=10.5,
                    textColor=colors.white if r_idx == 0 else colors.black,
                )
                data.append([Paragraph(inline(c), sty) for c in r])
            tbl = Table(data, repeatRows=1, hAlign="LEFT",
                        colWidths=[(17.0 / ncol) * cm] * ncol)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [colors.white, colors.HexColor("#eef3f8")]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b0b8c1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]))
            story.append(Spacer(1, 4))
            story.append(tbl)
            story.append(Spacer(1, 6))
            continue

        # headings
        if line.startswith("# "):
            story.append(Paragraph(inline(line[2:].strip()), h1))
            story.append(HRFlowable(width="100%", thickness=1,
                                    color=colors.HexColor("#1a3d6b"),
                                    spaceAfter=8))
        elif line.startswith("## "):
            story.append(Paragraph(inline(line[3:].strip()), h2))
        elif line.startswith("### "):
            story.append(Paragraph(inline(line[4:].strip()), h3))
        elif line.strip() in ("---", "***", "___"):
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=colors.HexColor("#cccccc"),
                                    spaceBefore=6, spaceAfter=6))
        elif line.strip().startswith(">"):
            q = ParagraphStyle("quote", parent=body,
                               leftIndent=10, textColor=colors.HexColor("#444444"),
                               backColor=colors.HexColor("#f7f9fb"),
                               borderColor=colors.HexColor("#1a5276"),
                               borderWidth=0, spaceBefore=4, spaceAfter=6)
            story.append(Paragraph(inline(line.strip().lstrip(">").strip()), q))
        elif re.match(r"^\s*[-*] ", line):
            # collect a bullet list
            items = []
            while i < n and re.match(r"^\s*[-*] ", lines[i]):
                items.append(ListItem(
                    Paragraph(inline(re.sub(r"^\s*[-*] ", "", lines[i])), body),
                    leftIndent=12))
                i += 1
            story.append(ListFlowable(items, bulletType="bullet",
                                      start="•", leftIndent=10))
            continue
        elif line.strip() == "":
            story.append(Spacer(1, 3))
        else:
            # metadata lines near the top (bold-prefixed single lines)
            style = meta if line.startswith("**") and line.count("**") >= 2 and \
                len(line) < 120 else body
            story.append(Paragraph(inline(line.strip()), style))
        i += 1

    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.HexColor("#888888"))
        canvas.drawString(2 * cm, 1.0 * cm,
                          "Credit Decision Data Platform — 30/60/90 Execution Plan")
        canvas.drawRightString(19 * cm, 1.0 * cm, f"Page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(
        PDF_PATH, pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=1.8 * cm, bottomMargin=1.6 * cm,
        title="Credit Decision Data Platform — 30/60/90 Execution Plan",
        author="Data Engineering",
    )
    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return _nonempty(PDF_PATH)


# --------------------------------------------------------------------------- #
# Method 3 — markdown -> HTML -> weasyprint / pdfkit
# --------------------------------------------------------------------------- #
def try_html_pdf() -> bool:
    try:
        import markdown  # noqa: F401
    except ImportError:
        print("[html-pdf] `markdown` not available — skipping.")
        return False
    import markdown as md_mod
    with open(MD_PATH, encoding="utf-8") as fh:
        html_body = md_mod.markdown(fh.read(), extensions=["tables", "fenced_code"])
    html = f"<html><head><meta charset='utf-8'></head><body>{html_body}</body></html>"

    try:
        from weasyprint import HTML
        print("[html-pdf] rendering via weasyprint...")
        HTML(string=html).write_pdf(PDF_PATH)
        return _nonempty(PDF_PATH)
    except Exception as exc:  # noqa: BLE001
        print(f"[html-pdf] weasyprint unavailable ({exc}); trying pdfkit...")
    try:
        import pdfkit
        print("[html-pdf] rendering via pdfkit...")
        pdfkit.from_string(html, PDF_PATH)
        return _nonempty(PDF_PATH)
    except Exception as exc:  # noqa: BLE001
        print(f"[html-pdf] pdfkit unavailable ({exc}) — skipping.")
        return False


def _nonempty(path: str) -> bool:
    return os.path.isfile(path) and os.path.getsize(path) > 0


def main() -> int:
    if not os.path.isfile(MD_PATH):
        print(f"ERROR: source not found: {MD_PATH}")
        return 1

    for name, fn in (("pandoc", try_pandoc),
                     ("reportlab", try_reportlab),
                     ("html-pdf", try_html_pdf)):
        try:
            if fn():
                size = os.path.getsize(PDF_PATH)
                print("=" * 60)
                print(f"SUCCESS via {name}: {PDF_PATH} ({size:,} bytes)")
                print("=" * 60)
                return 0
        except Exception as exc:  # noqa: BLE001
            print(f"[{name}] errored: {exc}")

    print("ERROR: no PDF method succeeded. Install pandoc OR reportlab OR "
          "markdown+weasyprint and re-run.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
