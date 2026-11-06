"""Render deck/*.md to a landscape, one-slide-per-page PDF via headless Chrome.

Usage: uv run --no-project --with markdown python tools/deck_to_pdf.py deck/FreshMart_Shelf_Guard_deck.md
Slides are separated by '---' lines in the Markdown.
"""
import pathlib
import subprocess
import sys

import markdown

CSS = """
@page { size: 13.333in 7.5in; margin: 0; }
* { box-sizing: border-box; }
body { margin: 0; font: 15px/1.45 -apple-system, "Helvetica Neue", Arial, sans-serif; color: #0b0b0b; }
.slide { width: 13.333in; height: 7.5in; padding: 0.55in 0.75in; page-break-after: always; overflow: hidden; position: relative;
         border-top: 10px solid #1b3139; }
.slide:first-child { background: #1b3139; color: #fcfcfb; display: flex; flex-direction: column; justify-content: center; border-top: none; }
.slide:first-child h1 { font-size: 46px; color: #fff; } .slide:first-child h3 { color: #ff6f4c; font-size: 24px; }
.slide:first-child p, .slide:first-child em { color: #c3c2b7; }
h1 { font-size: 34px; margin: 0 0 8px; letter-spacing: -0.01em; } h2 { font-size: 27px; margin: 0 0 14px; color: #1b3139; }
h3 { font-size: 19px; margin: 6px 0; }
table { border-collapse: collapse; width: 100%; margin: 8px 0 10px; font-size: 13.5px; }
th { text-align: left; background: #f4f4f2; border-bottom: 2px solid #1b3139; padding: 6px 8px; }
td { border-bottom: 1px solid #e3e2de; padding: 5px 8px; vertical-align: top; }
blockquote { margin: 8px 0; padding: 8px 14px; border-left: 5px solid #ff3621; background: #fff4f1; font-size: 15px; }
blockquote p { margin: 4px 0; }
code { font-size: 12px; background: #f4f4f2; padding: 1px 4px; border-radius: 3px; }
pre { font-size: 11.5px; line-height: 1.35; background: #f4f4f2; padding: 10px 14px; border-radius: 6px; overflow: hidden; }
pre code { background: none; padding: 0; }
ul, ol { margin: 6px 0 6px 22px; padding: 0; } li { margin: 3px 0; }
strong { color: #1b3139; } .slide:first-child strong { color: #fff; }
.num { position: absolute; bottom: 0.25in; right: 0.5in; font-size: 11px; color: #8a8984; }
"""


def main(md_path: str):
    src = pathlib.Path(md_path).read_text()
    slides = [s.strip() for s in src.split("\n---\n") if s.strip()]
    body = "".join(f'<section class="slide">{markdown.markdown(s, extensions=["tables", "fenced_code"])}'
                   f'<div class="num">FreshMart Shelf Guard · {i + 1}/{len(slides)}</div></section>' for i, s in enumerate(slides))
    html = pathlib.Path(md_path).with_suffix(".html")
    html.write_text(f"<!doctype html><html><head><meta charset='utf-8'><style>{CSS}</style></head><body>{body}</body></html>")
    pdf = pathlib.Path(md_path).with_suffix(".pdf")
    subprocess.run(["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "--headless=new", "--disable-gpu",
                    "--no-pdf-header-footer", f"--print-to-pdf={pdf.resolve()}", html.resolve().as_uri()], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{len(slides)} slides -> {pdf}")


if __name__ == "__main__":
    main(sys.argv[1])
