#!/usr/bin/env python3
"""
Extract PDF text with Docling one page at a time.

Why:
- Large PDFs can fail or stall when processed in one pass.
- Page-wise extraction is slower but much more resilient.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from docling.document_converter import DocumentConverter


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Docling page-by-page extractor")
    p.add_argument("pdf", type=Path, help="Path to PDF")
    p.add_argument("--start-page", type=int, default=1, help="1-based start page (default: 1)")
    p.add_argument("--end-page", type=int, required=True, help="1-based end page (inclusive)")
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path("./tmp/docling-pages"),
        help="Output directory (default: ./tmp/docling-pages)",
    )
    p.add_argument(
        "--merged-output",
        type=Path,
        default=None,
        help="Optional path for a single merged text file with all extracted pages",
    )
    p.add_argument(
        "--append-merged",
        action="store_true",
        help="Append to merged output instead of truncating it first",
    )
    p.add_argument("--sleep-ms", type=int, default=0, help="Optional pause between pages")
    p.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop on first page failure (default: continue and log errors)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    pdf = args.pdf.resolve()
    if not pdf.exists():
        print(f"ERROR: file not found: {pdf}", file=sys.stderr)
        return 2

    start_page = max(1, int(args.start_page))
    end_page = int(args.end_page)
    if end_page < start_page:
        print("ERROR: --end-page must be >= --start-page", file=sys.stderr)
        return 2

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    merged_output = args.merged_output.resolve() if args.merged_output else None
    if merged_output:
        merged_output.parent.mkdir(parents=True, exist_ok=True)
        if not args.append_merged:
            merged_output.write_text("", encoding="utf-8")

    converter = DocumentConverter()

    ok = 0
    failed = 0
    errors: list[dict[str, str | int]] = []

    for page in range(start_page, end_page + 1):
        t0 = time.time()
        print(f"[docling-pagewise] page={page} start")
        try:
            res = converter.convert(str(pdf), page_range=(page, page))
            text = res.document.export_to_text()
            out_file = out_dir / f"page_{page:04d}.txt"
            out_file.write_text(text, encoding="utf-8")
            if merged_output:
                with merged_output.open("a", encoding="utf-8") as fh:
                    fh.write(f"\n\n===== PAGE {page} =====\n\n")
                    fh.write(text)
            ok += 1
            elapsed = round(time.time() - t0, 2)
            print(f"[docling-pagewise] page={page} ok chars={len(text)} elapsed_s={elapsed} out={out_file}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            msg = str(exc)
            errors.append({"page": page, "error": msg})
            elapsed = round(time.time() - t0, 2)
            print(f"[docling-pagewise] page={page} failed elapsed_s={elapsed} error={msg}", file=sys.stderr)
            if args.stop_on_error:
                break
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    summary = {
        "ok": True,
        "pdf": str(pdf),
        "start_page": start_page,
        "end_page": end_page,
        "out_dir": str(out_dir),
        "merged_output": str(merged_output) if merged_output else None,
        "pages_ok": ok,
        "pages_failed": failed,
        "errors": errors,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
