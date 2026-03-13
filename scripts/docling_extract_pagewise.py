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
import os
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

from docling.document_converter import DocumentConverter

DEFAULT_PAGE_WORKER_MEMORY_LIMIT_MB = 0


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
    p.add_argument("--single-page-worker", action="store_true", help=argparse.SUPPRESS)
    return p.parse_args()


def extract_single_page_with_docling(pdf: Path, page: int) -> str:
    converter = DocumentConverter()
    res = converter.convert(str(pdf), page_range=(page, page))
    return res.document.export_to_text()


def extract_single_page_with_pdftotext(pdf: Path, page: int) -> str:
    if shutil.which("pdftotext") is None:
        raise RuntimeError("pdftotext is not installed")
    proc = subprocess.run(
        [
            "pdftotext",
            "-layout",
            "-f",
            str(page),
            "-l",
            str(page),
            str(pdf),
            "-",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise RuntimeError(f"pdftotext failed with exit code {proc.returncode}: {stderr}")
    return proc.stdout


def run_single_page_worker(pdf: Path, page: int) -> int:
    try:
        limit_mb = int(os.environ.get("DOCLING_PAGE_WORKER_MEMORY_LIMIT_MB", str(DEFAULT_PAGE_WORKER_MEMORY_LIMIT_MB)))
        if limit_mb > 0:
            limit_bytes = limit_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
        text = extract_single_page_with_docling(pdf, page)
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {"ok": False, "page": page, "error": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            {"ok": True, "page": page, "text": text},
            ensure_ascii=False,
        )
    )
    return 0


def run_docling_subprocess(pdf: Path, page: int) -> tuple[bool, str, str]:
    proc = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(pdf),
            "--start-page",
            str(page),
            "--end-page",
            str(page),
            "--single-page-worker",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode == 0:
        if not stdout:
            return False, "", "Docling worker returned no output"
        try:
            payload = json.loads(stdout.splitlines()[-1])
        except json.JSONDecodeError as exc:
            return False, "", f"Docling worker returned invalid JSON: {exc}"
        if not payload.get("ok"):
            return False, "", str(payload.get("error") or "Docling worker failed")
        return True, str(payload.get("text") or ""), ""

    if proc.returncode < 0:
        signal_num = -proc.returncode
        return False, "", f"Docling worker terminated by signal {signal_num}"

    detail = stderr or stdout or f"Docling worker exited with code {proc.returncode}"
    return False, "", detail


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

    if args.single_page_worker:
        if start_page != end_page:
            print("ERROR: --single-page-worker requires start_page=end_page", file=sys.stderr)
            return 2
        return run_single_page_worker(pdf, start_page)

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    merged_output = args.merged_output.resolve() if args.merged_output else None
    if merged_output:
        merged_output.parent.mkdir(parents=True, exist_ok=True)
        if not args.append_merged:
            merged_output.write_text("", encoding="utf-8")

    ok = 0
    failed = 0
    errors: list[dict[str, str | int]] = []

    for page in range(start_page, end_page + 1):
        t0 = time.time()
        print(f"[docling-pagewise] page={page} start")
        try:
            used_fallback = False
            text = ""
            docling_ok, docling_text, docling_error = run_docling_subprocess(pdf, page)
            if docling_ok:
                text = docling_text
            else:
                print(
                    f"[docling-pagewise] page={page} docling_failed error={docling_error} fallback=pdftotext",
                    file=sys.stderr,
                )
                text = extract_single_page_with_pdftotext(pdf, page)
                used_fallback = True
            out_file = out_dir / f"page_{page:04d}.txt"
            out_file.write_text(text, encoding="utf-8")
            if merged_output:
                with merged_output.open("a", encoding="utf-8") as fh:
                    fh.write(f"\n\n===== PAGE {page} =====\n\n")
                    fh.write(text)
            ok += 1
            elapsed = round(time.time() - t0, 2)
            fallback_tag = " fallback=pdftotext" if used_fallback else ""
            print(
                f"[docling-pagewise] page={page} ok chars={len(text)} elapsed_s={elapsed} out={out_file}{fallback_tag}"
            )
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
