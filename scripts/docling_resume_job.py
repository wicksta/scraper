#!/usr/bin/env python3
"""
Resume a partially completed Docling pagewise extraction job.

This script is meant for ad hoc recovery of stalled jobs in
/tmp/docling-pagewise-api/job_<id>. It reuses the existing pagewise extractor,
runs in small batches, and appends into the existing merged output as it goes.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


DEFAULT_PYTHON = Path("/usr/bin/python3")
DEFAULT_EXTRACTOR = Path("/opt/scraper/scripts/docling_extract_pagewise.py")
DEFAULT_BATCH_SIZE = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resume a stalled Docling pagewise job")
    parser.add_argument("job_dir", type=Path, help="Job directory, e.g. /tmp/docling-pagewise-api/job_4054")
    parser.add_argument("--start-page", type=int, default=None, help="Override detected start page")
    parser.add_argument("--end-page", type=int, default=None, help="Override final page")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Pages per subprocess")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Pause between pages inside each batch")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop on first page error")
    parser.add_argument("--dry-run", action="store_true", help="Print planned batches only")
    parser.add_argument("--python-bin", type=Path, default=DEFAULT_PYTHON, help="Python executable")
    parser.add_argument("--extractor", type=Path, default=DEFAULT_EXTRACTOR, help="Extractor script")
    return parser.parse_args()


def find_last_contiguous_page(pages_dir: Path) -> int:
    pattern = re.compile(r"page_(\d{4,})\.txt$")
    pages: set[int] = set()
    for entry in pages_dir.iterdir():
        if not entry.is_file():
            continue
        match = pattern.match(entry.name)
        if match:
            pages.add(int(match.group(1)))

    page = 1
    while page in pages:
        page += 1
    return page - 1


def pdf_page_count(pdf_path: Path) -> int | None:
    try:
        proc = subprocess.run(
            ["pdfinfo", str(pdf_path)],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None

    if proc.returncode != 0:
        return None

    for line in proc.stdout.splitlines():
        if line.startswith("Pages:"):
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return None
    return None


def resume_env(cache_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    user_site = "/home/james/.local/lib/python3.12/site-packages"
    pythonpath = env.get("PYTHONPATH", "")
    env["HOME"] = "/home/james"
    env["PYTHONUNBUFFERED"] = "1"
    env["XDG_CACHE_HOME"] = str(cache_dir)
    env["HF_HOME"] = str(cache_dir / "huggingface")
    env["TRANSFORMERS_CACHE"] = str(cache_dir / "huggingface" / "transformers")
    env["PYTHONPATH"] = user_site if not pythonpath else f"{user_site}{os.pathsep}{pythonpath}"
    return env


def batch_ranges(start_page: int, end_page: int, batch_size: int) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    page = start_page
    while page <= end_page:
        batch_end = min(end_page, page + batch_size - 1)
        ranges.append((page, batch_end))
        page = batch_end + 1
    return ranges


def main() -> int:
    args = parse_args()
    job_dir = args.job_dir.resolve()
    pdf_path = job_dir / "input.pdf"
    pages_dir = job_dir / "pages"
    merged_path = job_dir / "merged.txt"
    cache_dir = job_dir / "cache"
    resume_log = job_dir / "resume.log"

    if not job_dir.is_dir():
        print(f"ERROR: job directory not found: {job_dir}", file=sys.stderr)
        return 2
    if not pdf_path.is_file():
        print(f"ERROR: input PDF not found: {pdf_path}", file=sys.stderr)
        return 2
    if not pages_dir.is_dir():
        print(f"ERROR: pages directory not found: {pages_dir}", file=sys.stderr)
        return 2
    if not args.extractor.is_file():
        print(f"ERROR: extractor script not found: {args.extractor}", file=sys.stderr)
        return 2
    if not args.python_bin.is_file():
        print(f"ERROR: python executable not found: {args.python_bin}", file=sys.stderr)
        return 2
    if args.batch_size < 1:
        print("ERROR: --batch-size must be >= 1", file=sys.stderr)
        return 2

    last_page = find_last_contiguous_page(pages_dir)
    start_page = args.start_page if args.start_page is not None else last_page + 1

    detected_end = pdf_page_count(pdf_path)
    end_page = args.end_page if args.end_page is not None else detected_end
    if end_page is None:
        print("ERROR: could not determine PDF page count; pass --end-page", file=sys.stderr)
        return 2

    if start_page > end_page:
        print(
            f"Nothing to do. Last contiguous page is {last_page}, end page is {end_page}.",
            file=sys.stderr,
        )
        return 0

    env = resume_env(cache_dir)
    batches = batch_ranges(start_page, end_page, args.batch_size)

    print(f"Job dir: {job_dir}")
    print(f"Last contiguous page: {last_page}")
    print(f"Resuming from page {start_page} to {end_page}")
    print(f"Batch size: {args.batch_size}")
    print(f"Merged output: {merged_path}")

    if args.dry_run:
        for batch_start, batch_end in batches:
            print(f"DRY RUN batch {batch_start}-{batch_end}")
        return 0

    with resume_log.open("a", encoding="utf-8") as log_fh:
        log_fh.write(
            f"\n=== Resume start start_page={start_page} end_page={end_page} batch_size={args.batch_size} ===\n"
        )
        log_fh.flush()

        for batch_start, batch_end in batches:
            cmd = [
                str(args.python_bin),
                str(args.extractor),
                str(pdf_path),
                "--start-page",
                str(batch_start),
                "--end-page",
                str(batch_end),
                "--out-dir",
                str(pages_dir),
                "--merged-output",
                str(merged_path),
                "--append-merged",
            ]
            if args.sleep_ms > 0:
                cmd.extend(["--sleep-ms", str(args.sleep_ms)])
            if args.stop_on_error:
                cmd.append("--stop-on-error")

            banner = f"\n--- Batch {batch_start}-{batch_end} ---\n$ {' '.join(cmd)}\n"
            sys.stdout.write(banner)
            sys.stdout.flush()
            log_fh.write(banner)
            log_fh.flush()

            proc = subprocess.Popen(
                cmd,
                cwd=str(job_dir),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                sys.stdout.write(line)
                log_fh.write(line)
                log_fh.flush()
            rc = proc.wait()
            if rc != 0:
                print(f"Batch {batch_start}-{batch_end} failed with exit code {rc}", file=sys.stderr)
                log_fh.write(f"Batch {batch_start}-{batch_end} failed with exit code {rc}\n")
                log_fh.flush()
                return rc

        log_fh.write("=== Resume complete ===\n")
        log_fh.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
