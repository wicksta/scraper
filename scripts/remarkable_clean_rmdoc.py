#!/usr/bin/env python3
"""Replace a reMarkable .rmdoc PDF and remove handwritten .rm layers."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import zipfile
from pathlib import Path


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=4) + "\n", encoding="utf-8")


def clean_rmdoc(source_rmdoc: Path, replacement_pdf: Path, output_rmdoc: Path) -> dict:
    if not source_rmdoc.is_file():
        raise FileNotFoundError(f"Source rmdoc not found: {source_rmdoc}")
    if not replacement_pdf.is_file():
        raise FileNotFoundError(f"Replacement PDF not found: {replacement_pdf}")

    with tempfile.TemporaryDirectory(prefix="clean-rmdoc-") as tmp:
        workdir = Path(tmp)
        with zipfile.ZipFile(source_rmdoc) as zf:
            zf.extractall(workdir)

        content_files = sorted(workdir.glob("*.content"))
        pdf_files = sorted(workdir.glob("*.pdf"))
        rm_files = sorted(workdir.glob("*/*.rm"))

        if not content_files:
            raise RuntimeError("No .content manifest found in rmdoc")
        if not pdf_files:
            raise RuntimeError("No embedded PDF found in rmdoc")

        content_path = content_files[0]
        pdf_path = pdf_files[0]
        removed_rm = []

        shutil.copyfile(replacement_pdf, pdf_path)
        for rm_file in rm_files:
            removed_rm.append(str(rm_file.relative_to(workdir)))
            rm_file.unlink()

        content = load_json(content_path)
        content["sizeInBytes"] = str(replacement_pdf.stat().st_size)
        write_json(content_path, content)

        output_rmdoc.parent.mkdir(parents=True, exist_ok=True)
        if output_rmdoc.exists():
            output_rmdoc.unlink()
        with zipfile.ZipFile(output_rmdoc, "w", compression=zipfile.ZIP_STORED) as zf:
            for file_path in sorted(p for p in workdir.rglob("*") if p.is_file()):
                zf.write(file_path, file_path.relative_to(workdir).as_posix())

    return {
        "success": True,
        "source_rmdoc": str(source_rmdoc),
        "replacement_pdf": str(replacement_pdf),
        "output_rmdoc": str(output_rmdoc),
        "removed_rm_count": len(removed_rm),
        "removed_rm_files": removed_rm,
        "replacement_pdf_bytes": replacement_pdf.stat().st_size,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_rmdoc")
    parser.add_argument("replacement_pdf")
    parser.add_argument("output_rmdoc")
    args = parser.parse_args()

    result = clean_rmdoc(Path(args.source_rmdoc), Path(args.replacement_pdf), Path(args.output_rmdoc))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
