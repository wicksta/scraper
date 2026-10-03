#!/usr/bin/env python3
"""Render a reMarkable .rmdoc as full-page PNGs with handwritten annotations.

The base PDF is rendered with pdftoppm. The .rm annotation layers are parsed via
the project-local rmc/rmscene dependency and drawn onto the corresponding page.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

from PIL import Image, ImageDraw

VENDOR = Path("/opt/scraper/vendor/python")
if VENDOR.is_dir():
    sys.path.insert(0, str(VENDOR))

from rmscene import read_tree  # type: ignore
from rmscene import scene_items as si  # type: ignore

RM_TO_SVG_SCALE = 1872.0 / 596.70796460177


def extract_rmdoc(rmdoc: Path, workdir: Path) -> tuple[Path | None, list[Path], dict]:
    with zipfile.ZipFile(rmdoc) as zf:
        zf.extractall(workdir)

    pdfs = sorted(workdir.rglob("*.pdf"))

    content = {}
    content_files = sorted(workdir.glob("*.content"))
    if content_files:
        try:
            content = json.loads(content_files[0].read_text(encoding="utf-8"))
        except Exception:
            content = {}

    rm_files = sorted(workdir.rglob("*.rm"))
    content['_package_pdf_count'] = len(pdfs)
    content['_package_rm_count'] = len(rm_files)
    return (pdfs[0] if pdfs else None), rm_files, content


def positive_float(value, fallback: float) -> float:
    try:
        parsed = float(value)
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


def render_blank_pages(content: dict, rm_files: list[Path], outdir: Path, dpi: int) -> list[Path]:
    page_count = int(content.get("pageCount") or len(rm_files) or 1)
    native_width = positive_float(content.get("customZoomPageWidth"), 1404.0)
    native_height = positive_float(content.get("customZoomPageHeight"), 1872.0)
    page_width_pt = native_width / RM_TO_SVG_SCALE
    page_height_pt = native_height / RM_TO_SVG_SCALE

    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    for rm_file in rm_files:
        for _line, points in parse_rm_lines(rm_file):
            for point in points:
                x = float(point.x)
                y = float(point.y)
                min_x = min(min_x, x)
                max_x = max(max_x, x)
                min_y = min(min_y, y)
                max_y = max(max_y, y)
    if max_x != float("-inf"):
        margin_pt = 36.0
        stroke_half_width_pt = max(abs(min_x), abs(max_x)) / RM_TO_SVG_SCALE
        page_width_pt = max(page_width_pt, (stroke_half_width_pt * 2.0) + (margin_pt * 2.0))
        page_height_pt = max(page_height_pt, (max_y / RM_TO_SVG_SCALE) + margin_pt)

    image_width = max(1, round(page_width_pt * dpi / 72.0))
    image_height = max(1, round(page_height_pt * dpi / 72.0))

    pages = []
    for idx in range(page_count):
        path = outdir / f"blank-page-{idx + 1}.png"
        Image.new("RGB", (image_width, image_height), (255, 255, 255)).save(path)
        pages.append(path)
    return pages


def render_pdf_pages(pdf: Path, outdir: Path, dpi: int) -> list[Path]:
    prefix = outdir / "base-page"
    subprocess.run(
        ["pdftoppm", "-png", "-r", str(dpi), str(pdf), str(prefix)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return sorted(outdir.glob("base-page-*.png"))


def walk_lines(item):
    if isinstance(item, si.Group):
        for child in item.children.values():
            if child:
                yield from walk_lines(child)
    elif isinstance(item, si.Line):
        points = list(item.points)
        if len(points) >= 2:
            yield item, points


def parse_rm_lines(rm_file: Path):
    with rm_file.open("rb") as fh:
        tree = read_tree(fh)
    yield from walk_lines(tree.root)

def rm_point_to_pixel(
    x: float,
    y: float,
    image_width: int,
    image_height: int,
    page_width_pt: float,
    page_height_pt: float,
) -> tuple[float, float]:
    # rmc's scene-space Y coordinate maps naturally to SVG/raster top-down Y.
    # The base PDF is bottom-left internally, but pdftoppm has already rendered it
    # as a top-down raster image, so only X needs recentring into PDF page space.
    pdf_x = (x / RM_TO_SVG_SCALE) + (page_width_pt / 2.0)
    page_down_y = y / RM_TO_SVG_SCALE
    return (pdf_x / page_width_pt) * image_width, (page_down_y / page_height_pt) * image_height


def render_annotations(base_png: Path, rm_file: Path, output_png: Path, dpi: int) -> dict:
    image = Image.open(base_png).convert("RGB")
    page_width_pt = image.width * 72.0 / dpi
    page_height_pt = image.height * 72.0 / dpi
    min_bottom_margin_px = max(60, round(dpi * 0.35))
    stroke_count = 0
    point_count = 0
    rendered_lines = []
    max_y = 0.0

    for line, points in parse_rm_lines(rm_file):
        coords = [
            rm_point_to_pixel(
                float(point.x),
                float(point.y),
                image.width,
                image.height,
                page_width_pt,
                page_height_pt,
            )
            for point in points
        ]
        width = max(2, min(14, round(float(getattr(line, "thickness_scale", 1.0) or 1.0) * 2.0)))
        rendered_lines.append((coords, width))
        if coords:
            max_y = max(max_y, max(coord[1] for coord in coords))
        stroke_count += 1
        point_count += len(coords)

    bottom_padding_px = max(0, round(max_y + min_bottom_margin_px - image.height))
    if bottom_padding_px > 0:
        padded = Image.new("RGB", (image.width, image.height + bottom_padding_px), (255, 255, 255))
        padded.paste(image, (0, 0))
        image = padded

    draw = ImageDraw.Draw(image)
    for coords, width in rendered_lines:
        draw.line(coords, fill=(0, 0, 0), width=width, joint="curve")

    image.save(output_png)
    return {
        "page_png": str(output_png),
        "base_png": str(base_png),
        "rm_file": str(rm_file),
        "stroke_count": stroke_count,
        "point_count": point_count,
        "width": image.width,
        "height": image.height,
        "page_width_pt": page_width_pt,
        "page_height_pt": page_height_pt,
        "bottom_padding_px": bottom_padding_px,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("rmdoc", help="Downloaded .rmdoc file")
    parser.add_argument("--outdir", required=True, help="Directory for rendered PNGs")
    parser.add_argument("--dpi", type=int, default=180)
    args = parser.parse_args()

    rmdoc = Path(args.rmdoc)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="rmdoc-render-") as tmp:
        workdir = Path(tmp)
        pdf, rm_files, content = extract_rmdoc(rmdoc, workdir)
        base_source = "pdf" if pdf is not None else "blank"
        base_pages = render_pdf_pages(pdf, workdir, args.dpi) if pdf is not None else render_blank_pages(content, rm_files, workdir, args.dpi)

        results = []
        for idx, base_png in enumerate(base_pages):
            if idx >= len(rm_files):
                output_png = outdir / f"page-{idx + 1:03d}.png"
                Image.open(base_png).save(output_png)
                results.append({
                    "page_png": str(output_png),
                    "base_png": str(base_png),
                    "rm_file": None,
                    "stroke_count": 0,
                    "point_count": 0,
                })
                continue
            output_png = outdir / f"page-{idx + 1:03d}-annotated.png"
            results.append(render_annotations(base_png, rm_files[idx], output_png, args.dpi))

    print(json.dumps({
        "success": True,
        "rmdoc": str(rmdoc),
        "page_count": len(results),
        "content_page_count": content.get("pageCount"),
        "base_source": base_source,
        "embedded_pdf": str(pdf) if pdf is not None else None,
        "package_pdf_count": content.get("_package_pdf_count", 0),
        "package_rm_count": content.get("_package_rm_count", 0),
        "outputs": results,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
