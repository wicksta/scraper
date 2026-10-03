#!/usr/bin/env python3
"""Extract basic geometry from reMarkable v6 .rm files.

This intentionally stays small: it gives PHP workers stable JSON line bounds
while leaving full rendering/OCR decisions to later stages.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

VENDOR = Path("/opt/scraper/vendor/python")
if VENDOR.is_dir():
    sys.path.insert(0, str(VENDOR))

from rmscene import read_tree  # type: ignore
from rmscene import scene_items as si  # type: ignore
from rmc.exporters.svg import tree_to_svg  # type: ignore


def walk(item):
    if isinstance(item, si.Group):
        for child in item.children.values():
            if child:
                yield from walk(child)
    elif isinstance(item, si.Line):
        points = list(item.points)
        if not points:
            return
        xs = [float(p.x) for p in points]
        ys = [float(p.y) for p in points]
        yield {
            "type": "line",
            "tool": str(item.tool),
            "color": str(item.color),
            "point_count": len(points),
            "min_x": min(xs),
            "min_y": min(ys),
            "max_x": max(xs),
            "max_y": max(ys),
            "width": max(xs) - min(xs),
            "height": max(ys) - min(ys),
            "thickness_scale": float(getattr(item, "thickness_scale", 0.0) or 0.0),
            "points": [{"x": float(p.x), "y": float(p.y)} for p in points],
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to a reMarkable .rm file")
    parser.add_argument("--json", dest="json_path", help="Write extracted JSON here")
    parser.add_argument("--svg", dest="svg_path", help="Write rendered SVG here")
    args = parser.parse_args()

    input_path = Path(args.input)
    with input_path.open("rb") as fh:
        tree = read_tree(fh)

    lines = list(walk(tree.root))
    payload = {
        "source": str(input_path),
        "line_count": len(lines),
        "lines": lines,
    }

    if args.json_path:
        Path(args.json_path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    else:
        print(json.dumps(payload, ensure_ascii=False))

    if args.svg_path:
        with input_path.open("rb") as fh:
            tree = read_tree(fh)
        with Path(args.svg_path).open("w", encoding="utf-8") as out:
            tree_to_svg(tree, out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
