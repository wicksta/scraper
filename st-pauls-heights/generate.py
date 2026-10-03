#!/usr/bin/env python3
"""Export City of London St Paul's Heights GIS data to altitude-aware KML/KMZ.

The grid cache preserves source EPSG:27700 geometry.  PROJ's cs2cs performs
the EPSG:27700 -> EPSG:4326 conversion used in the KML.
"""
from __future__ import annotations

import argparse
import html
import json
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from shapely.geometry import LinearRing, Point, Polygon

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
OUTPUT = ROOT / "output"
GRID_URL = "https://www.mapping.cityoflondon.gov.uk/arcgis/rest/services/INSPIRE/MapServer/95"
CONTEXT_URL = "https://www.mapping.cityoflondon.gov.uk/arcgis/rest/services/COMPASS_Planning_St_Pauls_Heights/MapServer"
GRID_FIELDS = "OBJECTID,VIEWPOINTS,HEIGHT"
CONTEXT_LAYERS = {
    1: ("SPH Setbacks", "sph-setbacks"),
    3: ("St Paul's Heights Policy Area Boundary", "sph-boundary"),
    4: ("St Paul's Heights Policy Area", "sph-policy-area"),
}
TRANSFORM_CACHE: dict[tuple[float, float], tuple[float, float]] = {}


def request_all(layer_url: str, out_fields: str) -> dict[str, Any]:
    """Read every native ArcGIS feature, respecting record limits."""
    metadata = requests.get(layer_url, params={"f": "json"}, timeout=60).json()
    if "error" in metadata:
        raise RuntimeError(metadata["error"])
    count_payload = requests.get(
        f"{layer_url}/query",
        params={"f": "json", "where": "1=1", "returnCountOnly": "true"},
        timeout=60,
    ).json()
    expected = int(count_payload["count"])
    object_id = metadata.get("objectIdField") or next(
        field["name"] for field in metadata["fields"] if field["type"] == "esriFieldTypeOID"
    )
    page_size = min(int(metadata.get("maxRecordCount", 1000)), 1000)
    features: list[dict[str, Any]] = []
    offset = 0
    while True:
        payload = requests.get(
            f"{layer_url}/query",
            params={
                "f": "json", "where": "1=1", "outFields": out_fields,
                "returnGeometry": "true", "outSR": "27700", "orderByFields": object_id,
                "resultOffset": offset, "resultRecordCount": page_size,
            }, timeout=60,
        ).json()
        if "error" in payload:
            raise RuntimeError(payload["error"])
        page = payload.get("features", [])
        features.extend(page)
        if not payload.get("exceededTransferLimit") or not page:
            break
        offset += len(page)
    if len(features) != expected:
        raise RuntimeError(f"ArcGIS count mismatch: expected {expected}, received {len(features)}")
    return {
        "downloaded_at": datetime.now(timezone.utc).isoformat(), "source_url": layer_url,
        "spatial_reference": {"wkid": 27700, "name": "EPSG:27700 / British National Grid"},
        "layer": {key: metadata.get(key) for key in ("id", "name", "geometryType", "maxRecordCount", "fields")},
        "expected_feature_count": expected, "features": features,
    }


def source_geojson(cache: dict[str, Any]) -> dict[str, Any]:
    """A reproducible GeoJSON-style cache retaining BNG coordinates and ArcGIS properties."""
    def geometry(feature: dict[str, Any]) -> dict[str, Any] | None:
        raw = feature.get("geometry") or {}
        if "rings" in raw:
            return {"type": "Polygon", "coordinates": raw["rings"]}
        if "paths" in raw:
            return {"type": "MultiLineString", "coordinates": raw["paths"]}
        if "x" in raw:
            return {"type": "Point", "coordinates": [raw["x"], raw["y"]]}
        return None
    return {
        "type": "FeatureCollection", "crs": {"type": "name", "properties": {"name": "EPSG:27700"}},
        "metadata": {key: cache[key] for key in ("downloaded_at", "source_url", "layer", "expected_feature_count")},
        "features": [{"type": "Feature", "properties": feature.get("attributes", {}), "geometry": geometry(feature)} for feature in cache["features"]],
    }


def transform_points(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Use PROJ cs2cs; output is latitude longitude, KML needs longitude latitude."""
    if not points:
        return []
    missing = list(dict.fromkeys((float(x), float(y)) for x, y in points if (float(x), float(y)) not in TRANSFORM_CACHE))
    if missing:
        source = "\n".join(f"{x:.12f} {y:.12f}" for x, y in missing) + "\n"
        result = subprocess.run(
            ["cs2cs", "-f", "%.9f", "EPSG:27700", "EPSG:4326"], input=source,
            text=True, capture_output=True, check=True,
        )
        converted = [line.split() for line in result.stdout.splitlines() if line.strip()]
        if len(converted) != len(missing):
            raise RuntimeError("PROJ did not return a transformed coordinate for every source coordinate")
        TRANSFORM_CACHE.update({point: (float(values[1]), float(values[0])) for point, values in zip(missing, converted)})
    return [TRANSFORM_CACHE[(float(x), float(y))] for x, y in points]


def prime_transform_cache(caches: list[dict[str, Any]]) -> None:
    points: list[tuple[float, float]] = []
    for cache in caches:
        for feature in cache["features"]:
            geometry = feature.get("geometry") or {}
            for ring in geometry.get("rings", []):
                points.extend((point[0], point[1]) for point in ring)
            for path in geometry.get("paths", []):
                points.extend((point[0], point[1]) for point in path)
            if "x" in geometry:
                points.append((geometry["x"], geometry["y"]))
    transform_points(points)


def rings_to_polygons(rings: list[list[list[float]]]) -> list[tuple[list[list[float]], list[list[list[float]]]]]:
    """Split ArcGIS rings into exterior rings and their holes."""
    cleaned = [ring for ring in rings if len(ring) >= 4]
    exteriors = [ring for ring in cleaned if not LinearRing(ring).is_ccw]
    holes = [ring for ring in cleaned if LinearRing(ring).is_ccw]
    if not exteriors:  # Defensive fallback for non-standard ring orientation.
        return [(ring, []) for ring in cleaned]
    output = [(ring, []) for ring in exteriors]
    for hole in holes:
        probe = Point(hole[0])
        containers = [(idx, Polygon(outer)) for idx, (outer, _) in enumerate(output) if Polygon(outer).contains(probe)]
        if containers:
            idx = min(containers, key=lambda item: item[1].area)[0]
            output[idx][1].append(hole)
    return output


def kml_ring(ring: list[list[float]], altitude: float | None = None) -> str:
    transformed = transform_points([(float(x), float(y)) for x, y, *_ in ring])
    if altitude is None:
        return " ".join(f"{lon:.9f},{lat:.9f}" for lon, lat in transformed)
    return " ".join(f"{lon:.9f},{lat:.9f},{altitude:.2f}" for lon, lat in transformed)


def polygon_xml(rings: list[list[list[float]]], altitude: float | None = None) -> str:
    parts = []
    for outer, holes in rings_to_polygons(rings):
        mode = (
            "<extrude>0</extrude><tessellate>0</tessellate><altitudeMode>absolute</altitudeMode>"
            if altitude is not None else "<tessellate>1</tessellate><altitudeMode>clampToGround</altitudeMode>"
        )
        inner = "".join(f"<innerBoundaryIs><LinearRing><coordinates>{kml_ring(hole, altitude)}</coordinates></LinearRing></innerBoundaryIs>" for hole in holes)
        parts.append(f"<Polygon>{mode}<outerBoundaryIs><LinearRing><coordinates>{kml_ring(outer, altitude)}</coordinates></LinearRing></outerBoundaryIs>{inner}</Polygon>")
    return parts[0] if len(parts) == 1 else "<MultiGeometry>" + "".join(parts) + "</MultiGeometry>"


def grid_placemark(feature: dict[str, Any]) -> str:
    attrs = feature["attributes"]
    height = float(attrs["HEIGHT"])
    name = f"St Paul's Heights — {height:.2f} m AOD"
    style, band = grid_style(height)
    description = f"<b>HEIGHT:</b> {height:.2f} m AOD<br/><b>Height band:</b> {band}<br/><b>VIEWPOINTS:</b> {html.escape(str(attrs.get('VIEWPOINTS', '')))}<br/><b>OBJECTID:</b> {attrs.get('OBJECTID')}"
    return f"<Placemark><name>{html.escape(name)}</name><description><![CDATA[{description}]]></description><styleUrl>#{style}</styleUrl><ExtendedData><Data name=\"OBJECTID\"><value>{attrs.get('OBJECTID')}</value></Data><Data name=\"VIEWPOINTS\"><value>{html.escape(str(attrs.get('VIEWPOINTS', '')))}</value></Data><Data name=\"HEIGHT_AOD_M\"><value>{height:.2f}</value></Data></ExtendedData>{polygon_xml(feature['geometry']['rings'], height)}</Placemark>"


def grid_style(height: float) -> tuple[str, str]:
    """Shared KML styles: cool colours are lower AOD, warm colours higher."""
    if height <= 20:
        return "sph-grid-low", "up to 20 m AOD (pale orange)"
    if height <= 30:
        return "sph-grid-low-mid", "over 20 to 30 m AOD (light orange)"
    if height <= 40:
        return "sph-grid-mid", "over 30 to 40 m AOD (orange)"
    if height <= 50:
        return "sph-grid-high-mid", "over 40 to 50 m AOD (deep orange)"
    return "sph-grid-high", "over 50 m AOD (burnt orange)"


def context_placemark(feature: dict[str, Any], style: str, label: str) -> str:
    attrs = feature.get("attributes", {})
    name = attrs.get("NAME") or attrs.get("DESCRIPTION") or f"{label} {attrs.get('OBJECTID', '')}".strip()
    raw = feature.get("geometry") or {}
    if "rings" in raw:
        geometry = polygon_xml(raw["rings"])
    elif "paths" in raw:
        geometry = "<MultiGeometry>" + "".join(f"<LineString><altitudeMode>clampToGround</altitudeMode><coordinates>{kml_ring(path)}</coordinates></LineString>" for path in raw["paths"]) + "</MultiGeometry>"
    elif "x" in raw:
        lon, lat = transform_points([(raw["x"], raw["y"])])[0]
        geometry = f"<Point><altitudeMode>clampToGround</altitudeMode><coordinates>{lon:.9f},{lat:.9f}</coordinates></Point>"
    else:
        return ""
    return f"<Placemark><name>{html.escape(str(name))}</name><styleUrl>#{style}</styleUrl>{geometry}</Placemark>"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Download fresh data instead of using the grid cache")
    parser.add_argument("--no-context", action="store_true", help="Omit optional dedicated-service layers")
    args = parser.parse_args()
    DATA.mkdir(exist_ok=True); OUTPUT.mkdir(exist_ok=True)
    grid_cache_path = DATA / "st_pauls_heights_grid.arcgis.json"
    if args.refresh or not grid_cache_path.exists():
        grid = request_all(GRID_URL, GRID_FIELDS)
        grid_cache_path.write_text(json.dumps(grid, indent=2), encoding="utf-8")
        (DATA / "st_pauls_heights_grid.geojson").write_text(json.dumps(source_geojson(grid), indent=2), encoding="utf-8")
    else:
        grid = json.loads(grid_cache_path.read_text(encoding="utf-8"))
    contexts: list[tuple[str, str, dict[str, Any]]] = []
    if not args.no_context:
        for layer_id, (label, style) in CONTEXT_LAYERS.items():
            cache_path = DATA / f"context_{layer_id}.arcgis.json"
            if args.refresh or not cache_path.exists():
                cached = request_all(f"{CONTEXT_URL}/{layer_id}", "*")
                cache_path.write_text(json.dumps(cached, indent=2), encoding="utf-8")
            else:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
            contexts.append((label, style, cached))
    prime_transform_cache([grid] + [cached for _, _, cached in contexts])
    heights = [float(feature["attributes"]["HEIGHT"]) for feature in grid["features"]]
    styles = """<Style id="sph-grid-low"><LineStyle><color>ffa3d7ff</color><width>1.2</width></LineStyle><PolyStyle><color>66a3d7ff</color><outline>1</outline></PolyStyle></Style><Style id="sph-grid-low-mid"><LineStyle><color>ff5cb4ff</color><width>1.2</width></LineStyle><PolyStyle><color>665cb4ff</color><outline>1</outline></PolyStyle></Style><Style id="sph-grid-mid"><LineStyle><color>ff0066ff</color><width>1.2</width></LineStyle><PolyStyle><color>660066ff</color><outline>1</outline></PolyStyle></Style><Style id="sph-grid-high-mid"><LineStyle><color>ff0067e5</color><width>1.2</width></LineStyle><PolyStyle><color>660067e5</color><outline>1</outline></PolyStyle></Style><Style id="sph-grid-high"><LineStyle><color>ff0047b9</color><width>1.2</width></LineStyle><PolyStyle><color>660047b9</color><outline>1</outline></PolyStyle></Style><Style id="sph-boundary"><LineStyle><color>ff00ffff</color><width>2.4</width></LineStyle></Style><Style id="sph-policy-area"><LineStyle><color>ff00ff00</color><width>1.2</width></LineStyle><PolyStyle><color>2200ff00</color><outline>1</outline></PolyStyle></Style><Style id="sph-setbacks"><LineStyle><color>ffff00ff</color><width>1.2</width></LineStyle><PolyStyle><color>22ff00ff</color><outline>1</outline></PolyStyle></Style>"""
    grid_placemarks = [grid_placemark(feature) for feature in grid["features"]]
    folders = ["<Folder><name>3D St Paul’s Heights Grid</name>" + "".join(grid_placemarks) + "</Folder>"]
    for label, style, cached in contexts:
        folders.append(f"<Folder><name>{html.escape(label)} (context)</name>" + "".join(context_placemark(f, style, label) for f in cached["features"]) + "</Folder>")
    def kml_document(name: str, body: list[str]) -> str:
        return f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document><name>{name}</name><description>Grid cells are horizontal plates at their prescribed AOD height.</description>{styles}{''.join(body)}</Document></kml>"
    kml = kml_document("St Paul’s Heights control surface", folders)
    kml_path = OUTPUT / "st_pauls_heights.kml"
    kml_path.write_text(kml, encoding="utf-8")
    with zipfile.ZipFile(OUTPUT / "st_pauls_heights.kmz", "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(kml_path, arcname="st_pauls_heights.kml")
    # Earth Web can fall back to its data-layer importer for a large single KML.
    # These deliberately small equivalent files allow direct Web imports while
    # retaining the same absolute-altitude geometry as the master KML.
    web_dir = OUTPUT / "web_chunks"
    web_dir.mkdir(exist_ok=True)
    test_body = ["<Folder><name>3D St Paul’s Heights Grid — 25-cell test</name>" + "".join(grid_placemarks[:25]) + "</Folder>"]
    (web_dir / "st_pauls_heights_web_test_25.kml").write_text(kml_document("St Paul’s Heights — 25-cell Web test", test_body), encoding="utf-8")
    chunk_size = 250
    for start in range(0, len(grid_placemarks), chunk_size):
        number = start // chunk_size + 1
        chunk = grid_placemarks[start:start + chunk_size]
        body = [f"<Folder><name>3D St Paul’s Heights Grid — Web chunk {number}</name>" + "".join(chunk) + "</Folder>"]
        (web_dir / f"st_pauls_heights_web_{number:02d}.kml").write_text(kml_document(f"St Paul’s Heights — Web chunk {number}", body), encoding="utf-8")
    print(json.dumps({"feature_count": len(heights), "min_height": min(heights), "max_height": max(heights), "unique_heights": sorted(set(heights)), "source_downloaded_at": grid["downloaded_at"], "output": str(kml_path)}, indent=2))


if __name__ == "__main__":
    main()
