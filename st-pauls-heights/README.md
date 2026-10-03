# St Paul’s Heights Google Earth export

This tool downloads the authoritative City of London **St Paul’s Heights Grid** ArcGIS layer (INSPIRE MapServer layer 95), caches its native EPSG:27700 geometry, and creates an altitude-aware Google Earth KML/KMZ.

The current cached snapshot was downloaded at **2026-09-07T20:51:05.729572+00:00**. Each refresh records its own precise UTC timestamp in the raw cache metadata.

## Files

- `data/st_pauls_heights_grid.arcgis.json` — authoritative native ArcGIS response and download timestamp.
- `data/st_pauls_heights_grid.geojson` — reproducible source-geometry cache in EPSG:27700.
- `output/st_pauls_heights.kml` and `.kmz` — Google Earth outputs.

## Method

The generator paginates ArcGIS queries, retaining `OBJECTID`, `VIEWPOINTS`, `HEIGHT` and polygon geometry. It transforms British National Grid coordinates from EPSG:27700 to WGS84 using PROJ `cs2cs`. Every grid vertex is written with the feature’s `HEIGHT` value and KML `altitudeMode=absolute`; cells are horizontal, semi-transparent orange plates and are not extruded.

`HEIGHT` is metres AOD/ODN. For visualisation, it is passed straight to Google Earth as an absolute altitude. This assumes the ODN/AOD to Google Earth effective vertical-datum difference is sufficiently small for visual comparison. Do **not** add terrain elevation: these are absolute levels, not heights above ground.

The tool also fetches switchable context folders from the dedicated St Paul’s Heights service: setbacks, policy-area boundary and policy area. They are clamped to ground; the 3D grid is the operative output. The points layer is intentionally omitted to keep the display readable.

Grid plates use a shared five-band orange gradient: pale orange (up to 20 m AOD), light orange (20–30 m), orange (30–40 m), deep orange (40–50 m), and burnt orange (over 50 m).

## Run

```bash
cd /opt/scraper/st-pauls-heights
python3 generate.py --refresh
```

Later runs reuse the cached grid. Use `--refresh` to obtain a fresh authoritative snapshot, or `--no-context` for the grid only. Open `output/st_pauls_heights.kmz` (or `.kml`) in Google Earth Pro.

For Google Earth Web, first import `output/web_chunks/st_pauls_heights_web_test_25.kml`. It has the same absolute-altitude geometry but only 25 cells. If it renders correctly, import the 13 numbered 250-cell chunk files separately; this avoids the Web importer’s handling of a large single KML.

The source cache records the precise UTC download date/time and service URL for each retrieval.
