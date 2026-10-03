#!/usr/bin/env node
/** Export all MySQL planning_zones CAZ geometry as a translucent 30m AOD KML plane. */
import "../bootstrap.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import mysql from "mysql2/promise";

const outputIndex = process.argv.indexOf("--output");
const outputPath = resolve(outputIndex >= 0
  ? process.argv[outputIndex + 1]
  : "/mnt/ngist/public_html/uploads/caz_boundary_30m.kml");
if (outputIndex >= 0 && !process.argv[outputIndex + 1]) {
  throw new Error("Usage: node scripts/export_caz_kml.js [--output /path/to/file.kml]");
}

const xml = (value) => String(value ?? "").replace(/[&<>'"]/g, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&apos;", '"': "&quot;",
}[character]));

function ringKml(ring) {
  return ring.map(([longitude, latitude]) => `${Number(longitude).toFixed(8)},${Number(latitude).toFixed(8)},30.0`).join(" ");
}

function polygonKml(rings) {
  const [outer, ...holes] = rings;
  return `          <Polygon>
            <altitudeMode>absolute</altitudeMode>
            <outerBoundaryIs><LinearRing><coordinates>${ringKml(outer)}</coordinates></LinearRing></outerBoundaryIs>
${holes.map((hole) => `            <innerBoundaryIs><LinearRing><coordinates>${ringKml(hole)}</coordinates></LinearRing></innerBoundaryIs>`).join("\n")}
          </Polygon>`;
}

const connection = await mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: Number(process.env.MYSQL_PORT || 3306),
  database: process.env.MYSQL_DATABASE || process.env.MYSQL_DB,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS,
});

try {
  const [rows] = await connection.query(
    "SELECT id, name, metadata, ST_AsGeoJSON(geom) AS geometry_json FROM planning_zones WHERE type = 'caz' AND geom IS NOT NULL ORDER BY id",
  );
  if (!rows.length) throw new Error("No CAZ geometry records found in planning_zones.");

  const placemarks = rows.map((row) => {
    const geometry = typeof row.geometry_json === "string" ? JSON.parse(row.geometry_json) : row.geometry_json;
    if (geometry.type !== "MultiPolygon") throw new Error(`CAZ row ${row.id} is ${geometry.type}, not MultiPolygon.`);
    const metadata = typeof row.metadata === "string" ? JSON.parse(row.metadata || "{}") : (row.metadata || {});
    const reference = metadata.reference || `CAZ geometry ${row.id}`;
    const label = row.name || reference;
    return `      <Placemark>
        <name>${xml(label)}</name>
        <description>CAZ boundary plane at 30 m AOD. Source row ${row.id}; reference ${xml(reference)}.</description>
        <ExtendedData>
          <Data name="source_table"><value>planning_zones</value></Data>
          <Data name="source_id"><value>${row.id}</value></Data>
          <Data name="reference"><value>${xml(reference)}</value></Data>
          <Data name="elevation_aod_m"><value>30</value></Data>
        </ExtendedData>
        <Style>
          <LineStyle><color>ff00ffff</color><width>2</width></LineStyle>
          <PolyStyle><color>5500ffff</color><outline>1</outline></PolyStyle>
        </Style>
        <MultiGeometry>
${geometry.coordinates.map(polygonKml).join("\n")}
        </MultiGeometry>
      </Placemark>`;
  });

  const kml = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>Central Activities Zone boundary — 30 m AOD</name>
    <description>A translucent horizontal CAZ boundary plane, using the MySQL planning_zones geometry. It is a visual comparison aid, not a building-height assessment.</description>
${placemarks.join("\n")}
  </Document>
</kml>
`;
  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, kml, "utf8");
  console.log(`Wrote ${rows.length} CAZ source geometries to ${outputPath}`);
} finally {
  await connection.end();
}
