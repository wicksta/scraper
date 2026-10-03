#!/usr/bin/env node
/**
 * Export the LVMF geometry used by the nGISt viewer as a single KML file.
 *
 * The output groups the same areas drawn in lvmf1.php:
 *   - protected vista / foreground (A-C-D)
 *   - the two WSCA side areas (A-V-C and A-W-D), where supplied
 *   - background assessment area (V-W-Z-Y, or C-D-Z-Y where V/W are absent)
 */
import "../bootstrap.js";
import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import mysql from "mysql2/promise";

const defaultOutput = "/mnt/ngist/private/data/lvmf_kml/lvmf_viewer_areas.kml";
const outputIndex = process.argv.indexOf("--output");
const viewRefIndex = process.argv.indexOf("--view-ref");
const outputPath = resolve(outputIndex >= 0 ? process.argv[outputIndex + 1] : defaultOutput);
const viewRef = viewRefIndex >= 0 ? process.argv[viewRefIndex + 1] : null;

if ((outputIndex >= 0 && !process.argv[outputIndex + 1]) || (viewRefIndex >= 0 && !viewRef)) {
  throw new Error("Usage: node scripts/export_lvmf_kml.js [--view-ref 23A.1] [--output /path/to/file.kml]");
}

const xml = (value) => String(value ?? "").replace(/[&<>'"]/g, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&apos;", '"': "&quot;",
}[character]));

function point(row, letter) {
  const rawValues = [row[`${letter}Easting`], row[`${letter}Northing`], row[`${letter}Aod`]];
  // MySQL returns blank optional V/W coordinates as an empty string. Number("")
  // is zero in JavaScript, which would place a spurious vertex off south-west England.
  if (rawValues.some((value) => value == null || String(value).trim() === "")) return null;
  const [easting, northing, altitude] = rawValues.map(Number);
  return Number.isFinite(easting) && Number.isFinite(northing) && Number.isFinite(altitude)
    ? { easting, northing, altitude }
    : null;
}

async function toWgs84(points) {
  const input = points.map(({ easting, northing, altitude }) => `${easting} ${northing} ${altitude}`).join("\n");
  const stdout = await new Promise((resolveOutput, reject) => {
    const process = spawn("cs2cs", ["-f", "%.8f", "EPSG:27700", "EPSG:4326"]);
    let output = "";
    let errorOutput = "";
    process.stdout.on("data", (chunk) => { output += chunk; });
    process.stderr.on("data", (chunk) => { errorOutput += chunk; });
    process.on("error", reject);
    process.on("close", (code) => code === 0 ? resolveOutput(output) : reject(new Error(`cs2cs failed: ${errorOutput}`)));
    process.stdin.end(input);
  });
  const converted = stdout.trim().split(/\r?\n/).map((line) => line.trim().split(/\s+/).map(Number));
  if (converted.length !== points.length || converted.some((values) => values.length < 3 || values.some(Number.isNaN))) {
    throw new Error("Unable to convert all British National Grid coordinates to WGS84.");
  }
  // cs2cs emits latitude, longitude, height; KML requires longitude, latitude, height.
  return converted.map(([latitude, longitude, altitude]) => ({ latitude, longitude, altitude }));
}

function placemark(area, row, coordinates, colour) {
  const ring = [...coordinates, coordinates[0]]
    .map(({ longitude, latitude, altitude }) => `${longitude.toFixed(8)},${latitude.toFixed(8)},${altitude.toFixed(1)}`)
    .join(" ");
  return `      <Placemark>
        <name>${xml(row.viewRef)} — ${xml(area)}</name>
        <description>${xml(row.viewName)}. Geometry exported from the nGISt LVMF viewer dataset.</description>
        <ExtendedData>
          <Data name="view_ref"><value>${xml(row.viewRef)}</value></Data>
          <Data name="view_name"><value>${xml(row.viewName)}</value></Data>
          <Data name="area_type"><value>${xml(area)}</value></Data>
        </ExtendedData>
        <Style>
          <LineStyle><color>${colour.line}</color><width>2</width></LineStyle>
          <PolyStyle><color>${colour.fill}</color><outline>1</outline></PolyStyle>
        </Style>
        <Polygon>
          <altitudeMode>absolute</altitudeMode>
          <outerBoundaryIs><LinearRing><coordinates>${ring}</coordinates></LinearRing></outerBoundaryIs>
        </Polygon>
      </Placemark>`;
}

const areas = [
  { name: "Protected Vista / foreground", points: ["a", "c", "d"], colour: { line: "ff0000ff", fill: "550000ff" } },
  { name: "WSCA side area A", points: ["a", "v", "c"], colour: { line: "ff00a5ff", fill: "5500a5ff" } },
  { name: "WSCA side area B", points: ["a", "w", "d"], colour: { line: "ff00a5ff", fill: "5500a5ff" } },
  { name: "Background assessment area", points: ["v", "w", "z", "y"], fallbackPoints: ["c", "d", "z", "y"], colour: { line: "ffff8800", fill: "55888800" } },
];

const connection = await mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: Number(process.env.MYSQL_PORT || 3306),
  database: process.env.MYSQL_DATABASE || process.env.MYSQL_DB,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS,
});

try {
  const [rows] = await connection.query(
    `SELECT * FROM lvmf ${viewRef ? "WHERE viewRef = ?" : ""} ORDER BY viewRef`,
    viewRef ? [viewRef] : [],
  );
  if (viewRef && !rows.length) throw new Error(`No LVMF record found for ${viewRef}.`);
  const folders = new Map(areas.map((area) => [area.name, []]));

  for (const row of rows) {
    for (const area of areas) {
      let vertices = area.points.map((letter) => point(row, letter));
      if (vertices.some((vertex) => !vertex) && area.fallbackPoints) {
        vertices = area.fallbackPoints.map((letter) => point(row, letter));
      }
      if (vertices.some((vertex) => !vertex)) continue;
      folders.get(area.name).push(placemark(area.name, row, await toWgs84(vertices), area.colour));
    }
  }

  const kml = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>LVMF viewer areas</name>
    <description>Areas exported from the nGISt LVMF database. This is a screening aid only; use the current LVMF guidance and relevant management plan for formal assessment.</description>
${[...folders.entries()].map(([name, marks]) => `    <Folder>
      <name>${xml(name)}</name>
${marks.join("\n")}
    </Folder>`).join("\n")}
  </Document>
</kml>
`;
  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, kml, "utf8");
  console.log(`Wrote ${rows.length} views to ${outputPath}`);
  for (const [name, marks] of folders) console.log(`${name}: ${marks.length} polygons`);
} finally {
  await connection.end();
}
