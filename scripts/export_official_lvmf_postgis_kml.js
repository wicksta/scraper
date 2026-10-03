#!/usr/bin/env node
/**
 * Convert the supplied official MapInfo protected-vistas layer to WGS84 KML.
 * OGR reads MapInfo; PostGIS performs the EPSG:27700 -> EPSG:4326 transform.
 * No database objects are created.
 */
import "../bootstrap.js";
import { execFile } from "node:child_process";
import { writeFileSync } from "node:fs";
import { promisify } from "node:util";
import pg from "pg";

const source = "/mnt/ngist/public_html/uploads/Protected Vistas/Protected Vistas LVMF 2010.TAB";
const output = "/mnt/ngist/public_html/uploads/Protected Vistas/Protected Vistas LVMF 2010_postgis.kml";
const execFileAsync = promisify(execFile);
const xml = (value) => String(value ?? "").replace(/[&<>'"]/g, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&apos;", '"': "&quot;",
}[character]));

const { stdout } = await execFileAsync("ogr2ogr", ["-f", "GeoJSON", "/vsistdout/", source]);
const collection = JSON.parse(stdout);
if (!Array.isArray(collection.features) || !collection.features.length) throw new Error("The MapInfo layer contains no features.");

const client = new pg.Client(process.env.DATABASE_URL || {
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
});
await client.connect();
try {
  const placemarks = [];
  for (const feature of collection.features) {
    const { rows } = await client.query(
      "SELECT ST_AsKML(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($1), 27700), 4326), 8) AS kml",
      [JSON.stringify(feature.geometry)],
    );
    const properties = feature.properties || {};
    const type = properties.Type || "LVMF area";
    const name = [properties.View, type, properties.Description].filter(Boolean).join(" — ");
    placemarks.push(`      <Placemark>
        <name>${xml(name)}</name>
        <description>${xml(properties.Comments || "Official LVMF MapInfo geometry transformed by PostGIS from EPSG:27700 to WGS84.")}</description>
        <ExtendedData>
          <Data name="view"><value>${xml(properties.View)}</value></Data>
          <Data name="type"><value>${xml(type)}</value></Data>
          <Data name="height"><value>${xml(properties.Height)}</value></Data>
          <Data name="source"><value>${xml(properties.Source)}</value></Data>
        </ExtendedData>
        <Style>
          <LineStyle><color>${type === "PV" ? "ff0000ff" : "ff00ffff"}</color><width>2</width></LineStyle>
          <PolyStyle><color>${type === "PV" ? "550000ff" : "5500ffff"}</color><outline>1</outline></PolyStyle>
        </Style>
        ${rows[0].kml}
      </Placemark>`);
  }
  writeFileSync(output, `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>Official LVMF Protected Vistas — PostGIS transform</name>
    <description>Official July 2010 MapInfo geometry converted from British National Grid (EPSG:27700) to WGS84 by PostGIS. Geometry is clamped to ground to facilitate horizontal-projection comparison.</description>
${placemarks.join("\n")}
  </Document>
</kml>
`, "utf8");
  console.log(`Wrote ${placemarks.length} official LVMF features to ${output}`);
} finally {
  await client.end();
}
