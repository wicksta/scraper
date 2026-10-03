#!/usr/bin/env node
import "../bootstrap.js";
import { mkdir, writeFile } from "node:fs/promises";
import JSZip from "jszip";
import pg from "pg";

const outputDir = "/mnt/ngist/public_html/lvmf/exports";
const baseName = "lvmf_2026_protected_vistas";
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;" })[char]);
const styleForArea = (code) => ({ vc: "vc", llaa: "laa", rlaa: "laa", baa: "baa" })[code] || "vc";
const polygonKml = (vertices) => `<Polygon><altitudeMode>absolute</altitudeMode><tessellate>0</tessellate><outerBoundaryIs><LinearRing><coordinates>${vertices.map((vertex) => `${vertex.longitude},${vertex.latitude},${vertex.height_m_aod}`).join(" ")}</coordinates></LinearRing></outerBoundaryIs></Polygon>`;
const client = new pg.Client(process.env.DATABASE_URL || { host: process.env.PGHOST, port: Number(process.env.PGPORT || 5432), database: process.env.PGDATABASE, user: process.env.PGUSER, password: process.env.PGPASSWORD });

try {
  await client.connect();
  const dataset = await client.query("SELECT id FROM lvmf_2026_datasets WHERE version_label='2026-consultation-draft'");
  if (dataset.rowCount !== 1) throw new Error("LVMF 2026 consultation-draft dataset is not available.");
  const id = dataset.rows[0].id;
  const [areas, points, lines] = await Promise.all([
    client.query(`SELECT v.view_code,v.view_name,a.area_code,a.display_name,
      (SELECT jsonb_agg(jsonb_build_object('longitude',ST_X(ST_Transform(ST_Force2D(d.geom),4326)),'latitude',ST_Y(ST_Transform(ST_Force2D(d.geom),4326)),'height_m_aod',COALESCE(cp.height_m_aod,0)) ORDER BY d.path)
       FROM ST_DumpPoints(a.geom) d
       LEFT JOIN LATERAL (SELECT p.height_m_aod FROM lvmf_2026_control_points p WHERE p.view_id=a.view_id AND ST_Equals(ST_Force2D(p.geom),d.geom) LIMIT 1) cp ON true) vertices
      FROM lvmf_2026_areas a JOIN lvmf_2026_views v ON v.id=a.view_id WHERE v.dataset_id=$1 ORDER BY v.view_code,a.area_code`, [id]),
    client.query("SELECT v.view_code,v.view_name,p.point_code,p.height_m_aod,(v.view_code='5B' AND p.point_code IN ('l','m','n','o','p','q','r','s','t')) intermediate,ST_AsKML(ST_Transform(p.geom,4326),7) kml FROM lvmf_2026_control_points p JOIN lvmf_2026_views v ON v.id=p.view_id WHERE v.dataset_id=$1 ORDER BY v.view_code,p.point_code", [id]),
    client.query("SELECT v.view_code,l.line_code,l.display_name,ST_AsKML(ST_Transform(l.geom,4326),7) kml FROM lvmf_2026_control_lines l JOIN lvmf_2026_views v ON v.id=l.view_id WHERE v.dataset_id=$1 ORDER BY v.view_code,l.line_code", [id]),
  ]);
  const placemark = (name, style, description, geometry) => `<Placemark><name>${esc(name)}</name><styleUrl>#${style}</styleUrl><description><![CDATA[${description}]]></description>${geometry}</Placemark>`;
  const document = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Document><name>LVMF 2026 Protected Vistas</name><description><![CDATA[PostGIS export of the LVMF 2026 consultation-draft Protected Vista geometry. Source: Appendix E.]]></description>
<Style id="vc"><LineStyle><color>ff2828c6</color><width>2</width></LineStyle><PolyStyle><color>402828c6</color></PolyStyle></Style><Style id="laa"><LineStyle><color>ff0095e6</color><width>2</width></LineStyle><PolyStyle><color>400095e6</color></PolyStyle></Style><Style id="baa"><LineStyle><color>ff005566</color><width>2</width></LineStyle><PolyStyle><color>40005566</color></PolyStyle></Style><Style id="control"><IconStyle><color>ff832c5b</color><scale>0.8</scale></IconStyle></Style><Style id="intermediate"><IconStyle><color>ff6e760f</color><scale>1.1</scale></IconStyle></Style><Style id="line"><LineStyle><color>ff832c5b</color><width>2</width></LineStyle></Style>
<Folder><name>Protected Vista areas (3D AOD planes)</name>${areas.rows.map((row) => { const heights=row.vertices.map((vertex) => `${Number(vertex.height_m_aod).toFixed(1)} m AOD`).join(", "); return placemark(`${row.view_code} — ${row.view_name} — ${row.display_name}`, styleForArea(row.area_code), `<strong>${esc(row.view_code)} — ${esc(row.view_name)}</strong><br>${esc(row.display_name)}<br>Area type: ${esc(row.area_code)}<br><strong>Polygon vertex heights:</strong> ${esc(heights)}<br>Geometry uses absolute AOD altitudes at the source control vertices.`, polygonKml(row.vertices)); }).join("")}</Folder>
<Folder><name>Protected Vista control points</name>${points.rows.map((row) => placemark(`${row.view_code} ${row.point_code}`, row.intermediate ? "intermediate" : "control", `<strong>${esc(row.view_code)} — ${esc(row.view_name)}</strong><br>Control point ${esc(row.point_code)}${row.intermediate ? " (5B intermediate control)" : ""}<br>${esc(row.height_m_aod)} m AOD`, row.kml)).join("")}</Folder>
<Folder><name>5B intermediate control lines</name>${lines.rows.map((row) => placemark(`${row.view_code} — ${row.display_name}`, "line", `${esc(row.view_code)} — ${esc(row.display_name)}`, row.kml)).join("")}</Folder>
</Document></kml>`;
  await mkdir(outputDir, { recursive: true });
  await writeFile(`${outputDir}/${baseName}.kml`, document);
  const zip = new JSZip(); zip.file("doc.kml", document);
  await writeFile(`${outputDir}/${baseName}.kmz`, await zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE" }));
  console.log(JSON.stringify({ success: true, kml: `${outputDir}/${baseName}.kml`, kmz: `${outputDir}/${baseName}.kmz`, areas: areas.rowCount, control_points: points.rowCount, control_lines: lines.rowCount }, null, 2));
} finally { await client.end(); }
