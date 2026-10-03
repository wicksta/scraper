#!/usr/bin/env node
import "../bootstrap.js";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import pg from "pg";

const root = "/mnt/ngist/public_html/lvmf";
const appendixE = `${root}/all_views.csv`;
const appendixC = `${root}/LVMF_assessment_points_and_paths.csv`;
const version = "2026-consultation-draft";

function parseLine(line) {
  const out = []; let value = ""; let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') { if (quoted && line[i + 1] === '"') { value += char; i += 1; } else quoted = !quoted; }
    else if (char === "," && !quoted) { out.push(value); value = ""; } else value += char;
  }
  out.push(value); return out;
}
function csv(path) {
  const lines = readFileSync(path, "utf8").trim().split(/\r?\n/); const headers = parseLine(lines.shift());
  return lines.map((line) => Object.fromEntries(parseLine(line).map((value, i) => [headers[i], value])));
}
const numeric = (value) => value === "" || value == null ? null : Number(value);
const sha256 = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");
const eRows = csv(appendixE); const cRows = csv(appendixC);
const client = new pg.Client(process.env.DATABASE_URL || { host: process.env.PGHOST, port: Number(process.env.PGPORT || 5432), database: process.env.PGDATABASE, user: process.env.PGUSER, password: process.env.PGPASSWORD });
await client.connect();
try {
  await client.query("BEGIN");
  const meta = { appendix_e_csv: appendixE, appendix_c_csv: appendixC, appendix_e_sha256: sha256(appendixE), appendix_c_sha256: sha256(appendixC), source_pdfs: ["C Assessment Points and Assessment Paths.pdf", "D Photography details.pdf", "LVMF.pdf", "F Curvature of the Earth Compensation.pdf"] };
  const dataset = await client.query(`INSERT INTO lvmf_2026_datasets (version_label, source_metadata) VALUES ($1,$2::jsonb) ON CONFLICT (version_label) DO UPDATE SET source_metadata=EXCLUDED.source_metadata RETURNING id`, [version, JSON.stringify(meta)]);
  const datasetId = dataset.rows[0].id;
  // Delete dependent plan objects explicitly before source controls. This keeps
  // reloading idempotent despite area-vertex references to control points.
  await client.query("DELETE FROM lvmf_2026_areas WHERE view_id IN (SELECT id FROM lvmf_2026_views WHERE dataset_id=$1)", [datasetId]);
  await client.query("DELETE FROM lvmf_2026_control_lines WHERE view_id IN (SELECT id FROM lvmf_2026_views WHERE dataset_id=$1)", [datasetId]);
  await client.query("DELETE FROM lvmf_2026_views WHERE dataset_id=$1", [datasetId]);
  await client.query("DELETE FROM lvmf_2026_assessment_paths WHERE dataset_id=$1", [datasetId]);
  const views = new Map();
  for (const row of eRows) {
    if (!views.has(row.view_id)) {
      const inserted = await client.query(`INSERT INTO lvmf_2026_views (dataset_id,view_code,view_name,source_page,geometry_template) VALUES ($1,$2,$3,$4,$5) RETURNING id`, [datasetId, row.view_id, row.view_name, numeric(row.source_page), row.view_id === "5B" ? "protected_vista_extended" : "protected_vista"]);
      views.set(row.view_id, inserted.rows[0].id);
    }
    const viewId = views.get(row.view_id);
    const height = numeric(row.height_m_aod);
    await client.query(`INSERT INTO lvmf_2026_control_points (view_id,point_code,height_m_aod,geom,source_page) VALUES ($1,$2,$3,ST_SetSRID(ST_MakePoint($4,$5,$6),27700),$7)`, [viewId, row.point, height, numeric(row.easting_m), numeric(row.northing_m), height, numeric(row.source_page)]);
  }
  const pointId = new Map();
  const { rows: points } = await client.query(`SELECT p.id,v.view_code,p.point_code,ST_X(p.geom) x,ST_Y(p.geom) y FROM lvmf_2026_control_points p JOIN lvmf_2026_views v ON v.id=p.view_id WHERE v.dataset_id=$1`, [datasetId]);
  for (const p of points) pointId.set(`${p.view_code}:${p.point_code}`, p);
  // Appendix E plan footprints: d is a control/interpolation point, not a
  // corridor boundary.  The viewing corridor is a-c-e; lateral areas are
  // a-b-c and a-e-f respectively.
  const areaSpecs = [
    ["vc", "Viewing Corridor", ["a", "c", "e"]], ["llaa", "Left Landmark Assessment Area", ["a", "b", "c"]],
    ["rlaa", "Right Landmark Assessment Area", ["a", "e", "f"]], ["baa", "Background Assessment Area", ["v", "x", "y", "z", "w"]],
  ];
  for (const [viewCode, viewId] of views) for (const [code, name, letters] of areaSpecs) {
    const vertices = letters.map((letter) => pointId.get(`${viewCode}:${letter}`));
    if (vertices.some((p) => !p)) continue;
    const wkt = `POLYGON((${[...vertices, vertices[0]].map((p) => `${p.x} ${p.y}`).join(",")}))`;
    // Appendix E retains every source control in vertex order.  A small number
    // of BAA schedules contain an intermediate far-edge control inside the
    // outer boundary; the displayed plan footprint is therefore its convex hull.
    const geomExpression = code === "baa" ? "ST_ConvexHull(ST_GeomFromText($4,27700))" : "ST_GeomFromText($4,27700)";
    const area = await client.query(`INSERT INTO lvmf_2026_areas (view_id,area_code,display_name,geom) VALUES ($1,$2,$3,${geomExpression}) RETURNING id`, [viewId, code, name, wkt]);
    for (const [order, point] of vertices.entries()) await client.query(`INSERT INTO lvmf_2026_area_vertices (area_id,vertex_order,control_point_id) VALUES ($1,$2,$3)`, [area.rows[0].id, order + 1, point.id]);
    const rule = code === "vc" ? "curvature_refraction" : code === "baa" ? "source_control_surface" : "similar_triangles";
    await client.query(`INSERT INTO lvmf_2026_threshold_rules (view_id,area_code,rule_code,curvature_coefficient,source_reference) VALUES ($1,$2,$3,$4,$5)`, [viewId, code, rule, code === "vc" ? 0.0673 : null, code === "vc" ? "Appendix F paragraphs 681-687; Appendix E a/d" : "Appendix E source controls"]);
  }
  const specialLines = [["5B", "intermediate_ground", "5B intermediate ground control", ["l", "m", "n", "o", "p"]], ["5B", "intermediate_threshold", "5B intermediate threshold control", ["q", "r", "s", "t"]]];
  for (const [viewCode, code, name, letters] of specialLines) {
    const vertices = letters.map((letter) => pointId.get(`${viewCode}:${letter}`)); if (vertices.some((p) => !p)) continue;
    await client.query(`INSERT INTO lvmf_2026_control_lines (view_id,line_code,display_name,geom) VALUES ($1,$2,$3,ST_GeomFromText($4,27700))`, [views.get(viewCode), code, name, `LINESTRING(${vertices.map((p) => `${p.x} ${p.y}`).join(",")})`]);
  }
  for (const row of cRows) {
    const inserted = await client.query(`INSERT INTO lvmf_2026_assessment_paths (dataset_id,view_code,view_name,view_type,source_page) VALUES ($1,$2,$3,$4,$5) RETURNING id`, [datasetId, row.view_id, row.view_name, row.view_type, numeric(row.source_pdf_page)]);
    const fields = [["start", 1], ["representative", 2], ["representative_2", 3], ["intermediate", 4], ["end", 5]];
    for (const [role, order] of fields) {
      const east = numeric(row[`${role}_easting_m`]); const north = numeric(row[`${role}_northing_m`]); const height = numeric(row[`${role}_height_m_aod`]);
      if (east == null || north == null || height == null) continue;
      await client.query(`INSERT INTO lvmf_2026_assessment_path_points (assessment_path_id,point_role,point_order,height_m_aod,geom) VALUES ($1,$2,$3,$4,ST_SetSRID(ST_MakePoint($5,$6,$7),27700))`, [inserted.rows[0].id, role, order, height, east, north, height]);
    }
  }
  await client.query("COMMIT");
  console.log(JSON.stringify({ success: true, version, appendix_e_rows: eRows.length, appendix_c_rows: cRows.length, protected_vistas: views.size }, null, 2));
} catch (error) { await client.query("ROLLBACK"); throw error; } finally { await client.end(); }
