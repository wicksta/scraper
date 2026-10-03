#!/usr/bin/env node
import "../bootstrap.js";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import mysql from "mysql2/promise";

const sourcePath = "/mnt/ngist/public_html/lvmf/LVMF_2012_viewing_corridor_geometry.csv";
function parseLine(line) {
  const values = []; let value = ""; let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') { if (quoted && line[index + 1] === '"') { value += char; index += 1; } else quoted = !quoted; }
    else if (char === "," && !quoted) { values.push(value); value = ""; } else value += char;
  }
  values.push(value); return values;
}
const raw = readFileSync(sourcePath);
const lines = raw.toString("utf8").replace(/^\uFEFF/, "").trim().split(/\r?\n/);
const headers = parseLine(lines.shift());
const rows = lines.map((line) => Object.fromEntries(parseLine(line).map((value, index) => [headers[index], value])));
const number = (value) => value === "" ? null : Number(value);
function calculation(viewRef, corridorCode) {
  if (viewRef === "5A.2" && corridorCode === "VC1") return { model: "axis_curvature_refraction", endpoint_a: "a", endpoint_b: "m", curvature_coefficient: 0.0673, source_basis: "CSV point roles: viewpoint/apex to defining point at Tower Bridge" };
  if (viewRef === "5A.2" && corridorCode === "VC2") return { model: "polygon_control_surface", polygon_vertices: ["n", "c", "d", "o"], defining_point: "b", source_basis: "CSV polygon controls; n/o intentionally carry a different AOD from VC1" };
  return { model: "axis_curvature_refraction", endpoint_a: "a", endpoint_b: "b", curvature_coefficient: 0.0673, source_basis: "2012 legacy viewer convention" };
}
const connection = await mysql.createConnection({ host: process.env.MYSQL_HOST, port: Number(process.env.MYSQL_PORT || 3306), database: process.env.MYSQL_DATABASE || process.env.MYSQL_DB, user: process.env.MYSQL_USER, password: process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS });
try {
  await connection.beginTransaction();
  const sha = createHash("sha256").update(raw).digest("hex");
  await connection.execute("INSERT INTO lvmf_2012_geometry_datasets(source_name,source_sha256,source_path) VALUES(?,?,?) ON DUPLICATE KEY UPDATE source_sha256=VALUES(source_sha256),source_path=VALUES(source_path),imported_at=CURRENT_TIMESTAMP", ["LVMF 2012 viewing corridor geometry", sha, sourcePath]);
  const [[dataset]] = await connection.execute("SELECT id FROM lvmf_2012_geometry_datasets WHERE source_name=?", ["LVMF 2012 viewing corridor geometry"]);
  await connection.execute("DELETE FROM lvmf_2012_corridors WHERE dataset_id=?", [dataset.id]);
  const grouped = new Map();
  for (const row of rows) { const key = `${row.assessment_point}|${row.corridor}`; if (!grouped.has(key)) grouped.set(key, []); grouped.get(key).push(row); }
  for (const group of grouped.values()) {
    const first = group[0]; const definition = calculation(first.assessment_point, first.corridor);
    const [inserted] = await connection.execute("INSERT INTO lvmf_2012_corridors(dataset_id,view_ref,corridor_code,from_location,to_landmark,corridor_length_m,width_at_landmark_m,source_pdf_page,calculation_model,calculation_definition) VALUES(?,?,?,?,?,?,?,?,?,?)", [dataset.id, first.assessment_point, first.corridor, first.from_location, first.to_landmark, number(first.corridor_length_m), number(first.width_at_landmark_m), number(first.source_pdf_page), definition.model, JSON.stringify(definition)]);
    for (const point of group) await connection.execute("INSERT INTO lvmf_2012_corridor_control_points(corridor_id,point_label,point_role,polygon_order,easting,northing,height_m_aod,source_pdf_page) VALUES(?,?,?,?,?,?,?,?)", [inserted.insertId, point.point_label, point.point_role, number(point.polygon_order), number(point.easting), number(point.northing), number(point.height_mAOD), number(point.source_pdf_page)]);
  }
  await connection.commit();
  console.log(JSON.stringify({ success: true, source_rows: rows.length, corridors: grouped.size, source_sha256: sha }, null, 2));
} catch (error) { await connection.rollback(); throw error; } finally { await connection.end(); }
