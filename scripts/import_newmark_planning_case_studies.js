#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import pg from "pg";

const csvPath = process.argv[2] || "/mnt/ngist/public_html/newmark/case_studies.csv";
const table = "public.newmark_planning_case_studies";
const client = new pg.Client(process.env.DATABASE_URL || {
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
});

function parseCsv(text) {
  const rows = [];
  let row = [];
  let value = "";
  let quoted = false;

  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    const next = text[index + 1];
    if (character === '"' && quoted && next === '"') {
      value += '"';
      index += 1;
    } else if (character === '"') {
      quoted = !quoted;
    } else if (character === "," && !quoted) {
      row.push(value);
      value = "";
    } else if ((character === "\n" || character === "\r") && !quoted) {
      if (character === "\r" && next === "\n") index += 1;
      row.push(value);
      if (row.some((cell) => cell !== "")) rows.push(row);
      row = [];
      value = "";
    } else {
      value += character;
    }
  }
  row.push(value);
  if (row.some((cell) => cell !== "")) rows.push(row);

  const headings = (rows.shift() || []).map((heading) => heading.replace(/^\uFEFF/, "").trim());
  return rows.map((cells) => Object.fromEntries(headings.map((heading, index) => [heading, cells[index] || ""])));
}

function textValue(value) {
  const text = String(value ?? "").trim();
  return text === "" ? null : text;
}

function numberValue(value) {
  const text = textValue(value);
  if (text === null) return null;
  const number = Number(text);
  if (!Number.isFinite(number)) throw new Error(`Invalid numeric CSV value: ${text}`);
  return number;
}

function timestampValue(value) {
  const text = textValue(value);
  if (text === null) return null;
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) throw new Error(`Invalid timestamp CSV value: ${text}`);
  return date.toISOString();
}

const columns = [
  "source_row_number", "site", "source_name", "client", "borough", "location",
  "latitude", "longitude", "geocode_source", "source_file_name", "source_folder",
  "notes", "title", "editorial_strapline", "service_categories", "challenges",
  "our_role", "outcome", "enrichment_source_sha256", "enrichment_model", "enriched_at",
  "image_file",
];
const insertSql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${columns.map((_, index) => `$${index + 1}`).join(", ")})`;

const rows = parseCsv(fs.readFileSync(csvPath, "utf8"));
if (rows.length === 0) throw new Error("CSV contains no case-study rows.");

const values = rows.map((row, index) => [
  index + 1,
  textValue(row.Site),
  textValue(row.Name),
  textValue(row.Client),
  textValue(row.Borough),
  textValue(row.Location),
  numberValue(row.Latitude),
  numberValue(row.Longitude),
  textValue(row["Geocode source"]),
  textValue(row["File name"]),
  textValue(row.Folder),
  textValue(row.Notes),
  textValue(row.Title),
  textValue(row["Editorial strapline"]),
  textValue(row["Service categories"]),
  textValue(row["The Challenges"]),
  textValue(row["Our Role"]),
  textValue(row["The Outcome"]),
  textValue(row["Enrichment source SHA256"]),
  textValue(row["Enrichment model"]),
  timestampValue(row["Enriched at"]),
  textValue(row["Image file"]),
]);

await client.connect();
try {
  await client.query("BEGIN");
  await client.query(`TRUNCATE ${table} RESTART IDENTITY`);
  for (const row of values) await client.query(insertSql, row);
  await client.query("COMMIT");
  console.log(JSON.stringify({ success: true, table, rows: values.length, csvPath }, null, 2));
} catch (error) {
  await client.query("ROLLBACK");
  throw error;
} finally {
  await client.end();
}
