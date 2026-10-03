#!/usr/bin/env node
/**
 * Harvest Westminster /ADFULL and /ADLBC covering letters for condition-
 * discharge drafting examples.  Stores full text + one document embedding;
 * deliberately does not create chunks.
 */
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import pg from "pg";
import { chromium } from "playwright";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;
const DOCS_BASE = "https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=";
const CONTROLLED_TOPICS = [
  "materials", "windows_doors", "landscaping", "archaeology", "construction_logistics",
  "servicing_waste", "transport_cycle_parking", "noise", "energy_sustainability", "drainage",
  "ecology_bng", "lighting", "contamination", "fire_safety", "heritage_details", "other",
];

const argv = yargs(hideBin(process.argv))
  .scriptName("harvest-wcc-condition-discharge-letters")
  .option("limit", { type: "number", default: 100, describe: "Target number of newly ingested letters." })
  .option("candidate-limit", { type: "number", default: 500, describe: "Maximum applications to inspect." })
  .option("offset", { type: "number", default: 0 })
  .option("ons-code", { type: "string", default: "E09000033" })
  .option("artifacts-dir", { type: "string", default: "./artifacts/wcc_condition_discharge_letters" })
  .option("output-json", { type: "string", default: "./tmp/wcc_condition_discharge_harvest.json" })
  .option("timeout-ms", { type: "number", default: 60000 })
  .option("headed", { type: "boolean", default: false })
  .option("apply", { type: "boolean", default: false, describe: "Persist documents. Default is dry-run." })
  .strict().help().argv;

function pgConfig() {
  return process.env.DATABASE_URL ? { connectionString: process.env.DATABASE_URL } : {
    host: process.env.PGHOST, port: Number(process.env.PGPORT || 5432), database: process.env.PGDATABASE,
    user: process.env.PGUSER, password: process.env.PGPASSWORD,
  };
}
function log(event, payload = {}) { process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`); }
function text(value) { return String(value ?? "").replace(/\s+/g, " ").trim(); }
function sha(value) { return crypto.createHash("sha256").update(value).digest("hex"); }
function vectorLiteral(v) { return `[${v.map((x) => Number(x).toString()).join(",")}]`; }
function absolute(base, href) { try { return new URL(href, base).toString(); } catch { return null; } }
function mkdirFor(file) { fs.mkdirSync(path.dirname(path.resolve(file)), { recursive: true }); }

const CANDIDATES_SQL = `
  SELECT a.ons_code, a.reference, a.keyval, a.agent_company_name, a.address, a.application_type,
         a.application_validated, a.application_received, a.source_url, a.lat, a.lon
  FROM public.applications a
  WHERE a.ons_code = $1
    AND NULLIF(BTRIM(a.keyval), '') IS NOT NULL
    AND (a.reference ILIKE '%/ADFULL' OR a.reference ILIKE '%/ADLBC')
    AND (a.agent_company_name ILIKE '%newmark%' OR a.agent_company_name ILIKE '%gerald eve%')
  ORDER BY COALESCE(a.application_validated, a.application_received, a.scraped_at) DESC NULLS LAST, a.reference DESC
  LIMIT $2 OFFSET $3`;

function classifyCover(row) {
  const hay = `${text(row.document_type)} ${text(row.description)}`.toLowerCase();
  return /\bcover(?:ing)?\s+letter\b|\bcovering\b/.test(hay) && !/acknowledg|receipt|council/.test(hay);
}
async function parseDocs(page) {
  return page.$eval("#Documents", (table) => Array.from(table.querySelectorAll("tbody tr")).flatMap((row) => {
    const cells = Array.from(row.querySelectorAll("td")); if (cells.length < 4) return [];
    const link = cells.at(-1)?.querySelector("a")?.getAttribute("href"); if (!link) return [];
    return [{ date_published: cells.at(-4)?.textContent?.trim() || null, document_type: cells.at(-3)?.textContent?.trim() || null,
      description: cells.at(-2)?.textContent?.trim() || null, href: link }];
  }));
}
function pdfText(pdf) {
  return new Promise((resolve, reject) => {
    const child = spawn("pdftotext", [pdf, "-"], { stdio: ["ignore", "pipe", "pipe"] }); let out = ""; let err = "";
    child.stdout.on("data", (b) => { out += b; }); child.stderr.on("data", (b) => { err += b; });
    child.on("error", reject); child.on("close", (code) => code === 0 ? resolve(out) : reject(new Error(err || `pdftotext exited ${code}`)));
  });
}
async function openAiJson(system, user) {
  const key = String(process.env.OPENAI_API_KEY || "").trim(); if (!key) throw new Error("OPENAI_API_KEY is not set");
  const base = String(process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").replace(/\/+$/, "");
  const response = await fetch(`${base}/chat/completions`, { method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` },
    body: JSON.stringify({ model: "gpt-4.1-mini", temperature: 0, response_format: { type: "json_object" }, messages: [{ role: "system", content: system }, { role: "user", content: user }] }) });
  if (!response.ok) throw new Error(`OpenAI classification HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
  return JSON.parse(responseJsonText(await response.json()));
}
function responseJsonText(payload) { return String(payload?.choices?.[0]?.message?.content || "").replace(/^```json\s*|\s*```$/gi, ""); }
async function embed(input) {
  const key = String(process.env.OPENAI_API_KEY || "").trim(); if (!key) throw new Error("OPENAI_API_KEY is not set");
  const base = String(process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").replace(/\/+$/, "");
  const response = await fetch(`${base}/embeddings`, { method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` }, body: JSON.stringify({ model: "text-embedding-3-small", input: input.slice(0, 30000) }) });
  if (!response.ok) throw new Error(`OpenAI embedding HTTP ${response.status}`); const body = await response.json(); return body?.data?.[0]?.embedding || null;
}
async function classify(textValue) {
  const result = await openAiJson(
    "Classify UK planning covering letters. Return strict JSON only; do not invent facts.",
    `Is this a submission seeking approval/discharge of planning or listed-building conditions? Return JSON with:\n` +
    `eligible_for_examples:boolean, parent_permission_reference:string|null, development_description:string|null, condition_numbers:string[], condition_summary:string|null, controlled_topics:string[], free_tags:string[], confidence:high|medium|low.\n` +
    `controlled_topics must use only: ${CONTROLLED_TOPICS.join(", ")}.\n\nLETTER:\n${textValue.slice(0, 50000)}`,
  );
  const topics = Array.from(new Set((result.controlled_topics || []).map((x) => String(x).trim()).filter((x) => CONTROLLED_TOPICS.includes(x))));
  return { eligible_for_examples: result.eligible_for_examples === true, parent_permission_reference: text(result.parent_permission_reference) || null,
    development_description: text(result.development_description) || null, condition_numbers: (result.condition_numbers || []).map(text).filter(Boolean),
    condition_summary: text(result.condition_summary) || null, controlled_topics: topics.length ? topics : ["other"], free_tags: (result.free_tags || []).map(text).filter(Boolean).slice(0, 12),
    confidence: ["high", "medium", "low"].includes(result.confidence) ? result.confidence : "low", model: "gpt-4.1-mini", prompt_version: "wcc-condition-discharge-v1", classified_at: new Date().toISOString() };
}
async function exists(client, sourceUrl) { const r = await client.query("SELECT id FROM public.documents WHERE meta->>'source_doc_url' = $1 LIMIT 1", [sourceUrl]); return r.rows[0]?.id || null; }
async function insert(client, row) {
  const q = `INSERT INTO public.documents (source_file,sha256,bytes,mime_type,title,application_ref,document_type,local_authority,originator,meta,provenance,full_text,doc_vec,token_count,lpa_code,original_filename,site_point)
    VALUES ($1,$2,$3,$4,$5,$6,'cover_letter','Westminster City Council',$7,$8::jsonb,$9::jsonb,$10,$11::vector,$12,$13,$14,CASE WHEN $15::double precision IS NULL OR $16::double precision IS NULL THEN NULL ELSE ST_SetSRID(ST_MakePoint($15,$16),4326) END)
    ON CONFLICT (sha256) WHERE sha256 IS NOT NULL DO UPDATE SET meta=EXCLUDED.meta,provenance=EXCLUDED.provenance,full_text=EXCLUDED.full_text,doc_vec=EXCLUDED.doc_vec,updated_at=now() RETURNING id::text`;
  const values = [row.pdfPath,row.sha256,row.bytes,"application/pdf",`Condition discharge cover letter - ${row.app.reference}`,row.app.reference,row.app.agent_company_name,row.meta,row.provenance,row.fullText,vectorLiteral(row.embedding),row.tokenCount,row.app.ons_code,path.basename(row.pdfPath),row.app.lon,row.app.lat];
  return (await client.query(q, values)).rows[0]?.id || null;
}

async function main() {
  const client = new Client(pgConfig()); await client.connect(); let browser;
  const output = { generated_at: new Date().toISOString(), apply: Boolean(argv.apply), inspected: 0, ingested: 0, skipped: 0, failed: 0, rows: [] };
  try {
    const candidates = (await client.query(CANDIDATES_SQL, [String(argv["ons-code"]), Number(argv["candidate-limit"]), Number(argv.offset)])).rows;
    browser = await chromium.launch({ headless: !argv.headed }); const context = await browser.newContext({ extraHTTPHeaders: { "Accept-Language": "en-GB,en;q=0.9" } });
    for (const app of candidates) {
      if (output.ingested >= Number(argv.limit)) break; output.inspected += 1; const page = await context.newPage();
      try {
        const documentsUrl = `${DOCS_BASE}${encodeURIComponent(app.keyval)}`; await page.goto(documentsUrl, { waitUntil: "domcontentloaded", timeout: Number(argv["timeout-ms"]) });
        await page.waitForSelector("#Documents tbody tr", { timeout: Math.min(Number(argv["timeout-ms"]), 15000) }); const docs = (await parseDocs(page)).filter(classifyCover);
        if (!docs.length) { output.skipped += 1; continue; }
        const doc = docs[0]; const sourceUrl = absolute(page.url(), doc.href); if (!sourceUrl) { output.skipped += 1; continue; }
        if (await exists(client, sourceUrl)) { output.skipped += 1; output.rows.push({ reference: app.reference, status: "already_ingested" }); continue; }
        const response = await context.request.get(sourceUrl, { timeout: Number(argv["timeout-ms"]) }); const bytes = Buffer.from(await response.body());
        if (!response.ok() || bytes.subarray(0, 4).toString() !== "%PDF") throw new Error(`cover_letter_download_invalid_http_${response.status()}`);
        const safeRef = String(app.reference).replace(/[^A-Za-z0-9._-]/g, "_"); const pdfPath = path.resolve(String(argv["artifacts-dir"]), safeRef, "condition_discharge_cover_letter.pdf"); mkdirFor(pdfPath);
        if (argv.apply) fs.writeFileSync(pdfPath, bytes); else fs.writeFileSync(pdfPath, bytes); // text extraction needs a local temporary artifact in dry-run too
        const fullText = (await pdfText(pdfPath)).trim(); if (!fullText) throw new Error("cover_letter_text_empty"); const profile = await classify(fullText); const embedding = await embed(`${profile.condition_summary || ""}\n${profile.controlled_topics.join(" ")}\n${fullText}`);
        const meta = { pipeline: "wcc_condition_discharge_letter_harvest", source_doc_url: sourceUrl, source_doc_description: doc.description, documents_url, application_type: app.application_type, agent_company_name: app.agent_company_name, address: app.address, keyval: app.keyval, condition_discharge: profile };
        const row = { app, pdfPath, sha256: sha(`${sha(bytes)}|${app.reference}`), bytes: bytes.length, fullText, embedding, tokenCount: fullText.split(/\s+/).filter(Boolean).length, meta, provenance: [{ via: "harvest_wcc_condition_discharge_letters", source_doc_url: sourceUrl, documents_url, harvested_at: new Date().toISOString() }] };
        const docId = argv.apply ? await insert(client, row) : null; output.ingested += 1; output.rows.push({ reference: app.reference, status: argv.apply ? "ingested" : "would_ingest", doc_id: docId, condition_discharge: profile }); log("letter_processed", { reference: app.reference, doc_id: docId, eligible: profile.eligible_for_examples });
      } catch (error) { output.failed += 1; output.rows.push({ reference: app.reference, status: "error", error: error instanceof Error ? error.message : String(error) }); log("letter_error", { reference: app.reference, error: error instanceof Error ? error.message : String(error) }); }
      finally { await page.close().catch(() => {}); }
    }
    await context.close();
  } finally { await browser?.close().catch(() => {}); await client.end(); mkdirFor(String(argv["output-json"])); fs.writeFileSync(String(argv["output-json"]), `${JSON.stringify(output, null, 2)}\n`); }
}
main().catch((error) => { log("fatal", { error: error instanceof Error ? error.message : String(error) }); process.exit(1); });
