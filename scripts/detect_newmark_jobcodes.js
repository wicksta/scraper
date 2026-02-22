#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import mysql from "mysql2/promise";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { chromium } from "playwright";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("detect-newmark-jobcodes")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to scan.",
  })
  .option("limit", {
    type: "number",
    default: 200,
    describe: "Max candidate applications to process.",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into candidate list.",
  })
  .option("concurrency", {
    type: "number",
    default: 3,
    describe: "Number of concurrent browser workers.",
  })
  .option("headed", {
    type: "boolean",
    default: false,
    describe: "Run browser headed (debugging).",
  })
  .option("max-docs-per-app", {
    type: "number",
    default: 3,
    describe: "Max cover-letter-like docs to inspect per application.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/newmark_jobcode_report.json",
    describe: "Path to JSON report output.",
  })
  .option("output-csv", {
    type: "string",
    default: "./tmp/newmark_jobcode_report.csv",
    describe: "Path to CSV report output.",
  })
  .option("artifacts-dir", {
    type: "string",
    default: "/mnt/HC_Volume_103054926/newmark_jobcode_artifacts",
    describe: "Directory for downloaded PDFs / extracted text.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 60000,
    describe: "Navigation/request timeout in milliseconds.",
  })
  .option("extract-applicant-with-openai", {
    type: "boolean",
    default: false,
    describe: "Use OpenAI on extracted cover-letter text to infer applicant name/entity.",
  })
  .option("openai-model", {
    type: "string",
    default: "gpt-4.1-mini",
    describe: "OpenAI model for applicant extraction.",
  })
  .option("openai-max-chars", {
    type: "number",
    default: 24000,
    describe: "Max extracted text chars to send to OpenAI.",
  })
  .option("dry-run-fetch", {
    type: "boolean",
    default: false,
    describe: "Skip Playwright/PDF stages and only output candidate/comparison checks.",
  })
  .strict()
  .help()
  .argv;

const CANDIDATE_SQL = `
  SELECT
    ons_code,
    reference,
    keyval,
    agent_company_name,
    planit_json
  FROM public.applications
  WHERE ons_code = $1
    AND NULLIF(BTRIM(keyval), '') IS NOT NULL
    AND (
      agent_company_name ILIKE '%newmark%'
      OR agent_company_name ILIKE '%gerald eve%'
    )
  ORDER BY COALESCE(application_validated, application_received, date_added, current_date) DESC, reference ASC
  LIMIT $2 OFFSET $3
`;

const UPSERT_SQL = `
  INSERT INTO public.newmark_jobcode_candidates (
    ons_code,
    reference,
    keyval,
    agent_company_name,
    is_newmark,
    documents_url,
    source_doc_url,
    source_doc_description,
    cover_docs_considered,
    job_codes_found,
    job_code_parts,
    applicant_name_extracted,
    applicant_evidence_quote,
    applicant_confidence,
    applicant_extraction_model,
    match_confidence,
    notes,
    error,
    status,
    detected_at,
    updated_at
  ) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9::jsonb,
    $10::jsonb,
    $11::jsonb,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17::jsonb,
    $18,
    $19,
    now(),
    now()
  )
  ON CONFLICT (ons_code, reference) DO UPDATE SET
    keyval = EXCLUDED.keyval,
    agent_company_name = EXCLUDED.agent_company_name,
    is_newmark = EXCLUDED.is_newmark,
    documents_url = EXCLUDED.documents_url,
    source_doc_url = EXCLUDED.source_doc_url,
    source_doc_description = EXCLUDED.source_doc_description,
    cover_docs_considered = EXCLUDED.cover_docs_considered,
    job_codes_found = EXCLUDED.job_codes_found,
    job_code_parts = EXCLUDED.job_code_parts,
    applicant_name_extracted = EXCLUDED.applicant_name_extracted,
    applicant_evidence_quote = EXCLUDED.applicant_evidence_quote,
    applicant_confidence = EXCLUDED.applicant_confidence,
    applicant_extraction_model = EXCLUDED.applicant_extraction_model,
    match_confidence = EXCLUDED.match_confidence,
    notes = EXCLUDED.notes,
    error = EXCLUDED.error,
    status = EXCLUDED.status,
    detected_at = now(),
    updated_at = now()
`;

const DOCS_BASE = "https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=";
const COMBINED_CANDIDATE_COLUMNS = ["uid", "planit_uid", "ref_num", "planning_portal_ref"];
const COMBINED_ONS_COLUMN = "ons_code";

function getPgClientConfig() {
  return process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: process.env.PGHOST,
        port: process.env.PGPORT ? Number(process.env.PGPORT) : undefined,
        database: process.env.PGDATABASE,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
      };
}

function requireEnv(name) {
  const value = process.env[name];
  if (value == null || String(value).trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function getOpenAiApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

function parseJsonObjectLoose(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");
    if (first >= 0 && last > first) {
      const maybe = raw.slice(first, last + 1);
      try {
        return JSON.parse(maybe);
      } catch {
        return null;
      }
    }
    return null;
  }
}

async function extractApplicantWithOpenAi(text, model, maxChars, timeoutMs) {
  const apiKey = getOpenAiApiKey();
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not set");
  }

  const excerpt = String(text || "").slice(0, Math.max(2000, Number(maxChars) || 24000));
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
  const url = `${endpointBase}/chat/completions`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(5000, Number(timeoutMs) || 60000));
  try {
    const resp = await fetch(url, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "Extract structured data from UK planning cover letters. Return strict JSON only.",
          },
          {
            role: "user",
            content: `From this cover letter text, identify:
1) the applicant entity (often phrased as "on behalf of ...")
2) the Newmark team contacts responsible, often in a closing paragraph such as "If you have any queries, please contact X or Y"
Return JSON with keys:
- applicant_name (string or null)
- evidence_quote (short exact phrase from the text)
- confidence ("high" | "medium" | "low")
- team_contact_names (array of strings; empty array if none found)
- team_contact_evidence_quote (short exact phrase from the text, or null)
If unknown, set applicant_name to null.

TEXT:
${excerpt}`,
          },
        ],
      }),
    });

    if (!resp.ok) {
      const errBody = await resp.text().catch(() => "");
      throw new Error(`OpenAI HTTP ${resp.status}: ${errBody.slice(0, 500)}`);
    }
    const payload = await resp.json();
    const content = payload?.choices?.[0]?.message?.content;
    const parsed = parseJsonObjectLoose(content);
    if (!parsed || typeof parsed !== "object") {
      throw new Error("OpenAI response did not contain parseable JSON object");
    }

    const applicantName = parsed.applicant_name == null ? null : String(parsed.applicant_name).trim() || null;
    const evidenceQuote = parsed.evidence_quote == null ? null : String(parsed.evidence_quote).trim() || null;
    const confRaw = parsed.confidence == null ? "" : String(parsed.confidence).toLowerCase().trim();
    const confidence = ["high", "medium", "low"].includes(confRaw) ? confRaw : "low";
    const teamContactNames = Array.isArray(parsed.team_contact_names)
      ? parsed.team_contact_names
          .map((name) => String(name == null ? "" : name).trim())
          .filter(Boolean)
      : [];
    const teamContactEvidenceQuote =
      parsed.team_contact_evidence_quote == null
        ? null
        : String(parsed.team_contact_evidence_quote).trim() || null;

    return {
      applicant_name_extracted: applicantName,
      applicant_evidence_quote: evidenceQuote,
      applicant_confidence: confidence,
      team_contact_names: teamContactNames,
      team_contact_evidence_quote: teamContactEvidenceQuote,
    };
  } finally {
    clearTimeout(timeout);
  }
}

function errorDetails(err) {
  if (err instanceof Error) {
    return {
      message: err.message,
      name: err.name,
      code: err.code || null,
      stack: err.stack || null,
    };
  }
  return {
    message: String(err),
    name: typeof err,
    code: null,
    stack: null,
  };
}

function normalizeRefVariants(ref) {
  const base = String(ref || "").trim();
  if (!base) return [];
  const compact = base.replace(/\s+/g, "");
  const slashCompact = compact.replace(/\s*\/\s*/g, "/");
  return Array.from(new Set([base, compact, slashCompact].filter(Boolean)));
}

function safeParsePlanitJson(planitJson) {
  if (!planitJson) return null;
  if (typeof planitJson === "object") return planitJson;
  try {
    return JSON.parse(planitJson);
  } catch {
    return null;
  }
}

function mkdirpForFile(filePath) {
  fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
}

function mkdirp(dirPath) {
  fs.mkdirSync(path.resolve(dirPath), { recursive: true });
}

function resolveWritableArtifactsDir(preferredDir, fallbackDir) {
  const preferred = path.resolve(preferredDir);
  try {
    mkdirp(preferred);
    fs.accessSync(preferred, fs.constants.W_OK);
    return { dir: preferred, fallbackUsed: false };
  } catch {
    const fallback = path.resolve(fallbackDir);
    mkdirp(fallback);
    fs.accessSync(fallback, fs.constants.W_OK);
    return { dir: fallback, fallbackUsed: true };
  }
}

function escapeCsvField(value) {
  if (value == null) return "";
  const str = String(value);
  if (!/[",\n]/.test(str)) return str;
  return `"${str.replace(/"/g, "\"\"")}"`;
}

function toCsv(rows) {
  const headers = [
    "ons_code",
    "reference",
    "keyval",
    "agent_company_name",
    "combined_match_found",
    "combined_uid",
    "documents_url",
    "first_job_code",
    "job_codes_found",
    "applicant_name_extracted",
    "applicant_confidence",
    "match_confidence",
    "cover_docs_considered",
    "notes",
    "error",
  ];

  const lines = [headers.join(",")];
  for (const row of rows) {
    const values = [
      row.ons_code,
      row.reference,
      row.keyval,
      row.agent_company_name,
      row.combined_match_found,
      row.combined_uid || "",
      row.documents_url || "",
      row.job_codes_found?.[0] || "",
      (row.job_codes_found || []).join("; "),
      row.applicant_name_extracted || "",
      row.applicant_confidence || "",
      row.match_confidence || "none",
      (row.cover_docs_considered || []).map((d) => `${d.document_type || ""}|${d.description || ""}`).join("; "),
      (row.notes || []).join("; "),
      row.error || "",
    ].map(escapeCsvField);
    lines.push(values.join(","));
  }
  return `${lines.join("\n")}\n`;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(minMs, maxMs) {
  const floor = Math.max(0, Number(minMs) || 0);
  const ceil = Math.max(floor, Number(maxMs) || floor);
  return floor + Math.floor(Math.random() * (ceil - floor + 1));
}

function classifyCoverDoc(doc) {
  const hay = `${doc.document_type || ""} ${doc.description || ""}`.toLowerCase();
  return (
    /cover(?:ing)?\s+letter/.test(hay) ||
    /planning\s+letter/.test(hay) ||
    /applicant.*cover/.test(hay) ||
    /agent.*cover/.test(hay) ||
    /covering\s+statement/.test(hay)
  );
}

function toAbsoluteUrl(baseUrl, href) {
  try {
    return new URL(href, baseUrl).toString();
  } catch {
    return null;
  }
}

function runPdftotext(pdfPath) {
  return new Promise((resolve, reject) => {
    const child = spawn("pdftotext", ["-layout", pdfPath, "-"], { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (buf) => {
      stdout += String(buf);
    });
    child.stderr.on("data", (buf) => {
      stderr += String(buf);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`pdftotext exited with code ${code}: ${stderr.trim()}`));
        return;
      }
      resolve(stdout);
    });
  });
}

function extractJobCodesFromText(text) {
  const out = [];
  const seen = new Set();
  const rx = /([A-Z]{1,12})\s*\/\s*((?:[A-Z]{1,12}\s*\/\s*)+)([UJ])\s*0*([0-9]{3,8})/gi;
  let m;
  while ((m = rx.exec(text)) !== null) {
    const partnerInitials = String(m[1] || "").toUpperCase();
    const assistantInitials = String(m[2] || "")
      .split("/")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    const series = String(m[3] || "").toUpperCase();
    const rawNumber = String(m[4] || "");
    const canonDigits = Number.isFinite(Number(rawNumber))
      ? rawNumber.padStart(Math.max(rawNumber.length, series === "U" ? 7 : 4), "0")
      : rawNumber;
    const jobNumber = `${series}${canonDigits}`;
    const canonical = `${partnerInitials}/${assistantInitials.join("/")}/${jobNumber}`;
    if (seen.has(canonical)) continue;
    seen.add(canonical);
    out.push({
      raw: m[0],
      partner_initials: partnerInitials,
      assistant_initials: assistantInitials,
      job_number: jobNumber,
      canonical,
    });
  }

  // Fallback for letters that only include "Our ref: J12345" / "Our ref: U0012345"
  // (no slash-delimited initials present).
  const ourRefRx = /\bour\s*ref(?:erence)?\s*[:\-]?\s*([UJ])\s*0*([0-9]{3,8})\b/gi;
  let n;
  while ((n = ourRefRx.exec(text)) !== null) {
    const series = String(n[1] || "").toUpperCase();
    const rawNumber = String(n[2] || "");
    const canonDigits = Number.isFinite(Number(rawNumber))
      ? rawNumber.padStart(Math.max(rawNumber.length, series === "U" ? 7 : 4), "0")
      : rawNumber;
    const jobNumber = `${series}${canonDigits}`;
    const canonical = jobNumber;
    if (seen.has(canonical)) continue;
    seen.add(canonical);
    out.push({
      raw: n[0],
      partner_initials: null,
      assistant_initials: [],
      job_number: jobNumber,
      canonical,
    });
  }

  return out;
}

function pickConfidence(hits) {
  if (!hits.length) return "none";
  if (hits.some((h) => h.from_cover_letter)) return "high";
  return "medium";
}

function poolMap(items, concurrency, handler) {
  const n = Math.max(1, Number(concurrency) || 1);
  const results = new Array(items.length);
  let idx = 0;
  const workers = Array.from({ length: Math.min(n, items.length) }, async () => {
    while (true) {
      const current = idx;
      idx += 1;
      if (current >= items.length) return;
      results[current] = await handler(items[current], current);
    }
  });
  return Promise.all(workers).then(() => results);
}

async function discoverCombinedColumns(mysqlConn, dbName) {
  const [rows] = await mysqlConn.query(
    `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = 'app_combined_nmrk_planit'
    `,
    [dbName],
  );
  const colSet = new Set(rows.map((r) => String(r.COLUMN_NAME)));
  const refCols = COMBINED_CANDIDATE_COLUMNS.filter((c) => colSet.has(c));
  const onsColPresent = colSet.has(COMBINED_ONS_COLUMN);
  if (!refCols.length) {
    throw new Error("No usable reference columns found on app_combined_nmrk_planit.");
  }
  return { refCols, onsColPresent };
}

function buildCombinedMatchSql({ refCols, onsColPresent }) {
  const refParts = [];
  for (const col of refCols) {
    refParts.push(`REPLACE(TRIM(COALESCE(${col}, '')), ' ', '') IN (?)`);
  }
  const firstUidCol = refCols.includes("uid") ? "uid" : refCols[0];
  const baseClause = `(${refParts.join(" OR ")})`;
  const whereClause = onsColPresent ? `(${baseClause} AND ${COMBINED_ONS_COLUMN} = ?)` : baseClause;
  return `
    SELECT ${firstUidCol} AS combined_uid
    FROM app_combined_nmrk_planit
    WHERE ${whereClause}
    LIMIT 1
  `;
}

function flattenSqlParams(refVariants, onsCode, refCols, onsColPresent) {
  const compactVariantsRaw = refVariants.map((v) => String(v).replace(/\s+/g, ""));
  const compactVariants = compactVariantsRaw.length > 0 ? compactVariantsRaw : [""];
  const params = [];
  for (let i = 0; i < refCols.length; i += 1) {
    params.push(compactVariants);
  }
  if (onsColPresent) params.push(onsCode);
  return params;
}

async function parseDocumentsTable(page) {
  return page.$$eval("#Documents tbody tr, table#Documents tbody tr, table[id='Documents'] tbody tr", (rows) => {
    const docs = [];
    for (const row of rows) {
      const cells = row.querySelectorAll("td");
      if (!cells || cells.length < 4) continue;

      // Idox can render 5 columns (with checkbox) or 4 columns (without checkbox).
      // Extract from the right so both layouts are supported.
      const viewCell = cells[cells.length - 1];
      const descriptionCell = cells[cells.length - 2];
      const docTypeCell = cells[cells.length - 3];
      const dateCell = cells[cells.length - 4];

      const datePublished = dateCell?.textContent?.trim() || null;
      const documentType = docTypeCell?.textContent?.trim() || null;
      const description = descriptionCell?.textContent?.trim() || null;
      const viewLink = viewCell?.querySelector("a")?.getAttribute("href") || null;
      if (!viewLink) continue;
      docs.push({
        date_published: datePublished,
        document_type: documentType,
        description,
        href: viewLink,
      });
    }
    return docs;
  });
}

async function main() {
  const startedAt = Date.now();
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const rootDir = path.resolve(scriptDir, "..");
  const outputJsonPath = path.resolve(rootDir, argv["output-json"]);
  const outputCsvPath = path.resolve(rootDir, argv["output-csv"]);
  mkdirpForFile(outputJsonPath);
  mkdirpForFile(outputCsvPath);
  const artifactsResolution = resolveWritableArtifactsDir(argv["artifacts-dir"], "/tmp/newmark_jobcode_artifacts");
  const artifactsDir = artifactsResolution.dir;

  const pgClient = new Client(getPgClientConfig());
  await pgClient.connect();

  const mysqlHost = requireEnv("MYSQL_HOST");
  const mysqlUser = requireEnv("MYSQL_USER");
  const mysqlPassword = requireEnv("MYSQL_PASSWORD");
  const mysqlDatabase = requireEnv("MYSQL_DATABASE");
  const mysqlConn = await mysql.createConnection({
    host: mysqlHost,
    port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
    user: mysqlUser,
    password: mysqlPassword,
    database: mysqlDatabase,
    connectTimeout: process.env.MYSQL_TIMEOUT_MS ? Number(process.env.MYSQL_TIMEOUT_MS) : 10000,
  });

  let browser;
  try {
    logEvent("start", {
      ons_code: argv["ons-code"],
      limit: argv.limit,
      offset: argv.offset,
      concurrency: argv.concurrency,
      dry_run_fetch: argv["dry-run-fetch"],
      extract_applicant_with_openai: argv["extract-applicant-with-openai"],
    });
    if (artifactsResolution.fallbackUsed) {
      logEvent("artifacts_dir_fallback", {
        requested: String(argv["artifacts-dir"]),
        using: artifactsDir,
      });
    }

    const candidateRes = await pgClient.query(CANDIDATE_SQL, [argv["ons-code"], Number(argv.limit), Number(argv.offset)]);
    const candidates = candidateRes.rows || [];
    logEvent("candidates_loaded", { count: candidates.length });

    const { refCols, onsColPresent } = await discoverCombinedColumns(mysqlConn, mysqlDatabase);
    const combinedSql = buildCombinedMatchSql({ refCols, onsColPresent });
    logEvent("combined_schema_discovered", { refCols, onsColPresent });

    browser = argv["dry-run-fetch"] ? null : await chromium.launch({ headless: !argv.headed });

    let context = null;
    if (browser) {
      context = await browser.newContext({
        extraHTTPHeaders: { "Accept-Language": "en-GB,en;q=0.9" },
      });
    }

    const summary = {
      scanned: 0,
      combined_hits: 0,
      combined_misses: 0,
      docs_opened: 0,
      cover_docs_examined: 0,
      matches_found: 0,
      errors: 0,
    };

    const rows = await poolMap(candidates, Number(argv.concurrency), async (candidate, index) => {
      const row = {
        ons_code: String(candidate.ons_code || "").trim(),
        reference: String(candidate.reference || "").trim(),
        keyval: String(candidate.keyval || "").trim(),
        agent_company_name: String(candidate.agent_company_name || "").trim(),
        combined_match_found: false,
        combined_uid: null,
        documents_url: null,
        cover_docs_considered: [],
        job_codes_found: [],
        job_code_parts: [],
        applicant_name_extracted: null,
        applicant_evidence_quote: null,
        applicant_confidence: null,
        applicant_extraction_model: null,
        match_confidence: "none",
        notes: [],
        error: null,
        status: "new",
      };
      summary.scanned += 1;

      const planitObj = safeParsePlanitJson(candidate.planit_json);
      const planitUid = String(planitObj?.planit?.uid || planitObj?.uid || "").trim();
      const refVariants = Array.from(new Set([...normalizeRefVariants(row.reference), ...normalizeRefVariants(planitUid)]));

      try {
        const [combinedRows] = await mysqlConn.query(
          combinedSql,
          flattenSqlParams(refVariants, row.ons_code, refCols, onsColPresent),
        );
        if (combinedRows.length > 0) {
          row.combined_match_found = true;
          row.combined_uid = combinedRows[0]?.combined_uid ? String(combinedRows[0].combined_uid) : null;
          summary.combined_hits += 1;
        } else {
          row.notes.push("missing_in_app_combined_nmrk_planit");
          row.status = "missing_in_combined";
          summary.combined_misses += 1;
          logEvent("combined_check", { idx: index, reference: row.reference, status: "miss" });
          return row;
        }
      } catch (err) {
        row.error = `combined_check_failed: ${err instanceof Error ? err.message : String(err)}`;
        row.status = "error";
        summary.errors += 1;
        return row;
      }

      logEvent("combined_check", { idx: index, reference: row.reference, status: "hit", combined_uid: row.combined_uid });

      if (argv["dry-run-fetch"]) {
        row.notes.push("dry_run_fetch_enabled");
        row.status = "dry_run";
        return row;
      }

      if (!row.keyval) {
        row.notes.push("missing_keyval");
        row.status = "missing_keyval";
        return row;
      }

      const docsUrl = `${DOCS_BASE}${encodeURIComponent(row.keyval)}`;
      row.documents_url = docsUrl;

      const page = await context.newPage();
      try {
        let navOk = false;
        for (let attempt = 1; attempt <= 2; attempt += 1) {
          try {
            await page.goto(docsUrl, { waitUntil: "domcontentloaded", timeout: Number(argv["timeout-ms"]) });
            navOk = true;
            break;
          } catch (navErr) {
            if (attempt === 2) throw navErr;
          }
        }
        if (!navOk) {
          row.notes.push("documents_navigation_failed");
          row.status = "documents_navigation_failed";
          return row;
        }

        try {
          await page.waitForSelector("#Documents tbody tr, table#Documents tbody tr", {
            timeout: Math.min(Number(argv["timeout-ms"]), 15000),
          });
        } catch {
          row.notes.push("documents_table_not_detected_before_timeout");
        }
        await page.waitForTimeout(250);
        summary.docs_opened += 1;
        const docs = await parseDocumentsTable(page);
        logEvent("documents_parsed", {
          reference: row.reference,
          keyval: row.keyval,
          docs_count: docs.length,
          sample: docs.slice(0, 5).map((d) => ({
            type: d.document_type,
            description: d.description,
            date: d.date_published,
          })),
        });
        const likelyCover = docs.filter(classifyCoverDoc).slice(0, Number(argv["max-docs-per-app"]));
        row.cover_docs_considered = likelyCover;
        summary.cover_docs_examined += likelyCover.length;

        if (!likelyCover.length) {
          row.notes.push("no_cover_letter_doc_found");
          row.status = "no_cover_letter";
          return row;
        }
        row.source_doc_description = likelyCover[0]?.description || null;
        row.source_doc_url = toAbsoluteUrl(page.url(), likelyCover[0]?.href || "");

        const appDir = path.join(artifactsDir, row.reference.replace(/[^A-Za-z0-9._-]/g, "_") || `app_${index}`);
        mkdirp(appDir);

        const hits = [];
        let teamResponsibleNames = [];
        let applicantTried = false;
        for (let i = 0; i < likelyCover.length; i += 1) {
          const doc = likelyCover[i];
          const abs = toAbsoluteUrl(page.url(), doc.href);
          if (!abs) {
            row.notes.push(`doc_${i}_invalid_url`);
            continue;
          }

          const pdfPath = path.join(appDir, `doc_${i + 1}.pdf`);
          const txtPath = path.join(appDir, `doc_${i + 1}.txt`);

          let response;
          try {
            response = await context.request.get(abs, { timeout: Number(argv["timeout-ms"]) });
            if (!response.ok()) {
              throw new Error(`HTTP ${response.status()}`);
            }
          } catch (reqErr) {
            row.notes.push(`doc_${i}_download_failed`);
            logEvent("pdf_download_error", {
              reference: row.reference,
              idx: i,
              error: reqErr instanceof Error ? reqErr.message : String(reqErr),
            });
            continue;
          }

          const body = await response.body();
          fs.writeFileSync(pdfPath, body);

          let text = "";
          try {
            text = await runPdftotext(pdfPath);
            fs.writeFileSync(txtPath, text, "utf8");
          } catch (txtErr) {
            row.notes.push(`doc_${i}_pdftotext_failed`);
            logEvent("pdftotext_error", {
              reference: row.reference,
              idx: i,
              error: txtErr instanceof Error ? txtErr.message : String(txtErr),
            });
            continue;
          }

          const extracted = extractJobCodesFromText(text);
          for (const item of extracted) {
            hits.push({ ...item, from_cover_letter: true });
          }

          if (argv["extract-applicant-with-openai"] && !applicantTried) {
            applicantTried = true;
            try {
              const applicant = await extractApplicantWithOpenAi(
                text,
                String(argv["openai-model"]),
                Number(argv["openai-max-chars"]),
                Number(argv["timeout-ms"]),
              );
              row.applicant_name_extracted = applicant.applicant_name_extracted;
              row.applicant_evidence_quote = applicant.applicant_evidence_quote;
              row.applicant_confidence = applicant.applicant_confidence;
              row.applicant_extraction_model = String(argv["openai-model"]);
              teamResponsibleNames = applicant.team_contact_names || [];
            } catch (llmErr) {
              row.notes.push("applicant_extraction_failed");
              logEvent("applicant_extraction_error", {
                reference: row.reference,
                idx: i,
                error: llmErr instanceof Error ? llmErr.message : String(llmErr),
              });
            }
          }
        }

        if (hits.length) {
          const uniq = new Map();
          for (const hit of hits) {
            if (!uniq.has(hit.canonical)) uniq.set(hit.canonical, hit);
          }
          const finalHits = Array.from(uniq.values());
          row.job_codes_found = finalHits.map((h) => h.canonical);
          row.job_code_parts = finalHits.map((h) => ({
            partner_initials: h.partner_initials,
            assistant_initials: h.assistant_initials,
            job_number: h.job_number,
            raw: h.raw,
            team_responsible_names: teamResponsibleNames,
          }));
          row.match_confidence = pickConfidence(finalHits);
          row.status = "matched";
          summary.matches_found += 1;
        } else {
          row.notes.push("no_job_code_pattern_found");
          row.status = "no_pattern";
        }
      } catch (err) {
        row.error = `documents_scan_failed: ${err instanceof Error ? err.message : String(err)}`;
        row.status = "error";
        summary.errors += 1;
      } finally {
        await page.close().catch(() => {});
      }

      await delay(jitter(300, 900));
      return row;
    });

    let upserted = 0;
    let upsertErrors = 0;
    for (const row of rows) {
      try {
        await pgClient.query(UPSERT_SQL, [
          row.ons_code,
          row.reference,
          row.keyval,
          row.agent_company_name,
          true,
          row.documents_url,
          row.source_doc_url || null,
          row.source_doc_description || null,
          JSON.stringify(row.cover_docs_considered || []),
          JSON.stringify(row.job_codes_found || []),
          JSON.stringify(row.job_code_parts || []),
          row.applicant_name_extracted,
          row.applicant_evidence_quote,
          row.applicant_confidence,
          row.applicant_extraction_model,
          row.match_confidence,
          JSON.stringify(row.notes || []),
          row.error,
          row.status || "new",
        ]);
        upserted += 1;
      } catch (err) {
        upsertErrors += 1;
        logEvent("upsert_error", {
          reference: row.reference,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    const out = {
      generated_at: new Date().toISOString(),
      params: {
        ons_code: argv["ons-code"],
        limit: Number(argv.limit),
        offset: Number(argv.offset),
        concurrency: Number(argv.concurrency),
        dry_run_fetch: Boolean(argv["dry-run-fetch"]),
        max_docs_per_app: Number(argv["max-docs-per-app"]),
      },
      summary,
      rows,
    };

    fs.writeFileSync(outputJsonPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");
    fs.writeFileSync(outputCsvPath, toCsv(rows), "utf8");

    logEvent("done", {
      duration_ms: Date.now() - startedAt,
      output_json: outputJsonPath,
      output_csv: outputCsvPath,
      upserted,
      upsert_errors: upsertErrors,
      ...summary,
    });
  } finally {
    await Promise.allSettled([
      browser?.close(),
      mysqlConn?.end(),
      pgClient?.end(),
    ]);
  }
}

main().catch((err) => {
  const details = errorDetails(err);
  logEvent("fatal", { error: details.message, error_name: details.name, error_code: details.code, stack: details.stack });
  process.exit(1);
});
