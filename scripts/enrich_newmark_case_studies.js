#!/usr/bin/env node
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const DEFAULT_SOURCE_DIR = "/mnt/ngist/public_html/newmark/Planning Case Studies";
const DEFAULT_CSV = "/mnt/ngist/public_html/newmark/case_studies.csv";
const CATEGORIES = [
  "Central London",
  "Commercial",
  "Institutions",
  "Planning policy & strategy",
  "National regeneration",
  "Heritage",
  "Data centres & logistics",
];
const ENRICHMENT_COLUMNS = [
  "Title",
  "Editorial strapline",
  "Service categories",
  "The Challenges",
  "Our Role",
  "The Outcome",
  "Enrichment source SHA256",
  "Enrichment model",
  "Enriched at",
];

const argv = yargs(hideBin(process.argv))
  .scriptName("enrich-newmark-case-studies")
  .option("source-dir", {
    type: "string",
    default: DEFAULT_SOURCE_DIR,
    describe: "Root folder containing the case-study PDFs and DOCX files.",
  })
  .option("csv", {
    type: "string",
    default: DEFAULT_CSV,
    describe: "Case-studies CSV to enrich.",
  })
  .option("model", {
    type: "string",
    default: process.env.NEWMARK_CASE_STUDIES_MODEL || "gpt-5.6-luna",
    describe: "OpenAI model used for classification and copy extraction.",
  })
  .option("limit", {
    type: "number",
    default: 0,
    describe: "Maximum matching case-study files to process; 0 means all.",
  })
  .option("force", {
    type: "boolean",
    default: false,
    describe: "Reprocess studies even where the PDF hash is unchanged.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Call OpenAI and atomically update the CSV. Default is a read-only preview.",
  })
  .option("text-max-chars", {
    type: "number",
    default: 60000,
    describe: "Maximum extracted characters sent to OpenAI per PDF.",
  })
  .option("openai-timeout-ms", {
    type: "number",
    default: 120000,
    describe: "OpenAI request timeout in milliseconds.",
  })
  .strict()
  .help()
  .argv;

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function normaliseText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function sha256(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

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
  return {
    headings,
    rows: rows.map((cells) => Object.fromEntries(headings.map((heading, index) => [heading, cells[index] || ""]))),
  };
}

function csvValue(value) {
  const raw = String(value == null ? "" : value);
  return /[",\r\n]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
}

function serialiseCsv(headings, rows) {
  return `${headings.map(csvValue).join(",")}\n${rows.map((row) => headings.map((heading) => csvValue(row[heading])).join(",")).join("\n")}\n`;
}

function findSourceFiles(rootDir) {
  const out = [];
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(entryPath);
      else if (entry.isFile() && /\.(pdf|docx)$/i.test(entry.name)) out.push(entryPath);
    }
  }
  visit(rootDir);
  return out.sort((first, second) => first.localeCompare(second));
}

function runPdftotext(pdfPath) {
  return new Promise((resolve, reject) => {
    const child = spawn("pdftotext", ["-layout", "-enc", "UTF-8", pdfPath, "-"], { stdio: ["ignore", "pipe", "pipe"] });
    const stdout = [];
    const stderr = [];
    child.stdout.on("data", (chunk) => stdout.push(chunk));
    child.stderr.on("data", (chunk) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`pdftotext exited ${code}: ${Buffer.concat(stderr).toString("utf8").trim()}`));
        return;
      }
      resolve(Buffer.concat(stdout).toString("utf8").replace(/\u0000/g, "").trim());
    });
  });
}

function decodeXmlText(value) {
  return String(value || "")
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, decimal) => String.fromCodePoint(Number.parseInt(decimal, 10)))
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

function runDocxText(docxPath) {
  return new Promise((resolve, reject) => {
    const child = spawn("unzip", ["-p", docxPath, "word/document.xml"], { stdio: ["ignore", "pipe", "pipe"] });
    const stdout = [];
    const stderr = [];
    child.stdout.on("data", (chunk) => stdout.push(chunk));
    child.stderr.on("data", (chunk) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`DOCX text extraction exited ${code}: ${Buffer.concat(stderr).toString("utf8").trim()}`));
        return;
      }
      const xml = Buffer.concat(stdout).toString("utf8");
      const text = decodeXmlText(xml
        .replace(/<w:(?:tab|br|cr)[^>]*\/?\s*>/gi, "\n")
        .replace(/<\/w:p>/gi, "\n")
        .replace(/<[^>]+>/g, " "))
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
      resolve(text);
    });
  });
}

function extractSourceText(sourcePath) {
  if (/\.pdf$/i.test(sourcePath)) return runPdftotext(sourcePath);
  if (/\.docx$/i.test(sourcePath)) return runDocxText(sourcePath);
  throw new Error(`Unsupported case-study source: ${sourcePath}`);
}

function getOpenAiApiKey() {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) throw new Error("OPENAI_API_KEY is not set");
  return key;
}

function parseJsonObjectLoose(text) {
  const raw = String(text || "").trim();
  try {
    return JSON.parse(raw);
  } catch {
    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");
    if (first !== -1 && last > first) {
      try { return JSON.parse(raw.slice(first, last + 1)); } catch { return null; }
    }
    return null;
  }
}

function cleanSentence(value, maxLength) {
  return normaliseText(value).slice(0, maxLength);
}

function normaliseModelResult(raw) {
  const categorySet = new Set(CATEGORIES);
  const categories = [...new Set((Array.isArray(raw?.categories) ? raw.categories : [])
    .map((category) => normaliseText(category))
    .filter((category) => categorySet.has(category)))];
  const result = {
    title: cleanSentence(raw?.title, 160),
    strapline: cleanSentence(raw?.strapline, 280),
    categories,
    challenges: cleanSentence(raw?.challenges, 1100),
    role: cleanSentence(raw?.role, 1100),
    outcome: cleanSentence(raw?.outcome, 1100),
  };
  if (!result.title || !result.strapline || !result.challenges || !result.role || !result.outcome) {
    throw new Error("Model response omitted one or more required case-study fields");
  }
  return result;
}

async function enrichWithOpenAi({ sourceFile, existingRow, text }) {
  const apiKey = getOpenAiApiKey();
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(5000, Number(argv["openai-timeout-ms"]) || 120000));
  try {
    const response = await fetch(`${endpointBase}/chat/completions`, {
      method: "POST",
      signal: controller.signal,
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({
        model: String(argv.model),
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: [
              "You are preparing concise, factual portfolio copy for Newmark Town Planning case studies.",
              "Use only evidence in the supplied case-study PDF text and the supplied existing metadata; do not invent facts, achievements, locations, clients, dates, scales or permissions.",
              "Return strict JSON only, with title, strapline, categories, challenges, role and outcome.",
              "title: the clean project or place name only; remove dates, version labels, client names and marketing prefixes. Keep an address where it is the project name.",
              "strapline: one neutral, journalistic sentence describing the project and planning contribution; no unsupported claims.",
              "categories: one or more exact values from this list only: " + CATEGORIES.join(" | ") + ".",
              "challenges, role and outcome: each must be one or two concise sentences, written in third person. Where the source does not establish an outcome, say what the work was intended to enable rather than claiming it occurred.",
            ].join(" "),
          },
          {
            role: "user",
            content: [
              "Existing CSV metadata:",
              JSON.stringify({
                site: existingRow.Site,
                raw_name: existingRow.Name,
                client: existingRow.Client,
                borough: existingRow.Borough,
                location: existingRow.Location,
                source_file: sourceFile,
              }, null, 2),
              "",
              "Return this exact JSON shape:",
              '{"title":"","strapline":"","categories":[""],"challenges":"","role":"","outcome":""}',
              "",
              "PDF text:",
              text.slice(0, Math.max(1000, Number(argv["text-max-chars"]) || 60000)),
            ].join("\n"),
          },
        ],
      }),
    });
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`OpenAI HTTP ${response.status}: ${body.slice(0, 500)}`);
    }
    const payload = await response.json();
    const result = parseJsonObjectLoose(payload?.choices?.[0]?.message?.content);
    if (!result) throw new Error("OpenAI response did not contain a JSON object");
    return { model: payload?.model || String(argv.model), result: normaliseModelResult(result) };
  } finally {
    clearTimeout(timeout);
  }
}

function matchingRows(rows, pdfName) {
  const exact = rows.filter((row) => normaliseText(row["File name"]).toLocaleLowerCase() === pdfName.toLocaleLowerCase());
  return exact;
}

function isCurrent(row, sourceHash) {
  return row["Enrichment source SHA256"] === sourceHash
    && ENRICHMENT_COLUMNS.slice(0, 6).every((column) => normaliseText(row[column]));
}

function applyResult(row, result, sourceHash, model) {
  row.Title = result.title;
  row["Editorial strapline"] = result.strapline;
  row["Service categories"] = result.categories.join("; ");
  row["The Challenges"] = result.challenges;
  row["Our Role"] = result.role;
  row["The Outcome"] = result.outcome;
  row["Enrichment source SHA256"] = sourceHash;
  row["Enrichment model"] = model;
  row["Enriched at"] = new Date().toISOString();
}

function writeCsvAtomically(csvPath, headings, rows) {
  const tempPath = `${csvPath}.tmp-${process.pid}-${Date.now()}`;
  fs.writeFileSync(tempPath, serialiseCsv(headings, rows), "utf8");
  fs.renameSync(tempPath, csvPath);
}

async function main() {
  const sourceDir = path.resolve(String(argv["source-dir"]));
  const csvPath = path.resolve(String(argv.csv));
  if (!fs.existsSync(sourceDir)) throw new Error(`source folder not found: ${sourceDir}`);
  if (!fs.existsSync(csvPath)) throw new Error(`CSV not found: ${csvPath}`);
  logEvent("scan_started", { source_dir: sourceDir, csv: csvPath });
  const { headings: originalHeadings, rows } = parseCsv(fs.readFileSync(csvPath, "utf8"));
  logEvent("csv_loaded", { rows: rows.length });
  const headings = [...originalHeadings];
  for (const column of ENRICHMENT_COLUMNS) if (!headings.includes(column)) headings.push(column);
  const sourceFiles = findSourceFiles(sourceDir);
  logEvent("sources_found", { source_files: sourceFiles.length });
  const candidates = [];
  for (const sourcePath of sourceFiles) {
    const sourceName = path.basename(sourcePath);
    const matchedRows = matchingRows(rows, sourceName);
    if (!matchedRows.length) {
      logEvent("unmatched_source", { source: path.relative(sourceDir, sourcePath) });
      continue;
    }
    candidates.push({ sourcePath, sourceName, matchedRows });
  }
  const limit = Math.max(0, Number(argv.limit) || 0);
  const work = limit ? candidates.slice(0, limit) : candidates;
  logEvent("discovered", { source_files: sourceFiles.length, matching: candidates.length, queued: work.length, apply: argv.apply, model: argv.model });
  if (!argv.apply) {
    for (const item of work) logEvent("would_process", { source: path.relative(sourceDir, item.sourcePath), rows: item.matchedRows.length });
    return;
  }
  let changed = 0;
  for (const item of work) {
    const relativeSource = path.relative(sourceDir, item.sourcePath);
    try {
      const sourceHash = sha256(fs.readFileSync(item.sourcePath));
      if (!argv.force && item.matchedRows.every((row) => isCurrent(row, sourceHash))) {
        logEvent("skip_current", { source: relativeSource, rows: item.matchedRows.length });
        continue;
      }
      const text = await extractSourceText(item.sourcePath);
      if (!normaliseText(text)) throw new Error("text extraction returned no readable text");
      const { model, result } = await enrichWithOpenAi({ sourceFile: relativeSource, existingRow: item.matchedRows[0], text });
      for (const row of item.matchedRows) applyResult(row, result, item.sourceHash, model);
      writeCsvAtomically(csvPath, headings, rows);
      changed += item.matchedRows.length;
      logEvent("enriched", { source: relativeSource, rows: item.matchedRows.length, title: result.title, categories: result.categories });
    } catch (error) {
      logEvent("error", { source: relativeSource, error: error instanceof Error ? error.message : String(error) });
    }
  }
  if (!changed) {
    logEvent("complete", { changed: 0 });
    return;
  }
  logEvent("complete", { changed, csv: csvPath });
}

main().catch((error) => {
  logEvent("fatal", { error: error instanceof Error ? error.message : String(error) });
  process.exitCode = 1;
});
