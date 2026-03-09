#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";

const DEFAULT_INPUT = "/opt/scraper/ngist/public_html/test/passage_merged";
const DEFAULT_MODEL = "gpt-4o-mini";

function parseArgs(argv) {
  const out = {
    input: DEFAULT_INPUT,
    model: DEFAULT_MODEL,
    batchPages: 40,
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1]) {
      out.input = argv[++i];
      continue;
    }
    if (a === "--model" && argv[i + 1]) {
      out.model = argv[++i];
      continue;
    }
    if (a === "--batch-pages" && argv[i + 1]) {
      out.batchPages = Math.max(1, Number(argv[++i]) || 40);
      continue;
    }
    if (a === "--help" || a === "-h") {
      printHelpAndExit(0);
    }
    if (a === "--json-only") {
      out.jsonOnly = true;
      continue;
    }
    if (a === "--verbose") {
      out.verbose = true;
      continue;
    }
    throw new Error(`Unknown arg: ${a}`);
  }
  return out;
}

function printHelpAndExit(code) {
  const txt = `
Usage:
  node extract_letter_recipient_addresses.js [options]

Options:
  --input <path>         Input text file path (default: ${DEFAULT_INPUT})
  --model <name>         OpenAI model (default: ${DEFAULT_MODEL})
  --batch-pages <n>      Pages per API call (default: 40)
  --json-only            Print only final JSON
  --verbose              Print progress to stderr
  -h, --help             Show help

Auth:
  Uses OPENAI_API_KEY from environment (bootstrap loads /opt/scraper/.env).
  Example:
    OPENAI_API_KEY=sk-... node extract_letter_recipient_addresses.js --json-only
`;
  process.stdout.write(`${txt.trim()}\n`);
  process.exit(code);
}

function getApiKey() {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) {
    throw new Error(
      "OPENAI_API_KEY is not set. Set it in /opt/scraper/.env (loaded by bootstrap.js) or export it in your shell.",
    );
  }
  return key;
}

function getEndpointBase() {
  return process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
}

function splitIntoPages(text) {
  const src = String(text || "");
  const re = /===== PAGE\s+(\d+)\s+=====/g;
  const pages = [];
  let m;
  let lastIdx = 0;
  let lastPage = null;

  while ((m = re.exec(src)) !== null) {
    if (lastPage !== null) {
      pages.push({
        page: lastPage,
        text: src.slice(lastIdx, m.index).trim(),
      });
    }
    lastPage = Number(m[1]);
    lastIdx = re.lastIndex;
  }

  if (lastPage !== null) {
    pages.push({
      page: lastPage,
      text: src.slice(lastIdx).trim(),
    });
  } else {
    pages.push({ page: 1, text: src.trim() });
  }
  return pages;
}

function batches(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function normalizeSpace(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function normalizeKey(s) {
  return normalizeSpace(s).toLowerCase();
}

function getResponseText(resp) {
  if (typeof resp?.output_text === "string" && resp.output_text.trim()) return resp.output_text.trim();
  const out = Array.isArray(resp?.output) ? resp.output : [];
  const parts = [];
  for (const item of out) {
    if (item?.type !== "message") continue;
    const content = Array.isArray(item?.content) ? item.content : [];
    for (const c of content) {
      if ((c?.type === "output_text" || c?.type === "text") && typeof c?.text === "string") {
        parts.push(c.text);
      }
    }
  }
  return parts.join("\n").trim();
}

function parseLooseJsonObject(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    const s = raw.indexOf("{");
    const e = raw.lastIndexOf("}");
    if (s >= 0 && e > s) {
      try {
        return JSON.parse(raw.slice(s, e + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
}

async function callResponses({ model, instructions, input }) {
  const apiKey = getApiKey();
  const resp = await fetch(`${getEndpointBase()}/responses`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      temperature: 0,
      instructions,
      input,
    }),
  });

  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Responses HTTP ${resp.status}: ${body.slice(0, 1200)}`);
  }
  return resp.json();
}

async function extractFromBatch(batchPages, model) {
  const instructions = [
    "Extract recipient postal addresses from UK planning consultation letters.",
    "Return strict JSON only.",
    "Use only addresses the letters were sent to (usually after 'The Owner / Occupier').",
    "Do not return the application site address or council office addresses.",
  ].join(" ");

  const pagePayload = batchPages
    .map((p) => `PAGE ${p.page}\n${p.text}`)
    .join("\n\n-----\n\n");

  const prompt = `
Return JSON with this exact shape:
{
  "addresses": [
    {
      "page": integer,
      "address": string
    }
  ]
}

Rules:
- Keep each address as one line.
- If the same address appears on multiple pages, include each occurrence (dedupe happens later).
- Include only recipient addresses of addressees.

Input:
${pagePayload}
`;

  const response = await callResponses({
    model,
    instructions,
    input: prompt,
  });

  const txt = getResponseText(response);
  const parsed = parseLooseJsonObject(txt);
  if (!parsed || !Array.isArray(parsed.addresses)) {
    throw new Error(`Model output was not valid expected JSON. Raw: ${txt.slice(0, 500)}`);
  }

  return parsed.addresses
    .map((x) => ({
      page: Number(x?.page),
      address: normalizeSpace(x?.address),
    }))
    .filter((x) => Number.isFinite(x.page) && x.page > 0 && x.address);
}

async function main() {
  const args = parseArgs(process.argv);
  const inputPath = path.resolve(args.input);
  if (!fs.existsSync(inputPath)) throw new Error(`Input file not found: ${inputPath}`);

  const raw = fs.readFileSync(inputPath, "utf8");
  const pages = splitIntoPages(raw);
  const grouped = batches(pages, args.batchPages);
  const all = [];

  for (let i = 0; i < grouped.length; i++) {
    const g = grouped[i];
    if (args.verbose) {
      const first = g[0]?.page;
      const last = g[g.length - 1]?.page;
      process.stderr.write(`Extracting batch ${i + 1}/${grouped.length} (pages ${first}-${last})...\n`);
    }
    const rows = await extractFromBatch(g, args.model);
    all.push(...rows);
  }

  const unique = [];
  const seen = new Set();
  for (const row of all.sort((a, b) => a.page - b.page)) {
    const key = normalizeKey(row.address);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    unique.push(row);
  }

  const result = {
    ok: true,
    model: args.model,
    input_path: inputPath,
    pages: pages.length,
    addresses_found: unique.length,
    addresses: unique,
  };

  if (args.jsonOnly) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return;
  }

  process.stdout.write(`Model: ${result.model}\n`);
  process.stdout.write(`Input: ${result.input_path}\n`);
  process.stdout.write(`Pages: ${result.pages}\n`);
  process.stdout.write(`Unique recipient addresses: ${result.addresses_found}\n\n`);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`fatal: ${msg}\n`);
  process.exit(1);
});
