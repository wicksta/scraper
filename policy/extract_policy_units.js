#!/usr/bin/env node
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const ALLOWED_UNIT_TYPES = new Set([
  "policy_core",
  "supporting_text",
  "section_intro",
  "glossary",
  "contents_navigation",
  "appendix_schedule",
  "site_allocation",
  "front_matter",
  "unknown",
]);

const EXTRACTOR_VERSION = "policy_units_v1";
const OPENAI_RETRY_ATTEMPTS = 5;
const OPENAI_RETRY_BASE_DELAY_MS = 1500;
const OPENAI_RETRY_MAX_DELAY_MS = 15000;

const argv = yargs(hideBin(process.argv))
  .scriptName("extract-policy-units")
  .option("doc-id", {
    type: "string",
    demandOption: true,
    describe: "UUID of an existing public.documents policy document.",
  })
  .option("model", {
    type: "string",
    default: "gpt-5-mini",
    describe: "Model used to extract structured policy units from page windows.",
  })
  .option("guidance-file", {
    type: "string",
    default: "",
    describe: "Optional path to a JSON file containing document-specific extraction guidance.",
  })
  .option("page-start", {
    type: "number",
    default: 0,
    describe: "Optional first page to include (1-based). Default uses the full document.",
  })
  .option("page-end", {
    type: "number",
    default: 0,
    describe: "Optional last page to include (1-based, inclusive). Default uses the full document.",
  })
  .option("embedding-model", {
    type: "string",
    default: "text-embedding-3-small",
    describe: "Embedding model used for stored policy unit vectors.",
  })
  .option("window-pages", {
    type: "number",
    default: 4,
    describe: "Number of pages per extraction window.",
  })
  .option("overlap-pages", {
    type: "number",
    default: 1,
    describe: "Number of overlapping pages between windows.",
  })
  .option("embed-batch-size", {
    type: "number",
    default: 24,
    describe: "Batch size for unit embeddings.",
  })
  .option("embed", {
    type: "boolean",
    default: true,
    describe: "Generate unit embeddings. Disable for cheaper dry-run extraction review.",
  })
  .option("max-chars-per-embed", {
    type: "number",
    default: 8000,
    describe: "Maximum chars per embedded unit text.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Write extracted units into public.policy_units. Default is dry run.",
  })
  .option("limit-windows", {
    type: "number",
    default: 0,
    describe: "Optional limit on number of extraction windows for debugging (0 = all).",
  })
  .option("checkpoint-path", {
    type: "string",
    default: "",
    describe: "Optional path to a JSON checkpoint file used to resume completed extraction windows.",
  })
  .strict()
  .help()
  .argv;

function logProgress(event, payload = {}) {
  const line = {
    ts: new Date().toISOString(),
    event,
    ...payload,
  };
  process.stderr.write(`${JSON.stringify(line)}\n`);
}

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

function getOpenAiApiKey() {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) throw new Error("OPENAI_API_KEY is not set");
  return key;
}

function endpointBase() {
  return process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
}

function vectorLiteral(v) {
  return `[${v.map((x) => Number(x).toString()).join(",")}]`;
}

function md5(value) {
  return crypto.createHash("md5").update(String(value || "")).digest("hex");
}

function cleanText(value) {
  const text = String(value ?? "")
    .replace(/\u0000/g, "")
    .replace(/[\u0001-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "")
    .replace(/\r\n/g, "\n")
    .trim();
  return text || null;
}

function sanitizeJsonValue(value) {
  if (value == null) return value;
  if (typeof value === "string") return cleanText(value);
  if (Array.isArray(value)) {
    return value
      .map((item) => sanitizeJsonValue(item))
      .filter((item) => item !== null && item !== undefined);
  }
  if (typeof value === "object") {
    const out = {};
    for (const [rawKey, rawValue] of Object.entries(value)) {
      const key = cleanText(rawKey);
      if (!key) continue;
      const sanitized = sanitizeJsonValue(rawValue);
      if (sanitized === null || sanitized === undefined) continue;
      out[key] = sanitized;
    }
    return out;
  }
  return value;
}

function cleanList(values, limit = 24) {
  const out = [];
  const seen = new Set();
  for (const raw of Array.isArray(values) ? values : []) {
    const text = cleanText(raw);
    if (!text) continue;
    const key = text.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(text);
    if (out.length >= limit) break;
  }
  return out;
}

function policyCompositeText(unit) {
  return [
    unit.unit_type === "glossary" && unit.policy_title ? `Glossary term: ${unit.policy_title}` : null,
    unit.policy_number ? `Policy number: ${unit.policy_number}` : null,
    unit.policy_title ? `Policy title: ${unit.policy_title}` : null,
    unit.section_title ? `Section title: ${unit.section_title}` : null,
    Array.isArray(unit.heading_path_json) && unit.heading_path_json.length
      ? `Heading path: ${unit.heading_path_json.join(" > ")}`
      : null,
    unit.policy_text ? `Policy text:\n${unit.policy_text}` : null,
    unit.supporting_text ? `Supporting text:\n${unit.supporting_text}` : null,
  ]
    .filter(Boolean)
    .join("\n\n");
}

function responseText(payload) {
  if (typeof payload?.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }
  const output = Array.isArray(payload?.output) ? payload.output : [];
  const parts = [];
  for (const item of output) {
    if (item?.type !== "message") continue;
    for (const content of Array.isArray(item?.content) ? item.content : []) {
      if ((content?.type === "output_text" || content?.type === "text") && typeof content?.text === "string") {
        parts.push(content.text);
      }
    }
  }
  return parts.join("\n").trim();
}

function parseLooseJson(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");
    if (first >= 0 && last > first) {
      try {
        return JSON.parse(raw.slice(first, last + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
}

function loadGuidanceFile(filePath) {
  const path = String(filePath || "").trim();
  if (!path) return null;
  const raw = fs.readFileSync(path, "utf8");
  const parsed = JSON.parse(raw);
  return parsed && typeof parsed === "object" ? parsed : null;
}

function readCheckpoint(filePath) {
  const path = String(filePath || "").trim();
  if (!path || !fs.existsSync(path)) return null;
  try {
    const raw = fs.readFileSync(path, "utf8");
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (error) {
    logProgress("checkpoint.load_failed", {
      path,
      error: error?.message || String(error),
    });
    return null;
  }
}

function writeCheckpoint(filePath, payload) {
  const path = String(filePath || "").trim();
  if (!path) return;
  const tempPath = `${path}.tmp`;
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  fs.renameSync(tempPath, path);
}

function deleteCheckpoint(filePath) {
  const path = String(filePath || "").trim();
  if (!path) return;
  try {
    if (fs.existsSync(path)) {
      fs.unlinkSync(path);
    }
  } catch (error) {
    logProgress("checkpoint.delete_failed", {
      path,
      error: error?.message || String(error),
    });
  }
}

function checkpointKeyForWindow(window) {
  return `${Number(window.page_start)}-${Number(window.page_end)}`;
}

function checkpointMatchesRun(checkpoint, meta) {
  if (!checkpoint || typeof checkpoint !== "object") return false;
  return Number(checkpoint.version || 0) === 1
    && String(checkpoint.doc_id || "") === String(meta.doc_id || "")
    && String(checkpoint.model || "") === String(meta.model || "")
    && Number(checkpoint.window_pages || 0) === Number(meta.window_pages || 0)
    && Number(checkpoint.overlap_pages || 0) === Number(meta.overlap_pages || 0)
    && Number(checkpoint.page_start || 0) === Number(meta.page_start || 0)
    && Number(checkpoint.page_end || 0) === Number(meta.page_end || 0);
}

function checkpointPayload(meta, completedWindows) {
  return {
    version: 1,
    updated_at: new Date().toISOString(),
    ...meta,
    completed_windows: completedWindows,
  };
}

function parseJsonText(raw) {
  const text = String(raw || "").trim();
  if (!text) return null;
  try {
    const parsed = JSON.parse(text);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function normalizeGuidance(guidance) {
  if (!guidance || typeof guidance !== "object") return null;
  return {
    document_name: cleanText(guidance.document_name),
    policy_heading_rule: cleanText(guidance.policy_heading_rule),
    supporting_paragraph_rule: cleanText(guidance.supporting_paragraph_rule),
    subheading_rule: cleanText(guidance.subheading_rule),
    known_patterns: cleanList(guidance.known_patterns ?? [], 24),
    negative_examples: cleanList(guidance.negative_examples ?? [], 16),
    positive_examples: cleanList(guidance.positive_examples ?? [], 16),
    classification_notes: cleanList(guidance.classification_notes ?? [], 16),
  };
}

async function fetchGuidanceFromTable(client, docId) {
  const { rows } = await client.query(
    `
      SELECT
        explanation_text,
        custom_prompt_text,
        test_page_start,
        test_page_end
      FROM public.policy_document_guidance
      WHERE doc_id = $1::uuid
      LIMIT 1
    `,
    [docId],
  );
  const row = rows[0] || null;
  if (!row) return null;
  const parsedPrompt = parseJsonText(row.custom_prompt_text);
  const normalizedPrompt = normalizeGuidance(parsedPrompt);
  return {
    guidance: normalizedPrompt,
    explanation_text: cleanText(row.explanation_text),
    test_page_start: Number(row.test_page_start) || null,
    test_page_end: Number(row.test_page_end) || null,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function retryDelayMs(attempt) {
  const exp = OPENAI_RETRY_BASE_DELAY_MS * (2 ** Math.max(0, attempt - 1));
  const jitter = Math.floor(Math.random() * 500);
  return Math.min(OPENAI_RETRY_MAX_DELAY_MS, exp + jitter);
}

function shouldRetryOpenAiError({ status, error }) {
  if (status === 408 || status === 409 || status === 425 || status === 429) return true;
  if (status >= 500) return true;
  if (error) return true;
  return false;
}

async function fetchJsonWithRetries(url, options, meta = {}) {
  let lastError = null;

  for (let attempt = 1; attempt <= OPENAI_RETRY_ATTEMPTS; attempt += 1) {
    const startedAt = Date.now();
    let resp;
    let bodyText = "";
    let parsed = null;
    let fetchError = null;

    try {
      resp = await fetch(url, options);
      bodyText = await resp.text();
      try {
        parsed = JSON.parse(bodyText);
      } catch {
        parsed = null;
      }

      if (!resp.ok || parsed?.error) {
        const message = parsed?.error?.message || bodyText.slice(0, 1000) || `HTTP ${resp.status}`;
        const error = new Error(message);
        error.status = resp.status;
        error.payload = parsed;
        throw error;
      }

      return {
        status: resp.status,
        payload: parsed,
        durationMs: Date.now() - startedAt,
      };
    } catch (error) {
      fetchError = error;
      const status = Number(error?.status || resp?.status || 0);
      const retryable = shouldRetryOpenAiError({ status, error });
      lastError = error;

      if (!retryable || attempt === OPENAI_RETRY_ATTEMPTS) {
        break;
      }

      const waitMs = retryDelayMs(attempt);
      logProgress("openai.retry", {
        channel: meta.channel || "unknown",
        model: meta.model || null,
        attempt,
        max_attempts: OPENAI_RETRY_ATTEMPTS,
        status: status || null,
        wait_ms: waitMs,
        error: String(error?.message || "Unknown OpenAI fetch error").slice(0, 400),
      });
      await sleep(waitMs);
    }
  }

  if (lastError?.status) {
    throw new Error(`${meta.label || "OpenAI API"} error: ${lastError.message}`);
  }
  throw new Error(`${meta.label || "OpenAI API"} request failed: ${lastError?.message || "unknown error"}`);
}

async function responsesJson({ model, systemPrompt, userPrompt }) {
  const apiKey = getOpenAiApiKey();
  const { status, payload, durationMs } = await fetchJsonWithRetries(`${endpointBase()}/responses`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: systemPrompt }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: userPrompt }],
        },
      ],
      text: { format: { type: "json_object" } },
    }),
  }, {
    channel: "responses",
    model,
    label: "Responses API",
  });

  const text = responseText(payload);
  const parsed = parseLooseJson(text);
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Responses API returned invalid JSON output");
  }
  logProgress("responses.completed", {
    model,
    status,
    duration_ms: durationMs,
    output_chars: text.length,
  });
  return parsed;
}

async function embedTexts(inputs, { model, batchSize, maxCharsPerEmbed }) {
  if (!inputs.length) return [];
  const apiKey = getOpenAiApiKey();
  const out = [];
  for (let i = 0; i < inputs.length; i += batchSize) {
    const batchNo = Math.floor(i / batchSize) + 1;
    const batchCount = Math.ceil(inputs.length / batchSize);
    const batch = inputs.slice(i, i + batchSize).map((text) => String(text || "").slice(0, maxCharsPerEmbed));
    logProgress("embeddings.batch.start", {
      model,
      batch_no: batchNo,
      batch_count: batchCount,
      items: batch.length,
    });
    const { payload, durationMs } = await fetchJsonWithRetries(`${endpointBase()}/embeddings`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        input: batch,
      }),
    }, {
      channel: "embeddings",
      model,
      label: "Embeddings API",
    });
    for (const row of Array.isArray(payload?.data) ? payload.data : []) {
      out.push(Array.isArray(row?.embedding) ? row.embedding.map((x) => Number(x)) : null);
    }
    logProgress("embeddings.batch.completed", {
      model,
      batch_no: batchNo,
      batch_count: batchCount,
      items: batch.length,
      duration_ms: durationMs,
    });
  }
  return out;
}

async function fetchDocument(client, docId) {
  const { rows } = await client.query(
    `
      SELECT
        id::text AS id,
        document_type,
        title,
        originator,
        full_text,
        token_count,
        meta
      FROM public.documents
      WHERE id = $1::uuid
      LIMIT 1
    `,
    [docId],
  );
  const row = rows[0] || null;
  if (!row) return null;
  if (row.meta && typeof row.meta === "string") {
    try {
      row.meta = JSON.parse(row.meta);
    } catch {
      row.meta = {};
    }
  }
  if (!row.meta || typeof row.meta !== "object") {
    row.meta = {};
  }
  return row;
}

async function fetchPageChunks(client, docId) {
  const { rows } = await client.query(
    `
      SELECT
        id,
        page,
        text
      FROM public.chunks
      WHERE doc_id = $1::uuid
        AND kind = 'page'
      ORDER BY page ASC NULLS LAST, id ASC
    `,
    [docId],
  );
  return rows.map((row) => ({
    chunk_id: Number(row.id),
    page: Number(row.page),
    text: String(row.text || "").trim(),
  })).filter((row) => row.page > 0 && row.text);
}

function parsePagesFromFullText(fullText) {
  const raw = String(fullText || "").trim();
  if (!raw) return [];

  const parts = raw.split(/===== PAGE\s+(\d+)\s+=====/u);
  const pages = [];
  if (parts.length >= 3) {
    for (let i = 1; i < parts.length; i += 2) {
      const page = Number(parts[i] || 0);
      const text = String(parts[i + 1] || "").trim();
      if (page > 0 && text) {
        pages.push({ page, text, chunk_id: null });
      }
    }
  }

  if (!pages.length) {
    const fallback = raw.split(/\f+/g).map((x) => x.trim()).filter(Boolean);
    return fallback.map((text, idx) => ({ page: idx + 1, text, chunk_id: null }));
  }
  return pages;
}

function buildWindows(pages, windowPages, overlapPages, limitWindows = 0) {
  const windows = [];
  const size = Math.max(1, Number(windowPages) || 4);
  const overlap = Math.max(0, Math.min(size - 1, Number(overlapPages) || 0));
  const step = Math.max(1, size - overlap);
  for (let i = 0; i < pages.length; i += step) {
    const slice = pages.slice(i, i + size);
    if (!slice.length) break;
    windows.push({
      index: windows.length,
      pages: slice,
      page_start: slice[0].page,
      page_end: slice[slice.length - 1].page,
    });
    if (i + size >= pages.length) break;
    if (limitWindows > 0 && windows.length >= limitWindows) break;
  }
  return windows;
}

function filterPagesByRange(pages, pageStart, pageEnd) {
  const start = Number(pageStart) || 0;
  const end = Number(pageEnd) || 0;
  if (start <= 0 && end <= 0) {
    return pages;
  }
  const minPage = start > 0 ? start : 1;
  const maxPage = end > 0 ? end : Number.MAX_SAFE_INTEGER;
  return pages.filter((page) => page.page >= minPage && page.page <= maxPage);
}

function extractionSystemPrompt() {
  return [
    "You extract structured units from UK planning policy documents.",
    "Be literal and document-faithful.",
    "Do not invent policies, policy numbers, headings, or explanatory text.",
    "Treat the meaningful unit as a policy block or section block, not an isolated paragraph.",
    "Where a policy heading and its supporting explanatory text are visible, keep them linked in one unit, but separate policy_text from supporting_text.",
    "Classify units using only this vocabulary: policy_core, supporting_text, section_intro, glossary, contents_navigation, appendix_schedule, site_allocation, front_matter, unknown.",
    "If a numbered or named policy is visible, prefer a single policy_core unit for it.",
    "Use supporting_text only for explanatory text attached to that policy or section.",
    "For glossary entries, treat the defined word or phrase as one object and its definition as the associated text.",
    "For glossary units, put the term in glossary_term and the definition in glossary_definition. Also set policy_title to the term if possible.",
    "Ignore page furniture unless it helps identify the structure.",
    "Return strict JSON only as {\"units\": [...]}."
  ].join("\n");
}

function extractionUserPrompt(doc, window, guidance) {
  const pageText = window.pages
    .map((page) => `===== PAGE ${page.page} =====\n${page.text}`)
    .join("\n\n");
  const guidanceLines = guidance
    ? [
        "DOCUMENT-SPECIFIC EXTRACTION GUIDANCE:",
        guidance.document_name ? `- document_name: ${guidance.document_name}` : null,
        guidance.policy_heading_rule ? `- policy_heading_rule: ${guidance.policy_heading_rule}` : null,
        guidance.supporting_paragraph_rule ? `- supporting_paragraph_rule: ${guidance.supporting_paragraph_rule}` : null,
        guidance.subheading_rule ? `- subheading_rule: ${guidance.subheading_rule}` : null,
        ...(guidance.known_patterns.length ? ["- known_patterns:", ...guidance.known_patterns.map((x) => `  - ${x}`)] : []),
        ...(guidance.positive_examples.length ? ["- positive_examples:", ...guidance.positive_examples.map((x) => `  - ${x}`)] : []),
        ...(guidance.negative_examples.length ? ["- negative_examples:", ...guidance.negative_examples.map((x) => `  - ${x}`)] : []),
        ...(guidance.classification_notes.length ? ["- classification_notes:", ...guidance.classification_notes.map((x) => `  - ${x}`)] : []),
        "Apply this guidance when deciding what counts as policy text and what counts as explanatory/supporting text.",
        "",
      ].filter(Boolean)
    : [];
  return [
    `Document title: ${doc.title || "Untitled policy document"}`,
    `Originator: ${doc.originator || "Unknown"}`,
    `Window pages: ${window.page_start}-${window.page_end}`,
    "Extract the structured units visible in this page window.",
    "For each unit, output:",
    "- unit_type",
    "- section_title",
    "- heading_path (array of strings)",
    "- policy_number",
    "- policy_title",
    "- policy_text",
    "- supporting_text",
    "- glossary_term (for glossary units)",
    "- glossary_definition (for glossary units)",
    "- keywords (array)",
    "- topics (array)",
    "- page_start",
    "- page_end",
    "Prefer fewer, coherent units over fragmented paragraph-level output.",
    "If the text is clearly contents/foreword/glossary/appendix material, classify it accordingly.",
    "If unit_type is glossary, keep one word/phrase plus its definition together as a single unit instead of splitting them apart.",
    ...guidanceLines,
    "",
    pageText,
  ].join("\n");
}

function normalizeExtractedUnit(raw, window) {
  const headingPath = cleanList(raw?.heading_path ?? [], 12);
  const unitType = ALLOWED_UNIT_TYPES.has(String(raw?.unit_type || "").trim())
    ? String(raw.unit_type).trim()
    : "unknown";
  const glossaryTerm = cleanText(raw?.glossary_term);
  const glossaryDefinition = cleanText(raw?.glossary_definition);
  const policyText = cleanText(raw?.policy_text);
  const supportingText = cleanText(raw?.supporting_text);
  const fallbackBody = cleanText(raw?.content);
  const pageStart = Number.isFinite(Number(raw?.page_start)) ? Number(raw.page_start) : window.page_start;
  const pageEnd = Number.isFinite(Number(raw?.page_end)) ? Number(raw.page_end) : window.page_end;
  const normalizedPolicyTitle = cleanText(raw?.policy_title);
  const normalizedSectionTitle = cleanText(raw?.section_title);
  const normalizedPolicyText = unitType === "glossary"
    ? null
    : (policyText || (unitType === "policy_core" ? fallbackBody : null));
  const normalizedSupportingText = unitType === "glossary"
    ? (glossaryDefinition || supportingText || fallbackBody)
    : (supportingText || (unitType !== "policy_core" ? fallbackBody : null));
  const normalized = {
    unit_type: unitType,
    section_title: unitType === "glossary"
      ? (normalizedSectionTitle || "Glossary")
      : normalizedSectionTitle,
    heading_path_json: headingPath,
    policy_number: cleanText(raw?.policy_number),
    policy_title: unitType === "glossary"
      ? (glossaryTerm || normalizedPolicyTitle)
      : normalizedPolicyTitle,
    policy_text: normalizedPolicyText,
    supporting_text: normalizedSupportingText,
    keywords_json: cleanList(raw?.keywords ?? [], 20),
    topics_json: cleanList(raw?.topics ?? [], 16),
    page_start: Math.max(window.page_start, Math.min(pageStart, pageEnd)),
    page_end: Math.min(window.page_end, Math.max(pageStart, pageEnd)),
    source_meta_json: {
      extraction_window_index: window.index,
      extraction_window_pages: [window.page_start, window.page_end],
      raw_unit_type: cleanText(raw?.unit_type),
      glossary_term: unitType === "glossary" ? glossaryTerm : null,
    },
  };

  if (!normalized.policy_text && !normalized.supporting_text && !normalized.section_title && !normalized.policy_title) {
    return null;
  }
  return normalized;
}

function appendUniqueText(a, b) {
  const left = cleanText(a);
  const right = cleanText(b);
  if (!left) return right;
  if (!right) return left;
  if (left === right) return left;
  if (left.includes(right)) return left;
  if (right.includes(left)) return right;

  const leftNorm = normalizeTextForCompare(left);
  const rightNorm = normalizeTextForCompare(right);
  if (leftNorm.includes(rightNorm)) return left;
  if (rightNorm.includes(leftNorm)) return right;

  const leftSegments = splitTextSegments(left);
  const rightSegments = splitTextSegments(right);
  const overlap = findSegmentOverlap(leftSegments, rightSegments);
  const combined = dedupeSegments([
    ...leftSegments,
    ...rightSegments.slice(overlap),
  ]);
  return joinSegments(combined);
}

function normalizeTextForCompare(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/^[\-\u2022]\s*/gm, "")
    .trim();
}

function splitTextSegments(text) {
  const raw = String(text || "").trim();
  if (!raw) return [];

  let segments = raw
    .split(/\n{2,}/)
    .map((part) => cleanText(part))
    .filter(Boolean);

  if (segments.length <= 1) {
    segments = raw
      .replace(/\n+/g, " ")
      .split(/(?<=[.!?])\s+(?=(?:[A-Z0-9]|- The Council|\d+\.) )/u)
      .map((part) => cleanText(part))
      .filter(Boolean);
  }

  return segments.length ? segments : [raw];
}

function findSegmentOverlap(leftSegments, rightSegments) {
  const max = Math.min(leftSegments.length, rightSegments.length);
  for (let size = max; size > 0; size -= 1) {
    let matches = true;
    for (let i = 0; i < size; i += 1) {
      const leftNorm = normalizeTextForCompare(leftSegments[leftSegments.length - size + i]);
      const rightNorm = normalizeTextForCompare(rightSegments[i]);
      if (leftNorm !== rightNorm) {
        matches = false;
        break;
      }
    }
    if (matches) {
      return size;
    }
  }
  return 0;
}

function dedupeSegments(segments) {
  const out = [];
  const seen = new Set();
  for (const segment of segments) {
    const text = cleanText(segment);
    if (!text) continue;
    const normalized = normalizeTextForCompare(text);
    if (!normalized) continue;

    let duplicate = false;
    for (const existing of out) {
      const existingNorm = normalizeTextForCompare(existing);
      if (existingNorm === normalized || existingNorm.includes(normalized) || normalized.includes(existingNorm)) {
        duplicate = true;
        break;
      }
    }
    if (duplicate || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    out.push(text);
  }
  return out;
}

function joinSegments(segments) {
  return segments
    .map((segment) => cleanText(segment))
    .filter(Boolean)
    .join("\n\n");
}

function unitComparableTitle(unit) {
  return cleanText(unit.policy_title || unit.section_title || unit.policy_number || unit.unit_key || "");
}

function unitComparableBody(unit) {
  return cleanText([
    unit.policy_text || "",
    unit.supporting_text || "",
  ].filter(Boolean).join("\n\n"));
}

function normalizedWordCount(text) {
  const parts = normalizeTextForCompare(text).split(/\s+/).filter(Boolean);
  return parts.length;
}

function unitsAreComparableForContainment(a, b) {
  if (String(a.unit_type || "") !== String(b.unit_type || "")) {
    return false;
  }

  const aPolicyNumber = cleanText(a.policy_number);
  const bPolicyNumber = cleanText(b.policy_number);
  if (aPolicyNumber || bPolicyNumber) {
    return aPolicyNumber !== null && aPolicyNumber === bPolicyNumber;
  }

  const aTitle = normalizeTextForCompare(unitComparableTitle(a));
  const bTitle = normalizeTextForCompare(unitComparableTitle(b));
  if (aTitle && bTitle) {
    return aTitle === bTitle || aTitle.includes(bTitle) || bTitle.includes(aTitle);
  }

  return true;
}

function unitPageRangesOverlapOrTouch(a, b, slackPages = 1) {
  const aStart = Number(a.page_start ?? 0) || 0;
  const aEnd = Number(a.page_end ?? aStart) || aStart;
  const bStart = Number(b.page_start ?? 0) || 0;
  const bEnd = Number(b.page_end ?? bStart) || bStart;
  return aStart <= (bEnd + slackPages) && bStart <= (aEnd + slackPages);
}

function pruneContainedUnits(units) {
  return units.filter((unit, index) => {
    const unitBody = unitComparableBody(unit);
    if (!unitBody) {
      return true;
    }
    const unitNorm = normalizeTextForCompare(unitBody);
    const unitWords = normalizedWordCount(unitBody);
    if (unitWords < 8) {
      return true;
    }

    for (let i = 0; i < units.length; i += 1) {
      if (i === index) continue;
      const other = units[i];
      if (!unitsAreComparableForContainment(unit, other)) {
        continue;
      }
      if (!unitPageRangesOverlapOrTouch(unit, other, 2)) {
        continue;
      }
      const otherBody = unitComparableBody(other);
      if (!otherBody) continue;
      const otherWords = normalizedWordCount(otherBody);
      if (otherWords <= unitWords) {
        continue;
      }
      const otherNorm = normalizeTextForCompare(otherBody);
      if (!otherNorm.includes(unitNorm)) {
        continue;
      }
      if (otherWords < Math.max(unitWords + 12, Math.floor(unitWords * 1.35))) {
        continue;
      }
      return false;
    }
    return true;
  });
}

function mergePolicyCoreUnits(units) {
  const policyMap = new Map();
  const miscMap = new Map();

  for (const unit of units) {
    const policyNumber = cleanText(unit.policy_number);
    const mergeKey = policyNumber ? `policy:${policyNumber.toUpperCase()}` : null;
    if (mergeKey) {
      const existing = policyMap.get(mergeKey) || {
        unit_type: "policy_core",
        section_title: null,
        heading_path_json: [],
        policy_number: policyNumber,
        policy_title: null,
        policy_text: null,
        supporting_text: null,
        keywords_json: [],
        topics_json: [],
        page_start: unit.page_start,
        page_end: unit.page_end,
        source_meta_json: {
          merged_from_windows: [],
          extracted_unit_types: [],
        },
      };
      existing.section_title = existing.section_title || unit.section_title;
      existing.policy_title = existing.policy_title || unit.policy_title;
      existing.heading_path_json = cleanList([...(existing.heading_path_json || []), ...(unit.heading_path_json || [])], 16);
      existing.policy_text = appendUniqueText(existing.policy_text, unit.policy_text);
      existing.supporting_text = appendUniqueText(existing.supporting_text, unit.supporting_text);
      existing.keywords_json = cleanList([...(existing.keywords_json || []), ...(unit.keywords_json || [])], 24);
      existing.topics_json = cleanList([...(existing.topics_json || []), ...(unit.topics_json || [])], 24);
      existing.page_start = Math.min(Number(existing.page_start ?? unit.page_start), Number(unit.page_start));
      existing.page_end = Math.max(Number(existing.page_end ?? unit.page_end), Number(unit.page_end));
      existing.source_meta_json.merged_from_windows = cleanList([
        ...(existing.source_meta_json.merged_from_windows || []),
        `window:${unit.source_meta_json?.extraction_window_index}`,
      ], 32);
      existing.source_meta_json.extracted_unit_types = cleanList([
        ...(existing.source_meta_json.extracted_unit_types || []),
        unit.unit_type,
      ], 16);
      policyMap.set(mergeKey, existing);
      continue;
    }

    const miscSignature = md5(JSON.stringify([
      unit.unit_type,
      unit.section_title,
      unit.policy_title,
      unit.page_start,
      unit.page_end,
      unit.policy_text || "",
      unit.supporting_text || "",
    ]));
    if (!miscMap.has(miscSignature)) {
      miscMap.set(miscSignature, {
        ...unit,
        source_meta_json: {
          ...(unit.source_meta_json || {}),
          merged_from_windows: cleanList([`window:${unit.source_meta_json?.extraction_window_index}`], 16),
          extracted_unit_types: cleanList([unit.unit_type], 8),
        },
      });
    }
  }

  const merged = pruneContainedUnits([
    ...Array.from(policyMap.values()),
    ...Array.from(miscMap.values()),
  ]);

  merged.sort((a, b) => {
    const pageCmp = Number(a.page_start ?? 0) - Number(b.page_start ?? 0);
    if (pageCmp !== 0) return pageCmp;
    const endCmp = Number(a.page_end ?? 0) - Number(b.page_end ?? 0);
    if (endCmp !== 0) return endCmp;
    return String(a.policy_number || a.policy_title || a.section_title || a.unit_type).localeCompare(
      String(b.policy_number || b.policy_title || b.section_title || b.unit_type),
    );
  });

  return merged.map((unit, index) => ({
    ...unit,
    unit_key: `pu:${index + 1}:${md5(JSON.stringify([
      unit.unit_type,
      unit.policy_number,
      unit.policy_title,
      unit.page_start,
      unit.page_end,
      unit.policy_text || "",
      unit.supporting_text || "",
    ])).slice(0, 16)}`,
  }));
}

async function storeUnits(client, docId, units, vectors) {
  logProgress("store.begin", {
    doc_id: docId,
    units: units.length,
  });
  await client.query("BEGIN");
  try {
    await client.query("DELETE FROM public.policy_units WHERE doc_id = $1::uuid", [docId]);

    const ids = [];
    for (let i = 0; i < units.length; i += 1) {
      const unit = units[i];
      const vec = vectors[i] ? vectorLiteral(vectors[i]) : null;
      const headingPathJson = JSON.stringify(sanitizeJsonValue(unit.heading_path_json || []));
      const keywordsJson = JSON.stringify(sanitizeJsonValue(unit.keywords_json || []));
      const topicsJson = JSON.stringify(sanitizeJsonValue(unit.topics_json || []));
      const sourceMetaJson = JSON.stringify(sanitizeJsonValue(unit.source_meta_json || {}));
      const { rows } = await client.query(
        `
          INSERT INTO public.policy_units (
            doc_id,
            unit_key,
            unit_type,
            section_title,
            heading_path_json,
            policy_number,
            policy_title,
            policy_text,
            supporting_text,
            keywords_json,
            topics_json,
            page_start,
            page_end,
            source_meta_json,
            unit_vec
          ) VALUES (
            $1::uuid,
            $2,
            $3,
            $4,
            $5::jsonb,
            $6,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11::jsonb,
            $12,
            $13,
            $14::jsonb,
            ${vec ? `$15::vector` : "NULL"}
          )
          RETURNING id
        `,
        vec
          ? [
              docId,
              unit.unit_key,
              unit.unit_type,
              unit.section_title,
              headingPathJson,
              unit.policy_number,
              unit.policy_title,
              unit.policy_text,
              unit.supporting_text,
              keywordsJson,
              topicsJson,
              unit.page_start,
              unit.page_end,
              sourceMetaJson,
              vec,
            ]
          : [
              docId,
              unit.unit_key,
              unit.unit_type,
              unit.section_title,
              headingPathJson,
              unit.policy_number,
              unit.policy_title,
              unit.policy_text,
              unit.supporting_text,
              keywordsJson,
              topicsJson,
              unit.page_start,
              unit.page_end,
              sourceMetaJson,
            ],
      );
      ids.push(Number(rows[0]?.id));
    }

    for (let i = 0; i < ids.length; i += 1) {
      await client.query(
        `
          UPDATE public.policy_units
          SET prev_unit_id = $2::bigint,
              next_unit_id = $3::bigint,
              updated_at = now()
          WHERE id = $1::bigint
        `,
        [
          ids[i],
          i > 0 ? ids[i - 1] : null,
          i < ids.length - 1 ? ids[i + 1] : null,
        ],
      );
    }

    await client.query("COMMIT");
    logProgress("store.completed", {
      doc_id: docId,
      units: ids.length,
    });
    return ids.length;
  } catch (error) {
    await client.query("ROLLBACK");
    logProgress("store.failed", {
      doc_id: docId,
      error: error?.message || String(error),
    });
    throw error;
  }
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    logProgress("extractor.start", {
      doc_id: argv.docId,
      model: argv.model,
      embed: Boolean(argv.embed),
      apply: Boolean(argv.apply),
    });
    const doc = await fetchDocument(client, argv.docId);
    if (!doc) {
      throw new Error(`Document not found: ${argv.docId}`);
    }
    if (String(doc.document_type || "") !== "policy_document") {
      throw new Error(`Document ${argv.docId} is not a policy_document`);
    }

    const fileGuidance = normalizeGuidance(argv.guidanceFile ? loadGuidanceFile(argv.guidanceFile) : null);
    const tableGuidanceRow = await fetchGuidanceFromTable(client, argv.docId);
    const tableGuidance = tableGuidanceRow?.guidance || null;
    const docMetaGuidance = normalizeGuidance(doc.meta?.policy_extraction_guidance || null);
    const guidance = fileGuidance || tableGuidance || docMetaGuidance;
    logProgress("document.loaded", {
      doc_id: argv.docId,
      title: doc.title || null,
      guidance_applied: Boolean(guidance),
      guidance_source: fileGuidance ? "file" : (tableGuidance ? "policy_document_guidance" : (docMetaGuidance ? "documents.meta" : null)),
    });

    let pages = await fetchPageChunks(client, argv.docId);
    if (!pages.length) {
      pages = parsePagesFromFullText(doc.full_text);
    }
    if (!pages.length) {
      throw new Error("No page text available for policy extraction");
    }

    const filteredPages = filterPagesByRange(pages, argv.pageStart, argv.pageEnd);
    if (!filteredPages.length) {
      throw new Error(`No pages available in requested range ${argv.pageStart || "start"}-${argv.pageEnd || "end"}`);
    }

    const windows = buildWindows(filteredPages, argv.windowPages, argv.overlapPages, argv.limitWindows);
    logProgress("windows.built", {
      doc_id: argv.docId,
      pages: filteredPages.length,
      pages_total: pages.length,
      windows: windows.length,
      window_pages: argv.windowPages,
      overlap_pages: argv.overlapPages,
      page_start: argv.pageStart || null,
      page_end: argv.pageEnd || null,
    });
    const checkpointPath = cleanText(argv.checkpointPath);
    const checkpointMeta = {
      doc_id: argv.docId,
      model: argv.model,
      window_pages: Number(argv.windowPages) || 4,
      overlap_pages: Number(argv.overlapPages) || 0,
      page_start: Number(argv.pageStart) || 0,
      page_end: Number(argv.pageEnd) || 0,
    };
    const checkpoint = readCheckpoint(checkpointPath);
    const resumedWindows = checkpointMatchesRun(checkpoint, checkpointMeta)
      ? (Array.isArray(checkpoint.completed_windows) ? checkpoint.completed_windows : [])
      : [];
    const resumedMap = new Map(
      resumedWindows
        .filter((entry) => entry && typeof entry === "object" && entry.window_key)
        .map((entry) => [String(entry.window_key), entry]),
    );
    if (checkpointPath) {
      logProgress("checkpoint.ready", {
        path: checkpointPath,
        resumed_windows: resumedMap.size,
      });
    }
    const extracted = [];
    const completedWindows = [];
    for (const [index, window] of windows.entries()) {
      const windowKey = checkpointKeyForWindow(window);
      const resumed = resumedMap.get(windowKey);
      if (resumed) {
        const resumedUnits = Array.isArray(resumed.units) ? resumed.units : [];
        extracted.push(...resumedUnits);
        completedWindows.push({
          window_key: windowKey,
          window_no: index + 1,
          page_start: window.page_start,
          page_end: window.page_end,
          units: resumedUnits,
        });
        logProgress("window.resumed", {
          doc_id: argv.docId,
          window_no: index + 1,
          window_count: windows.length,
          page_start: window.page_start,
          page_end: window.page_end,
          units_accepted: resumedUnits.length,
        });
        continue;
      }

      logProgress("window.start", {
        doc_id: argv.docId,
        window_no: index + 1,
        window_count: windows.length,
        page_start: window.page_start,
        page_end: window.page_end,
      });
      const payload = await responsesJson({
        model: argv.model,
        systemPrompt: extractionSystemPrompt(),
        userPrompt: extractionUserPrompt(doc, window, guidance),
      });
      const units = Array.isArray(payload?.units) ? payload.units : [];
      let accepted = 0;
      const acceptedUnits = [];
      for (const rawUnit of units) {
        const unit = normalizeExtractedUnit(rawUnit, window);
        if (unit) {
          extracted.push(unit);
          acceptedUnits.push(unit);
          accepted += 1;
        }
      }
      completedWindows.push({
        window_key: windowKey,
        window_no: index + 1,
        page_start: window.page_start,
        page_end: window.page_end,
        units: acceptedUnits,
      });
      if (checkpointPath) {
        writeCheckpoint(checkpointPath, checkpointPayload(checkpointMeta, completedWindows));
      }
      logProgress("window.completed", {
        doc_id: argv.docId,
        window_no: index + 1,
        window_count: windows.length,
        page_start: window.page_start,
        page_end: window.page_end,
        units_raw: units.length,
        units_accepted: accepted,
      });
    }

    const mergedUnits = mergePolicyCoreUnits(extracted).map((unit) => ({
      ...unit,
        source_meta_json: {
          ...(unit.source_meta_json || {}),
          extractor_version: EXTRACTOR_VERSION,
          extraction_model: argv.model,
          doc_title: doc.title || null,
          guidance_file: cleanText(argv.guidanceFile),
          guidance_applied: Boolean(guidance),
          guidance_explanation_text: tableGuidanceRow?.explanation_text || null,
        },
      }));
    logProgress("merge.completed", {
      doc_id: argv.docId,
      extracted_units_raw: extracted.length,
      units_final: mergedUnits.length,
    });

    const vectors = argv.embed
      ? await embedTexts(mergedUnits.map(policyCompositeText), {
          model: argv.embeddingModel,
          batchSize: Math.max(1, Number(argv.embedBatchSize) || 24),
          maxCharsPerEmbed: Math.max(1000, Number(argv.maxCharsPerEmbed) || 8000),
        })
      : new Array(mergedUnits.length).fill(null);

    let stored = 0;
    if (argv.apply) {
      stored = await storeUnits(client, argv.docId, mergedUnits, vectors);
    }

    logProgress("extractor.completed", {
      doc_id: argv.docId,
      units_final: mergedUnits.length,
      stored_units: stored,
      apply: Boolean(argv.apply),
    });
    deleteCheckpoint(checkpointPath);

    process.stdout.write(`${JSON.stringify({
      success: true,
      doc_id: argv.docId,
      doc_title: doc.title || null,
      pages: filteredPages.length,
      pages_total: pages.length,
      windows: windows.length,
      extracted_units_raw: extracted.length,
      units_final: mergedUnits.length,
      stored_units: stored,
      apply: Boolean(argv.apply),
      embed: Boolean(argv.embed),
      guidance_applied: Boolean(guidance),
      guidance_source: fileGuidance ? "file" : (tableGuidance ? "policy_document_guidance" : (docMetaGuidance ? "documents.meta" : null)),
      page_start: argv.pageStart || null,
      page_end: argv.pageEnd || null,
      extractor_version: EXTRACTOR_VERSION,
      model: argv.model,
      embedding_model: argv.embeddingModel,
      preview: mergedUnits.slice(0, 5).map((unit) => ({
        unit_key: unit.unit_key,
        unit_type: unit.unit_type,
        policy_number: unit.policy_number,
        policy_title: unit.policy_title,
        section_title: unit.section_title,
        page_start: unit.page_start,
        page_end: unit.page_end,
        policy_text: unit.policy_text,
        supporting_text: unit.supporting_text,
        heading_path: unit.heading_path_json || [],
        keywords: unit.keywords_json || [],
        topics: unit.topics_json || [],
        source_meta: unit.source_meta_json || {},
      })),
    }, null, 2)}\n`);
  } finally {
    await client.end().catch(() => {});
  }
}

main().catch((error) => {
  logProgress("extractor.failed", {
    doc_id: argv.docId,
    error: error?.message || String(error),
  });
  process.stderr.write(`${error?.stack || error?.message || String(error)}\n`);
  process.exit(1);
});
