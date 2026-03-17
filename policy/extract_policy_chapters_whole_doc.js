#!/usr/bin/env node
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;
const EXTRACTOR_VERSION = "policy_chapters_whole_doc_v1";
const OPENAI_RETRY_ATTEMPTS = 5;
const OPENAI_RETRY_BASE_DELAY_MS = 1500;
const OPENAI_RETRY_MAX_DELAY_MS = 15000;

const argv = yargs(hideBin(process.argv))
  .scriptName("extract-policy-chapters-whole-doc")
  .option("doc-id", {
    type: "string",
    demandOption: true,
    describe: "UUID of an existing public.documents policy document.",
  })
  .option("model", {
    type: "string",
    default: "gpt-5.4",
    describe: "Whole-document reasoning model.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Write extracted chapters into public.policy_chapters.",
  })
  .option("checkpoint-path", {
    type: "string",
    default: "",
    describe: "Optional checkpoint file path for completed whole-document chapter extraction.",
  })
  .strict()
  .help()
  .argv;

function logProgress(event, payload = {}) {
  process.stderr.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
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
  if (Array.isArray(value)) return value.map((item) => sanitizeJsonValue(item)).filter((item) => item !== null && item !== undefined);
  if (typeof value === "object") {
    const out = {};
    for (const [key, raw] of Object.entries(value)) {
      const cleanKey = cleanText(key);
      if (!cleanKey) continue;
      const cleanVal = sanitizeJsonValue(raw);
      if (cleanVal === null || cleanVal === undefined) continue;
      out[cleanKey] = cleanVal;
    }
    return out;
  }
  return value;
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

function responseText(payload) {
  if (typeof payload?.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }
  const parts = [];
  for (const item of Array.isArray(payload?.output) ? payload.output : []) {
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
    const firstObject = raw.indexOf("{");
    const lastObject = raw.lastIndexOf("}");
    if (firstObject >= 0 && lastObject > firstObject) {
      try {
        return JSON.parse(raw.slice(firstObject, lastObject + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
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
    let parsed = null;
    try {
      resp = await fetch(url, options);
      const bodyText = await resp.text();
      try {
        parsed = JSON.parse(bodyText);
      } catch {
        parsed = null;
      }
      if (!resp.ok || parsed?.error) {
        const message = parsed?.error?.message || bodyText.slice(0, 1000) || `HTTP ${resp.status}`;
        const error = new Error(message);
        error.status = resp.status;
        throw error;
      }
      return { status: resp.status, payload: parsed, durationMs: Date.now() - startedAt };
    } catch (error) {
      const status = Number(error?.status || resp?.status || 0);
      lastError = error;
      if (!shouldRetryOpenAiError({ status, error }) || attempt === OPENAI_RETRY_ATTEMPTS) {
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
        error: String(error?.message || "Unknown OpenAI fetch error").slice(0, 300),
      });
      await sleep(waitMs);
    }
  }
  if (lastError?.status) throw new Error(`${meta.label || "OpenAI API"} error: ${lastError.message}`);
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
        { role: "system", content: [{ type: "input_text", text: systemPrompt }] },
        { role: "user", content: [{ type: "input_text", text: userPrompt }] },
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
  logProgress("responses.completed", { model, status, duration_ms: durationMs, output_chars: text.length });
  return parsed;
}

async function fetchDocument(client, docId) {
  const { rows } = await client.query(`
    SELECT id::text AS id, document_type, title, originator, full_text, meta
    FROM public.documents
    WHERE id = $1::uuid
    LIMIT 1
  `, [docId]);
  const row = rows[0] || null;
  if (!row) return null;
  if (row.meta && typeof row.meta === "string") {
    try { row.meta = JSON.parse(row.meta); } catch { row.meta = {}; }
  }
  if (!row.meta || typeof row.meta !== "object") row.meta = {};
  return row;
}

async function fetchPageChunks(client, docId) {
  const { rows } = await client.query(`
    SELECT id, page, text
    FROM public.chunks
    WHERE doc_id = $1::uuid
      AND kind = 'page'
    ORDER BY page ASC NULLS LAST, id ASC
  `, [docId]);
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
      if (page > 0 && text) pages.push({ page, text, chunk_id: null });
    }
  }
  return pages;
}

function wholeDocSystemPrompt() {
  return [
    "You are extracting top-level chapter starts from a UK planning policy document.",
    "You are given the full extracted document text with explicit extracted page markers.",
    "First, look in the opening pages for a contents section if one exists, and use it as the primary clue for the chapter structure.",
    "Then verify those chapter starts against the real page text in the document.",
    "If there is no usable contents page, infer the chapter starts from the document's major headings.",
    "Return only top-level chapter starts, in order, using the extracted page numbers where each chapter actually begins.",
    "Include appendices, glossary, and other end matter only if they are top-level sections.",
    "Do not return policies, subheadings, figures, tables, maps, or glossary terms.",
    "Return strict JSON as {\"chapters\": [{\"chapter_number\": string|null, \"chapter_title\": string, \"page_start\": number}]}",
  ].join("\n");
}

function wholeDocUserPrompt(doc, pages) {
  const fullPayload = pages.map((page) => `===== PAGE ${page.page} =====\n${page.text}`).join("\n\n");
  return [
    `Document title: ${doc.title || "Untitled policy document"}`,
    "Extract the ordered top-level chapter starts from this full document.",
    fullPayload,
  ].join("\n\n");
}

function normalizeHeading(value) {
  return String(value || "")
    .replace(/^#+\s*/g, "")
    .replace(/\|/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeWholeDocChapters(entries, maxPage) {
  const out = [];
  const seen = new Set();
  for (const raw of Array.isArray(entries) ? entries : []) {
    const chapterTitle = normalizeHeading(raw?.chapter_title);
    const chapterNumber = cleanText(raw?.chapter_number);
    const pageStart = Number(raw?.page_start || 0);
    if (!chapterTitle || pageStart <= 0 || pageStart > maxPage) continue;
    const key = `${pageStart}|${String(chapterNumber || "").toLowerCase()}|${chapterTitle.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      chapter_number: chapterNumber,
      chapter_title: chapterTitle,
      page_start: pageStart,
      source: "whole_doc_llm",
      source_meta_json: { detected_from: "whole_doc_llm" },
    });
  }
  return out.sort((a, b) => a.page_start - b.page_start || String(a.chapter_title).localeCompare(String(b.chapter_title)));
}

function pagesToChapterText(pages) {
  return pages
    .map((page) => `===== PAGE ${page.page} =====\n${page.text}`)
    .join("\n\n");
}

function md5(value) {
  return crypto.createHash("md5").update(String(value || "")).digest("hex");
}

function buildChaptersFromStarts(chapterStarts, pages, detectionMeta) {
  const maxPage = pages[pages.length - 1]?.page || 0;
  const sorted = [...chapterStarts].sort((a, b) => a.page_start - b.page_start || String(a.chapter_title).localeCompare(String(b.chapter_title)));
  const chapters = [];
  for (let i = 0; i < sorted.length; i += 1) {
    const current = sorted[i];
    const next = sorted[i + 1];
    const pageStart = current.page_start;
    const pageEnd = next ? Math.max(pageStart, next.page_start - 1) : maxPage;
    const chapterPages = pages.filter((page) => page.page >= pageStart && page.page <= pageEnd);
    chapters.push({
      chapter_key: `pc:${i + 1}:${md5(JSON.stringify([current.chapter_number, current.chapter_title, pageStart, pageEnd])).slice(0, 16)}`,
      chapter_order: i + 1,
      chapter_number: current.chapter_number || null,
      chapter_title: current.chapter_title,
      page_start: pageStart,
      page_end: pageEnd,
      chapter_text: pagesToChapterText(chapterPages),
      heading_path_json: [current.chapter_title],
      source_meta_json: sanitizeJsonValue({
        ...(current.source_meta_json || {}),
        extractor_version: EXTRACTOR_VERSION,
        extraction_model: argv.model,
        detection_mode: "llm_whole_doc",
        doc_title: detectionMeta.doc_title || null,
      }),
    });
  }
  return chapters;
}

async function storeChapters(client, docId, chapters) {
  logProgress("store.begin", { doc_id: docId, chapters: chapters.length });
  await client.query("BEGIN");
  try {
    await client.query("DELETE FROM public.policy_chapters WHERE doc_id = $1::uuid", [docId]);
    for (const chapter of chapters) {
      await client.query(`
        INSERT INTO public.policy_chapters (
          doc_id, chapter_key, chapter_order, chapter_number, chapter_title, chapter_text,
          page_start, page_end, heading_path_json, source_meta_json
        ) VALUES (
          $1::uuid, $2, $3, $4, $5, $6,
          $7, $8, $9::jsonb, $10::jsonb
        )
      `, [
        docId,
        chapter.chapter_key,
        chapter.chapter_order,
        chapter.chapter_number,
        chapter.chapter_title,
        chapter.chapter_text,
        chapter.page_start,
        chapter.page_end,
        JSON.stringify(sanitizeJsonValue(chapter.heading_path_json || [])),
        JSON.stringify(sanitizeJsonValue(chapter.source_meta_json || {})),
      ]);
    }
    await client.query("COMMIT");
    logProgress("store.completed", { doc_id: docId, chapters: chapters.length });
    return chapters.length;
  } catch (error) {
    await client.query("ROLLBACK");
    logProgress("store.failed", { doc_id: docId, error: error?.message || String(error) });
    throw error;
  }
}

function readCheckpoint(filePath) {
  const path = String(filePath || "").trim();
  if (!path || !fs.existsSync(path)) return null;
  try {
    return JSON.parse(fs.readFileSync(path, "utf8"));
  } catch (error) {
    logProgress("checkpoint.load_failed", { path, error: error?.message || String(error) });
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
    if (fs.existsSync(path)) fs.unlinkSync(path);
  } catch (error) {
    logProgress("checkpoint.delete_failed", { path, error: error?.message || String(error) });
  }
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    logProgress("extractor.start", {
      doc_id: argv.docId,
      model: argv.model,
      apply: Boolean(argv.apply),
    });

    const doc = await fetchDocument(client, argv.docId);
    if (!doc) throw new Error(`Document not found: ${argv.docId}`);
    if (String(doc.document_type || "") !== "policy_document") {
      throw new Error(`Document ${argv.docId} is not a policy_document`);
    }

    logProgress("document.loaded", {
      doc_id: argv.docId,
      title: doc.title || null,
    });

    let pages = await fetchPageChunks(client, argv.docId);
    if (!pages.length) pages = parsePagesFromFullText(doc.full_text);
    if (!pages.length) throw new Error("No page text available for whole-document chapter extraction");

    logProgress("pages.ready", {
      doc_id: argv.docId,
      pages: pages.length,
      pages_total: pages.length,
    });

    const checkpointPath = cleanText(argv.checkpointPath);
    const checkpoint = readCheckpoint(checkpointPath);
    if (checkpoint && Array.isArray(checkpoint.chapters) && checkpoint.doc_id === argv.docId) {
      logProgress("checkpoint.resumed", { path: checkpointPath, chapters: checkpoint.chapters.length });
      const stored = argv.apply ? await storeChapters(client, argv.docId, checkpoint.chapters) : 0;
      deleteCheckpoint(checkpointPath);
      process.stdout.write(`${JSON.stringify({
        success: true,
        doc_id: argv.docId,
        doc_title: doc.title || null,
        pages: pages.length,
        chapters: checkpoint.chapters,
        chapters_count: checkpoint.chapters.length,
        stored_chapters: stored,
        apply: Boolean(argv.apply),
        detection_mode: checkpoint.detection_mode || "checkpoint",
        extractor_version: EXTRACTOR_VERSION,
        model: argv.model,
      }, null, 2)}\n`);
      return;
    }

    logProgress("whole_doc.start", { doc_id: argv.docId, pages: pages.length });
    const payload = await responsesJson({
      model: argv.model,
      systemPrompt: wholeDocSystemPrompt(),
      userPrompt: wholeDocUserPrompt(doc, pages),
    });
    const normalized = normalizeWholeDocChapters(payload?.chapters, pages[pages.length - 1]?.page || 0);
    if (normalized.length < 2) {
      throw new Error("Whole-document chapter extractor returned too few chapters.");
    }

    const chapters = buildChaptersFromStarts(normalized, pages, { doc_title: doc.title || null });
    writeCheckpoint(checkpointPath, {
      version: 1,
      doc_id: argv.docId,
      detection_mode: "llm_whole_doc",
      chapters,
      updated_at: new Date().toISOString(),
    });

    logProgress("merge.completed", {
      doc_id: argv.docId,
      detection_mode: "llm_whole_doc",
      chapters_final: chapters.length,
    });

    let stored = 0;
    if (argv.apply) {
      stored = await storeChapters(client, argv.docId, chapters);
    }

    logProgress("extractor.completed", {
      doc_id: argv.docId,
      chapters_final: chapters.length,
      stored_chapters: stored,
      apply: Boolean(argv.apply),
    });
    deleteCheckpoint(checkpointPath);
    process.stdout.write(`${JSON.stringify({
      success: true,
      doc_id: argv.docId,
      doc_title: doc.title || null,
      pages: pages.length,
      chapters,
      chapters_count: chapters.length,
      stored_chapters: stored,
      apply: Boolean(argv.apply),
      detection_mode: "llm_whole_doc",
      extractor_version: EXTRACTOR_VERSION,
      model: argv.model,
    }, null, 2)}\n`);
  } finally {
    await client.end().catch(() => {});
  }
}

main().catch((error) => {
  logProgress("extractor.failed", {
    doc_id: argv.docId,
    error: error?.stack || error?.message || String(error),
  });
  process.exitCode = 1;
});
