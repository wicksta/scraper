#!/usr/bin/env node
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;
const EXTRACTOR_VERSION = "policy_chapters_v1";
const OPENAI_RETRY_ATTEMPTS = 5;
const OPENAI_RETRY_BASE_DELAY_MS = 1500;
const OPENAI_RETRY_MAX_DELAY_MS = 15000;

const argv = yargs(hideBin(process.argv))
  .scriptName("extract-policy-chapters")
  .option("doc-id", {
    type: "string",
    demandOption: true,
    describe: "UUID of an existing public.documents policy document.",
  })
  .option("model", {
    type: "string",
    default: "gpt-5-mini",
    describe: "Model used only when chapter boundaries need LLM fallback.",
  })
  .option("page-start", {
    type: "number",
    default: 0,
    describe: "Optional first page to include.",
  })
  .option("page-end", {
    type: "number",
    default: 0,
    describe: "Optional last page to include.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Write extracted chapters into public.policy_chapters.",
  })
  .option("checkpoint-path", {
    type: "string",
    default: "",
    describe: "Optional checkpoint file path for completed chapter-detection windows.",
  })
  .strict()
  .help()
  .argv;

function logProgress(event, payload = {}) {
  process.stderr.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
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

function filterPagesByRange(pages, pageStart, pageEnd) {
  const start = Number(pageStart) || 0;
  const end = Number(pageEnd) || 0;
  if (start <= 0 && end <= 0) return pages;
  const min = start > 0 ? start : 1;
  const max = end > 0 ? end : Number.MAX_SAFE_INTEGER;
  return pages.filter((page) => page.page >= min && page.page <= max);
}

function nonEmptyLines(text, limit = 20) {
  return String(text || "")
    .split(/\r\n|\n|\r/)
    .map((line) => cleanText(line))
    .filter(Boolean)
    .slice(0, limit);
}

function normalizeHeadingTitle(text) {
  return String(text || "")
    .replace(/^#+\s*/g, "")
    .replace(/\|/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\s+[.·•]+$/g, "")
    .trim();
}

function dedupeRepeatedTitleParts(text) {
  const cleaned = normalizeHeadingTitle(text);
  if (!cleaned) return cleaned;
  const words = cleaned.split(/\s+/).filter(Boolean);
  if (words.length >= 2 && words.length % 2 === 0) {
    const half = words.length / 2;
    const firstHalf = words.slice(0, half).join(" ");
    const secondHalf = words.slice(half).join(" ");
    if (firstHalf.toLowerCase() === secondHalf.toLowerCase()) {
      return firstHalf;
    }
  }
  return cleaned.replace(/\b([A-Za-z][A-Za-z'&/-]*)\s+\1\b/gi, "$1").replace(/\s+/g, " ").trim();
}

function parseContentsTableLine(rawLine) {
  const line = String(rawLine || "").trim();
  if (!line.startsWith("|")) return null;
  const cells = line
    .split("|")
    .map((cell) => normalizeHeadingTitle(cell))
    .filter(Boolean);
  if (cells.length < 2) return null;
  const lastCell = cells[cells.length - 1];
  if (!/^\d{1,4}$/.test(lastCell)) return null;
  const pageStart = Number(lastCell);
  const textCells = cells.slice(0, -1).filter((cell) => !/^[-:]+$/.test(cell));
  if (!textCells.length) return null;
  const uniqueTextCells = [];
  for (const cell of textCells) {
    if (!uniqueTextCells.length || uniqueTextCells[uniqueTextCells.length - 1].toLowerCase() !== cell.toLowerCase()) {
      uniqueTextCells.push(cell);
    }
  }
  const title = dedupeRepeatedTitleParts(uniqueTextCells.join(" "));
  if (!title) return null;
  return { title, pageStart };
}

function detectContentsEntries(pages) {
  const entries = [];
  const seen = new Set();
  let contentsMode = false;
  let sawExplicitChapter = false;
  for (const page of pages.slice(0, 40)) {
    const lower = String(page.text || "").toLowerCase();
    const pipeLineCount = (String(page.text || "").match(/\|/g) || []).length;
    if (lower.includes("contents")) {
      contentsMode = true;
    } else if (contentsMode && pipeLineCount < 8) {
      contentsMode = false;
    }
    if (!contentsMode) continue;
    for (const rawLine of String(page.text || "").split(/\r\n|\n|\r/)) {
      const line = normalizeHeadingTitle(rawLine);
      if (!line) continue;
      const tableRow = parseContentsTableLine(rawLine);
      if (tableRow) {
        const titleOnly = dedupeRepeatedTitleParts(tableRow.title);
        const pageStart = Number(tableRow.pageStart || 0);
        if (
          !titleOnly
          || pageStart <= 0
          || /^(policy|figure|figures|table|tables|map|diagram|appendix|annex|glossary|main contents)\b/i.test(titleOnly)
        ) {
          continue;
        }
        const key = `${pageStart}:${titleOnly.toLowerCase()}`;
        if (seen.has(key)) continue;
        seen.add(key);
        entries.push({
          chapter_number: null,
          chapter_title: titleOnly,
          page_start: pageStart,
          source: "contents",
          heading_path_json: ["Contents", titleOnly],
          source_meta_json: { detected_from: "contents", contents_page: page.page, parsed_from: "table_row" },
        });
        continue;
      }
      let match = line.match(/^(chapter|part)\s+([a-z0-9]+)\s*[:.\-–]?\s*(.+?)\s+(\d{1,4})$/i);
      if (match) {
        sawExplicitChapter = true;
        const chapterNumber = `${match[1]} ${match[2]}`.replace(/\s+/g, " ").trim();
        const chapterTitle = dedupeRepeatedTitleParts(match[3]);
        const pageStart = Number(match[4] || 0);
        if (!chapterTitle || pageStart <= 0) continue;
        const key = `${pageStart}:${chapterTitle.toLowerCase()}`;
        if (seen.has(key)) continue;
        seen.add(key);
        entries.push({
          chapter_number: chapterNumber,
          chapter_title: chapterTitle,
          page_start: pageStart,
          source: "contents",
          heading_path_json: ["Contents", chapterTitle],
          source_meta_json: { detected_from: "contents", contents_page: page.page },
        });
        continue;
      }

      if (sawExplicitChapter) continue;
      match = line.match(/^([a-z][a-z0-9 '&()\/\-]{4,120})\s+(\d{1,4})$/i);
      if (!match) continue;
      const titleOnly = dedupeRepeatedTitleParts(match[1]);
      const pageStart = Number(match[2] || 0);
      if (
        !titleOnly
        || pageStart <= 0
        || /^(policy|figure|figures|table|tables|map|diagram|appendix|annex|glossary|main contents)\b/i.test(titleOnly)
      ) {
        continue;
      }
      const key = `${pageStart}:${titleOnly.toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);
      entries.push({
        chapter_number: null,
        chapter_title: titleOnly,
        page_start: pageStart,
        source: "contents",
        heading_path_json: ["Contents", titleOnly],
        source_meta_json: { detected_from: "contents", contents_page: page.page },
      });
    }
  }
  return entries.sort((a, b) => a.page_start - b.page_start);
}

function isDubiousChapterTitle(title) {
  const normalized = dedupeRepeatedTitleParts(title);
  if (!normalized) return true;
  if (/\b([A-Za-z][A-Za-z'&/-]*)\s+\1\b/i.test(normalized)) return true;
  if (/^(contents|main contents)$/i.test(normalized)) return true;
  return false;
}

function hasNearbyHeadingMatch(entry, headingCandidates) {
  const title = String(entry.chapter_title || "").toLowerCase();
  return headingCandidates.some((candidate) => {
    const candidateTitle = String(candidate.chapter_title || "").toLowerCase();
    const pageDelta = Math.abs(Number(candidate.page_start || 0) - Number(entry.page_start || 0));
    if (pageDelta > 2) return false;
    return candidateTitle === title || candidateTitle.includes(title) || title.includes(candidateTitle);
  });
}

function shouldUseLlmReconciliation(contentsEntries, headingCandidates, merged) {
  if (!merged.length) return true;
  const dubiousTitles = merged.filter((entry) => isDubiousChapterTitle(entry.chapter_title)).length;
  const nearbyMatches = merged.filter((entry) => hasNearbyHeadingMatch(entry, headingCandidates)).length;
  const veryLongFirstSpan = merged.length >= 2 && (merged[1].page_start - merged[0].page_start) > 35;
  const mostlyUnmatched = merged.length >= 3 && nearbyMatches / merged.length < 0.4;
  const manyDubious = dubiousTitles > 0;
  const weakContents = contentsEntries.length >= 2 && merged.every((entry) => !entry.chapter_number);
  return manyDubious || mostlyUnmatched || veryLongFirstSpan || weakContents;
}

function normalizeChapterNumber(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function inferContentsOffset(contentsEntries, headingCandidates) {
  const deltas = new Map();
  for (const entry of contentsEntries) {
    if (!entry.chapter_number) continue;
    const chapterNumber = normalizeChapterNumber(entry.chapter_number);
    const matches = headingCandidates.filter((candidate) => normalizeChapterNumber(candidate.chapter_number) === chapterNumber);
    for (const match of matches) {
      const delta = Number(match.page_start || 0) - Number(entry.page_start || 0);
      if (delta < 0 || delta > 40) continue;
      deltas.set(delta, (deltas.get(delta) || 0) + 1);
    }
  }
  let bestDelta = 0;
  let bestCount = 0;
  for (const [delta, count] of deltas.entries()) {
    if (count > bestCount) {
      bestDelta = delta;
      bestCount = count;
    }
  }
  return bestCount > 0 ? bestDelta : 0;
}

function detectHeadingCandidates(pages) {
  const candidates = [];
  const seen = new Set();
  for (const page of pages) {
    const lines = nonEmptyLines(page.text, 8);
    if (!lines.length) continue;
    const first = normalizeHeadingTitle(lines[0]);
    const second = normalizeHeadingTitle(lines[1] || "");

    let chapterNumber = null;
    let chapterTitle = null;

    let match = first.match(/^(chapter|part)\s+([a-z0-9]+)\b(?:\s*[:.\-–]?\s*(.+))?$/i);
    if (match) {
      chapterNumber = `${match[1]} ${match[2]}`.replace(/\s+/g, " ").trim();
      chapterTitle = normalizeHeadingTitle((match[3] || second || "").replace(/\s+\d+(?:\s+\d+)*$/, ""));
    } else {
      match = first.match(/^(\d{1,2})\s+([A-Z][A-Za-z0-9 ,&'\/()\-]{3,120})$/);
      if (match && !/[.:;]$/.test(first)) {
        chapterNumber = match[1];
        chapterTitle = normalizeHeadingTitle(match[2]);
      }
      if (!chapterTitle && second && !/[.:;]$/.test(second) && second.length < 120 && !/^\d+(\.\d+)*\b/.test(second)) {
        chapterTitle = normalizeHeadingTitle(second.replace(/\s+\d+(?:\s+\d+)*$/, ""));
      }
    }

    if (!chapterTitle || chapterTitle.length < 3) continue;
    const key = `${page.page}:${chapterNumber || ""}:${chapterTitle.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    candidates.push({
      chapter_number: chapterNumber,
      chapter_title: chapterTitle,
      page_start: page.page,
      source: "heading_scan",
      heading_path_json: [chapterTitle],
      source_meta_json: {
        detected_from: "heading_scan",
        page_top_lines: lines.slice(0, 3),
      },
    });
  }
  return candidates.sort((a, b) => a.page_start - b.page_start);
}

function mergeChapterStarts(contentsEntries, headingCandidates, maxPage) {
  const byPage = new Map();
  const preferContentsOnly = contentsEntries.length >= 2;
  const contentsOffset = preferContentsOnly ? inferContentsOffset(contentsEntries, headingCandidates) : 0;
  for (const rawEntry of contentsEntries) {
    const entry = {
      ...rawEntry,
      page_start: rawEntry.page_start + contentsOffset,
      source_meta_json: {
        ...(rawEntry.source_meta_json || {}),
        contents_page_offset: contentsOffset,
      },
    };
    if (entry.page_start <= 0 || entry.page_start > maxPage) continue;
    byPage.set(entry.page_start, { ...entry });
  }
  for (const candidate of headingCandidates) {
    if (candidate.page_start <= 0 || candidate.page_start > maxPage) continue;
    const existing = byPage.get(candidate.page_start);
    if (!existing) {
      if (!preferContentsOnly) {
        byPage.set(candidate.page_start, { ...candidate });
      }
      continue;
    }
    existing.chapter_number = existing.chapter_number || candidate.chapter_number;
    if (!existing.chapter_title && candidate.chapter_title) existing.chapter_title = candidate.chapter_title;
    existing.source_meta_json = {
      ...(existing.source_meta_json || {}),
      merged_heading_scan: true,
      heading_scan_title: candidate.chapter_title || null,
    };
  }

  const merged = Array.from(byPage.values())
    .filter((entry) => cleanText(entry.chapter_title))
    .sort((a, b) => a.page_start - b.page_start);

  return merged.filter((entry, index) => {
    if (index === 0) return true;
    const prev = merged[index - 1];
    return !(prev.page_start === entry.page_start && String(prev.chapter_title).toLowerCase() === String(entry.chapter_title).toLowerCase());
  });
}

function chapterFallbackSystemPrompt() {
  return [
    "You extract top-level chapters from UK planning policy documents.",
    "Return only major chapter or part boundaries, not subchapters, policies, appendices, or numbered paragraphs.",
    "Prefer entries that represent the main chapter structure of the document.",
    "Return strict JSON as {\"chapters\": [{\"chapter_number\": string|null, \"chapter_title\": string, \"page_start\": number}]}.",
  ].join("\n");
}

function chapterReviewSystemPrompt() {
  return [
    "You are reviewing a provisional top-level chapter list for a UK planning policy document.",
    "Your job is to spot obvious omissions, numbering gaps, or clearly wrong chapter titles/page starts.",
    "Be conservative: only change the list if the evidence from contents entries or heading candidates makes the issue clear.",
    "Do not add policies, subchapters, figures, tables, glossary terms, appendices, or numbered paragraphs unless they are true top-level chapters.",
    "If a chapter is missing, insert it in the right order using the best supported page start.",
    "Return strict JSON as {\"chapters\": [{\"chapter_number\": string|null, \"chapter_title\": string, \"page_start\": number}]}.",
  ].join("\n");
}

function chapterFallbackUserPrompt(doc, contentsEntries, headingCandidates, pageCount) {
  return [
    `Document title: ${doc.title || "Untitled policy document"}`,
    `Page count: ${pageCount}`,
    "Choose the top-level chapter starts for this document.",
    "Use the contents entries if they are reliable. Otherwise use the page-top heading candidates.",
    "Some contents tables are messy and may duplicate cells like 'CONTEXT CONTEXT'. Clean those up rather than copying them literally.",
    "Prefer a sensible human chapter structure over a mechanically parsed contents table.",
    "Do not return policies, subchapters, appendices, or glossary headings unless they are truly top-level chapters.",
    "",
    "Contents entries:",
    JSON.stringify(contentsEntries.slice(0, 80), null, 2),
    "",
    "Page-top heading candidates:",
    JSON.stringify(headingCandidates.slice(0, 120), null, 2),
  ].join("\n");
}

function chapterReviewUserPrompt(doc, provisionalChapters, contentsEntries, headingCandidates, pageCount) {
  return [
    `Document title: ${doc.title || "Untitled policy document"}`,
    `Page count: ${pageCount}`,
    "Review the provisional top-level chapter list below.",
    "Check whether any obvious chapter is missing, misnumbered, or has the wrong title/page start.",
    "Only change the list when the contents entries or page-top heading candidates clearly support the correction.",
    "",
    "Provisional chapters:",
    JSON.stringify(provisionalChapters, null, 2),
    "",
    "Contents entries:",
    JSON.stringify(contentsEntries.slice(0, 120), null, 2),
    "",
    "Page-top heading candidates:",
    JSON.stringify(headingCandidates.slice(0, 160), null, 2),
  ].join("\n");
}

function normalizeFallbackChapters(rawChapters, maxPage) {
  const out = [];
  const seen = new Set();
  for (const raw of Array.isArray(rawChapters) ? rawChapters : []) {
    const title = normalizeHeadingTitle(raw?.chapter_title);
    const number = cleanText(raw?.chapter_number);
    const pageStart = Number(raw?.page_start || 0);
    if (!title || pageStart <= 0 || pageStart > maxPage) continue;
    const key = `${pageStart}:${title.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      chapter_number: number,
      chapter_title: title,
      page_start: pageStart,
      source: "llm_fallback",
      heading_path_json: [title],
      source_meta_json: { detected_from: "llm_fallback" },
    });
  }
  return out.sort((a, b) => a.page_start - b.page_start);
}

function extractChapterSequenceNumber(chapterNumber) {
  const match = String(chapterNumber || "").match(/(\d{1,3})/);
  return match ? Number(match[1]) : null;
}

function hasNumberingGap(chapters) {
  const numbers = chapters
    .map((chapter) => extractChapterSequenceNumber(chapter.chapter_number))
    .filter((value) => Number.isFinite(value));
  if (numbers.length < 3) return false;
  for (let i = 1; i < numbers.length; i += 1) {
    if (numbers[i] - numbers[i - 1] > 1) return true;
  }
  return false;
}

async function reviewChapterStarts(doc, provisionalChapters, contentsEntries, headingCandidates, maxPage) {
  if (!Array.isArray(provisionalChapters) || provisionalChapters.length < 2) {
    return provisionalChapters;
  }
  logProgress("chapters.review.start", {
    doc_id: doc.id,
    provisional_count: provisionalChapters.length,
    numbering_gap: hasNumberingGap(provisionalChapters),
  });
  const payload = await responsesJson({
    model: argv.model,
    systemPrompt: chapterReviewSystemPrompt(),
    userPrompt: chapterReviewUserPrompt(doc, provisionalChapters, contentsEntries, headingCandidates, maxPage),
  });
  const normalized = normalizeFallbackChapters(payload?.chapters, maxPage).map((chapter) => ({
    ...chapter,
    source: "llm_review",
    source_meta_json: { detected_from: "llm_review" },
  }));
  if (normalized.length < 2) {
    logProgress("chapters.review.discarded", {
      doc_id: doc.id,
      returned_count: normalized.length,
    });
    return provisionalChapters;
  }
  logProgress("chapters.review.completed", {
    doc_id: doc.id,
    original_count: provisionalChapters.length,
    revised_count: normalized.length,
  });
  return normalized;
}

async function detectChapterStarts(doc, pages) {
  const contentsEntries = detectContentsEntries(pages);
  const headingCandidates = detectHeadingCandidates(pages);
  logProgress("chapters.candidates", {
    doc_id: doc.id,
    contents_entries: contentsEntries.length,
    heading_candidates: headingCandidates.length,
  });

  const merged = mergeChapterStarts(contentsEntries, headingCandidates, pages[pages.length - 1]?.page || 0);
  if (merged.length >= 2 && !shouldUseLlmReconciliation(contentsEntries, headingCandidates, merged)) {
    const reviewed = await reviewChapterStarts(
      doc,
      merged,
      contentsEntries,
      headingCandidates,
      pages[pages.length - 1]?.page || 0,
    );
    const reviewChanged = JSON.stringify(reviewed) !== JSON.stringify(merged);
    return {
      chapters: reviewed,
      detection_mode: reviewChanged
        ? `${contentsEntries.length ? "deterministic_contents" : "deterministic_heading_scan"}+llm_review`
        : (contentsEntries.length ? "deterministic_contents" : "deterministic_heading_scan"),
    };
  }

  logProgress("chapters.fallback.start", {
    doc_id: doc.id,
    contents_entries: contentsEntries.length,
    heading_candidates: headingCandidates.length,
    reconciliation: merged.length >= 2,
  });
  const payload = await responsesJson({
    model: argv.model,
    systemPrompt: chapterFallbackSystemPrompt(),
    userPrompt: chapterFallbackUserPrompt(doc, contentsEntries, headingCandidates, pages.length),
  });
  const normalized = normalizeFallbackChapters(payload?.chapters, pages[pages.length - 1]?.page || 0);
  const reviewed = await reviewChapterStarts(
    doc,
    normalized,
    contentsEntries,
    headingCandidates,
    pages[pages.length - 1]?.page || 0,
  );
  const reviewChanged = JSON.stringify(reviewed) !== JSON.stringify(normalized);
  return {
    chapters: reviewed,
    detection_mode: reviewChanged ? "llm_fallback+llm_review" : "llm_fallback",
  };
}

function pagesToChapterText(pages) {
  return pages
    .map((page) => `===== PAGE ${page.page} =====\n${page.text}`)
    .join("\n\n");
}

function buildChaptersFromStarts(chapterStarts, pages) {
  const maxPage = pages[pages.length - 1]?.page || 0;
  const chapters = [];
  for (let i = 0; i < chapterStarts.length; i += 1) {
    const current = chapterStarts[i];
    const next = chapterStarts[i + 1];
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
      heading_path_json: Array.isArray(current.heading_path_json) ? current.heading_path_json : [current.chapter_title],
      source_meta_json: sanitizeJsonValue({
        ...(current.source_meta_json || {}),
        chapter_detection_mode: current.source || null,
      }),
    });
  }
  return chapters.filter((chapter) => chapter.chapter_title && chapter.page_start > 0 && chapter.page_end >= chapter.page_start);
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
    if (!pages.length) throw new Error("No page text available for policy chapter extraction");

    const filteredPages = filterPagesByRange(pages, argv.pageStart, argv.pageEnd);
    if (!filteredPages.length) {
      throw new Error(`No pages available in requested range ${argv.pageStart || "start"}-${argv.pageEnd || "end"}`);
    }

    logProgress("pages.ready", {
      doc_id: argv.docId,
      pages: filteredPages.length,
      pages_total: pages.length,
      page_start: argv.pageStart || null,
      page_end: argv.pageEnd || null,
    });

    const checkpointPath = cleanText(argv.checkpointPath);
    const checkpoint = readCheckpoint(checkpointPath);
    if (checkpoint && Array.isArray(checkpoint.chapters) && checkpoint.doc_id === argv.docId) {
      logProgress("checkpoint.resumed", {
        path: checkpointPath,
        chapters: checkpoint.chapters.length,
      });
      const stored = argv.apply ? await storeChapters(client, argv.docId, checkpoint.chapters) : 0;
      deleteCheckpoint(checkpointPath);
      process.stdout.write(`${JSON.stringify({
        success: true,
        doc_id: argv.docId,
        doc_title: doc.title || null,
        pages: filteredPages.length,
        pages_total: pages.length,
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

    const detection = await detectChapterStarts(doc, filteredPages);
    const chapters = buildChaptersFromStarts(detection.chapters, filteredPages).map((chapter) => ({
      ...chapter,
      source_meta_json: {
        ...(chapter.source_meta_json || {}),
        extractor_version: EXTRACTOR_VERSION,
        extraction_model: detection.detection_mode === "llm_fallback" ? argv.model : null,
        detection_mode: detection.detection_mode,
        doc_title: doc.title || null,
      },
    }));

    writeCheckpoint(checkpointPath, {
      version: 1,
      doc_id: argv.docId,
      detection_mode: detection.detection_mode,
      chapters,
      updated_at: new Date().toISOString(),
    });

    logProgress("merge.completed", {
      doc_id: argv.docId,
      detection_mode: detection.detection_mode,
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
      pages: filteredPages.length,
      pages_total: pages.length,
      chapters,
      chapters_count: chapters.length,
      stored_chapters: stored,
      apply: Boolean(argv.apply),
      detection_mode: detection.detection_mode,
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
    error: error?.message || String(error),
  });
  process.stderr.write(`${error?.stack || error?.message || String(error)}\n`);
  process.exit(1);
});
