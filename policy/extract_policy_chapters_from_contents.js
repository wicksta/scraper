#!/usr/bin/env node
import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;
const EXTRACTOR_VERSION = "policy_chapters_from_contents_v1";
const OPENAI_RETRY_ATTEMPTS = 5;
const OPENAI_RETRY_BASE_DELAY_MS = 1500;
const OPENAI_RETRY_MAX_DELAY_MS = 15000;

const argv = yargs(hideBin(process.argv))
  .scriptName("extract-policy-chapters-from-contents")
  .option("doc-id", {
    type: "string",
    demandOption: true,
    describe: "UUID of an existing public.documents policy document.",
  })
  .option("model", {
    type: "string",
    default: "gpt-5-mini",
    describe: "Model used for contents and anchor validation.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Write extracted chapters into public.policy_chapters.",
  })
  .option("checkpoint-path", {
    type: "string",
    default: "",
    describe: "Optional checkpoint file path for completed contents-led chapter extraction.",
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

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHeading(value) {
  return String(value || "")
    .replace(/^#+\s*/g, "")
    .replace(/\|/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripPolicySuffix(value) {
  return String(value || "")
    .replace(/\bpolicies\b/ig, "")
    .replace(/\bpolicy\b/ig, "")
    .replace(/\s+/g, " ")
    .trim();
}

function uniqueStrings(values) {
  const seen = new Set();
  const out = [];
  for (const value of values) {
    const clean = cleanText(value);
    if (!clean) continue;
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
  }
  return out;
}

function contentsScanSystemPrompt() {
  return [
    "You are extracting top-level chapter headings from the contents pages of a UK planning policy document.",
    "Read only the provided first pages of extracted text.",
    "Identify whether a contents section exists and return only top-level contents entries.",
    "Top-level means the main chapter or section level only.",
    "Do not return second-level policy lists, numbered policy items, area policies, site-specific entries, or subchapter rows.",
    "For example, return items like 'Context', 'Objectives', 'Spatial Strategy Policies', 'Housing Policies', 'Appendices'.",
    "Do not return items like '24. Soho Special Policy Area' or 'Policy H1'.",
    "Include main body chapters and top-level end matter if listed, including appendices, glossary, credits, or other back matter.",
    "Exclude policies, subheadings, figures, tables, maps, and glossary terms unless they are themselves top-level contents entries.",
    "Return strict JSON as {\"has_contents\": boolean, \"contents_pages\": [number], \"entries\": [{\"chapter_number\": string|null, \"chapter_title\": string, \"contents_label\": string|null, \"contents_page_ref\": number|null, \"entry_type\": string}]}",
  ].join("\n");
}

function contentsScanUserPrompt(doc, pages) {
  const payload = pages.map((page) => `===== PAGE ${page.page} =====\n${page.text}`).join("\n\n");
  return [
    `Document title: ${doc.title || "Untitled policy document"}`,
    "Review the following first pages of extracted text and identify the contents page block and its top-level chapter entries.",
    payload,
  ].join("\n\n");
}

function normalizeContentsEntries(entries) {
  const out = [];
  const seen = new Set();
  let index = 0;
  for (const raw of Array.isArray(entries) ? entries : []) {
    const chapterTitle = normalizeHeading(raw?.chapter_title);
    const chapterNumber = cleanText(raw?.chapter_number);
    const contentsLabel = cleanText(raw?.contents_label) || chapterTitle;
    const entryType = cleanText(raw?.entry_type) || "chapter";
    const contentsPageRef = Number(raw?.contents_page_ref || 0) || null;
    if (!chapterTitle) continue;
    const key = `${String(chapterNumber || "").toLowerCase()}|${chapterTitle.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      chapter_number: chapterNumber,
      chapter_title: chapterTitle,
      contents_label: contentsLabel,
      contents_page_ref: contentsPageRef,
      entry_type: entryType,
      _original_index: index,
    });
    index += 1;
  }
  return out.sort((a, b) => {
    const aRef = Number.isFinite(a.contents_page_ref) ? a.contents_page_ref : Number.MAX_SAFE_INTEGER;
    const bRef = Number.isFinite(b.contents_page_ref) ? b.contents_page_ref : Number.MAX_SAFE_INTEGER;
    if (aRef !== bRef) return aRef - bRef;
    const aNum = extractChapterSequenceNumber(a.chapter_number);
    const bNum = extractChapterSequenceNumber(b.chapter_number);
    const aNumVal = /^[0-9]+$/.test(String(aNum || "")) ? Number(aNum) : Number.MAX_SAFE_INTEGER;
    const bNumVal = /^[0-9]+$/.test(String(bNum || "")) ? Number(bNum) : Number.MAX_SAFE_INTEGER;
    if (aNumVal !== bNumVal) return aNumVal - bNumVal;
    return (a._original_index || 0) - (b._original_index || 0);
  });
}

function filterToLikelyTopLevelContents(entries) {
  if (entries.length <= 25) return entries;
  return entries.filter((entry) => {
    const num = extractChapterSequenceNumber(entry.chapter_number);
    const numVal = /^[0-9]+$/.test(String(num || "")) ? Number(num) : null;
    const title = String(entry.chapter_title || "");
    const isAppendixLike = /append|annex/i.test(String(entry.chapter_number || "")) || /append|annex|glossary|credit/i.test(title);
    const isMajorUntitled = !num && /context|approach|objective|strategy|allocation|housing|economy|connection|environment|design|heritage|monitoring|append|glossary|foreword/i.test(title);
    const isSmallChapterNumber = numVal !== null && numVal <= 15;
    return isAppendixLike || isMajorUntitled || isSmallChapterNumber;
  });
}

function extractChapterSequenceNumber(chapterNumber) {
  const match = String(chapterNumber || "").match(/(\d{1,3}|[A-Z])$/i) || String(chapterNumber || "").match(/(\d{1,3}|[A-Z])/i);
  return match ? String(match[1]).toUpperCase() : null;
}

function buildHeadingVariants(entry) {
  const number = extractChapterSequenceNumber(entry.chapter_number);
  const title = cleanText(entry.chapter_title) || "";
  const titleWithoutPolicy = cleanText(stripPolicySuffix(title)) || "";
  const normalizedTitle = normalizeText(title);
  const normalizedWithoutPolicy = normalizeText(titleWithoutPolicy);
  const variants = [
    title,
    titleWithoutPolicy,
    normalizedTitle,
    normalizedWithoutPolicy,
    cleanText(entry.contents_label),
    cleanText(stripPolicySuffix(entry.contents_label)),
  ];
  if (number) {
    variants.push(`chapter ${number}`);
    variants.push(`chapter ${number} ${title}`);
    if (titleWithoutPolicy) variants.push(`chapter ${number} ${titleWithoutPolicy}`);
    variants.push(`${number} ${title}`);
    if (titleWithoutPolicy) variants.push(`${number} ${titleWithoutPolicy}`);
    variants.push(`appendix ${number} ${title}`);
    variants.push(`annex ${number} ${title}`);
  }
  return uniqueStrings(variants);
}

function topLines(text, limit = 14) {
  return String(text || "")
    .split(/\r\n|\n|\r/)
    .map((line) => normalizeHeading(line))
    .filter(Boolean)
    .slice(0, limit);
}

function pageHasHeadingLikeMatch(entry, pageTopLines) {
  const topJoined = normalizeText(pageTopLines.join(" "));
  const titleVariants = buildHeadingVariants(entry)
    .map((variant) => normalizeText(variant))
    .filter(Boolean);
  for (const variant of titleVariants) {
    if (!variant) continue;
    if (topJoined.includes(variant)) return true;
  }
  const titleCore = normalizeText(stripPolicySuffix(entry.chapter_title));
  if (titleCore && topJoined.includes(titleCore)) return true;
  return false;
}

function scorePageForEntry(entry, page) {
  const pageTop = topLines(page.text, 14);
  const pageTopText = normalizeText(pageTop.join(" "));
  const fullText = normalizeText(page.text);
  const titleNorm = normalizeText(entry.chapter_title);
  const titleCore = normalizeText(stripPolicySuffix(entry.chapter_title));
  const number = extractChapterSequenceNumber(entry.chapter_number);
  let score = 0;
  let headingLike = false;

  if (titleNorm && pageTopText.includes(titleNorm)) {
    score += 90;
    headingLike = true;
  } else if (titleCore && pageTopText.includes(titleCore)) {
    score += 82;
    headingLike = true;
  } else if (titleNorm && fullText.includes(titleNorm)) {
    score += 18;
  } else if (titleCore && fullText.includes(titleCore)) {
    score += 14;
  }

  if (number && pageTopText.includes(`chapter ${normalizeText(number)}`)) {
    score += 28;
    headingLike = true;
  }
  if (number && pageTopText.startsWith(normalizeText(number))) {
    score += 22;
    headingLike = true;
  }

  for (const variant of buildHeadingVariants(entry)) {
    const norm = normalizeText(variant);
    if (!norm) continue;
    if (pageTopText.includes(norm)) {
      score += 22;
      headingLike = true;
      break;
    }
  }

  if (/^append/i.test(entry.entry_type || "") && pageTopText.includes("appendix")) score += 20;
  if (/^glossary$/i.test(entry.entry_type || "") && pageTopText.includes("glossary")) score += 25;
  if (/credit/i.test(entry.entry_type || "") && pageTopText.includes("credit")) score += 15;

  if (!headingLike && pageHasHeadingLikeMatch(entry, pageTop.slice(0, 5))) {
    headingLike = true;
    score += 15;
  }

  if (!headingLike && score < 30) {
    return { score: 0, snippet: pageTop.slice(0, 5).join(" | "), page: page.page, heading_like: false };
  }

  return { score, snippet: pageTop.slice(0, 5).join(" | "), page: page.page, heading_like: headingLike };
}

function findAnchorCandidates(entry, pages) {
  const candidates = [];
  for (const page of pages) {
    const scored = scorePageForEntry(entry, page);
    if (scored.score > 0) {
      candidates.push(scored);
    }
  }
  const headingLikeCandidates = candidates.filter((candidate) => candidate.heading_like);
  const pool = headingLikeCandidates.length ? headingLikeCandidates : candidates;
  return pool.sort((a, b) => b.score - a.score || a.page - b.page).slice(0, 8);
}

function strongAnchorCandidate(candidates) {
  if (!candidates.length) return null;
  const best = candidates[0];
  const next = candidates[1] || null;
  if (best.score >= 90) return best;
  if (best.score >= 75 && (!next || best.score - next.score >= 20)) return best;
  return null;
}

function anchorValidationSystemPrompt() {
  return [
    "You are validating the real extracted page start for a chapter heading in a UK planning policy document.",
    "Choose the best candidate page where the chapter or top-level end-matter heading actually begins in the extracted document.",
    "Prefer top-of-page heading matches over running references inside body text.",
    "Return strict JSON as {\"page_start\": number|null, \"reason\": string}.",
  ].join("\n");
}

function anchorValidationUserPrompt(entry, candidates) {
  return [
    "Resolve the best extracted page start for this contents entry.",
    JSON.stringify({
      entry,
      candidates,
    }, null, 2),
  ].join("\n\n");
}

async function validateAnchorWithLlm(entry, candidates) {
  const payload = await responsesJson({
    model: argv.model,
    systemPrompt: anchorValidationSystemPrompt(),
    userPrompt: anchorValidationUserPrompt(entry, candidates),
  });
  const pageStart = Number(payload?.page_start || 0) || null;
  return {
    page_start: pageStart,
    reason: cleanText(payload?.reason) || null,
  };
}

function inferContentsOffsetStats(resolvedEntries) {
  const deltas = resolvedEntries
    .filter((entry) => Number.isFinite(entry.contents_page_ref) && Number.isFinite(entry.page_start))
    .map((entry) => Number(entry.page_start) - Number(entry.contents_page_ref))
    .filter((delta) => Number.isFinite(delta));
  if (!deltas.length) return null;
  const sorted = [...deltas].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    median,
    count: sorted.length,
  };
}

function buildRangePageCandidates(anchorPages, estimatedPage, offsetStats, lowerBound = null, upperBound = null) {
  const baseLower = Math.max(1, Math.floor(estimatedPage + Math.min(0, (offsetStats?.min ?? 0) - (offsetStats?.median ?? 0)) - 3));
  const baseUpper = Math.ceil(estimatedPage + Math.max(0, (offsetStats?.max ?? 0) - (offsetStats?.median ?? 0)) + 3);
  const lower = lowerBound != null ? Math.max(baseLower, lowerBound) : baseLower;
  const upper = upperBound != null ? Math.min(baseUpper, upperBound) : baseUpper;
  let pages = anchorPages.filter((page) => page.page >= lower && page.page <= upper);
  if (!pages.length) {
    const fallbackLower = lowerBound != null ? Math.max(lowerBound, estimatedPage - 8) : Math.max(1, estimatedPage - 8);
    const fallbackUpper = upperBound != null ? Math.min(upperBound, estimatedPage + 8) : estimatedPage + 8;
    pages = anchorPages.filter((page) => page.page >= fallbackLower && page.page <= fallbackUpper);
  }
  const pageMap = new Map(anchorPages.map((page) => [page.page, page]));
  return pages.slice(0, 18).map((page) => {
    const prev = pageMap.get(page.page - 1) || null;
    const next = pageMap.get(page.page + 1) || null;
    return {
      page: page.page,
      top_lines: topLines(page.text, 8),
      text_preview: cleanText(String(page.text || "").slice(0, 1400)),
      page_window: {
        previous_page: prev ? {
          page: prev.page,
          top_lines: topLines(prev.text, 5),
          text_preview: cleanText(String(prev.text || "").slice(0, 700)),
        } : null,
        current_page: {
          page: page.page,
          top_lines: topLines(page.text, 8),
          text_preview: cleanText(String(page.text || "").slice(0, 1400)),
        },
        next_page: next ? {
          page: next.page,
          top_lines: topLines(next.text, 5),
          text_preview: cleanText(String(next.text || "").slice(0, 700)),
        } : null,
      },
    };
  });
}

function anchorBackfillSystemPrompt() {
  return [
    "You are locating the real extracted start page for a top-level chapter in a UK planning policy document.",
    "You are given a contents entry, an estimated page range derived from known chapter offsets, neighbor chapter bounds, and a set of candidate pages from that range.",
    "Choose the page where the top-level section actually begins, even if the exact wording differs slightly from the contents text.",
    "Examples: 'Spatial Strategy Policies' in contents may appear as 'Spatial Strategy' in the document.",
    "Prefer a clear top-level heading near the top of the page.",
    "Do not choose a page just because the body text discusses the same theme.",
    "Only choose a page within the supplied allowed range.",
    "Use previous and next resolved chapter bounds to keep the result in sequence.",
    "Use the surrounding page window to spot where a section genuinely starts across a page transition, not just isolated page-top text.",
    "Return strict JSON as {\"page_start\": number|null, \"reason\": string}.",
  ].join("\n");
}

function anchorBackfillUserPrompt(entry, estimatedPage, offsetStats, pageCandidates, lowerBound, upperBound, previousResolved, nextResolved) {
  return [
    "Resolve the best extracted page start for this contents entry using the estimated page range.",
    JSON.stringify({
      entry,
      estimated_page: estimatedPage,
      offset_stats: offsetStats,
      allowed_range: {
        min_page: lowerBound,
        max_page: upperBound,
      },
      previous_resolved: previousResolved || null,
      next_resolved: nextResolved || null,
      page_candidates: pageCandidates,
    }, null, 2),
  ].join("\n\n");
}

async function backfillAnchorWithOffsetLlm(entry, estimatedPage, offsetStats, pageCandidates, lowerBound, upperBound, previousResolved, nextResolved) {
  const payload = await responsesJson({
    model: argv.model,
    systemPrompt: anchorBackfillSystemPrompt(),
    userPrompt: anchorBackfillUserPrompt(entry, estimatedPage, offsetStats, pageCandidates, lowerBound, upperBound, previousResolved, nextResolved),
  });
  const pageStart = Number(payload?.page_start || 0) || null;
  return {
    page_start: pageStart && (lowerBound == null || pageStart >= lowerBound) && (upperBound == null || pageStart <= upperBound) ? pageStart : null,
    reason: cleanText(payload?.reason) || null,
  };
}

function dedupeResolvedEntries(entries) {
  const out = [];
  const seenPage = new Set();
  for (const entry of entries.sort((a, b) => a.page_start - b.page_start || String(a.chapter_title).localeCompare(String(b.chapter_title)))) {
    if (!entry.page_start) continue;
    const key = `${entry.page_start}|${String(entry.chapter_title).toLowerCase()}`;
    if (seenPage.has(key)) continue;
    seenPage.add(key);
    out.push(entry);
  }
  return out;
}

function numericChapterValue(entry) {
  const raw = extractChapterSequenceNumber(entry?.chapter_number);
  if (!/^[0-9]+$/.test(String(raw || ""))) return null;
  return Number(raw);
}

function enforceNumericChapterMonotonicity(resolvedEntries) {
  const orderedByContents = [...resolvedEntries].sort((a, b) => {
    const aRef = Number.isFinite(a.contents_page_ref) ? Number(a.contents_page_ref) : Number.MAX_SAFE_INTEGER;
    const bRef = Number.isFinite(b.contents_page_ref) ? Number(b.contents_page_ref) : Number.MAX_SAFE_INTEGER;
    if (aRef !== bRef) return aRef - bRef;
    return String(a.chapter_title || "").localeCompare(String(b.chapter_title || ""));
  });

  const kept = [];
  const rejected = [];
  let lastNumericPage = null;
  let lastNumericNumber = null;

  for (const entry of orderedByContents) {
    const chapterNum = numericChapterValue(entry);
    if (chapterNum == null) {
      kept.push(entry);
      continue;
    }
    if (lastNumericNumber != null && chapterNum > lastNumericNumber && Number(entry.page_start || 0) <= Number(lastNumericPage || 0)) {
      rejected.push({
        ...entry,
        monotonicity_reason: `chapter_${chapterNum}_resolved_before_chapter_${lastNumericNumber}`,
      });
      continue;
    }
    kept.push(entry);
    lastNumericNumber = chapterNum;
    lastNumericPage = Number(entry.page_start || 0);
  }

  return { kept, rejected };
}

function findNeighborBounds(entries, targetEntry, anchorPages) {
  const ordered = entries
    .filter((entry) => Number.isFinite(entry.page_start))
    .sort((a, b) => a.page_start - b.page_start);
  const targetRef = Number(targetEntry.contents_page_ref || Number.MAX_SAFE_INTEGER);
  let previousResolved = null;
  let nextResolved = null;
  for (const entry of ordered) {
    const ref = Number(entry.contents_page_ref || Number.MAX_SAFE_INTEGER);
    if (ref < targetRef) previousResolved = entry;
    if (ref > targetRef) {
      nextResolved = entry;
      break;
    }
  }
  const minAnchorPage = anchorPages[0]?.page || 1;
  const maxAnchorPage = anchorPages[anchorPages.length - 1]?.page || Number.MAX_SAFE_INTEGER;
  const lowerBound = previousResolved ? previousResolved.page_start + 1 : minAnchorPage;
  const upperBound = nextResolved ? Math.max(lowerBound, nextResolved.page_start - 1) : maxAnchorPage;
  return { lowerBound, upperBound, previousResolved, nextResolved };
}

function buildOrderedEntriesForBackfill(resolvedEntries, unresolvedEntries, offsetStats) {
  const combined = [];
  for (const entry of resolvedEntries) {
    combined.push({
      ...entry,
      _kind: "resolved",
      _sort_ref: Number.isFinite(entry.contents_page_ref) ? Number(entry.contents_page_ref) : Number.MAX_SAFE_INTEGER,
      _estimated_page: Number.isFinite(entry.page_start) ? Number(entry.page_start) : null,
    });
  }
  for (const entry of unresolvedEntries) {
    const contentsRef = Number.isFinite(entry.contents_page_ref) ? Number(entry.contents_page_ref) : null;
    const estimated = contentsRef != null && offsetStats ? contentsRef + Number(offsetStats.median || 0) : null;
    combined.push({
      ...entry,
      _kind: "unresolved",
      _sort_ref: contentsRef != null ? contentsRef : Number.MAX_SAFE_INTEGER,
      _estimated_page: estimated,
    });
  }
  return combined.sort((a, b) => {
    if (a._sort_ref !== b._sort_ref) return a._sort_ref - b._sort_ref;
    const aPage = a._estimated_page ?? Number.MAX_SAFE_INTEGER;
    const bPage = b._estimated_page ?? Number.MAX_SAFE_INTEGER;
    if (aPage !== bPage) return aPage - bPage;
    return String(a.chapter_title || "").localeCompare(String(b.chapter_title || ""));
  });
}

function findSequentialBackfillBounds(orderedEntries, targetEntry, anchorPages) {
  const index = orderedEntries.findIndex((entry) =>
    String(entry.chapter_title || "").toLowerCase() === String(targetEntry.chapter_title || "").toLowerCase()
    && String(entry.chapter_number || "") === String(targetEntry.chapter_number || "")
    && entry._kind === "unresolved"
  );
  const minAnchorPage = anchorPages[0]?.page || 1;
  const maxAnchorPage = anchorPages[anchorPages.length - 1]?.page || Number.MAX_SAFE_INTEGER;
  let previousResolved = null;
  let nextResolved = null;
  let previousEstimated = null;
  let nextEstimated = null;

  for (let i = index - 1; i >= 0; i -= 1) {
    const entry = orderedEntries[i];
    if (entry._kind === "resolved" && Number.isFinite(entry.page_start)) {
      previousResolved = entry;
      break;
    }
    if (previousEstimated == null && Number.isFinite(entry._estimated_page)) {
      previousEstimated = Number(entry._estimated_page);
    }
  }
  for (let i = index + 1; i < orderedEntries.length; i += 1) {
    const entry = orderedEntries[i];
    if (entry._kind === "resolved" && Number.isFinite(entry.page_start)) {
      nextResolved = entry;
      break;
    }
    if (nextEstimated == null && Number.isFinite(entry._estimated_page)) {
      nextEstimated = Number(entry._estimated_page);
    }
  }

  let lowerBound = previousResolved ? previousResolved.page_start + 1 : minAnchorPage;
  let upperBound = nextResolved ? Math.max(lowerBound, nextResolved.page_start - 1) : maxAnchorPage;
  const currentEstimated = Number.isFinite(targetEntry._estimated_page) ? Number(targetEntry._estimated_page) : null;

  if (currentEstimated != null && previousEstimated != null) {
    lowerBound = Math.max(lowerBound, Math.floor((previousEstimated + currentEstimated) / 2));
  }
  if (currentEstimated != null && nextEstimated != null) {
    upperBound = Math.min(upperBound, Math.ceil((currentEstimated + nextEstimated) / 2));
  }
  if (upperBound < lowerBound) {
    upperBound = lowerBound;
  }

  return {
    lowerBound,
    upperBound,
    previousResolved,
    nextResolved,
    previousEstimated,
    nextEstimated,
  };
}

async function detectChaptersFromContents(doc, pages) {
  const scanPages = pages.slice(0, 20);
  logProgress("contents_scan.start", { doc_id: doc.id, pages: scanPages.length });
  const contentsPayload = await responsesJson({
    model: argv.model,
    systemPrompt: contentsScanSystemPrompt(),
    userPrompt: contentsScanUserPrompt(doc, scanPages),
  });
  const contentsPages = Array.isArray(contentsPayload?.contents_pages) ? contentsPayload.contents_pages.map((v) => Number(v)).filter((v) => v > 0) : [];
  const entries = filterToLikelyTopLevelContents(normalizeContentsEntries(contentsPayload?.entries));
  const maxContentsPage = contentsPages.length ? Math.max(...contentsPages) : 0;
  const anchorPages = maxContentsPage > 0 ? pages.filter((page) => page.page > maxContentsPage) : pages;
  logProgress("contents_scan.completed", {
    doc_id: doc.id,
    has_contents: Boolean(contentsPayload?.has_contents),
    contents_pages: contentsPages,
    entries: entries.length,
  });
  if (!contentsPayload?.has_contents || entries.length < 2) {
    throw new Error("No usable contents page found in first 20 pages.");
  }

  const resolved = [];
  const unresolved = [];
  let lastResolvedPage = maxContentsPage;
  for (const entry of entries) {
    const forwardPages = anchorPages.filter((page) => page.page > lastResolvedPage);
    let candidates = findAnchorCandidates(entry, forwardPages);
    if (!candidates.length) {
      candidates = findAnchorCandidates(entry, anchorPages);
    }
    const strong = strongAnchorCandidate(candidates);
    if (strong) {
      resolved.push({
        ...entry,
        page_start: strong.page,
        source: "heuristic_anchor",
        source_meta_json: {
          anchor_method: "heuristic",
          anchor_score: strong.score,
          anchor_snippet: strong.snippet,
          candidate_count: candidates.length,
        },
      });
      lastResolvedPage = strong.page;
      continue;
    }
    if (!candidates.length) {
      unresolved.push({ ...entry, reason: "no_candidates" });
      continue;
    }
    logProgress("anchor_validation.start", {
      doc_id: doc.id,
      chapter_title: entry.chapter_title,
      chapter_number: entry.chapter_number || null,
      candidates: candidates.length,
    });
    const validated = await validateAnchorWithLlm(entry, candidates);
    if (validated.page_start) {
      resolved.push({
        ...entry,
        page_start: validated.page_start,
        source: "llm_anchor_validation",
        source_meta_json: {
          anchor_method: "llm_validation",
          validation_reason: validated.reason,
          candidates: candidates.slice(0, 5),
        },
      });
      lastResolvedPage = validated.page_start;
      continue;
    }
    unresolved.push({ ...entry, reason: validated.reason || "llm_not_found" });
  }

  const offsetStats = inferContentsOffsetStats(resolved);
  if (offsetStats && unresolved.length) {
    const orderedEntries = buildOrderedEntriesForBackfill(resolved, unresolved, offsetStats);
    const remaining = [];
    for (const entry of unresolved) {
      if (!Number.isFinite(entry.contents_page_ref)) {
        remaining.push(entry);
        continue;
      }
      const estimatedPage = Number(entry.contents_page_ref) + Number(offsetStats.median || 0);
      const { lowerBound, upperBound, previousResolved, nextResolved } = findSequentialBackfillBounds(orderedEntries, {
        ...entry,
        _kind: "unresolved",
        _estimated_page: estimatedPage,
      }, anchorPages);
      const pageCandidates = buildRangePageCandidates(anchorPages, estimatedPage, offsetStats, lowerBound, upperBound);
      if (!pageCandidates.length) {
        remaining.push(entry);
        continue;
      }
      logProgress("anchor_backfill.start", {
        doc_id: doc.id,
        chapter_title: entry.chapter_title,
        chapter_number: entry.chapter_number || null,
        estimated_page: estimatedPage,
        lower_bound: lowerBound,
        upper_bound: upperBound,
        candidate_pages: pageCandidates.length,
      });
      const backfilled = await backfillAnchorWithOffsetLlm(entry, estimatedPage, offsetStats, pageCandidates, lowerBound, upperBound, previousResolved, nextResolved);
      if (backfilled.page_start) {
        resolved.push({
          ...entry,
          page_start: backfilled.page_start,
          source: "offset_backfill_validation",
          source_meta_json: {
            anchor_method: "offset_backfill_llm",
            estimated_page: estimatedPage,
            offset_stats: offsetStats,
            validation_reason: backfilled.reason,
          },
        });
        continue;
      }
      remaining.push({ ...entry, reason: backfilled.reason || entry.reason || "offset_backfill_not_found" });
    }
    unresolved.length = 0;
    unresolved.push(...remaining);
  }

  const deduped = dedupeResolvedEntries(resolved);
  const monotonic = enforceNumericChapterMonotonicity(deduped);
  if (monotonic.rejected.length) {
    unresolved.push(...monotonic.rejected.map((entry) => ({
      ...entry,
      reason: entry.monotonicity_reason || "numeric_monotonicity_failed",
    })));
  }
  const finalResolved = monotonic.kept;
  logProgress("anchor_resolution.completed", {
    doc_id: doc.id,
    resolved: finalResolved.length,
    unresolved: unresolved.length,
  });
  if (finalResolved.length < 2) {
    throw new Error("Unable to resolve enough chapter headings to real extracted pages.");
  }

  if (unresolved.length && offsetStats) {
    const remaining = [];
    const monotonicOrdered = buildOrderedEntriesForBackfill(finalResolved, unresolved, offsetStats);
    const additionalResolved = [];
    for (const entry of unresolved) {
      if (!Number.isFinite(entry.contents_page_ref)) {
        remaining.push(entry);
        continue;
      }
      const estimatedPage = Number(entry.contents_page_ref) + Number(offsetStats.median || 0);
      const { lowerBound, upperBound, previousResolved, nextResolved } = findSequentialBackfillBounds(monotonicOrdered, {
        ...entry,
        _kind: "unresolved",
        _estimated_page: estimatedPage,
      }, anchorPages);
      const strictLower = previousResolved ? Math.max(lowerBound, Number(previousResolved.page_start || 0) + 1) : lowerBound;
      const strictUpper = nextResolved ? Math.min(upperBound, Number(nextResolved.page_start || 0) - 1) : upperBound;
      const pageCandidates = buildRangePageCandidates(anchorPages, estimatedPage, offsetStats, strictLower, strictUpper);
      if (!pageCandidates.length) {
        remaining.push(entry);
        continue;
      }
      logProgress("anchor_recheck.start", {
        doc_id: doc.id,
        chapter_title: entry.chapter_title,
        chapter_number: entry.chapter_number || null,
        estimated_page: estimatedPage,
        lower_bound: strictLower,
        upper_bound: strictUpper,
        candidate_pages: pageCandidates.length,
      });
      const backfilled = await backfillAnchorWithOffsetLlm(entry, estimatedPage, offsetStats, pageCandidates, strictLower, strictUpper, previousResolved, nextResolved);
      if (backfilled.page_start) {
        additionalResolved.push({
          ...entry,
          page_start: backfilled.page_start,
          source: "numeric_monotonicity_recheck",
          source_meta_json: {
            anchor_method: "numeric_monotonicity_recheck",
            estimated_page: estimatedPage,
            offset_stats: offsetStats,
            validation_reason: backfilled.reason,
          },
        });
      } else {
        remaining.push({
          ...entry,
          reason: backfilled.reason || entry.reason || "numeric_monotonicity_recheck_failed",
        });
      }
    }
    const rechecked = dedupeResolvedEntries([...finalResolved, ...additionalResolved]);
    const monotonicAgain = enforceNumericChapterMonotonicity(rechecked);
    unresolved.length = 0;
    unresolved.push(...remaining, ...monotonicAgain.rejected.map((entry) => ({
      ...entry,
      reason: entry.monotonicity_reason || "numeric_monotonicity_failed_after_recheck",
    })));
    return { chapters: monotonicAgain.kept, unresolved, contents_pages: contentsPages };
  }

  return { chapters: finalResolved, unresolved, contents_pages: contentsPages };
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
        entry_type: current.entry_type || "chapter",
        contents_label: current.contents_label || current.chapter_title,
        contents_page_ref: current.contents_page_ref || null,
        contents_pages: detectionMeta.contents_pages || [],
        unresolved_entries: detectionMeta.unresolved || [],
        extractor_version: EXTRACTOR_VERSION,
        extraction_model: argv.model,
        detection_mode: "llm_contents_then_anchor",
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
    if (!pages.length) throw new Error("No page text available for contents-led chapter extraction");

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

    const detection = await detectChaptersFromContents(doc, pages);
    const chapters = buildChaptersFromStarts(detection.chapters, pages, {
      contents_pages: detection.contents_pages,
      unresolved: detection.unresolved,
      doc_title: doc.title || null,
    });

    writeCheckpoint(checkpointPath, {
      version: 1,
      doc_id: argv.docId,
      detection_mode: "llm_contents_then_anchor",
      chapters,
      updated_at: new Date().toISOString(),
    });

    logProgress("merge.completed", {
      doc_id: argv.docId,
      detection_mode: "llm_contents_then_anchor",
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
      detection_mode: "llm_contents_then_anchor",
      extractor_version: EXTRACTOR_VERSION,
      model: argv.model,
      unresolved_entries: detection.unresolved,
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
