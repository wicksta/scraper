import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import mysql from "mysql2/promise";
import { ImapFlow } from "imapflow";
import nodemailer from "nodemailer";

const execFileAsync = promisify(execFile);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requireEnv(name, fallback = null) {
  const value = process.env[name] ?? fallback;
  if (value == null || String(value).trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return String(value).trim();
}

function parseBool(value, defaultValue = false) {
  if (value == null || value === "") return defaultValue;
  return /^(1|true|yes|on)$/i.test(String(value).trim());
}

function decodeQuotedPrintable(value) {
  const source = String(value || "").replace(/=\r?\n/g, "");
  const bytes = [];
  let text = "";
  for (let index = 0; index < source.length; index += 1) {
    if (source[index] === "=" && /^[A-Fa-f0-9]{2}$/.test(source.slice(index + 1, index + 3))) {
      bytes.push(parseInt(source.slice(index + 1, index + 3), 16));
      index += 2;
      continue;
    }
    if (bytes.length) { text += Buffer.from(bytes).toString("utf8"); bytes.length = 0; }
    text += source[index];
  }
  if (bytes.length) text += Buffer.from(bytes).toString("utf8");
  return text;
}

function stripHtml(value) {
  return String(value || "")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<\/div>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#39;/gi, "'")
    .replace(/&quot;/gi, '"');
}

function normalizeWhitespace(value) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function looksLikeMinutesRequest(subject, bodyText) {
  const text = `${String(subject || "")}\n${String(bodyText || "")}`.toLowerCase();
  const lines = normalizeWhitespace(bodyText || "").split("\n").map((line) => line.trim()).filter(Boolean);

  const hasExplicitMinutesRequest =
    text.includes("minutes format") ||
    text.includes("meeting minutes") ||
    text.includes("put these into minutes") ||
    text.includes("please put these into minutes");

  const hasMeetingSignal =
    text.includes("microsoft teams") ||
    text.includes("ms teams") ||
    text.includes("zoom") ||
    text.includes("on teams") ||
    text.includes("attendees") ||
    text.includes("objectives");

  const attendeeishLines = lines.filter((line) =>
    /[–-]/.test(line) &&
    /[A-Za-z]/.test(line) &&
    line.length < 140 &&
    !line.endsWith(".")
  ).length;

  return hasExplicitMinutesRequest || (hasMeetingSignal && attendeeishLines >= 3);
}

function splitEmailHeaderBody(raw) {
  const text = String(raw || "");
  const idx = text.search(/\r?\n\r?\n/);
  if (idx === -1) {
    return { headerText: text, bodyText: "" };
  }
  const sepLength = text.slice(idx, idx + 4).startsWith("\r\n\r\n") ? 4 : 2;
  return {
    headerText: text.slice(0, idx),
    bodyText: text.slice(idx + sepLength),
  };
}

function parseHeaders(headerText) {
  const out = new Map();
  const lines = String(headerText || "").replace(/\r\n/g, "\n").split("\n");
  let currentKey = null;
  for (const line of lines) {
    if (/^\s/.test(line) && currentKey) {
      out.set(currentKey, `${out.get(currentKey)} ${line.trim()}`);
      continue;
    }
    const idx = line.indexOf(":");
    if (idx === -1) continue;
    currentKey = line.slice(0, idx).trim().toLowerCase();
    out.set(currentKey, line.slice(idx + 1).trim());
  }
  return out;
}

function getContentTypeParts(contentType) {
  const raw = String(contentType || "");
  const [typePart, ...paramParts] = raw.split(";");
  const params = {};
  for (const part of paramParts) {
    const idx = part.indexOf("=");
    if (idx === -1) continue;
    const key = part.slice(0, idx).trim().toLowerCase();
    let value = part.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    params[key] = value;
  }
  return {
    mimeType: typePart.trim().toLowerCase(),
    params,
  };
}

function decodeTransferBody(bodyText, encoding) {
  const enc = String(encoding || "").trim().toLowerCase();
  if (enc === "base64") {
    const cleaned = String(bodyText || "").replace(/\s+/g, "");
    return Buffer.from(cleaned, "base64").toString("utf8");
  }
  if (enc === "quoted-printable") {
    return decodeQuotedPrintable(bodyText);
  }
  return String(bodyText || "");
}

function parseHeaderParams(value) {
  return getContentTypeParts(value).params;
}

function decodeHeaderFilename(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const decoded = raw.replace(/=\?utf-8\?b\?([^?]+)\?=/gi, (_, data) => {
    try {
      return Buffer.from(data, "base64").toString("utf8");
    } catch {
      return data;
    }
  }).replace(/=\?utf-8\?q\?([^?]+)\?=/gi, (_, data) => {
    return decodeQuotedPrintable(String(data).replaceAll("_", " "));
  });
  return path.basename(decoded).replace(/[\r\n]/g, "").trim();
}

function extractPngAttachmentsFromMime(raw) {
  const attachments = [];

  function visit(partRaw) {
    const { headerText, bodyText } = splitEmailHeaderBody(partRaw);
    const headers = parseHeaders(headerText);
    const contentType = headers.get("content-type") || "text/plain";
    const { mimeType, params } = getContentTypeParts(contentType);
    const disposition = headers.get("content-disposition") || "";
    const dispositionParams = parseHeaderParams(disposition);
    const encoding = headers.get("content-transfer-encoding") || "";

    if (mimeType.startsWith("multipart/")) {
      const boundary = params.boundary;
      if (!boundary) return;
      const marker = `--${boundary}`;
      for (const segment of String(bodyText || "").split(marker)) {
        const trimmed = segment.trim();
        if (!trimmed || trimmed === "--" || trimmed.startsWith("--")) continue;
        visit(trimmed);
      }
      return;
    }

    const filename = decodeHeaderFilename(
      dispositionParams.filename ||
      dispositionParams["filename*"] ||
      params.name ||
      params["name*"] ||
      "handwriting.png"
    );
    const looksPng = mimeType === "image/png" || /\.png$/i.test(filename);
    if (!looksPng) return;

    let buffer;
    if (String(encoding).trim().toLowerCase() === "base64") {
      buffer = Buffer.from(String(bodyText || "").replace(/\s+/g, ""), "base64");
    } else {
      buffer = Buffer.from(decodeTransferBody(bodyText, encoding), "binary");
    }
    if (buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) {
      attachments.push({
        filename: filename || `remarkable_${attachments.length + 1}.png`,
        buffer,
      });
    }
  }

  visit(raw);
  return attachments;
}
function extractBestTextFromMime(raw) {
  const { headerText, bodyText } = splitEmailHeaderBody(raw);
  const headers = parseHeaders(headerText);
  const { mimeType, params } = getContentTypeParts(headers.get("content-type") || "text/plain");
  const encoding = headers.get("content-transfer-encoding") || "";

  if (mimeType.startsWith("multipart/")) {
    const boundary = params.boundary;
    if (!boundary) return normalizeWhitespace(bodyText);
    const marker = `--${boundary}`;
    const endMarker = `--${boundary}--`;
    const segments = String(bodyText || "")
      .split(marker)
      .map((segment) => segment.trim())
      .filter((segment) => segment && segment !== "--" && segment !== endMarker);

    let htmlFallback = "";
    for (const segment of segments) {
      const part = extractBestTextFromMime(segment);
      if (!part) continue;
      if (!htmlFallback) htmlFallback = part;
      if (part && !/<[a-z][\s\S]*>/i.test(part)) {
        return part;
      }
    }
    return htmlFallback;
  }

  const decoded = decodeTransferBody(bodyText, encoding);
  if (mimeType === "text/html") {
    return normalizeWhitespace(stripHtml(decoded));
  }
  return normalizeWhitespace(decoded);
}

function normalizeSubject(subject) {
  const raw = String(subject || "").trim();
  if (!raw) return "Re: your email";
  return /^re:/i.test(raw) ? raw : `Re: ${raw}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function firstNameFromSender(from) {
  const rawName = String(from?.name || "").trim();
  if (rawName) {
    const cleaned = rawName
      .replace(/^[A-Za-z]+\.?\s+/u, "")
      .replace(/\s+/g, " ")
      .trim();
    const first = cleaned.split(" ").filter(Boolean)[0] || "";
    if (first) return first;
  }

  const email = normalizeEmail(from?.address || "");
  const localPart = email.split("@")[0] || "";
  const bits = localPart.split(/[._-]+/).filter(Boolean);
  return bits[0] ? bits[0].charAt(0).toUpperCase() + bits[0].slice(1) : "there";
}

async function firstNameForResolvedUser(userId) {
  if (!userId) {
    return null;
  }
  const pool = getMysqlPool();
  const [rows] = await pool.query(
    `SELECT first_name
       FROM users
      WHERE id = ?
      LIMIT 1`,
    [userId]
  );
  const firstName = String(rows?.[0]?.first_name || "").trim();
  return firstName || null;
}

function chooseRandom(items) {
  if (!Array.isArray(items) || items.length === 0) return "";
  return items[Math.floor(Math.random() * items.length)];
}

function randomGreeting(name) {
  const hour = new Date().getHours();
  const greetings = [
    `Hi ${name},`,
    `Hello ${name},`,
    `Dear ${name},`,
  ];

  if (hour >= 5 && hour < 12) {
    greetings.push(`Good morning ${name},`);
  } else if (hour >= 12 && hour < 18) {
    greetings.push(`Good afternoon ${name},`);
  } else {
    greetings.push(`Good evening ${name},`);
  }

  return chooseRandom(greetings);
}

function randomSignoff() {
  return chooseRandom([
    "Regards,",
    "Best regards,",
    "Kind regards,",
    "Best wishes,",
  ]);
}

function formatReplyText(message) {
  const greetingName = firstNameFromSender(message.from);
  const configuredBody = String(process.env.MAIL_HANDLER_REPLY_TEXT || "").trim();

  if (configuredBody) {
    return configuredBody.replaceAll("{{name}}", greetingName);
  }

  return [
    randomGreeting(greetingName),
    "",
    "Thanks for your email. We have received your message and will reply as soon as possible.",
    "",
    randomSignoff(),
    requireEnv("MAIL_HANDLER_FROM_NAME", requireEnv("MAIL_HANDLER_FROM_ADDRESS")),
  ].join("\n");
}

function formatDocumentGenerationReply({ greetingName, classification, jobId }) {
  const docType = classification?.doc_type || "note";

  return [
    randomGreeting(greetingName),
    "",
    `I treated your email as a \`${docType}\` request and ran it through the document generation stack on Otso.`,
    `Job ID: ${jobId}`,
    "",
    "The generated Word draft is attached.",
    "",
    randomSignoff(),
    requireEnv("MAIL_HANDLER_FROM_NAME", requireEnv("MAIL_HANDLER_FROM_ADDRESS")),
  ].join("\n");
}

function buildReplyHtml(text) {
  return text
    .split("\n")
    .map((line) => line.trim() === "" ? "<p>&nbsp;</p>" : `<p>${escapeHtml(line)}</p>`)
    .join("");
}

function addressesContain(addresses, targetEmail) {
  const target = String(targetEmail || "").trim().toLowerCase();
  return (addresses || []).some((entry) => String(entry.address || "").trim().toLowerCase() === target);
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function messageTargetsActionAddress(message) {
  const actionAddress = normalizeEmail(actionFromAddress);
  if (!actionAddress) return false;
  if (addressesContain(message.to || [], actionAddress)) return true;

  const deliveredTo = normalizeEmail(getHeaderValue(message.headers, "delivered-to"));
  const originalTo = normalizeEmail(getHeaderValue(message.headers, "x-original-to"));
  const envelopeTo = normalizeEmail(getHeaderValue(message.headers, "envelope-to"));
  return [deliveredTo, originalTo, envelopeTo].some((value) => value === actionAddress);
}

function isRemarkableSender(address) {
  return normalizeEmail(address) === "my@remarkable.com";
}
function isAllowedSender(address) {
  const email = normalizeEmail(address);
  if (!email || !email.includes("@")) return false;
  if (ALLOWED_ADDRESSES.has(email)) return true;
  const [, domain = ""] = email.split("@");
  return ALLOWED_DOMAINS.has(domain);
}

function looksLikeNoReply(address) {
  const email = normalizeEmail(address);
  return /(^|[._-])(no[\s._-]?reply|donotreply|do[\s._-]?not[\s._-]?reply)(@|$)/i.test(email);
}

function hasListHeaders(message) {
  return Boolean(
    getHeaderValue(message.headers, "list-id") ||
    getHeaderValue(message.headers, "list-unsubscribe") ||
    getHeaderValue(message.headers, "precedence")
  );
}

function getMessageKey(message) {
  const messageId = String(message.envelope?.messageId || "").trim();
  if (messageId) return `message-id:${messageId}`;
  const from = normalizeEmail(message.from?.address);
  const subject = normalizeSubject(message.envelope?.subject || "");
  return `fallback:${from}:${subject}`;
}

function shouldSkipMessage(message, myAddress) {
  if (!message.from?.address) return "missing from address";
  const fromEmail = normalizeEmail(message.from.address);
  if (fromEmail === myAddress.toLowerCase()) {
    return "message is from reply account";
  }
  if (!isAllowedSender(fromEmail)) {
    return "sender not in allowlist";
  }
  if (looksLikeNoReply(fromEmail)) {
    return "no-reply sender";
  }
  if (addressesContain(message.to, myAddress) && addressesContain(message.from ? [message.from] : [], myAddress)) {
    return "message appears self-addressed";
  }
  if (message.autoSubmitted && String(message.autoSubmitted).toLowerCase() !== "no") {
    return "auto-submitted message";
  }
  if (hasListHeaders(message)) {
    return "mailing-list style message";
  }
  return null;
}

function log(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function getHeaderValue(headers, name) {
  if (!headers || !name) return null;
  const target = String(name).trim().toLowerCase();

  if (typeof headers.get === "function") {
    const value = headers.get(target) ?? headers.get(name);
    if (value == null) return null;
    if (Array.isArray(value)) return value.join(", ");
    return String(value);
  }

  if (Array.isArray(headers)) {
    for (const entry of headers) {
      if (!entry) continue;
      const key = String(entry.key ?? entry.name ?? "").trim().toLowerCase();
      if (key === target) {
        const value = entry.value ?? entry.line ?? null;
        return value == null ? null : String(value);
      }
    }
    return null;
  }

  if (typeof headers === "object") {
    for (const [key, value] of Object.entries(headers)) {
      if (String(key).trim().toLowerCase() === target) {
        if (value == null) return null;
        if (Array.isArray(value)) return value.join(", ");
        return String(value);
      }
    }
  }

  return null;
}

async function withTimeout(label, promise, ms) {
  let timer = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${ms}ms`));
        }, ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function postJson(url, payload, headers = {}) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...headers,
    },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }
  if (!response.ok) {
    const apiError = parsed?.error;
    const apiMessage = typeof apiError === "string" ? apiError : apiError?.message;
    const apiCode = typeof apiError === "object" && apiError?.code ? ` (${apiError.code})` : "";
    throw new Error(apiMessage ? `${apiMessage}${apiCode}` : `HTTP ${response.status}: ${text.slice(0, 500)}`);
  }
  return parsed;
}

async function classifyEmailForDocumentGeneration({ from, subject, bodyText }) {
  if (looksLikeMinutesRequest(subject, bodyText)) {
    const cleanSubject = normalizeWhitespace(subject || "");
    const cleanBody = normalizeWhitespace(bodyText || "");
    const prefixed = cleanSubject
      ? normalizeWhitespace(`Meeting title/project: ${cleanSubject}\n\n${cleanBody}`)
      : cleanBody;

    return {
      doc_type: "minutes",
      notes_text: prefixed,
      reasoning: "Rule-based minutes classification from explicit request / meeting signals.",
      model_returned: "rule-based",
    };
  }

  const apiKey = requireEnv("OPENAI_API_KEY");
  const model = requireEnv("MAIL_HANDLER_CLASSIFIER_MODEL", "gpt-4o-mini");
  const bodySnippet = String(bodyText || "").slice(0, 20000);
  const response = await postJson("https://api.openai.com/v1/chat/completions", {
    model,
    temperature: 0,
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content: [
          "You classify incoming emails for a document-generation pipeline.",
          "Choose exactly one doc_type from: note, letter, minutes.",
          "Then produce draft notes_text to send into the document generator.",
          "Preserve factual content from the email and do not invent facts.",
          "If the best fit is minutes, prefer the email subject as the meeting title/project placeholder unless the body clearly supplies a better one.",
          "Use tags like [H1], [H2], [P], [AP], [Q] only when helpful.",
          "Return valid JSON only.",
        ].join(" "),
      },
      {
        role: "user",
        content: [
          "Classify this incoming email for document generation.",
          "Return strict JSON with keys:",
          "- doc_type",
          "- notes_text",
          "- reasoning",
          "",
          `FROM_NAME: ${from?.name || ""}`,
          `FROM_EMAIL: ${from?.address || ""}`,
          `SUBJECT: ${subject || ""}`,
          "",
          "EMAIL_BODY:",
          bodySnippet || "(empty body)",
        ].join("\n"),
      },
    ],
  }, {
    Authorization: `Bearer ${apiKey}`,
  });

  const content = response?.choices?.[0]?.message?.content || "";
  const parsed = JSON.parse(String(content).replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "").trim());
  const docType = String(parsed?.doc_type || "").trim().toLowerCase();
  const notesText = normalizeWhitespace(parsed?.notes_text || "");
  if (!["note", "letter", "minutes"].includes(docType)) {
    throw new Error(`Classifier returned invalid doc_type: ${docType || "(empty)"}`);
  }
  if (!notesText) {
    throw new Error("Classifier returned empty notes_text");
  }
  return {
    doc_type: docType,
    notes_text: notesText,
    reasoning: String(parsed?.reasoning || "").trim(),
    model_returned: response?.model || model,
  };
}

function applySubjectPreferenceToClassification(classification, subject) {
  const cleanSubject = normalizeWhitespace(subject || "");
  if (classification?.doc_type !== "minutes" || !cleanSubject) {
    return classification;
  }

  const notesText = normalizeWhitespace(classification.notes_text || "");
  const subjectLine = `Meeting title/project: ${cleanSubject}`;
  if (notesText.toLowerCase().includes(cleanSubject.toLowerCase())) {
    return classification;
  }

  return {
    ...classification,
    notes_text: normalizeWhitespace(`${subjectLine}\n\n${notesText}`),
  };
}

function mysqlConfigFromEnv() {
  return {
    host: requireEnv("MYSQL_HOST"),
    port: Number(requireEnv("MYSQL_PORT", "3306")),
    user: requireEnv("MYSQL_USER"),
    password: String(process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS || "").trim(),
    database: requireEnv("MYSQL_DATABASE", process.env.MYSQL_DB || ""),
  };
}

let mysqlPool = null;

function getMysqlPool() {
  if (mysqlPool) return mysqlPool;
  mysqlPool = mysql.createPool({
    ...mysqlConfigFromEnv(),
    waitForConnections: true,
    connectionLimit: 4,
    queueLimit: 0,
  });
  return mysqlPool;
}

async function fetchJobRow(jobId) {
  const pool = getMysqlPool();
  const [rows] = await pool.query(
    `SELECT id, kind, status, percentage_complete, current_action, error_message, final_output, updated_at
       FROM app_ingest_jobs
      WHERE id = ?`,
    [jobId]
  );
  return rows?.[0] || null;
}

function splitNmrkLocalPartToName(email) {
  const normalized = normalizeEmail(email);
  const [localPart = "", domain = ""] = normalized.split("@");
  if (domain !== "nmrk.com") {
    return null;
  }
  const bits = localPart
    .split(".")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (bits.length < 2) {
    return null;
  }
  return {
    first_name: bits.slice(0, -1).join(" "),
    last_name: bits[bits.length - 1],
  };
}

async function resolveUserIdForEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) {
    return null;
  }

  const pool = getMysqlPool();
  const [emailRows] = await pool.query(
    `SELECT id
       FROM users
      WHERE LOWER(email) = ?
      LIMIT 1`,
    [normalized]
  );
  if (Array.isArray(emailRows) && emailRows[0]?.id) {
    return Number(emailRows[0].id);
  }

  const nmrkName = splitNmrkLocalPartToName(normalized);
  if (!nmrkName) {
    return null;
  }

  const [nameRows] = await pool.query(
    `SELECT id
       FROM users
      WHERE LOWER(TRIM(first_name)) = ?
        AND LOWER(TRIM(last_name)) = ?
      LIMIT 1`,
    [nmrkName.first_name.toLowerCase(), nmrkName.last_name.toLowerCase()]
  );
  if (Array.isArray(nameRows) && nameRows[0]?.id) {
    return Number(nameRows[0].id);
  }

  return null;
}

async function startDocumentGeneration(payload) {
  const endpoint = requireEnv("MAIL_HANDLER_DOCGEN_START_URL", "http://127.0.0.1/document_generation_start.php");
  const apiKey = requireEnv(
    "MAIL_HANDLER_DOCGEN_API_KEY",
    process.env.INTERNAL_UPSTREAM_API_KEY || process.env.PDF_EXTRACT_KEY || ""
  );
  const form = new URLSearchParams();
  form.set("api_key", apiKey);
  form.set("doc_type", payload.doc_type);
  form.set("notes_text", payload.notes_text);
  if (payload.user_id != null) {
    form.set("user_id", String(payload.user_id));
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: form.toString(),
  });
  const text = await response.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }
  if (!response.ok || !parsed?.job_id) {
    throw new Error(parsed?.error || `Document-generation start failed: ${text.slice(0, 500)}`);
  }
  return parsed;
}

async function waitForDocumentGeneration(jobId) {
  const timeoutMs = Number(requireEnv("MAIL_HANDLER_DOCGEN_TIMEOUT_MS", "180000"));
  const pollMs = Number(requireEnv("MAIL_HANDLER_DOCGEN_POLL_MS", "1500"));
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await fetchJobRow(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found in app_ingest_jobs`);
    }

    log("docgen.poll", {
      job_id: jobId,
      status: job.status,
      percentage_complete: job.percentage_complete,
      current_action: job.current_action,
    });

    if (job.status === "completed") {
      return typeof job.final_output === "string" ? JSON.parse(job.final_output) : job.final_output;
    }
    if (["error", "failed", "cancelled", "canceled"].includes(String(job.status || "").toLowerCase())) {
      const payload = typeof job.final_output === "string" ? JSON.parse(job.final_output || "null") : job.final_output;
      throw new Error(payload?.error || job.error_message || `Job ${jobId} failed`);
    }

    await sleep(pollMs);
  }

  throw new Error(`Timed out waiting for document-generation job ${jobId}`);
}

async function renderDocumentWord(resultPayload) {
  const requestDir = path.resolve(process.cwd(), "tmp", "mail_handler");
  fs.mkdirSync(requestDir, { recursive: true });
  const requestPath = path.join(requestDir, `render_request_${Date.now()}_${process.pid}.json`);
  fs.writeFileSync(requestPath, `${JSON.stringify({
    doc_type: resultPayload?.doc_type,
    result: resultPayload?.result,
  }, null, 2)}\n`, "utf8");

  try {
    const { stdout, stderr } = await execFileAsync("php", [
      "/opt/scraper/workers/document_render_word.php",
      `--request_json_path=${requestPath}`,
    ], {
      cwd: process.cwd(),
      maxBuffer: 10 * 1024 * 1024,
    });

    const parsed = JSON.parse(String(stdout || "").trim());
    if (!parsed?.success || !parsed?.path) {
      throw new Error(parsed?.error || `Render worker did not return a file path. stderr=${String(stderr || "").trim()}`);
    }
    return parsed;
  } finally {
    fs.rmSync(requestPath, { force: true });
  }
}

const imapConfig = {
  host: requireEnv("MAIL_HANDLER_IMAP_HOST"),
  port: Number(requireEnv("MAIL_HANDLER_IMAP_PORT", "993")),
  secure: parseBool(process.env.MAIL_HANDLER_IMAP_SECURE, true),
  auth: {
    user: requireEnv("MAIL_HANDLER_IMAP_USER"),
    pass: requireEnv("MAIL_HANDLER_IMAP_PASSWORD"),
  },
  logger: false,
};

const actionImapConfig = {
  host: requireEnv("MAIL_HANDLER_ACTION_IMAP_HOST", imapConfig.host),
  port: Number(requireEnv("MAIL_HANDLER_ACTION_IMAP_PORT", String(imapConfig.port))),
  secure: parseBool(process.env.MAIL_HANDLER_ACTION_IMAP_SECURE, imapConfig.secure),
  auth: {
    user: requireEnv("MAIL_HANDLER_ACTION_IMAP_USER", "action@ngist.app"),
    pass: requireEnv("MAIL_HANDLER_ACTION_IMAP_PASSWORD", imapConfig.auth.pass),
  },
  logger: false,
};

const notesImapConfig = {
  host: requireEnv("MAIL_HANDLER_NOTES_IMAP_HOST", imapConfig.host),
  port: Number(requireEnv("MAIL_HANDLER_NOTES_IMAP_PORT", String(imapConfig.port))),
  secure: parseBool(process.env.MAIL_HANDLER_NOTES_IMAP_SECURE, imapConfig.secure),
  auth: { user: requireEnv("MAIL_HANDLER_NOTES_IMAP_USER", "notes@ngist.app"), pass: requireEnv("MAIL_HANDLER_NOTES_IMAP_PASSWORD", imapConfig.auth.pass) },
  logger: false,
};

const smtpConfig = {
  host: requireEnv("MAIL_HANDLER_SMTP_HOST", imapConfig.host),
  port: Number(requireEnv("MAIL_HANDLER_SMTP_PORT", "465")),
  secure: parseBool(process.env.MAIL_HANDLER_SMTP_SECURE, true),
  auth: {
    user: requireEnv("MAIL_HANDLER_SMTP_USER", imapConfig.auth.user),
    pass: requireEnv("MAIL_HANDLER_SMTP_PASSWORD", imapConfig.auth.pass),
  },
};

const mailbox = requireEnv("MAIL_HANDLER_IMAP_MAILBOX", "INBOX");
const actionMailbox = requireEnv("MAIL_HANDLER_ACTION_IMAP_MAILBOX", "INBOX");
const actionFromAddress = requireEnv("MAIL_HANDLER_ACTION_FROM_ADDRESS", actionImapConfig.auth.user);
const actionFromName = requireEnv("MAIL_HANDLER_ACTION_FROM_NAME", "nGISt Actions");
const notesMailbox = requireEnv("MAIL_HANDLER_NOTES_IMAP_MAILBOX", "INBOX");
const notesFromAddress = requireEnv("MAIL_HANDLER_NOTES_FROM_ADDRESS", notesImapConfig.auth.user);
const notesFromName = requireEnv("MAIL_HANDLER_NOTES_FROM_NAME", "nGISt Notes");
const pollMs = Number(requireEnv("MAIL_HANDLER_POLL_MS", "30000"));
const fromAddress = requireEnv("MAIL_HANDLER_FROM_ADDRESS", smtpConfig.auth.user);
const fromName = requireEnv("MAIL_HANDLER_FROM_NAME", fromAddress);
const smtpVerifyTimeoutMs = Number(requireEnv("MAIL_HANDLER_SMTP_VERIFY_TIMEOUT_MS", "15000"));
const imapConnectTimeoutMs = Number(requireEnv("MAIL_HANDLER_IMAP_CONNECT_TIMEOUT_MS", "20000"));
const imapOpenTimeoutMs = Number(requireEnv("MAIL_HANDLER_IMAP_OPEN_TIMEOUT_MS", "15000"));
const reconnectDelayMs = Number(requireEnv("MAIL_HANDLER_IMAP_RECONNECT_DELAY_MS", "5000"));
const stateFilePath = process.env.MAIL_HANDLER_STATE_FILE
  ? path.resolve(process.env.MAIL_HANDLER_STATE_FILE)
  : path.resolve(process.cwd(), "mail_handler", "reply_state.json");
const ALLOWED_DOMAINS = new Set(["nmrk.com", "geraldeve.com"]);
const ALLOWED_ADDRESSES = new Set(["james@wickhams.co.uk"]);

function loadState() {
  try {
    if (!fs.existsSync(stateFilePath)) {
      return { replied: {} };
    }
    const parsed = JSON.parse(fs.readFileSync(stateFilePath, "utf8"));
    if (!parsed || typeof parsed !== "object") {
      return { replied: {} };
    }
    return {
      replied: parsed.replied && typeof parsed.replied === "object" ? parsed.replied : {},
    };
  } catch {
    return { replied: {} };
  }
}

function saveState(state) {
  const dir = path.dirname(stateFilePath);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(stateFilePath, JSON.stringify(state, null, 2));
}

const state = loadState();

const transporter = nodemailer.createTransport(smtpConfig);
let imap = null;
let actionImap = null;
let notesImap = null;
let running = false;
let actionRunning = false;
let notesRunning = false;
let reconnecting = false;
let actionReconnecting = false;

function createImapClient() {
  return new ImapFlow(imapConfig);
}

function createActionImapClient() {
  return new ImapFlow(actionImapConfig);
}

function rememberReply(message, extra = {}) {
  const key = getMessageKey(message);
  state.replied[key] = {
    replied_at: new Date().toISOString(),
    from: normalizeEmail(message.from?.address),
    subject: String(message.envelope?.subject || ""),
    ...extra,
  };
  saveState(state);
}

function alreadyReplied(message) {
  const key = getMessageKey(message);
  return Boolean(state.replied[key]);
}


function buildActionTaskInput(subject, bodyText) {
  const cleanSubject = normalizeWhitespace(subject || "");
  const cleanBody = normalizeWhitespace(bodyText || "");
  return [cleanSubject, cleanBody].filter(Boolean).join("\n").trim();
}

function stripLikelyEmailSignature(bodyText) {
  const lines = normalizeWhitespace(bodyText || "").split("\n");
  const kept = [];
  for (let index = 0; index < lines.length; index += 1) {
    const rawLine = lines[index];
    const line = rawLine.trim();
    const signatureWindow = lines.slice(index, index + 10).map((entry) => entry.trim()).filter(Boolean).join("\n");
    if (/^--\s*$/.test(line)) break;
    if (/^(regards|kind regards|best regards|best wishes|thanks|many thanks|sent from my)/i.test(line)) break;
    if (/^on .+ wrote:$/i.test(line)) break;
    if (/^(please consider the environment|disclaimer\s*:|this email is intended solely|newmark|gerald eve)/i.test(line)) break;
    if (/^[A-Z][A-Za-z' -]{2,80}$/.test(line) && /\b(partner|director|associate|consultant)\b/i.test(signatureWindow) && /(?:\bnewmark\b|\bgerald eve\b|\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b|\b(?:tel|mobile|m)\b)/i.test(signatureWindow)) break;
    kept.push(rawLine);
  }
  return normalizeWhitespace(kept.join("\n"));
}

async function cleanNoteBody({ from, subject, bodyText }) {
  const deterministic = stripLikelyEmailSignature(bodyText);
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();
  if (!apiKey || !deterministic) return deterministic;
  try {
    const model = requireEnv("MAIL_HANDLER_NOTE_CLEANUP_MODEL", "gpt-5.6-luna");
    const response = await postJson("https://api.openai.com/v1/chat/completions", {
      model, response_format: { type: "json_object" },
      messages: [
        { role: "system", content: [
          "Clean an emailed personal note for storage in a task and notes system.",
          "Return JSON only with key cleaned_text.",
          "Preserve substantive note text and original meaning.",
          "Preserve explicit Action: lines exactly, including associated Due: lines.",
          "Remove greetings, sign-offs, email signatures, names only when they are part of a signature or contact block, email addresses, phone numbers, legal footers, quoted replies, isolated mail artefacts, and mojibake artefacts.",
          "Do not invent, summarise, rephrase, reorder, or add information.",
          "If no substantive note remains, return an empty cleaned_text string."
        ].join(" ") },
        { role: "user", content: [
          `FROM_NAME: ${from?.name || ""}`, `FROM_EMAIL: ${from?.address || ""}`, `SUBJECT: ${subject || ""}`, "",
          "NOTE_BODY:", deterministic.slice(0, 12000)
        ].join("\n") }
      ]
    }, { Authorization: `Bearer ${apiKey}` });
    const content = response?.choices?.[0]?.message?.content || "";
    const parsed = JSON.parse(String(content).trim());
    const cleaned = normalizeWhitespace(parsed?.cleaned_text || "");
    if (cleaned) return cleaned;
    log("note.cleanup_empty", { subject: String(subject || "").slice(0, 160) });
  } catch (error) {
    log("note.cleanup_error", { message: error instanceof Error ? error.message : String(error) });
  }
  return deterministic;
}

async function prepareActionTaskInput({ from, subject, bodyText }) {
  const cleanSubject = normalizeWhitespace(subject || "");
  const subjectTask = /^(re|fw|fwd):?\s*$/i.test(cleanSubject) ? "" : cleanSubject;
  const fallback = buildActionTaskInput(subjectTask, stripLikelyEmailSignature(bodyText));
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();
  if (!apiKey) return fallback;

  try {
    const model = requireEnv("MAIL_HANDLER_ACTION_CLEANUP_MODEL", "gpt-4o-mini");
    const response = await postJson("https://api.openai.com/v1/chat/completions", {
      model,
      temperature: 0,
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content: [
            "Extract personal task actions from a quick email.",
            "Return JSON only with key task_text.",
            "task_text must be plain text with one task per line.",
            "If SUBJECT is non-empty and is not only Re/Fwd noise, task_text MUST include SUBJECT as the first line exactly as written.",
            "Then append any additional real action lines from EMAIL_BODY only if they are clearly task instructions.",
            "Preserve Project: Task format when present in the subject.",
            "Ignore greetings, sign-offs, signatures, names, email addresses, phone numbers, legal footers, quoted replies, and contact details.",
            "Do not invent tasks. If SUBJECT is empty and the body contains no task, return an empty task_text string.",
          ].join(" "),
        },
        {
          role: "user",
          content: [
            `FROM_NAME: ${from?.name || ""}`,
            `FROM_EMAIL: ${from?.address || ""}`,
            `SUBJECT: ${subject || ""}`,
            "",
            "EMAIL_BODY:",
            String(bodyText || "").slice(0, 12000),
          ].join("\n"),
        },
      ],
    }, {
      Authorization: `Bearer ${apiKey}`,
    });

    const content = response?.choices?.[0]?.message?.content || "";
    const parsed = JSON.parse(String(content).replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "").trim());
    const taskText = normalizeWhitespace(parsed?.task_text || "");
    if (!subjectTask) return taskText || fallback;
    if (!taskText) return subjectTask;
    if (taskText.toLowerCase().includes(subjectTask.toLowerCase())) return taskText;
    return normalizeWhitespace(`${subjectTask}\n${taskText}`);
  } catch (error) {
    log("action.cleanup_error", { message: error instanceof Error ? error.message : String(error) });
  }

  return subjectTask || fallback;
}
function formatActionTaskReply({ greetingName, jobId, result }) {
  const tasks = Array.isArray(result?.tasks_created) ? result.tasks_created : [];
  const taskLines = tasks.length
    ? tasks.map((task) => `- ${task.task_ref}: ${task.title}`).join("\n")
    : "- Task creation completed, but no task details were returned.";

  return [
    randomGreeting(greetingName),
    "",
    "I created the following personal task action(s) from your email:",
    "",
    taskLines,
    "",
    `Job ID: ${jobId}`,
    "",
    randomSignoff(),
    actionFromName,
  ].join("\n");
}

async function createPersonalTaskJob() {
  const pool = getMysqlPool();
  const [result] = await pool.query(
    `INSERT INTO app_ingest_jobs
        (kind, user_id, stage, status, percentage_complete, current_action, updated_at)
     VALUES
        (?, ?, 1, 'queued', 0, 'queued from action email', NOW())`,
    ["beta/personal_task_create", 10]
  );
  return Number(result.insertId);
}

function stagePersonalTaskRequest({ input, from, subject }) {
  const requestDir = path.resolve(process.cwd(), "ngist", "private", "tmp");
  fs.mkdirSync(requestDir, { recursive: true });
  const requestPath = path.join(requestDir, `personal_task_request_mail_${Date.now()}_${process.pid}.json`);
  const payload = {
    input,
    source: "email_action",
    user_id: 10,
    parser_version: "deterministic_v1",
    created_at: new Date().toISOString(),
    email: {
      from: from?.address || null,
      from_name: from?.name || null,
      subject: subject || "",
    },
  };
  fs.writeFileSync(requestPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return requestPath;
}

function phpWorkerEnv() {
  const workerEnv = { ...process.env };
  for (const key of [
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_DATABASE",
    "MYSQL_DB",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_PASS",
  ]) {
    delete workerEnv[key];
  }
  return workerEnv;
}

function formatHandwritingReply({ greetingName, jobId, result }) {
  const transcription = normalizeWhitespace(
    result?.result?.cleaned_transcription ||
    result?.result?.literal_transcription ||
    result?.result?.transcription ||
    ""
  );
  const candidates = Array.isArray(result?.personal_tasks?.candidates_created) ? result.personal_tasks.candidates_created : [];
  const candidateLines = candidates.length
    ? candidates.map((candidate) => `- ${candidate.title}`).join("\n")
    : "- No actions were detected.";

  return [
    randomGreeting(greetingName),
    "",
    "I processed the PNG attachment from your reMarkable email through handwriting transcription.",
    `Job ID: ${jobId}`,
    "",
    "Candidates awaiting review:",
    candidateLines,
    "",
    "Transcription:",
    transcription ? transcription.slice(0, 2000) : "(No transcription text returned.)",
    "",
    randomSignoff(),
    fromName,
  ].join("\n");
}

async function createHandwritingJob() {
  const pool = getMysqlPool();
  const [result] = await pool.query(
    `INSERT INTO app_ingest_jobs
        (kind, user_id, stage, status, percentage_complete, current_action, updated_at)
     VALUES
        (?, ?, 1, 'queued', 0, 'queued from reMarkable email', NOW())`,
    ["beta/handwriting_transcribe", 10]
  );
  return Number(result.insertId);
}

function stageRemarkablePngAttachment(attachment) {
  const requestDir = path.resolve(process.cwd(), "ngist", "private", "tmp");
  fs.mkdirSync(requestDir, { recursive: true });
  const safeName = path.basename(attachment.filename || "remarkable.png").replace(/[^A-Za-z0-9._-]+/g, "_");
  const filePath = path.join(requestDir, `remarkable_mail_${Date.now()}_${process.pid}_${safeName || "attachment.png"}`);
  fs.writeFileSync(filePath, attachment.buffer);
  return filePath;
}

async function runHandwritingTranscribeWorker({ attachment }) {
  const jobId = await createHandwritingJob();
  const filePath = stageRemarkablePngAttachment(attachment);
  try {
    const workerResult = await execFileAsync("php", [
      path.resolve(process.cwd(), "ngist", "private", "workers", "handwriting_transcribe_worker.php"),
      `--job=${jobId}`,
      `--file=${filePath}`,
      `--original_filename=${attachment.filename || "remarkable.png"}`,
      `--model=${requireEnv("MAIL_HANDLER_HANDWRITING_MODEL", "gpt-5.6-luna")}`,
      "--mime_type=image/png",
      "--user_id=10",
      "--personal_tasks=1",
      "--personal_tasks_mode=proposed",
    ], {
      cwd: process.cwd(),
      env: phpWorkerEnv(),
      maxBuffer: 10 * 1024 * 1024,
    });
    const workerStdout = String(workerResult.stdout || "").trim();
    const workerStderr = String(workerResult.stderr || "").trim();
    if (workerStdout || workerStderr) {
      log("remarkable.worker.output", { job_id: jobId, stdout: workerStdout.slice(0, 500), stderr: workerStderr.slice(0, 500) });
    }

    const job = await fetchJobRow(jobId);
    const finalOutput = typeof job?.final_output === "string" ? JSON.parse(job.final_output) : job?.final_output;
    if (!job || job.status !== "completed" || !finalOutput?.success) {
      throw new Error(finalOutput?.error || job?.error_message || `Handwriting worker did not complete for job ${jobId}`);
    }
    return { jobId, result: finalOutput };
  } catch (error) {
    const job = await fetchJobRow(jobId).catch(() => null);
    const finalOutput = typeof job?.final_output === "string" ? JSON.parse(job.final_output) : job?.final_output;
    throw new Error(finalOutput?.error || job?.error_message || (error instanceof Error ? error.message : String(error)));
  }
}
async function runPersonalTaskCreateWorker({ input, from, subject }) {
  const jobId = await createPersonalTaskJob();
  const requestPath = stagePersonalTaskRequest({ input, from, subject });
  const workerEnv = phpWorkerEnv();

  try {
    const workerResult = await execFileAsync("php", [
      path.resolve(process.cwd(), "ngist", "private", "workers", "personal_task_create_worker.php"),
      `--job=${jobId}`,
      `--request_json_path=${requestPath}`,
      "--user_id=10",
    ], {
      cwd: process.cwd(),
      env: workerEnv,
      maxBuffer: 10 * 1024 * 1024,
    });
    const workerStdout = String(workerResult.stdout || "").trim();
    const workerStderr = String(workerResult.stderr || "").trim();
    if (workerStdout || workerStderr) {
      log("action.worker.output", { job_id: jobId, stdout: workerStdout.slice(0, 500), stderr: workerStderr.slice(0, 500) });
    }

    const job = await fetchJobRow(jobId);
    const finalOutput = typeof job?.final_output === "string" ? JSON.parse(job.final_output) : job?.final_output;
    if (!job || job.status !== "completed" || !finalOutput?.success) {
      throw new Error(finalOutput?.error || job?.error_message || `Task worker did not complete for job ${jobId}`);
    }
    return { jobId, result: finalOutput };
  } finally {
    fs.rmSync(requestPath, { force: true });
  }
}

async function createPersonalNoteJob() {
  const pool = getMysqlPool();
  const [result] = await pool.query(
    `INSERT INTO app_ingest_jobs (kind, user_id, stage, status, percentage_complete, current_action, updated_at)
     VALUES (?, ?, 1, 'queued', 0, 'queued from notes email', NOW())`,
    ["beta/personal_note_create", 10]
  );
  return Number(result.insertId);
}

function stagePersonalNoteRequest({ subject, body, from, messageId }) {
  const requestDir = path.resolve(process.cwd(), "ngist", "private", "tmp");
  fs.mkdirSync(requestDir, { recursive: true });
  const requestPath = path.join(requestDir, `personal_note_request_mail_${Date.now()}_${process.pid}.json`);
  fs.writeFileSync(requestPath, `${JSON.stringify({
    source: "email_note",
    user_id: 10,
    subject: subject || "",
    body: body || "",
    from: from?.address || null,
    from_name: from?.name || null,
    message_id: messageId || null,
  }, null, 2)}\n`, "utf8");
  return requestPath;
}

async function runPersonalNoteCreateWorker({ subject, body, from, messageId }) {
  const jobId = await createPersonalNoteJob();
  const requestPath = stagePersonalNoteRequest({ subject, body, from, messageId });
  try {
    const workerResult = await execFileAsync("php", [
      path.resolve(process.cwd(), "ngist", "private", "workers", "personal_note_create_worker.php"),
      `--job=${jobId}`,
      `--request_json_path=${requestPath}`,
      "--user_id=10",
    ], { cwd: process.cwd(), env: phpWorkerEnv(), maxBuffer: 10 * 1024 * 1024 });
    const workerStdout = String(workerResult.stdout || "").trim();
    const workerStderr = String(workerResult.stderr || "").trim();
    if (workerStdout || workerStderr) log("notes.worker.output", { job_id: jobId, stdout: workerStdout.slice(0, 500), stderr: workerStderr.slice(0, 500) });
    const job = await fetchJobRow(jobId);
    const finalOutput = typeof job?.final_output === "string" ? JSON.parse(job.final_output) : job?.final_output;
    if (!job || job.status !== "completed" || !finalOutput?.success) throw new Error(finalOutput?.error || job?.error_message || `Note worker did not complete for job ${jobId}`);
    return { jobId, result: finalOutput };
  } finally {
    fs.rmSync(requestPath, { force: true });
  }
}

function formatNoteReply({ greetingName, jobId, result }) {
  const note = result?.note || {};
  const tasks = Array.isArray(result?.tasks_created) ? result.tasks_created : [];
  const remarkable = result?.remarkable || {};
  const remarkableLine = remarkable.success
    ? `reMarkable: added ${remarkable.remarkable_document || "note"} to ${remarkable.remarkable_folder || "root"}.`
    : "reMarkable: upload was not completed; the note was still saved.";
  const taskLines = tasks.length ? tasks.map((task) => `- ${task.task_ref}: ${task.title}`).join("\n") : "- No actions were created.";
  return [
    randomGreeting(greetingName), "", `Saved note: ${note.title || "Email note"}`,
    `Project: ${note.project || "Notes Inbox"}`, "", "Actions:", taskLines, "", remarkableLine,
    "", `Job ID: ${jobId}`, "", randomSignoff(), notesFromName,
  ].join("\n");
}

async function processNotesMailbox() {
  if (notesRunning || !notesImap) return;
  notesRunning = true;
  try {
    const lock = await notesImap.getMailboxLock(notesMailbox);
    try {
      const uids = await notesImap.search({ seen: false });
      log("notes.mail.scan.found", { mailbox: notesMailbox, count: uids.length, uids });
      for (const uid of uids) {
        const message = await notesImap.fetchOne(uid, { uid: true, envelope: true, flags: true, headers: true, source: false });
        if (!message?.envelope) continue;
        const from = message.envelope.from?.[0] || null;
        if (alreadyReplied({ envelope: message.envelope, from })) { await notesImap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]); continue; }
        const skipReason = shouldSkipMessage({ from, to: message.envelope.to || [], autoSubmitted: getHeaderValue(message.headers, "auto-submitted"), headers: message.headers, envelope: message.envelope }, notesFromAddress);
        if (skipReason) { log("notes.mail.skip", { uid, reason: skipReason }); await notesImap.messageFlagsAdd(uid, ["\\Seen"]); continue; }
        try {
          const fullMessage = await notesImap.fetchOne(uid, { uid: true, source: true });
          const rawBody = extractBestTextFromMime(fullMessage?.source ? fullMessage.source.toString("utf8") : "");
          const subject = message.envelope.subject || "";
          const body = await cleanNoteBody({ from, subject, bodyText: rawBody });
          if (!normalizeWhitespace(subject) && !body) throw new Error("No note subject or body supplied.");
          const { jobId, result } = await runPersonalNoteCreateWorker({ subject, body, from, messageId: message.envelope.messageId || null });
          const resolvedUserId = await resolveUserIdForEmail(from.address);
          const greetingName = await firstNameForResolvedUser(resolvedUserId) || firstNameFromSender(from);
          const replyText = formatNoteReply({ greetingName, jobId, result });
          await transporter.sendMail({
            from: { name: notesFromName, address: notesFromAddress }, to: from.address, subject: normalizeSubject(subject),
            text: replyText, html: buildReplyHtml(replyText), inReplyTo: message.envelope.messageId || undefined,
            references: message.envelope.messageId ? [message.envelope.messageId] : undefined,
            headers: { "Auto-Submitted": "auto-replied", "X-Auto-Response-Suppress": "All" },
          });
          await notesImap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]);
          rememberReply({ envelope: message.envelope, from }, { uid, account: notesImapConfig.auth.user, job_id: jobId, route: "personal_note_create", note_id: result?.note?.id || null, task_count: Number(result?.task_count || 0) });
          log("notes.mail.replied", { uid, job_id: jobId, note_id: result?.note?.id || null });
        } catch (error) {
          const messageText = error instanceof Error ? error.message : String(error);
          log("notes.mail.process_error", { uid, message: messageText }); await notesImap.messageFlagsAdd(uid, ["\\Seen"]);
          rememberReply({ envelope: message.envelope, from }, { uid, account: notesImapConfig.auth.user, status: "error", route: "personal_note_create", error: messageText });
        }
      }
    } finally { lock.release(); }
  } finally { notesRunning = false; }
}

async function connectNotesImap() {
  notesImap = new ImapFlow(notesImapConfig);
  await withTimeout("notes IMAP connect", notesImap.connect(), imapConnectTimeoutMs);
  await withTimeout("notes IMAP mailbox open", notesImap.mailboxOpen(notesMailbox), imapOpenTimeoutMs);
  notesImap.on("close", () => { notesImap = null; setTimeout(() => connectNotesImap().then(processNotesMailbox).catch((error) => log("notes.imap.reconnect_error", { message: String(error) })), reconnectDelayMs).unref(); });
  notesImap.on("error", (error) => log("notes.imap.error", { message: String(error?.message || error) }));
}

async function connectImap() {
  if (imap) {
    try {
      imap.removeAllListeners();
    } catch {}
  }
  imap = createImapClient();
  log("imap.connecting", {
    host: imapConfig.host,
    port: imapConfig.port,
    secure: imapConfig.secure,
    user: imapConfig.auth.user,
    timeout_ms: imapConnectTimeoutMs,
  });
  await withTimeout("IMAP connect", imap.connect(), imapConnectTimeoutMs);
  log("imap.connected", { host: imapConfig.host });

  log("imap.mailbox_opening", { mailbox, timeout_ms: imapOpenTimeoutMs });
  await withTimeout("IMAP mailbox open", imap.mailboxOpen(mailbox), imapOpenTimeoutMs);
  log("imap.ready", { host: imapConfig.host, mailbox });
}

async function reconnectImap(reason) {
  if (reconnecting) return;
  reconnecting = true;
  try {
    log("imap.reconnect_scheduled", { reason, delay_ms: reconnectDelayMs });
    await sleep(reconnectDelayMs);
    if (imap) {
      await imap.logout().catch(() => {});
    }
    await connectImap();
    bindImapEvents();
    log("imap.reconnected", { mailbox });
    await processMailbox();
  } catch (error) {
    log("imap.reconnect_error", { message: error instanceof Error ? error.message : String(error) });
    setTimeout(() => {
      reconnectImap("retry-after-failure").catch(() => {});
    }, reconnectDelayMs).unref();
  } finally {
    reconnecting = false;
  }
}

function bindImapEvents() {
  if (!imap) return;

  imap.on("exists", async () => {
    log("imap.exists", { mailbox });
    await processMailbox();
  });

  imap.on("close", () => {
    log("imap.closed", { mailbox });
    reconnectImap("close").catch(() => {});
  });

  imap.on("error", (error) => {
    log("imap.connection_error", { message: error instanceof Error ? error.message : String(error) });
    reconnectImap("error").catch(() => {});
  });
}


async function connectActionImap() {
  if (actionImap) {
    try {
      actionImap.removeAllListeners();
    } catch {}
  }
  actionImap = createActionImapClient();
  log("action.imap.connecting", {
    host: actionImapConfig.host,
    port: actionImapConfig.port,
    secure: actionImapConfig.secure,
    user: actionImapConfig.auth.user,
    timeout_ms: imapConnectTimeoutMs,
  });
  await withTimeout("action IMAP connect", actionImap.connect(), imapConnectTimeoutMs);
  log("action.imap.connected", { host: actionImapConfig.host });

  log("action.imap.mailbox_opening", { mailbox: actionMailbox, timeout_ms: imapOpenTimeoutMs });
  await withTimeout("action IMAP mailbox open", actionImap.mailboxOpen(actionMailbox), imapOpenTimeoutMs);
  log("action.imap.ready", { host: actionImapConfig.host, mailbox: actionMailbox });
}

async function reconnectActionImap(reason) {
  if (actionReconnecting) return;
  actionReconnecting = true;
  try {
    log("action.imap.reconnect_scheduled", { reason, delay_ms: reconnectDelayMs });
    await sleep(reconnectDelayMs);
    if (actionImap) {
      await actionImap.logout().catch(() => {});
    }
    await connectActionImap();
    bindActionImapEvents();
    log("action.imap.reconnected", { mailbox: actionMailbox });
    await processActionMailbox();
  } catch (error) {
    log("action.imap.reconnect_error", { message: error instanceof Error ? error.message : String(error) });
    setTimeout(() => {
      reconnectActionImap("retry-after-failure").catch(() => {});
    }, reconnectDelayMs).unref();
  } finally {
    actionReconnecting = false;
  }
}

function bindActionImapEvents() {
  if (!actionImap) return;

  actionImap.on("exists", async () => {
    log("action.imap.exists", { mailbox: actionMailbox });
    await processActionMailbox();
  });

  actionImap.on("close", () => {
    log("action.imap.closed", { mailbox: actionMailbox });
    reconnectActionImap("close").catch(() => {});
  });

  actionImap.on("error", (error) => {
    log("action.imap.connection_error", { message: error instanceof Error ? error.message : String(error) });
    reconnectActionImap("error").catch(() => {});
  });
}

async function processActionMailbox() {
  if (actionRunning || !actionImap) return;
  actionRunning = true;

  try {
    const lock = await actionImap.getMailboxLock(actionMailbox);
    try {
      log("action.mail.scan.begin", { mailbox: actionMailbox });
      const uids = await actionImap.search({ seen: false });
      log("action.mail.scan.found", { mailbox: actionMailbox, count: uids.length, uids });

      for (const uid of uids) {
        const message = await actionImap.fetchOne(uid, {
          uid: true,
          envelope: true,
          flags: true,
          headers: true,
          source: false,
        });

        if (!message?.envelope) continue;

        const from = message.envelope.from?.[0] || null;
        log("action.mail.scan.message", {
          uid,
          subject: message.envelope.subject || "",
          from: from?.address || null,
          flags: Array.isArray(message.flags) ? message.flags.map((flag) => String(flag)) : [],
        });

        if (alreadyReplied({ envelope: message.envelope, from })) {
          log("action.mail.skip", { uid, reason: "already handled from state" });
          await actionImap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]);
          continue;
        }

        const skipReason = shouldSkipMessage(
          {
            from,
            to: message.envelope.to || [],
            autoSubmitted: getHeaderValue(message.headers, "auto-submitted"),
            headers: message.headers,
            envelope: message.envelope,
          },
          actionFromAddress
        );

        if (skipReason) {
          log("action.mail.skip", { uid, reason: skipReason });
          await actionImap.messageFlagsAdd(uid, ["\\Seen"]);
          continue;
        }

        try {
          const fullMessage = await actionImap.fetchOne(uid, {
            uid: true,
            source: true,
          });
          const bodyText = extractBestTextFromMime(fullMessage?.source ? fullMessage.source.toString("utf8") : "");
          const input = await prepareActionTaskInput({ from, subject: message.envelope.subject || "", bodyText });
          if (!input) {
            throw new Error("No subject or body text supplied for task creation.");
          }

          log("action.mail.body_extracted", {
            uid,
            subject_chars: String(message.envelope.subject || "").length,
            body_chars: bodyText.length,
            input_chars: input.length,
          });

          const { jobId, result } = await runPersonalTaskCreateWorker({
            input,
            from,
            subject: message.envelope.subject || "",
          });
          const resolvedUserId = await resolveUserIdForEmail(from.address);
          const resolvedFirstName = await firstNameForResolvedUser(resolvedUserId);
          const greetingName = resolvedFirstName || firstNameFromSender(from);
          const replyText = formatActionTaskReply({ greetingName, jobId, result });

          await transporter.sendMail({
            from: { name: actionFromName, address: actionFromAddress },
            to: from.address,
            subject: normalizeSubject(message.envelope.subject),
            text: replyText,
            html: buildReplyHtml(replyText),
            inReplyTo: message.envelope.messageId || undefined,
            references: message.envelope.messageId ? [message.envelope.messageId] : undefined,
            headers: {
              "Auto-Submitted": "auto-replied",
              "X-Auto-Response-Suppress": "All",
            },
          });

          await actionImap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]);
          rememberReply({ envelope: message.envelope, from }, {
            uid,
            account: actionImapConfig.auth.user,
            job_id: jobId,
            route: "personal_task_create",
            task_count: Array.isArray(result?.tasks_created) ? result.tasks_created.length : null,
          });
          log("action.mail.replied", {
            uid,
            to: from.address,
            subject: normalizeSubject(message.envelope.subject),
            job_id: jobId,
            task_count: Array.isArray(result?.tasks_created) ? result.tasks_created.length : null,
          });
        } catch (error) {
          const messageText = error instanceof Error ? error.message : String(error);
          log("action.mail.process_error", { uid, message: messageText });
          await actionImap.messageFlagsAdd(uid, ["\\Seen"]);
          rememberReply({ envelope: message.envelope, from }, {
            uid,
            account: actionImapConfig.auth.user,
            status: "error",
            route: "personal_task_create",
            error: messageText,
          });
        }
      }
      log("action.mail.scan.end", { mailbox: actionMailbox });
    } finally {
      lock.release();
    }
  } catch (error) {
    log("action.mail.error", { message: error instanceof Error ? error.message : String(error) });
  } finally {
    actionRunning = false;
  }
}

async function processMailbox() {
  if (running) return;
  running = true;

  try {
    const lock = await imap.getMailboxLock(mailbox);
    try {
      log("mail.scan.begin", { mailbox });
      const uids = await imap.search({ seen: false });
      log("mail.scan.found", { mailbox, count: uids.length, uids });

      for (const uid of uids) {
        const message = await imap.fetchOne(uid, {
          uid: true,
          envelope: true,
          flags: true,
          headers: true,
          source: false,
        });

        if (!message?.envelope) continue;

        const from = message.envelope.from?.[0] || null;
        log("mail.scan.message", {
          uid,
          subject: message.envelope.subject || "",
          from: from?.address || null,
          flags: Array.isArray(message.flags) ? message.flags.map((flag) => String(flag)) : [],
        });
        if (alreadyReplied({ envelope: message.envelope, from })) {
          log("mail.skip", { uid, reason: "already replied from state" });
          await imap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]);
          continue;
        }

        if (isRemarkableSender(from?.address)) {
          try {
            const fullMessage = await imap.fetchOne(uid, {
              uid: true,
              source: true,
            });
            const sourceText = fullMessage?.source ? fullMessage.source.toString("utf8") : "";
            const attachments = extractPngAttachmentsFromMime(sourceText);
            log("remarkable.mail.attachments", {
              uid,
              subject: message.envelope.subject || "",
              png_count: attachments.length,
            });
            if (attachments.length === 0) {
              throw new Error("No PNG attachment found on reMarkable email.");
            }

            const attachment = attachments[0];
            const { jobId } = await runHandwritingTranscribeWorker({ attachment });

            await imap.messageFlagsAdd(uid, ["\\Seen"]);
            rememberReply({ envelope: message.envelope, from }, {
              uid,
              account: imapConfig.auth.user,
              route: "remarkable_handwriting_transcribe",
              job_id: jobId,
              filename: attachment.filename,
              png_count: attachments.length,
              replied: false,
            });
            log("remarkable.mail.processed", {
              uid,
              subject: message.envelope.subject || "",
              job_id: jobId,
              filename: attachment.filename,
              replied: false,
            });
          } catch (error) {
            const messageText = error instanceof Error ? error.message : String(error);
            log("remarkable.mail.process_error", { uid, message: messageText });
            await imap.messageFlagsAdd(uid, ["\\Seen"]);
            rememberReply({ envelope: message.envelope, from }, {
              uid,
              account: imapConfig.auth.user,
              status: "error",
              route: "remarkable_handwriting_transcribe",
              error: messageText,
            });
          }
          continue;
        }

        const skipReason = shouldSkipMessage(
          {
            from,
            to: message.envelope.to || [],
            autoSubmitted: getHeaderValue(message.headers, "auto-submitted"),
            headers: message.headers,
            envelope: message.envelope,
          },
          fromAddress
        );

        if (skipReason) {
          log("mail.skip", { uid, reason: skipReason });
          await imap.messageFlagsAdd(uid, ["\\Seen"]);
          continue;
        }

        if (messageTargetsActionAddress({
          to: message.envelope.to || [],
          headers: message.headers,
        })) {
          try {
            const fullMessage = await imap.fetchOne(uid, {
              uid: true,
              source: true,
            });
            const bodyText = extractBestTextFromMime(fullMessage?.source ? fullMessage.source.toString("utf8") : "");
            const input = await prepareActionTaskInput({ from, subject: message.envelope.subject || "", bodyText });
            if (!input) {
              throw new Error("No subject or body text supplied for task creation.");
            }

            log("mail.action_route", {
              uid,
              subject_chars: String(message.envelope.subject || "").length,
              body_chars: bodyText.length,
              input_chars: input.length,
            });

            const { jobId, result } = await runPersonalTaskCreateWorker({
              input,
              from,
              subject: message.envelope.subject || "",
            });
            const resolvedUserId = await resolveUserIdForEmail(from.address);
            const resolvedFirstName = await firstNameForResolvedUser(resolvedUserId);
            const greetingName = resolvedFirstName || firstNameFromSender(from);
            const replyText = formatActionTaskReply({ greetingName, jobId, result });

            await transporter.sendMail({
              from: { name: actionFromName, address: actionFromAddress },
              to: from.address,
              subject: normalizeSubject(message.envelope.subject),
              text: replyText,
              html: buildReplyHtml(replyText),
              inReplyTo: message.envelope.messageId || undefined,
              references: message.envelope.messageId ? [message.envelope.messageId] : undefined,
              headers: {
                "Auto-Submitted": "auto-replied",
                "X-Auto-Response-Suppress": "All",
              },
            });

            await imap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]);
            rememberReply({ envelope: message.envelope, from }, {
              uid,
              account: imapConfig.auth.user,
              routed_to: actionFromAddress,
              job_id: jobId,
              route: "personal_task_create",
              task_count: Array.isArray(result?.tasks_created) ? result.tasks_created.length : null,
            });
            log("mail.action_replied", {
              uid,
              to: from.address,
              subject: normalizeSubject(message.envelope.subject),
              job_id: jobId,
              task_count: Array.isArray(result?.tasks_created) ? result.tasks_created.length : null,
            });
          } catch (error) {
            const messageText = error instanceof Error ? error.message : String(error);
            log("mail.action_process_error", { uid, message: messageText });
            await imap.messageFlagsAdd(uid, ["\\Seen"]);
            rememberReply({ envelope: message.envelope, from }, {
              uid,
              account: imapConfig.auth.user,
              routed_to: actionFromAddress,
              status: "error",
              route: "personal_task_create",
              error: messageText,
            });
          }
          continue;
        }

        try {
          const fullMessage = await imap.fetchOne(uid, {
            uid: true,
            source: true,
          });
          const bodyText = extractBestTextFromMime(fullMessage?.source ? fullMessage.source.toString("utf8") : "");
          log("mail.body_extracted", {
            uid,
            body_chars: bodyText.length,
          });

          const classification = await classifyEmailForDocumentGeneration({
            from,
            subject: message.envelope.subject || "",
            bodyText,
          });
          const adjustedClassification = applySubjectPreferenceToClassification(
            classification,
            message.envelope.subject || ""
          );
          const resolvedUserId = await resolveUserIdForEmail(from.address);
          const resolvedFirstName = await firstNameForResolvedUser(resolvedUserId);
          const greetingName = resolvedFirstName || firstNameFromSender(from);
          log("mail.classified", {
            uid,
            doc_type: adjustedClassification.doc_type,
            classifier_model: adjustedClassification.model_returned,
            user_id: resolvedUserId,
          });

          const start = await startDocumentGeneration({
            ...adjustedClassification,
            user_id: resolvedUserId,
          });
          const result = await waitForDocumentGeneration(Number(start.job_id));
          const rendered = await renderDocumentWord(result);
          const replyText = formatDocumentGenerationReply({
            greetingName,
            classification: adjustedClassification,
            jobId: Number(start.job_id),
          });

          await transporter.sendMail({
            from: { name: fromName, address: fromAddress },
            to: from.address,
            subject: normalizeSubject(message.envelope.subject),
            text: replyText,
            html: buildReplyHtml(replyText),
            inReplyTo: message.envelope.messageId || undefined,
            references: message.envelope.messageId ? [message.envelope.messageId] : undefined,
            headers: {
              "Auto-Submitted": "auto-replied",
              "X-Auto-Response-Suppress": "All",
            },
            attachments: [
              {
                filename: rendered.file || `${classification.doc_type || "document"}.docx`,
                path: rendered.path,
              },
            ],
          });

          await imap.messageFlagsAdd(uid, ["\\Seen", "\\Answered"]);
          rememberReply({ envelope: message.envelope, from }, {
            uid,
            job_id: Number(start.job_id),
            doc_type: classification.doc_type,
          });
          log("mail.replied", {
            uid,
            to: from.address,
            subject: normalizeSubject(message.envelope.subject),
            job_id: Number(start.job_id),
            doc_type: adjustedClassification.doc_type,
          });
        } catch (error) {
          const messageText = error instanceof Error ? error.message : String(error);
          log("mail.process_error", { uid, message: messageText });
          await imap.messageFlagsAdd(uid, ["\\Seen"]);
          rememberReply({ envelope: message.envelope, from }, {
            uid,
            status: "error",
            error: messageText,
          });
        }
      }
      log("mail.scan.end", { mailbox });
    } finally {
      lock.release();
    }
  } catch (error) {
    log("mail.error", { message: error instanceof Error ? error.message : String(error) });
  } finally {
    running = false;
  }
}

async function main() {
  log("startup.begin", { mailbox, pollMs, state_file: stateFilePath });
  log("smtp.connecting", {
    host: smtpConfig.host,
    port: smtpConfig.port,
    secure: smtpConfig.secure,
    user: smtpConfig.auth.user,
    timeout_ms: smtpVerifyTimeoutMs,
  });
  await withTimeout("SMTP verify", transporter.verify(), smtpVerifyTimeoutMs);
  log("smtp.ready", { host: smtpConfig.host, port: smtpConfig.port });

  await connectImap();
  bindImapEvents();

  await processMailbox();
  log("watching", { mailbox, poll_ms: pollMs });

  try {
    await connectActionImap();
    bindActionImapEvents();
    await processActionMailbox();
    log("action.watching", { mailbox: actionMailbox, user: actionImapConfig.auth.user, poll_ms: pollMs });

    setInterval(() => {
      processActionMailbox().catch((error) => {
        log("action.mail.interval_error", { message: error instanceof Error ? error.message : String(error) });
      });
    }, pollMs).unref();
  } catch (error) {
    log("action.startup_error", { message: error instanceof Error ? error.message : String(error) });
  }

  try {
    await connectNotesImap();
    await processNotesMailbox();
    log("notes.watching", { mailbox: notesMailbox, user: notesImapConfig.auth.user, poll_ms: pollMs });
    setInterval(() => {
      processNotesMailbox().catch((error) => log("notes.mail.interval_error", { message: error instanceof Error ? error.message : String(error) }));
    }, pollMs).unref();
  } catch (error) {
    log("notes.startup_error", { message: error instanceof Error ? error.message : String(error) });
  }

  setInterval(() => {
    processMailbox().catch((error) => {
      log("mail.interval_error", { message: error instanceof Error ? error.message : String(error) });
    });
  }, pollMs).unref();

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, async () => {
      log("shutdown", { signal });
      if (imap) {
        await imap.logout().catch(() => {});
      }
      if (actionImap) {
        await actionImap.logout().catch(() => {});
      }
      if (notesImap) {
        await notesImap.logout().catch(() => {});
      }
      process.exit(0);
    });
  }

  // Keep the process obviously alive in logs during long idle periods.
  while (true) {
    await sleep(5 * 60 * 1000);
    log("heartbeat", { mailbox, action_mailbox: actionMailbox, notes_mailbox: notesMailbox });
  }
}

main().catch((error) => {
  log("startup.error", { message: error instanceof Error ? error.message : String(error) });
  process.exit(1);
});
