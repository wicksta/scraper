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
  return String(value || "")
    .replace(/=\r?\n/g, "")
    .replace(/=([A-Fa-f0-9]{2})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
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
    throw new Error(parsed?.error || `HTTP ${response.status}: ${text.slice(0, 500)}`);
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
let running = false;
let reconnecting = false;

function createImapClient() {
  return new ImapFlow(imapConfig);
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
      process.exit(0);
    });
  }

  // Keep the process obviously alive in logs during long idle periods.
  while (true) {
    await sleep(5 * 60 * 1000);
    log("heartbeat", { mailbox });
  }
}

main().catch((error) => {
  log("startup.error", { message: error instanceof Error ? error.message : String(error) });
  process.exit(1);
});
