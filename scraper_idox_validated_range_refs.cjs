#!/usr/bin/env node
/* eslint-disable no-console */

// Idox advanced search: fetch application references by "Date Validated" range.
//
// This is intentionally separate from scraper.cjs (which fetches per-application details).
// Typical usage:
//   node scraper_idox_validated_range_refs.cjs \
//     --start-url 'https://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=advanced&searchType=Application' \
//     --validated-start 2026-02-01 --validated-end 2026-02-14

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

const argv = yargs(hideBin(process.argv))
  .scriptName("idox-validated-range-refs")
  .option("start-url", {
    type: "string",
    describe: "Idox advanced search URL (search.do?action=advanced&searchType=Application)",
    demandOption: true,
  })
  .option("validated-start", {
    type: "string",
    describe: 'Start date for "Date Validated" range. Accepts dd/MM/yyyy or YYYY-MM-DD.',
    demandOption: true,
  })
  .option("validated-end", {
    type: "string",
    describe: 'End date for "Date Validated" range. Accepts dd/MM/yyyy or YYYY-MM-DD.',
    demandOption: true,
  })
  .option("max-pages", {
    type: "number",
    describe: "Max results pages to traverse (safety limit).",
    default: 100,
  })
  .option("ipv4", {
    type: "boolean",
    describe: "Force IPv4 for curl (-4). Helpful if IPv6/DNS is flaky.",
    default: false,
  })
  .option("curl-resolve", {
    type: "array",
    describe:
      'Optional curl --resolve entries (repeatable): e.g. --curl-resolve "www.planning2.cityoflondon.gov.uk:443:1.2.3.4"',
    default: [],
  })
  .option("headed", { type: "boolean", describe: "Run with browser visible", default: false })
  .option("artifacts", {
    type: "boolean",
    describe: "Write HTML/JSON artifacts to disk (debugging).",
    default: false,
  })
  .option("emit-json", {
    type: "boolean",
    describe: "Emit JSON marker to stdout for downstream parsing.",
    default: true,
  })
  .strict()
  .help()
  .argv;

function stamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(
    d.getMinutes(),
  )}${pad(d.getSeconds())}`;
}

function safeFilename(s) {
  return String(s).replace(/[^a-z0-9._-]+/gi, "_").replace(/^_+|_+$/g, "").slice(0, 120);
}

function toIdoxDmy(input) {
  const raw = String(input || "").trim();
  if (!raw) throw new Error("Empty date.");

  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const [yyyy, mm, dd] = raw.split("-");
    return `${dd}/${mm}/${yyyy}`;
  }

  // dd/MM/yyyy (allow 1-2 digits for day/month; normalize)
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const dd = String(parseInt(m[1], 10)).padStart(2, "0");
    const mm = String(parseInt(m[2], 10)).padStart(2, "0");
    const yyyy = m[3];
    return `${dd}/${mm}/${yyyy}`;
  }

  throw new Error(`Unsupported date format: ${raw} (expected dd/MM/yyyy or YYYY-MM-DD)`);
}

function detectChallenge(html, title) {
  // Best-effort detection for common bot-blocking pages.
  const t = (title || "").toLowerCase();
  const h = (html || "").toLowerCase();

  if (h.includes("cf-ray") || h.includes("cloudflare")) return "cloudflare";
  if (t.includes("attention required") || t.includes("access denied")) return "access_denied";
  if (h.includes("checking your browser before accessing")) return "browser_check";

  // real captcha widget markers (not just a css class)
  if (h.includes('class="g-recaptcha"') || h.includes("data-sitekey")) return "captcha_widget";
  if (h.includes("please verify you are human")) return "verify_human";

  return null;
}

function stripHtmlToText(html) {
  return String(html || "")
    .replace(/<script\b[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function decodeHtmlEntities(s) {
  return String(s || "")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&nbsp;/gi, " ");
}

function parseTagAttributes(tagHtml) {
  const attrs = {};
  const re = /([a-zA-Z0-9_:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))/g;
  let m;
  // eslint-disable-next-line no-cond-assign
  while ((m = re.exec(tagHtml))) {
    const k = m[1].toLowerCase();
    const v = m[2] ?? m[3] ?? m[4] ?? "";
    attrs[k] = decodeHtmlEntities(v);
  }
  return attrs;
}

function parseAdvancedSearchForm(html) {
  const formOpen = html.match(/<form\b[^>]*id\s*=\s*["']advancedSearchForm["'][^>]*>/i);
  if (!formOpen) throw new Error('Could not find <form id="advancedSearchForm">.');
  const openTag = formOpen[0];
  const attrs = parseTagAttributes(openTag);
  const action = attrs.action;
  if (!action) throw new Error("Advanced search form is missing action=.");

  const bodyMatch = html.match(
    /<form\b[^>]*id\s*=\s*["']advancedSearchForm["'][^>]*>([\s\S]*?)<\/form>/i,
  );
  if (!bodyMatch) throw new Error("Could not extract advanced search form HTML.");
  const body = bodyMatch[1];

  const hidden = new Map();
  const inputRe = /<input\b[^>]*>/gi;
  let im;
  // eslint-disable-next-line no-cond-assign
  while ((im = inputRe.exec(body))) {
    const tag = im[0];
    const a = parseTagAttributes(tag);
    const name = a.name;
    if (!name) continue;

    const type = (a.type || "text").toLowerCase();
    if (type !== "hidden") continue;

    hidden.set(name, a.value ?? "");
  }

  return { action, hidden };
}

function extractRefsFromResultsHtml(html) {
  // City of London search results include "Ref. No: 26/00192/NMA" in text.
  const text = stripHtmlToText(html);
  const refRe = /\b\d{2}\/\d{4,6}\/[A-Za-z0-9]+(?:\/[A-Za-z0-9]+)?\b/g;
  const matches = text.match(refRe) || [];
  // Preserve order while de-duping.
  const out = [];
  const seen = new Set();
  for (const r of matches) {
    if (!seen.has(r)) {
      seen.add(r);
      out.push(r);
    }
  }
  return out;
}

function extractNextHrefFromResultsHtml(html) {
  // Typical:
  // <a href="/online-applications/pagedSearchResults.do?action=page&amp;searchCriteria.page=2" class="next">Next...</a>
  const nextByClass = html.match(
    /<a\b[^>]*class\s*=\s*(?:"[^"]*\bnext\b[^"]*"|'[^']*\bnext\b[^']*')[^>]*>/i,
  );
  if (nextByClass) {
    const attrs = parseTagAttributes(nextByClass[0]);
    if (attrs.href) return attrs.href;
  }

  const relNext = html.match(/<a\b[^>]*rel\s*=\s*(?:"next"|'next')[^>]*>/i);
  if (relNext) {
    const attrs = parseTagAttributes(relNext[0]);
    if (attrs.href) return attrs.href;
  }

  return null;
}

function detectNoResults(html) {
  const t = stripHtmlToText(html).toLowerCase();
  return t.includes("no results found") || t.includes("your search returned no results");
}

function parseTitle(html) {
  const m = String(html || "").match(/<title>\s*([\s\S]*?)\s*<\/title>/i);
  return m ? stripHtmlToText(m[1]) : "";
}

function runCurl(args, { timeoutMs = 60000 } = {}) {
  return new Promise((resolve, reject) => {
    const resolveArgs = [];
    for (const entry of argv["curl-resolve"] || []) {
      resolveArgs.push("--resolve", String(entry));
    }
    const v4 = argv.ipv4 ? ["-4"] : [];
    const child = spawn("curl", ["-sS", ...v4, ...resolveArgs, ...args], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
    }, timeoutMs);

    child.stdout.on("data", (c) => {
      stdout += c.toString();
    });
    child.stderr.on("data", (c) => {
      stderr += c.toString();
    });

    child.on("error", (e) => {
      clearTimeout(timer);
      reject(e);
    });

    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ code, stdout, stderr });
    });
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isRetryableCurlResult(r) {
  const err = (r.stderr || "").toLowerCase();
  if (r.code === 0) return false;
  if (err.includes("could not resolve host")) return true; // curl (6)
  if (err.includes("failed to connect")) return true;
  if (err.includes("connection timed out")) return true;
  if (err.includes("connection reset by peer")) return true;
  if (err.includes("empty reply from server")) return true;
  return false;
}

async function runCurlWithRetry(args, { timeoutMs = 60000, retries = 4, baseDelayMs = 750 } = {}) {
  let last = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    // eslint-disable-next-line no-await-in-loop
    const r = await runCurl(args, { timeoutMs });
    last = r;
    if (r.code === 0) return r;
    if (!isRetryableCurlResult(r) || attempt >= retries) break;
    // eslint-disable-next-line no-await-in-loop
    await sleep(baseDelayMs * Math.pow(2, attempt));
  }
  return last;
}

(async () => {
  const ts = stamp();
  const base = `idox_validated_range_${ts}`;

  const startUrl = argv["start-url"];
  const validatedStart = toIdoxDmy(argv["validated-start"]);
  const validatedEnd = toIdoxDmy(argv["validated-end"]);
  const maxPages = Number(argv["max-pages"] || 100);

  if (argv.headed) {
    console.warn("⚠️ --headed is ignored by this scraper (HTTP mode; no browser).");
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "idox_refs_"));
  const cookieFile = path.join(tmpDir, "cookies.txt");

  console.log("GET:", startUrl);
  const res0 = await runCurlWithRetry(["-L", "-c", cookieFile, startUrl], { timeoutMs: 60000 });
  if (res0.code !== 0) throw new Error(`curl failed fetching start URL. ${res0.stderr.trim()}`);
  const html0 = res0.stdout;
  const ch0 = detectChallenge(html0, parseTitle(html0));
  if (ch0) throw new Error(`Challenge detected on start page: ${ch0}`);

  if (argv.artifacts) fs.writeFileSync(`${base}_advanced.html`, html0, "utf8");

  const { action, hidden } = parseAdvancedSearchForm(html0);
  const postUrl = new URL(action, startUrl).toString();

  const params = new URLSearchParams();
  for (const [k, v] of hidden.entries()) params.set(k, v);
  params.set("date(applicationValidatedStart)", validatedStart);
  params.set("date(applicationValidatedEnd)", validatedEnd);

  // Some Idox installs require this hidden field; ensure it exists.
  if (!params.get("searchType")) params.set("searchType", "Application");

  console.log("POST:", postUrl);
  const curlPostArgs = ["-L", "-b", cookieFile, "-c", cookieFile, "-X", "POST"];
  // Include all hidden inputs by default to match real browser submissions more closely.
  for (const [k, v] of hidden.entries()) {
    curlPostArgs.push("--data-urlencode", `${k}=${v}`);
  }
  curlPostArgs.push("--data-urlencode", `date(applicationValidatedStart)=${validatedStart}`);
  curlPostArgs.push("--data-urlencode", `date(applicationValidatedEnd)=${validatedEnd}`);
  if (!hidden.has("searchType")) curlPostArgs.push("--data-urlencode", "searchType=Application");
  curlPostArgs.push(postUrl);

  const res1 = await runCurlWithRetry(curlPostArgs, { timeoutMs: 60000 });
  if (res1.code !== 0) throw new Error(`curl failed submitting search. ${res1.stderr.trim()}`);
  let pageUrl = postUrl;
  let resultsHtml = res1.stdout;

  const ch1 = detectChallenge(resultsHtml, parseTitle(resultsHtml));
  if (ch1) throw new Error(`Challenge detected on results page 1: ${ch1}`);

  if (argv.artifacts) fs.writeFileSync(`${base}_results_page1.html`, resultsHtml, "utf8");

  const out = {
    fetched_at: new Date().toISOString(),
    start_url: startUrl,
    validated_start: validatedStart,
    validated_end: validatedEnd,
    pages_visited: 0,
    refs: [],
  };

  const seen = new Set();

  for (let i = 1; i <= maxPages; i++) {
    out.pages_visited = i;
    const pageRefs = extractRefsFromResultsHtml(resultsHtml);
    for (const r of pageRefs) {
      if (!seen.has(r)) {
        seen.add(r);
        out.refs.push(r);
      }
    }

    const noResults = detectNoResults(resultsHtml);
    const nextHref = extractNextHrefFromResultsHtml(resultsHtml);
    if (noResults || !nextHref) break;

    const nextUrl = new URL(nextHref, pageUrl).toString();
    console.log("GET:", nextUrl);
    const resN = await runCurlWithRetry(["-L", "-b", cookieFile, "-c", cookieFile, nextUrl], { timeoutMs: 60000 });
    if (resN.code !== 0) throw new Error(`curl failed fetching next page ${i + 1}. ${resN.stderr.trim()}`);
    pageUrl = nextUrl;
    resultsHtml = resN.stdout;

    const chN = detectChallenge(resultsHtml, parseTitle(resultsHtml));
    if (chN) throw new Error(`Challenge detected on results page ${i + 1}: ${chN}`);

    if (argv.artifacts && i < 5) {
      const tag = safeFilename(`${base}_results_page${i + 1}`);
      fs.writeFileSync(`${tag}.html`, resultsHtml, "utf8");
    }
  }

  if (argv.artifacts) {
    fs.writeFileSync(`${base}_refs.json`, JSON.stringify(out, null, 2), "utf8");
    console.log(`Saved artifacts: ${base}_refs.json`);
  }

  console.log(`Found ${out.refs.length} application references.`);
  out.refs.forEach((r) => console.log(r));

  if (argv["emit-json"]) {
    console.log(`__IDOX_VALIDATED_RANGE_REFS__=${JSON.stringify(out)}`);
  }
})().catch((e) => {
  console.error("❌ Scrape failed:", e);
  process.exitCode = 1;
});
