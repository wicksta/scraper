#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { chromium } from "playwright";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("detect-newmark-jobcode-for-uid")
  .option("uid", {
    type: "string",
    demandOption: true,
    describe: "Application UID/reference to inspect (e.g. 25/07409/ADFULL).",
  })
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to scope lookup.",
  })
  .option("headed", {
    type: "boolean",
    default: false,
    describe: "Run browser headed for debugging.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 60000,
    describe: "Navigation/request timeout in milliseconds.",
  })
  .option("artifacts-dir", {
    type: "string",
    default: "/mnt/HC_Volume_103054926/newmark_jobcode_uid_artifacts",
    describe: "Directory for downloaded PDFs / extracted text.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/newmark_jobcode_uid_result.json",
    describe: "Path for single-run JSON output.",
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
  .strict()
  .help()
  .argv;

const DOCS_BASE = "https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=";

const FIND_APP_SQL = `
  SELECT
    ons_code,
    reference,
    keyval,
    agent_company_name,
    planit_json
  FROM public.applications
  WHERE ons_code = $1
    AND (
      reference = $2
      OR planit_json #>> '{planit,uid}' = $2
      OR planit_json #>> '{uid}' = $2
    )
  ORDER BY CASE WHEN reference = $2 THEN 0 ELSE 1 END, reference ASC
  LIMIT 1
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

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function mkdirp(dirPath) {
  fs.mkdirSync(path.resolve(dirPath), { recursive: true });
}

function mkdirpForFile(filePath) {
  mkdirp(path.dirname(path.resolve(filePath)));
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

function isNewmarkAgent(name) {
  const txt = String(name || "").toLowerCase();
  return txt.includes("newmark") || txt.includes("gerald eve");
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

function bufferLooksLikePdf(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 5) return false;
  return buf.subarray(0, 5).toString("utf8") === "%PDF-";
}

function responseLooksLikePdf(response, body) {
  const headers = typeof response?.headers === "function" ? response.headers() : {};
  const contentType = String(headers?.["content-type"] || "").toLowerCase();
  return contentType.includes("application/pdf") || bufferLooksLikePdf(body);
}

function extractJobCodesFromText(text) {
  const out = [];
  const seen = new Set();

  const slashRx = /([A-Z]{1,12})\s*\/\s*((?:[A-Z]{1,12}\s*\/\s*)+)([UJ])\s*0*([0-9]{3,8})/gi;
  let m;
  while ((m = slashRx.exec(text)) !== null) {
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

async function parseDocumentsTable(page) {
  return page.$$eval("#Documents tbody tr, table#Documents tbody tr, table[id='Documents'] tbody tr", (rows) => {
    const docs = [];
    for (const row of rows) {
      const cells = row.querySelectorAll("td");
      if (!cells || cells.length < 4) continue;

      const viewCell = cells[cells.length - 1];
      const descriptionCell = cells[cells.length - 2];
      const docTypeCell = cells[cells.length - 3];
      const dateCell = cells[cells.length - 4];

      const viewLink = viewCell?.querySelector("a")?.getAttribute("href") || null;
      if (!viewLink) continue;
      docs.push({
        date_published: dateCell?.textContent?.trim() || null,
        document_type: docTypeCell?.textContent?.trim() || null,
        description: descriptionCell?.textContent?.trim() || null,
        href: viewLink,
      });
    }
    return docs;
  });
}

function buildDefaultResult(uid, onsCode) {
  return {
    uid,
    ons_code: onsCode,
    reference: null,
    keyval: null,
    agent_company_name: null,
    is_newmark: false,
    documents_url: null,
    source_doc_url: null,
    source_doc_description: null,
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

async function main() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const rootDir = path.resolve(scriptDir, "..");
  const outputJsonPath = path.resolve(rootDir, argv["output-json"]);
  mkdirpForFile(outputJsonPath);
  const artifactsResolution = resolveWritableArtifactsDir(argv["artifacts-dir"], "/tmp/newmark_jobcode_uid_artifacts");
  const artifactsRoot = artifactsResolution.dir;

  const uid = String(argv.uid || "").trim();
  const onsCode = String(argv["ons-code"] || "").trim();
  const result = buildDefaultResult(uid, onsCode);

  const pgClient = new Client(getPgClientConfig());
  await pgClient.connect();

  let browser;
  try {
    logEvent("start", { uid, ons_code: onsCode });
    if (artifactsResolution.fallbackUsed) {
      logEvent("artifacts_dir_fallback", {
        requested: String(argv["artifacts-dir"]),
        using: artifactsRoot,
      });
    }

    const appRes = await pgClient.query(FIND_APP_SQL, [onsCode, uid]);
    if (!appRes.rows.length) {
      result.notes.push("application_not_found");
      result.error = "No matching application found in public.applications";
      result.status = "not_found";
      logEvent("application_lookup", { status: "miss", uid, ons_code: onsCode });
      fs.writeFileSync(outputJsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
      return;
    }

    const app = appRes.rows[0];
    result.reference = String(app.reference || "").trim();
    result.keyval = String(app.keyval || "").trim();
    result.agent_company_name = String(app.agent_company_name || "").trim();
    result.is_newmark = isNewmarkAgent(result.agent_company_name);
    logEvent("application_lookup", {
      status: "hit",
      reference: result.reference,
      is_newmark: result.is_newmark,
      has_keyval: Boolean(result.keyval),
    });

    if (!result.is_newmark) {
      result.notes.push("not_newmark_agent_company");
      result.status = "ignored_not_newmark";
    } else if (!result.keyval) {
      result.notes.push("missing_keyval");
      result.status = "missing_keyval";
    } else {
      browser = await chromium.launch({ headless: !argv.headed });
      const context = await browser.newContext({
        extraHTTPHeaders: { "Accept-Language": "en-GB,en;q=0.9" },
      });
      const page = await context.newPage();
      try {
        result.documents_url = `${DOCS_BASE}${encodeURIComponent(result.keyval)}`;
        await page.goto(result.documents_url, { waitUntil: "domcontentloaded", timeout: Number(argv["timeout-ms"]) });
        await page.waitForSelector("#Documents tbody tr, table#Documents tbody tr", {
          timeout: Math.min(Number(argv["timeout-ms"]), 15000),
        });

        const docs = await parseDocumentsTable(page);
        logEvent("documents_parsed", {
          reference: result.reference,
          keyval: result.keyval,
          docs_count: docs.length,
        });

        const coverDocs = docs.filter(classifyCoverDoc);
        result.cover_docs_considered = coverDocs;
        if (!coverDocs.length) {
          result.notes.push("no_cover_letter_doc_found");
          result.status = "no_cover_letter";
        } else {
          const first = coverDocs[0];
          result.source_doc_description = first.description || null;
          result.source_doc_url = toAbsoluteUrl(page.url(), first.href);

          if (!result.source_doc_url) {
            result.notes.push("cover_letter_url_invalid");
            result.status = "cover_letter_url_invalid";
          } else {
            const appDir = path.join(artifactsRoot, result.reference.replace(/[^A-Za-z0-9._-]/g, "_"));
            mkdirp(appDir);
            const pdfPath = path.join(appDir, "cover_letter.pdf");
            const txtPath = path.join(appDir, "cover_letter.txt");

            const response = await context.request.get(result.source_doc_url, { timeout: Number(argv["timeout-ms"]) });
            if (!response.ok()) {
              throw new Error(`cover_letter_download_http_${response.status()}`);
            }
            const body = await response.body();
            if (!responseLooksLikePdf(response, body)) {
              result.notes.push("cover_letter_non_pdf_skipped");
              result.status = "unsupported_non_pdf_doc";
              logEvent("cover_letter_non_pdf_skipped", {
                reference: result.reference,
                source_doc_url: result.source_doc_url,
                content_type: response.headers()["content-type"] || null,
              });
            } else {
              fs.writeFileSync(pdfPath, body);

              let text = "";
              try {
                text = await runPdftotext(pdfPath);
              } catch (pdfErr) {
                result.notes.push("cover_letter_pdftotext_failed");
                result.status = "pdf_text_extract_failed";
                result.error = result.error || `cover_letter_pdftotext_failed: ${pdfErr instanceof Error ? pdfErr.message : String(pdfErr)}`;
                logEvent("cover_letter_pdftotext_failed", {
                  reference: result.reference,
                  source_doc_url: result.source_doc_url,
                  error: pdfErr instanceof Error ? pdfErr.message : String(pdfErr),
                });
              }

              if (text) {
                fs.writeFileSync(txtPath, text, "utf8");

                const hits = extractJobCodesFromText(text);
                result.job_codes_found = hits.map((h) => h.canonical);
                let teamResponsibleNames = [];
                result.match_confidence = hits.length ? "high" : "none";
                result.status = hits.length ? "matched" : "no_pattern";
                if (!hits.length) result.notes.push("no_job_code_pattern_found");

                if (argv["extract-applicant-with-openai"]) {
                  try {
                    const applicant = await extractApplicantWithOpenAi(
                      text,
                      String(argv["openai-model"]),
                      Number(argv["openai-max-chars"]),
                      Number(argv["timeout-ms"]),
                    );
                    result.applicant_name_extracted = applicant.applicant_name_extracted;
                    result.applicant_evidence_quote = applicant.applicant_evidence_quote;
                    result.applicant_confidence = applicant.applicant_confidence;
                    result.applicant_extraction_model = String(argv["openai-model"]);
                    teamResponsibleNames = applicant.team_contact_names || [];
                  } catch (llmErr) {
                    result.notes.push("applicant_extraction_failed");
                    result.error = result.error || `applicant_extraction_failed: ${llmErr instanceof Error ? llmErr.message : String(llmErr)}`;
                  }
                }

                result.job_code_parts = hits.map((h) => ({
                  partner_initials: h.partner_initials,
                  assistant_initials: h.assistant_initials,
                  job_number: h.job_number,
                  raw: h.raw,
                  team_responsible_names: teamResponsibleNames,
                }));
              }
            }
          }
        }
      } finally {
        await page.close().catch(() => {});
        await context.close().catch(() => {});
      }
    }

    if (result.reference) {
      await pgClient.query(UPSERT_SQL, [
        result.ons_code,
        result.reference,
        result.keyval,
        result.agent_company_name,
        result.is_newmark,
        result.documents_url,
        result.source_doc_url,
        result.source_doc_description,
        JSON.stringify(result.cover_docs_considered || []),
        JSON.stringify(result.job_codes_found || []),
        JSON.stringify(result.job_code_parts || []),
        result.applicant_name_extracted,
        result.applicant_evidence_quote,
        result.applicant_confidence,
        result.applicant_extraction_model,
        result.match_confidence,
        JSON.stringify(result.notes || []),
        result.error,
        result.status,
      ]);
      logEvent("upsert_complete", { ons_code: result.ons_code, reference: result.reference, status: result.status });
    }

    fs.writeFileSync(outputJsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
    logEvent("done", {
      uid,
      reference: result.reference,
      status: result.status,
      matches_found: result.job_codes_found.length,
      output_json: outputJsonPath,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    result.error = message;
    result.status = "error";
    fs.writeFileSync(outputJsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
    logEvent("fatal", { uid, error: message });
    process.exit(1);
  } finally {
    await Promise.allSettled([browser?.close(), pgClient?.end()]);
  }
}

main();
