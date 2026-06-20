#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("classify-wcc-retrofit-lbc")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to analyse. Default is Westminster.",
  })
  .option("years", {
    type: "number",
    default: 5,
    describe: "Rolling year window based on validated/received/date_added.",
  })
  .option("limit", {
    type: "number",
    default: 0,
    describe: "Maximum applications to inspect after the base SQL filter (0 = no limit).",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into the base SQL result set.",
  })
  .option("batch-size", {
    type: "number",
    default: 25,
    describe: "Applications per LLM batch.",
  })
  .option("openai-model", {
    type: "string",
    default: "gpt-4.1-mini",
    describe: "OpenAI model for first-cut retrofit classification.",
  })
  .option("openai-timeout-ms", {
    type: "number",
    default: 120000,
    describe: "Timeout for each OpenAI request in milliseconds.",
  })
  .option("prompt-version", {
    type: "string",
    default: "wcc-retrofit-lbc-v1",
    describe: "Prompt version recorded with the analysis rows.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/wcc_retrofit_lbc_classification_summary.json",
    describe: "Path for a summary JSON artifact.",
  })
  .strict()
  .help()
  .argv;

const BASE_SQL = `
  SELECT
    a.ons_code,
    a.reference,
    a.keyval,
    a.proposal,
    a.address,
    a.application_type,
    a.application_received,
    a.application_validated,
    a.decision_made_date,
    a.decision_issued_date,
    a.decision,
    a.actual_decision_level,
    a.source_url,
    a.planit_json,
    a.unified_json
  FROM public.applications a
  WHERE a.ons_code = $1
    AND a.reference ~ '/LBC$'
    AND COALESCE(a.application_validated, a.application_received, a.date_added, current_date)
          >= (CURRENT_DATE - make_interval(years => $2::int))
  ORDER BY COALESCE(a.application_validated, a.application_received, a.date_added, current_date) DESC, a.reference ASC
  OFFSET $3
`;

const UPSERT_SQL = `
  INSERT INTO public.wcc_retrofit_lbc_applications (
    ons_code,
    reference,
    keyval,
    application_received,
    application_validated,
    decision_made_date,
    decision_issued_date,
    decision,
    actual_decision_level,
    proposal,
    address,
    application_type,
    source_snapshot_json,
    prefilter_status,
    prefilter_keywords,
    analysis_status,
    is_candidate,
    confidence,
    reasoning,
    retrofit_themes,
    model,
    prompt_version,
    raw_model_json,
    determination_days,
    approval_status,
    decision_route,
    documents_url,
    first_classified_at,
    last_classified_at,
    updated_at
  ) VALUES (
    $1, $2, $3,
    $4::date, $5::date, $6::date, $7::date,
    $8, $9, $10, $11, $12,
    $13::jsonb, $14, $15::jsonb, $16, $17, $18, $19, $20::jsonb,
    $21, $22, $23::jsonb, $24, $25, $26, $27, now(), now(), now()
  )
  ON CONFLICT (ons_code, reference) DO UPDATE SET
    keyval = EXCLUDED.keyval,
    application_received = COALESCE(EXCLUDED.application_received, public.wcc_retrofit_lbc_applications.application_received),
    application_validated = COALESCE(EXCLUDED.application_validated, public.wcc_retrofit_lbc_applications.application_validated),
    decision_made_date = COALESCE(EXCLUDED.decision_made_date, public.wcc_retrofit_lbc_applications.decision_made_date),
    decision_issued_date = COALESCE(EXCLUDED.decision_issued_date, public.wcc_retrofit_lbc_applications.decision_issued_date),
    decision = COALESCE(EXCLUDED.decision, public.wcc_retrofit_lbc_applications.decision),
    actual_decision_level = COALESCE(EXCLUDED.actual_decision_level, public.wcc_retrofit_lbc_applications.actual_decision_level),
    proposal = COALESCE(EXCLUDED.proposal, public.wcc_retrofit_lbc_applications.proposal),
    address = COALESCE(EXCLUDED.address, public.wcc_retrofit_lbc_applications.address),
    application_type = COALESCE(EXCLUDED.application_type, public.wcc_retrofit_lbc_applications.application_type),
    source_snapshot_json = EXCLUDED.source_snapshot_json,
    prefilter_status = EXCLUDED.prefilter_status,
    prefilter_keywords = EXCLUDED.prefilter_keywords,
    analysis_status = EXCLUDED.analysis_status,
    is_candidate = EXCLUDED.is_candidate,
    confidence = EXCLUDED.confidence,
    reasoning = EXCLUDED.reasoning,
    retrofit_themes = EXCLUDED.retrofit_themes,
    model = EXCLUDED.model,
    prompt_version = EXCLUDED.prompt_version,
    raw_model_json = EXCLUDED.raw_model_json,
    determination_days = EXCLUDED.determination_days,
    approval_status = EXCLUDED.approval_status,
    decision_route = EXCLUDED.decision_route,
    documents_url = EXCLUDED.documents_url,
    last_classified_at = now(),
    updated_at = now(),
    first_classified_at = COALESCE(public.wcc_retrofit_lbc_applications.first_classified_at, now())
`;

const PREFILTER_RULES = [
  { keyword: "solar", pattern: /\bsolar\b|\bpv\b|\bphotovoltaic\b|\bsolar thermal\b/i },
  { keyword: "heat_pump", pattern: /\bheat pump\b|\bair source\b|\bground source\b|\bashp\b|\bgshp\b/i },
  { keyword: "insulation", pattern: /\binsulation\b|\binsulated\b|\bthermal\b|\benergy efficiency\b/i },
  { keyword: "windows_glazing", pattern: /\bwindow\b|\bwindows\b|\bglazing\b|\bsecondary glazing\b|\bdouble glazing\b|\btriple glazing\b/i },
  { keyword: "draughtproofing", pattern: /\bdraught[ -]?proof/i },
  { keyword: "ventilation", pattern: /\bmvhr\b|\bventilation\b|\bmechanical ventilation\b/i },
  { keyword: "heating_services", pattern: /\bheating\b|\bboiler\b|\bplant\b|\bservices\b|\bmechanical\b|\belectrical\b/i },
  { keyword: "roof_fabric", pattern: /\broof\b|\brooflight\b|\brooflights\b|\bceiling\b|\bfabric\b|\bcladding\b|\bfa[cç]ade\b/i },
  { keyword: "doors_envelope", pattern: /\bdoor\b|\bdoors\b|\bshutter\b|\bweatherproof/i },
  { keyword: "retrofit_general", pattern: /\bretrofit\b|\bdecarbon/i },
];

const ALLOWED_THEMES = new Set([
  "solar_pv",
  "heat_pump",
  "windows_glazing",
  "insulation",
  "draughtproofing",
  "ventilation",
  "heating_services",
  "roof_fabric",
  "doors_envelope",
  "retrofit_general",
  "other_performance_upgrade",
]);

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

function mkdirpForFile(filePath) {
  fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
}

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
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
      try {
        return JSON.parse(raw.slice(first, last + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
}

function normalizeText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function buildDocumentsUrl(keyval) {
  const clean = normalizeText(keyval);
  if (!clean) return null;
  return `https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=${encodeURIComponent(clean)}`;
}

function computeDeterminationDays(row) {
  const startRaw = row.application_validated || row.application_received || null;
  const endRaw = row.decision_issued_date || row.decision_made_date || null;
  if (!startRaw || !endRaw) return null;
  const start = new Date(startRaw);
  const end = new Date(endRaw);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return null;
  return Math.round((end.getTime() - start.getTime()) / 86400000);
}

function normalizeApprovalStatus(decision) {
  const value = normalizeText(decision).toLowerCase();
  if (!value || value === "(null)") return "unknown";
  if (value.includes("permit") || value.includes("approved") || value.includes("consent")) return "approved";
  if (value.includes("refus")) return "refused";
  if (value.includes("withdraw")) return "withdrawn";
  if (value.includes("no further action")) return "other";
  if (value === "nr" || value === "not required") return "other";
  return "other";
}

function normalizeDecisionRoute(actualDecisionLevel) {
  const value = normalizeText(actualDecisionLevel).toLowerCase();
  if (!value || value === "not available" || value === "(null)") return "unknown";
  if (value.includes("committee") || value.includes("sub-committee")) return "committee";
  if (value.includes("delegated")) return "delegated";
  return "unknown";
}

function computePrefilterKeywords(row) {
  const hay = [
    row.proposal,
    row.application_type,
  ].map((part) => normalizeText(part)).filter(Boolean).join(" | ");
  if (!hay) return [];
  return PREFILTER_RULES.filter((rule) => rule.pattern.test(hay)).map((rule) => rule.keyword);
}

function buildSourceSnapshot(row) {
  return {
    reference: row.reference,
    keyval: row.keyval,
    proposal: row.proposal,
    address: row.address,
    application_type: row.application_type,
    application_received: row.application_received,
    application_validated: row.application_validated,
    decision_made_date: row.decision_made_date,
    decision_issued_date: row.decision_issued_date,
    decision: row.decision,
    actual_decision_level: row.actual_decision_level,
    source_url: row.source_url || null,
  };
}

async function classifyBatchWithOpenAi(batch) {
  const apiKey = getOpenAiApiKey();
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(5000, Number(argv["openai-timeout-ms"]) || 120000));
  try {
    const resp = await fetch(`${endpointBase}/chat/completions`, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: String(argv["openai-model"]),
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: [
              "You are screening Westminster listed building consent applications for a first-cut environmental retrofit study.",
              "The study is intentionally broad.",
              "Mark an application as a candidate if the proposal appears to involve environmental retrofit or building-improvement works that plausibly improve environmental performance.",
              "Include obvious cases such as PV panels, solar, heat pumps, insulation, secondary glazing, window upgrades, draughtproofing, ventilation and heating/services upgrades.",
              "Also include broader fabric or envelope works if they plausibly support energy or thermal performance, even if the proposal is not explicitly framed as retrofit.",
              "Exclude clearly unrelated heritage, internal fit-out or alteration proposals with no plausible performance angle.",
              "Do not invent facts.",
              "Return strict JSON only.",
            ].join(" "),
          },
          {
            role: "user",
            content: [
              "For each application, return one result object.",
              "Output shape:",
              "{",
              '  "results": [',
              '    {',
              '      "reference": "string",',
              '      "is_candidate": true,',
              '      "confidence": "high|medium|low",',
              '      "retrofit_themes": ["solar_pv|heat_pump|windows_glazing|insulation|draughtproofing|ventilation|heating_services|roof_fabric|doors_envelope|retrofit_general|other_performance_upgrade"],',
              '      "reasoning": "short explanation"',
              "    }",
              "  ]",
              "}",
              "",
              "Applications:",
              JSON.stringify(batch, null, 2),
            ].join("\n"),
          },
        ],
      }),
    });

    if (!resp.ok) {
      const errBody = await resp.text().catch(() => "");
      throw new Error(`OpenAI HTTP ${resp.status}: ${errBody.slice(0, 500)}`);
    }
    const payload = await resp.json();
    const parsed = parseJsonObjectLoose(payload?.choices?.[0]?.message?.content || "");
    if (!parsed || !Array.isArray(parsed.results)) {
      throw new Error("OpenAI response did not contain a results array");
    }
    return {
      modelReturned: payload?.model || String(argv["openai-model"]),
      results: parsed.results,
    };
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeModelResult(raw, rowByReference) {
  const reference = normalizeText(raw?.reference);
  if (!reference || !rowByReference.has(reference)) return null;
  const conf = normalizeText(raw?.confidence).toLowerCase();
  const confidence = ["high", "medium", "low"].includes(conf) ? conf : "low";
  const themes = Array.isArray(raw?.retrofit_themes)
    ? raw.retrofit_themes
        .map((theme) => normalizeText(theme))
        .filter((theme) => ALLOWED_THEMES.has(theme))
    : [];
  return {
    reference,
    is_candidate: Boolean(raw?.is_candidate),
    confidence,
    retrofit_themes: [...new Set(themes)],
    reasoning: normalizeText(raw?.reasoning) || null,
    raw_model_json: raw,
  };
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const outputPath = path.resolve(String(argv["output-json"]));
    mkdirpForFile(outputPath);

    const limitClause = Number(argv.limit) > 0 ? ` LIMIT ${Math.max(1, Number(argv.limit))}` : "";
    const sql = `${BASE_SQL}${limitClause}`;
    const baseRes = await client.query(sql, [
      String(argv["ons-code"]),
      Math.max(1, Number(argv.years) || 5),
      Math.max(0, Number(argv.offset) || 0),
    ]);

    const allRows = baseRes.rows;
    const prefiltered = [];
    let prefilterExcluded = 0;

    for (const row of allRows) {
      const prefilterKeywords = computePrefilterKeywords(row);
      const determinationDays = computeDeterminationDays(row);
      const approvalStatus = normalizeApprovalStatus(row.decision);
      const decisionRoute = normalizeDecisionRoute(row.actual_decision_level);
      const common = {
        ons_code: row.ons_code,
        reference: row.reference,
        keyval: row.keyval || null,
        application_received: row.application_received || null,
        application_validated: row.application_validated || null,
        decision_made_date: row.decision_made_date || null,
        decision_issued_date: row.decision_issued_date || null,
        decision: row.decision || null,
        actual_decision_level: row.actual_decision_level || null,
        proposal: row.proposal || null,
        address: row.address || null,
        application_type: row.application_type || null,
        source_snapshot_json: buildSourceSnapshot(row),
        prefilter_keywords: prefilterKeywords,
        determination_days: determinationDays,
        approval_status: approvalStatus,
        decision_route: decisionRoute,
        documents_url: buildDocumentsUrl(row.keyval),
      };

      if (!prefilterKeywords.length) {
        await client.query(UPSERT_SQL, [
          common.ons_code,
          common.reference,
          common.keyval,
          common.application_received,
          common.application_validated,
          common.decision_made_date,
          common.decision_issued_date,
          common.decision,
          common.actual_decision_level,
          common.proposal,
          common.address,
          common.application_type,
          JSON.stringify(common.source_snapshot_json),
          "keyword_excluded",
          JSON.stringify(prefilterKeywords),
          "prefilter_excluded",
          false,
          null,
          null,
          JSON.stringify([]),
          null,
          String(argv["prompt-version"]),
          JSON.stringify({ prefilter_only: true }),
          common.determination_days,
          common.approval_status,
          common.decision_route,
          common.documents_url,
        ]);
        prefilterExcluded += 1;
        continue;
      }

      prefiltered.push({ ...common });
    }

    const batchSize = Math.max(1, Number(argv["batch-size"]) || 25);
    const summary = {
      ons_code: String(argv["ons-code"]),
      years: Math.max(1, Number(argv.years) || 5),
      prompt_version: String(argv["prompt-version"]),
      model: String(argv["openai-model"]),
      total_base_rows: allRows.length,
      prefilter_excluded: prefilterExcluded,
      sent_to_model: prefiltered.length,
      candidates: 0,
      not_candidates: 0,
      batches: 0,
      references: [],
    };

    for (let i = 0; i < prefiltered.length; i += batchSize) {
      const chunk = prefiltered.slice(i, i + batchSize);
      const batchPayload = chunk.map((row) => ({
        reference: row.reference,
        proposal: row.proposal,
        application_type: row.application_type,
        address: row.address,
      }));
      const rowByReference = new Map(chunk.map((row) => [row.reference, row]));
      logEvent("classify_batch_start", { batch_index: summary.batches + 1, batch_size: chunk.length });
      const ai = await classifyBatchWithOpenAi(batchPayload);
      const normalizedResults = new Map();
      for (const raw of ai.results) {
        const normalized = normalizeModelResult(raw, rowByReference);
        if (normalized) normalizedResults.set(normalized.reference, normalized);
      }

      for (const row of chunk) {
        const result = normalizedResults.get(row.reference) || {
          reference: row.reference,
          is_candidate: false,
          confidence: "low",
          retrofit_themes: [],
          reasoning: "No valid model result returned for this reference.",
          raw_model_json: null,
        };
        await client.query(UPSERT_SQL, [
          row.ons_code,
          row.reference,
          row.keyval,
          row.application_received,
          row.application_validated,
          row.decision_made_date,
          row.decision_issued_date,
          row.decision,
          row.actual_decision_level,
          row.proposal,
          row.address,
          row.application_type,
          JSON.stringify(row.source_snapshot_json),
          "keyword_matched",
          JSON.stringify(row.prefilter_keywords),
          "classified",
          result.is_candidate,
          result.confidence,
          result.reasoning,
          JSON.stringify(result.retrofit_themes),
          ai.modelReturned,
          String(argv["prompt-version"]),
          JSON.stringify(result.raw_model_json),
          row.determination_days,
          row.approval_status,
          row.decision_route,
          row.documents_url,
        ]);
        if (result.is_candidate) {
          summary.candidates += 1;
          summary.references.push(row.reference);
        } else {
          summary.not_candidates += 1;
        }
      }
      summary.batches += 1;
      logEvent("classify_batch_done", {
        batch_index: summary.batches,
        batch_size: chunk.length,
        candidates_so_far: summary.candidates,
      });
    }

    fs.writeFileSync(outputPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    logEvent("done", {
      total_base_rows: summary.total_base_rows,
      prefilter_excluded: summary.prefilter_excluded,
      sent_to_model: summary.sent_to_model,
      candidates: summary.candidates,
      output_json: outputPath,
    });
  } finally {
    await client.end().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack || err.message : String(err));
  process.exit(1);
});
