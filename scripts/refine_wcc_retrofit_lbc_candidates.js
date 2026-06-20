#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("refine-wcc-retrofit-lbc-candidates")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to refine. Default is Westminster.",
  })
  .option("limit", {
    type: "number",
    default: 0,
    describe: "Maximum current candidates to refine (0 = no limit).",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into the current candidate set.",
  })
  .option("batch-size", {
    type: "number",
    default: 25,
    describe: "Applications per LLM batch.",
  })
  .option("openai-model", {
    type: "string",
    default: "gpt-4.1-mini",
    describe: "OpenAI model for the stricter second-pass refinement.",
  })
  .option("openai-timeout-ms", {
    type: "number",
    default: 120000,
    describe: "Timeout for each OpenAI request in milliseconds.",
  })
  .option("prompt-version", {
    type: "string",
    default: "wcc-retrofit-lbc-v2-tight",
    describe: "Prompt version recorded with the refined analysis rows.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/wcc_retrofit_lbc_refine_summary.json",
    describe: "Path for a summary JSON artifact.",
  })
  .strict()
  .help()
  .argv;

const FIND_SQL = `
  SELECT
    id,
    ons_code,
    reference,
    keyval,
    proposal,
    address,
    application_type,
    application_received,
    application_validated,
    decision_made_date,
    decision_issued_date,
    decision,
    actual_decision_level,
    source_snapshot_json,
    prefilter_keywords,
    determination_days,
    approval_status,
    decision_route,
    documents_url
  FROM public.wcc_retrofit_lbc_applications
  WHERE ons_code = $1
    AND analysis_status = 'classified'
    AND is_candidate = true
  ORDER BY last_classified_at DESC NULLS LAST, reference ASC
  OFFSET $2
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

async function refineBatchWithOpenAi(batch) {
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
              "You are reviewing Westminster listed building consent applications for a stricter environmental retrofit study.",
              "Only keep applications that clearly involve environmental retrofit or explicit building-performance improvement works.",
              "Positive examples include photovoltaics, solar panels, heat pumps, insulation, secondary glazing, double or triple glazing, refurbishment or replacement of windows where the proposal clearly implies a thermal or glazing upgrade, draughtproofing, and explicit energy-efficiency upgrades.",
              "Treat generic air conditioning, chillers, condensers, air handling units, unspecified plant replacement, maintenance, repairs, redecoration, extensions, rearrangements and ordinary like-for-like repairs as NOT retrofit unless the proposal explicitly ties them to performance improvement.",
              "If the proposal is ambiguous, prefer false rather than true.",
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
    const res = await client.query(`${FIND_SQL}${limitClause}`, [
      String(argv["ons-code"]),
      Math.max(0, Number(argv.offset) || 0),
    ]);

    const rows = res.rows;
    const batchSize = Math.max(1, Number(argv["batch-size"]) || 25);
    const summary = {
      ons_code: String(argv["ons-code"]),
      prompt_version: String(argv["prompt-version"]),
      model: String(argv["openai-model"]),
      reviewed: rows.length,
      retained: 0,
      removed: 0,
      batches: 0,
      retained_references: [],
      removed_references: [],
    };

    for (let i = 0; i < rows.length; i += batchSize) {
      const chunk = rows.slice(i, i + batchSize);
      const batchPayload = chunk.map((row) => ({
        reference: row.reference,
        proposal: row.proposal,
        application_type: row.application_type,
        address: row.address,
        prefilter_keywords: row.prefilter_keywords || [],
        first_pass_themes: row.source_snapshot_json?.retrofit_themes || null,
      }));
      const rowByReference = new Map(chunk.map((row) => [row.reference, row]));
      logEvent("refine_batch_start", { batch_index: summary.batches + 1, batch_size: chunk.length });
      const ai = await refineBatchWithOpenAi(batchPayload);
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
          reasoning: "No valid refinement result returned for this reference.",
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
          JSON.stringify(row.source_snapshot_json || {}),
          "keyword_matched",
          JSON.stringify(row.prefilter_keywords || []),
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
          summary.retained += 1;
          summary.retained_references.push(row.reference);
        } else {
          summary.removed += 1;
          summary.removed_references.push(row.reference);
        }
      }

      summary.batches += 1;
      logEvent("refine_batch_done", {
        batch_index: summary.batches,
        batch_size: chunk.length,
        retained_so_far: summary.retained,
        removed_so_far: summary.removed,
      });
    }

    fs.writeFileSync(outputPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    logEvent("done", {
      reviewed: summary.reviewed,
      retained: summary.retained,
      removed: summary.removed,
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
