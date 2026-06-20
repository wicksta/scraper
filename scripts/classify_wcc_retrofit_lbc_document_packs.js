#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("classify-wcc-retrofit-lbc-document-packs")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to analyse. Default is Westminster.",
  })
  .option("limit", {
    type: "number",
    default: 0,
    describe: "Maximum inventory rows to inspect (0 = no limit).",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into the candidate inventory set.",
  })
  .option("batch-size", {
    type: "number",
    default: 25,
    describe: "Inventory rows per OpenAI batch.",
  })
  .option("only-unclassified", {
    type: "boolean",
    default: true,
    describe: "When true, only process rows without a completed pack analysis result.",
  })
  .option("skip-openai", {
    type: "boolean",
    default: false,
    describe: "When true, do not send ambiguous rows to OpenAI; leave them as needs_model.",
  })
  .option("openai-model", {
    type: "string",
    default: "gpt-4.1-mini",
    describe: "OpenAI model for ambiguous document metadata classification.",
  })
  .option("openai-timeout-ms", {
    type: "number",
    default: 120000,
    describe: "Timeout for each OpenAI request in milliseconds.",
  })
  .option("prompt-version", {
    type: "string",
    default: "wcc-retrofit-lbc-document-pack-v2-report-split",
    describe: "Prompt version recorded with analysis rows.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/wcc_retrofit_lbc_document_pack_classification_summary.json",
    describe: "Path for a summary JSON artifact.",
  })
  .strict()
  .help()
  .argv;

const FIND_SQL = `
  SELECT
    d.id AS inventory_id,
    d.application_analysis_id,
    d.ons_code,
    d.reference,
    d.document_url,
    d.description,
    d.document_type,
    d.published_date,
    d.normalized_class,
    d.raw_source_json,
    a.retrofit_themes,
    p.analysis_status AS existing_analysis_status
  FROM public.wcc_retrofit_lbc_document_inventory d
  JOIN public.wcc_retrofit_lbc_applications a
    ON a.id = d.application_analysis_id
  LEFT JOIN public.wcc_retrofit_lbc_document_pack_analysis p
    ON p.inventory_id = d.id
  WHERE d.ons_code = $1
    AND a.is_candidate = true
    AND a.analysis_status = 'classified'
    AND (
      $2::boolean = false
      OR p.inventory_id IS NULL
      OR p.analysis_status IN ('new', 'needs_model', 'failed')
    )
  ORDER BY d.reference ASC, d.id ASC
  OFFSET $3
`;

const UPSERT_SQL = `
  INSERT INTO public.wcc_retrofit_lbc_document_pack_analysis (
    inventory_id,
    application_analysis_id,
    ons_code,
    reference,
    analysis_status,
    include_in_pack,
    exclusion_reason,
    normalized_pack_class,
    normalized_pack_label,
    classifier_stage,
    model_confidence,
    model_reasoning,
    prompt_version,
    model,
    raw_model_json,
    analyzed_at,
    updated_at
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb, now(), now()
  )
  ON CONFLICT (inventory_id) DO UPDATE SET
    application_analysis_id = EXCLUDED.application_analysis_id,
    ons_code = EXCLUDED.ons_code,
    reference = EXCLUDED.reference,
    analysis_status = EXCLUDED.analysis_status,
    include_in_pack = EXCLUDED.include_in_pack,
    exclusion_reason = EXCLUDED.exclusion_reason,
    normalized_pack_class = EXCLUDED.normalized_pack_class,
    normalized_pack_label = EXCLUDED.normalized_pack_label,
    classifier_stage = EXCLUDED.classifier_stage,
    model_confidence = EXCLUDED.model_confidence,
    model_reasoning = EXCLUDED.model_reasoning,
    prompt_version = EXCLUDED.prompt_version,
    model = EXCLUDED.model,
    raw_model_json = EXCLUDED.raw_model_json,
    analyzed_at = now(),
    updated_at = now()
`;

const ALLOWED_CLASSES = new Set([
  "application_form",
  "location_site_plan",
  "existing_drawings",
  "proposed_drawings",
  "general_drawings",
  "design_access_statement",
  "heritage_statement",
  "planning_statement",
  "energy_sustainability_statement",
  "cover_letter",
  "window_specification",
  "insulation_specification",
  "plant_mep_specification",
  "roof_details",
  "flood_drainage_report",
  "daylight_sunlight_report",
  "structural_report",
  "acoustic_air_quality_report",
  "ecology_arboriculture_report",
  "fire_safety_report",
  "transport_report",
  "building_condition_fabric_report",
  "technical_report",
  "method_statement",
  "statement_of_significance",
  "schedule",
  "photographs",
  "other_supporting_document",
  "exclude_procedural",
]);

const LABELS = {
  application_form: "Application form",
  location_site_plan: "Location / site plan",
  existing_drawings: "Existing drawings",
  proposed_drawings: "Proposed drawings",
  general_drawings: "General drawings",
  design_access_statement: "Design and access statement",
  heritage_statement: "Heritage statement",
  planning_statement: "Planning statement",
  energy_sustainability_statement: "Energy / sustainability statement",
  cover_letter: "Cover letter",
  window_specification: "Window / glazing specification",
  insulation_specification: "Insulation specification",
  plant_mep_specification: "Plant / MEP specification",
  roof_details: "Roof details",
  flood_drainage_report: "Flood / drainage report",
  daylight_sunlight_report: "Daylight / sunlight report",
  structural_report: "Structural report",
  acoustic_air_quality_report: "Acoustic / air quality report",
  ecology_arboriculture_report: "Ecology / arboriculture report",
  fire_safety_report: "Fire safety report",
  transport_report: "Transport report",
  building_condition_fabric_report: "Building condition / fabric report",
  technical_report: "Technical report",
  method_statement: "Method statement",
  statement_of_significance: "Statement of significance",
  schedule: "Schedule",
  photographs: "Photographs",
  other_supporting_document: "Other supporting document",
  exclude_procedural: "Procedural / council-generated",
};

const EXCLUDE_TYPE_PATTERNS = [
  /\bdecision notice\b/i,
  /\backrec\b/i,
  /\breport\b/i,
  /\bconsultee comment\b/i,
  /\bobjection comment\b/i,
  /\bpublic comment\b/i,
  /\bincoming correspondence\b/i,
  /\boutgoing correspondence\b/i,
  /\bsuperseded document\b/i,
];

const EXCLUDE_TEXT_PATTERNS = [
  /\breceipt of application\b/i,
  /\bgrant of listed building consent\b/i,
  /\brefusal[- ]listed building consent\b/i,
  /\bdelegated report\b/i,
  /\bcommittee report\b/i,
  /\battachmentsummary\b/i,
  /\bhistoric england\b/i,
  /\bconsultee\b/i,
];

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

function normalizeText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
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

function labelForClass(normalizedPackClass) {
  return LABELS[normalizedPackClass] || "Other supporting document";
}

function proceduralExclusion(row) {
  const type = normalizeText(row.document_type);
  const desc = normalizeText(row.description);
  const hay = `${type} ${desc}`;
  if (EXCLUDE_TYPE_PATTERNS.some((pattern) => pattern.test(type))) {
    return "procedural_document_type";
  }
  if (EXCLUDE_TEXT_PATTERNS.some((pattern) => pattern.test(hay))) {
    return "procedural_description";
  }
  return null;
}

function deterministicPackClassification(row) {
  const exclusionReason = proceduralExclusion(row);
  if (exclusionReason) {
    return {
      analysis_status: "classified",
      include_in_pack: false,
      exclusion_reason: exclusionReason,
      normalized_pack_class: "exclude_procedural",
      normalized_pack_label: labelForClass("exclude_procedural"),
      classifier_stage: "rule",
      model_confidence: "high",
      model_reasoning: "Excluded as procedural, council-generated or superseded material.",
      raw_model_json: null,
    };
  }

  const type = normalizeText(row.document_type).toLowerCase();
  const desc = normalizeText(row.description).toLowerCase();
  const existingClass = normalizeText(row.normalized_class).toLowerCase();
  const hay = `${type} ${desc} ${existingClass}`;

  const include = (normalizedPackClass, reasoning) => ({
    analysis_status: "classified",
    include_in_pack: true,
    exclusion_reason: null,
    normalized_pack_class: normalizedPackClass,
    normalized_pack_label: labelForClass(normalizedPackClass),
    classifier_stage: "rule",
    model_confidence: "high",
    model_reasoning: reasoning,
    raw_model_json: null,
  });

  if (/\bapplication form\b/i.test(hay)) return include("application_form", "Application form identified from metadata.");
  if (/\bdesign and access\b/i.test(hay)) return include("design_access_statement", "Design and access statement identified from metadata.");
  if (/\bheritage statement\b/i.test(hay)) return include("heritage_statement", "Heritage statement identified from metadata.");
  if (/\bplanning statement\b/i.test(hay)) return include("planning_statement", "Planning statement identified from metadata.");
  if (/\benergy assessment\b|\benergy statement\b|\benergy strategy\b|\bsustainability\b/i.test(hay)) {
    return include("energy_sustainability_statement", "Energy or sustainability statement identified from metadata.");
  }
  if (/\bstatement of significance\b/i.test(hay)) return include("statement_of_significance", "Statement of significance identified from metadata.");
  if (/\bcover(?:ing)? letter\b/i.test(hay)) return include("cover_letter", "Cover letter identified from metadata.");
  if (/\bmethod statement\b/i.test(hay)) return include("method_statement", "Method statement identified from metadata.");
  if (/\bschedule\b/i.test(hay)) return include("schedule", "Schedule identified from metadata.");
  if (/\bphoto\b|\bphotograph\b/i.test(hay)) return include("photographs", "Photographic supporting material identified from metadata.");
  if (/\blocation plan\b|\bsite block plan\b|\bsite plan\b|\bblock plan\b/i.test(hay)) {
    return include("location_site_plan", "Location or site plan identified from metadata.");
  }
  if (/\binsulation\b/i.test(hay)) return include("insulation_specification", "Insulation detail or specification identified from metadata.");
  if (/\bwindow\b|\bglazing\b|\bjoinery details?\b/i.test(hay)) return include("window_specification", "Window or glazing detail identified from metadata.");
  if (/\bheat pump\b|\bair source\b|\bground source\b|\bcondensers?\b|\bmechanical\b|\belectrical\b|\bservices\b|\bplant\b|\bacoustic report\b|\bnoise impact assessment\b/i.test(hay)) {
    return include("plant_mep_specification", "Plant or building-services technical document identified from metadata.");
  }
  if (/\broof\b|\brooflight\b|\brooftop\b/i.test(hay) && /\bdetail\b|\bplan\b|\belevation\b|\bsection\b/i.test(hay)) {
    return include("roof_details", "Roof-related detail drawing identified from metadata.");
  }
  if (/\bflood risk\b|\bdrainage statement\b|\bfoul sewage\b|\bsuds\b|\butilities assessment\b/i.test(hay)) {
    return include("flood_drainage_report", "Flood, drainage or utilities report identified from metadata.");
  }
  if (/\bdaylight\b|\bsunlight\b|\btownscape and views?\b|\bviews assessment\b/i.test(hay)) {
    return include("daylight_sunlight_report", "Daylight, sunlight or views report identified from metadata.");
  }
  if (/\bstructural\b|\bcracking survey\b|\bexisting floors?\b|\bstructural methodology\b/i.test(hay)) {
    return include("structural_report", "Structural or related technical report identified from metadata.");
  }
  if (/\bacoustic\b|\bnoise\b|\bair quality\b|\bodou?r\b|\blighting assessment\b/i.test(hay)) {
    return include("acoustic_air_quality_report", "Acoustic, noise, air quality or lighting report identified from metadata.");
  }
  if (/\barboricultural\b|\becolog(?:ical|y)\b|\bbiodiversity\b|\btree survey\b/i.test(hay)) {
    return include("ecology_arboriculture_report", "Ecology, biodiversity or arboriculture report identified from metadata.");
  }
  if (/\bfire safety\b|\bfire statement\b|\bfire strategy\b|\bgateway 1\b/i.test(hay)) {
    return include("fire_safety_report", "Fire safety report identified from metadata.");
  }
  if (/\btransport assessment\b|\btransport statement\b|\btravel plan\b/i.test(hay)) {
    return include("transport_report", "Transport report identified from metadata.");
  }
  if (/\bdamp report\b|\bcondition survey\b|\bcondition assessment\b|\bbuilding fabric\b|\bperformance evaluation\b|\bpreservation treatments\b/i.test(hay)) {
    return include("building_condition_fabric_report", "Building condition, damp or fabric report identified from metadata.");
  }
  if (/\beconomic statement\b|\bsite waste management plan\b|\beuropean technical assessment\b|\btechnical datasheet\b|\bdata books?\b|\bconservation report\b|\barchaeological\b/i.test(hay)) {
    return include("technical_report", "Supporting technical report identified from metadata.");
  }
  if (existingClass === "existing_drawings" || (/\bexisting\b/i.test(hay) && /\bplan\b|\bdrawing\b|\belevation\b|\bsection\b/i.test(hay))) {
    return include("existing_drawings", "Existing drawings identified from metadata.");
  }
  if (existingClass === "proposed_drawings" || (/\bproposed\b/i.test(hay) && /\bplan\b|\bdrawing\b|\belevation\b|\bsection\b/i.test(hay))) {
    return include("proposed_drawings", "Proposed drawings identified from metadata.");
  }
  if (existingClass === "drawings" || /\bdrawing\b|\belevation\b|\bsection\b|\bplan\b/i.test(hay)) {
    return include("general_drawings", "General drawing material identified from metadata.");
  }

  if (existingClass === "other" || existingClass === "" || type === "background papers" || type === "other") {
    return null;
  }

  return include("other_supporting_document", "Retained as substantive applicant-side supporting material.");
}

async function classifyAmbiguousBatch(batch) {
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
              "You are classifying Westminster listed building consent document inventory metadata for a retrofit application study.",
              "You only see document title/type metadata, not the files themselves.",
              "Decide whether each item should be counted as part of the applicant-side submission pack.",
              "Strictly exclude council-generated, procedural or case-management items such as decision notices, reports, receipts, acknowledgements, consultee comments, public comments, correspondence wrappers and superseded documents.",
              "Retain substantive applicant-side material such as forms, plans, drawings, statements, technical reports, specifications, schedules, photographs and method statements.",
              "Return one normalized_pack_class from this list only:",
              Array.from(ALLOWED_CLASSES).join(", "),
              "Use exclude_procedural only when the item should be excluded.",
              "If the item is substantive but unclear, prefer other_supporting_document.",
              "Return strict JSON only.",
            ].join(" "),
          },
          {
            role: "user",
            content: [
              "For each inventory row return one result object.",
              "Output shape:",
              "{",
              '  "results": [',
              '    {',
              '      "inventory_id": 123,',
              '      "include_in_pack": true,',
              '      "normalized_pack_class": "application_form|location_site_plan|existing_drawings|proposed_drawings|general_drawings|design_access_statement|heritage_statement|planning_statement|energy_sustainability_statement|cover_letter|window_specification|insulation_specification|plant_mep_specification|roof_details|flood_drainage_report|daylight_sunlight_report|structural_report|acoustic_air_quality_report|ecology_arboriculture_report|fire_safety_report|transport_report|building_condition_fabric_report|technical_report|method_statement|statement_of_significance|schedule|photographs|other_supporting_document|exclude_procedural",',
              '      "confidence": "high|medium|low",',
              '      "reasoning": "short explanation"',
              "    }",
              "  ]",
              "}",
              "",
              "Inventory rows:",
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

function normalizeModelResult(raw, rowById) {
  const inventoryId = Number(raw?.inventory_id);
  if (!Number.isInteger(inventoryId) || !rowById.has(inventoryId)) return null;
  const normalizedPackClass = normalizeText(raw?.normalized_pack_class);
  if (!ALLOWED_CLASSES.has(normalizedPackClass)) return null;
  const conf = normalizeText(raw?.confidence).toLowerCase();
  const confidence = ["high", "medium", "low"].includes(conf) ? conf : "low";
  const includeInPack =
    normalizedPackClass === "exclude_procedural" ? false : Boolean(raw?.include_in_pack);
  return {
    inventory_id: inventoryId,
    analysis_status: "classified",
    include_in_pack: includeInPack,
    exclusion_reason: includeInPack ? null : "model_excluded",
    normalized_pack_class: normalizedPackClass,
    normalized_pack_label: labelForClass(normalizedPackClass),
    classifier_stage: "model",
    model_confidence: confidence,
    model_reasoning: normalizeText(raw?.reasoning) || null,
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
      Boolean(argv["only-unclassified"]),
      Math.max(0, Number(argv.offset) || 0),
    ]);

    const rows = res.rows;
    const summary = {
      ons_code: String(argv["ons-code"]),
      prompt_version: String(argv["prompt-version"]),
      model: String(argv["openai-model"]),
      reviewed: rows.length,
      rule_classified: 0,
      model_classified: 0,
      included: 0,
      excluded: 0,
      needs_model: 0,
      by_class: {},
    };

    const ambiguous = [];

    for (const row of rows) {
      const deterministic = deterministicPackClassification(row);
      if (!deterministic) {
        if (Boolean(argv["skip-openai"])) {
          await client.query(UPSERT_SQL, [
            row.inventory_id,
            row.application_analysis_id,
            row.ons_code,
            row.reference,
            "needs_model",
            null,
            null,
            null,
            null,
            "pending_model",
            null,
            "Ambiguous metadata; requires model classification.",
            String(argv["prompt-version"]),
            null,
            JSON.stringify(null),
          ]);
          summary.needs_model += 1;
        } else {
          ambiguous.push(row);
        }
        continue;
      }

      await client.query(UPSERT_SQL, [
        row.inventory_id,
        row.application_analysis_id,
        row.ons_code,
        row.reference,
        deterministic.analysis_status,
        deterministic.include_in_pack,
        deterministic.exclusion_reason,
        deterministic.normalized_pack_class,
        deterministic.normalized_pack_label,
        deterministic.classifier_stage,
        deterministic.model_confidence,
        deterministic.model_reasoning,
        String(argv["prompt-version"]),
        null,
        JSON.stringify(deterministic.raw_model_json),
      ]);
      summary.rule_classified += 1;
      if (deterministic.include_in_pack) summary.included += 1;
      else summary.excluded += 1;
      if (deterministic.normalized_pack_class) {
        summary.by_class[deterministic.normalized_pack_class] = (summary.by_class[deterministic.normalized_pack_class] || 0) + 1;
      }
    }

    if (!argv["skip-openai"] && ambiguous.length > 0) {
      const batchSize = Math.max(1, Number(argv["batch-size"]) || 25);
      for (let i = 0; i < ambiguous.length; i += batchSize) {
        const chunk = ambiguous.slice(i, i + batchSize);
        const batchPayload = chunk.map((row) => ({
          inventory_id: row.inventory_id,
          reference: row.reference,
          document_type: row.document_type,
          description: row.description,
          current_normalized_class: row.normalized_class,
          retrofit_themes: row.retrofit_themes || [],
        }));
        const rowById = new Map(chunk.map((row) => [Number(row.inventory_id), row]));
        logEvent("document_pack_batch_start", { batch_index: Math.floor(i / batchSize) + 1, batch_size: chunk.length });
        const ai = await classifyAmbiguousBatch(batchPayload);
        const normalizedResults = new Map();
        for (const raw of ai.results) {
          const normalized = normalizeModelResult(raw, rowById);
          if (normalized) normalizedResults.set(normalized.inventory_id, normalized);
        }

        for (const row of chunk) {
          const result = normalizedResults.get(Number(row.inventory_id)) || {
            inventory_id: Number(row.inventory_id),
            analysis_status: "failed",
            include_in_pack: null,
            exclusion_reason: null,
            normalized_pack_class: null,
            normalized_pack_label: null,
            classifier_stage: "model",
            model_confidence: "low",
            model_reasoning: "No valid model classification returned for this inventory row.",
            raw_model_json: null,
          };
          await client.query(UPSERT_SQL, [
            row.inventory_id,
            row.application_analysis_id,
            row.ons_code,
            row.reference,
            result.analysis_status,
            result.include_in_pack,
            result.exclusion_reason,
            result.normalized_pack_class,
            result.normalized_pack_label,
            result.classifier_stage,
            result.model_confidence,
            result.model_reasoning,
            String(argv["prompt-version"]),
            ai.modelReturned,
            JSON.stringify(result.raw_model_json),
          ]);

          if (result.analysis_status === "classified") {
            summary.model_classified += 1;
            if (result.include_in_pack) summary.included += 1;
            else summary.excluded += 1;
            if (result.normalized_pack_class) {
              summary.by_class[result.normalized_pack_class] = (summary.by_class[result.normalized_pack_class] || 0) + 1;
            }
          }
        }
      }
    }

    fs.writeFileSync(outputPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    logEvent("done", summary);
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  process.stderr.write(`${error?.stack || error?.message || String(error)}\n`);
  process.exit(1);
});
