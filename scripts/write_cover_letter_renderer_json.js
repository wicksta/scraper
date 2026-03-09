#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import pg from "pg";
import mysql from "mysql2/promise";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("write-cover-letter-renderer-json")
  .option("request-json-path", {
    type: "string",
    default: "",
    describe: "Path to a JSON file containing the request payload.",
  })
  .option("brief", {
    type: "string",
    default: "",
    describe: "Optional freeform brief. If omitted, one is assembled from the structured fields.",
  })
  .option("job-number", {
    type: "string",
    default: "",
    describe: "Newmark job/reference number.",
  })
  .option("site-address", {
    type: "string",
    default: "",
    describe: "Application site address.",
  })
  .option("development-description", {
    type: "string",
    default: "",
    describe: "Description of the proposed development.",
  })
  .option("key-instructions", {
    type: "string",
    default: "",
    describe: "Additional drafting instructions from the user.",
  })
  .option("ons-code", {
    type: "string",
    default: "",
    describe: "Optional explicit ONS code for example retrieval.",
  })
  .option("model", {
    type: "string",
    default: "gpt-5.2",
    describe: "Responses API model.",
  })
  .option("parser-model", {
    type: "string",
    default: "gpt-4.1-nano",
    describe: "Cheap model used to parse brief into retrieval keywords/filters.",
  })
  .option("embedding-model", {
    type: "string",
    default: "text-embedding-3-small",
    describe: "Embedding model for retrieving examples.",
  })
  .option("example-k", {
    type: "number",
    default: 4,
    describe: "Number of full cover-letter examples to include.",
  })
  .option("cityplan-path", {
    type: "string",
    default: "/opt/scraper/cityplan_merged.txt",
    describe: "Path to merged City Plan text.",
  })
  .option("additional-context-path", {
    type: "string",
    default: "",
    describe: "Optional path to an additional planning context text file.",
  })
  .option("additional-context-text", {
    type: "string",
    default: "",
    describe: "Optional inline additional planning context text.",
  })
  .option("cityplan-sections", {
    type: "number",
    default: 8,
    describe: "Max City Plan sections to include after relevance scoring.",
  })
  .option("cityplan-mode", {
    type: "string",
    default: "retrieved",
    choices: ["retrieved", "full"],
    describe: "City Plan context mode: retrieved sections or full-file passthrough.",
  })
  .option("max-example-chars", {
    type: "number",
    default: 18000,
    describe: "Max chars per full cover-letter example passed to model.",
  })
  .option("max-cityplan-chars", {
    type: "number",
    default: 50000,
    describe: "Global max chars for included City Plan context.",
  })
  .option("max-additional-context-chars", {
    type: "number",
    default: 30000,
    describe: "Max chars for included additional context.",
  })
  .option("json", {
    type: "boolean",
    default: true,
    describe: "Output JSON.",
  })
  .strict()
  .help()
  .argv;

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

function getMysqlConfig() {
  return {
    host: process.env.MYSQL_HOST,
    port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
    database: process.env.MYSQL_DATABASE,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
  };
}

function getOpenAiApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key || !String(key).trim()) throw new Error("OPENAI_API_KEY is not set");
  return String(key).trim();
}

function vectorLiteral(v) {
  return `[${v.map((x) => Number(x).toString()).join(",")}]`;
}

function getResponseText(resp) {
  if (typeof resp?.output_text === "string" && resp.output_text.trim()) return resp.output_text.trim();
  const out = Array.isArray(resp?.output) ? resp.output : [];
  const parts = [];
  for (const item of out) {
    if (item?.type !== "message") continue;
    const content = Array.isArray(item.content) ? item.content : [];
    for (const c of content) {
      if (c?.type === "output_text" && typeof c.text === "string") parts.push(c.text);
      if (c?.type === "text" && typeof c.text === "string") parts.push(c.text);
    }
  }
  return parts.join("\n").trim();
}

function keywordTerms(text, limit = 12) {
  const stop = new Set([
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "have",
    "your",
    "you",
    "our",
    "are",
    "was",
    "were",
    "will",
    "into",
    "about",
    "application",
    "cover",
    "letter",
    "planning",
  ]);
  const terms = String(text || "")
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .map((x) => x.trim())
    .filter((x) => x.length >= 3 && !stop.has(x));
  return Array.from(new Set(terms)).slice(0, limit);
}

function parseLooseJsonObject(text) {
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

function loadRequestPayload(requestJsonPath) {
  const p = String(requestJsonPath || "").trim();
  if (!p) return {};
  const raw = fs.readFileSync(p, "utf8");
  const decoded = JSON.parse(raw);
  if (!decoded || typeof decoded !== "object") {
    throw new Error("Request JSON must decode to an object");
  }
  return decoded;
}

function buildBriefFromRequest(req) {
  if (String(req.brief || "").trim()) return String(req.brief).trim();
  return [
    `Job number: ${String(req.job_number || "").trim() || "n/a"}`,
    `Site address: ${String(req.site_address || "").trim() || "n/a"}`,
    `Development description: ${String(req.development_description || "").trim() || "n/a"}`,
    `Key instructions: ${String(req.key_instructions || "").trim() || "n/a"}`,
  ].join("\n");
}

function extractUkPostcode(text) {
  const normalized = String(text || "").replace(/\s+/g, " ").trim().toUpperCase();
  const pattern = /\b(GIR\s?0AA|[A-PR-UWYZ]\d[A-Z\d]?\s?\d[ABD-HJLNP-UW-Z]{2}|[A-PR-UWYZ][A-HK-Y]\d[A-Z\d]?\s?\d[ABD-HJLNP-UW-Z]{2})\b/i;
  const match = normalized.match(pattern);
  if (!match) return null;
  const compact = match[0].replace(/\s+/g, "");
  return compact.length > 3 ? `${compact.slice(0, -3)} ${compact.slice(-3)}` : compact;
}

async function inferOnsCodeFromSiteAddress(siteAddress) {
  const postcode = extractUkPostcode(siteAddress);
  if (!postcode) {
    return { ons_code: null, postcode: null, lpa_name: null, error: "No valid UK postcode found" };
  }

  const api = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ONSPD_AUG_2025_UK/FeatureServer/0/query";
  const params = new URLSearchParams({
    where: `pcds='${postcode}' AND doterm IS NULL`,
    outFields: "pcds,lad25cd,lat,long",
    f: "json",
    returnIdsOnly: "false",
    returnCountOnly: "false",
  });
  const resp = await fetch(`${api}?${params.toString()}`);
  if (!resp.ok) {
    return { ons_code: null, postcode, lpa_name: null, error: `ONSPD HTTP ${resp.status}` };
  }
  const json = await resp.json();
  const attr = json?.features?.[0]?.attributes;
  if (!attr?.lad25cd) {
    return { ons_code: null, postcode, lpa_name: null, error: "Postcode not found in ONSPD" };
  }

  let lpaName = null;
  let mysqlError = null;
  try {
    const mysqlConn = await mysql.createConnection(getMysqlConfig());
    try {
      const [rows] = await mysqlConn.execute("SELECT lpa_name FROM lpa_codes WHERE ons_code = ? LIMIT 1", [attr.lad25cd]);
      lpaName = Array.isArray(rows) && rows[0] ? String(rows[0].lpa_name || "").trim() || null : null;
    } finally {
      await mysqlConn.end().catch(() => {});
    }
  } catch (err) {
    mysqlError = err instanceof Error ? err.message : String(err);
  }

  return {
    ons_code: String(attr.lad25cd || "").trim() || null,
    postcode: String(attr.pcds || postcode).trim() || postcode,
    lpa_name: lpaName,
    lat: attr.lat == null ? null : Number(attr.lat),
    long: attr.long == null ? null : Number(attr.long),
    error: mysqlError,
  };
}

async function openaiEmbeddings(input, model) {
  const apiKey = getOpenAiApiKey();
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
  const resp = await fetch(`${endpointBase}/embeddings`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({ model, input }),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Embeddings HTTP ${resp.status}: ${body.slice(0, 500)}`);
  }
  const payload = await resp.json();
  const emb = payload?.data?.[0]?.embedding;
  if (!Array.isArray(emb)) throw new Error("Embeddings response missing vector");
  return emb.map((x) => Number(x));
}

async function openaiResponses(payload) {
  const apiKey = getOpenAiApiKey();
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
  const resp = await fetch(`${endpointBase}/responses`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Responses HTTP ${resp.status}: ${body.slice(0, 1200)}`);
  }
  return await resp.json();
}

async function parseBriefForRetrieval(brief, parserModel, onsCodeHint) {
  const instructions = [
    "Extract retrieval hints for UK planning cover-letter search.",
    "Return strict JSON object only.",
    "Keep it concise and deterministic.",
  ].join(" ");

  const schemaPrompt = `
Return JSON with keys:
- semantic_query: string
- keyword_terms: string[] (max 12, lowercase, no duplicates)
- document_type: string (default "cover_letter")
- ons_code: string or null
- intent: string

If unsure, keep semantic_query equal to user brief and keyword_terms minimal.
`;

  const resp = await openaiResponses({
    model: String(parserModel),
    instructions,
    input: `USER_BRIEF:\n${brief}\n\nONS_CODE_HINT:\n${onsCodeHint || ""}\n\n${schemaPrompt}`,
    temperature: 0,
  });

  const txt = getResponseText(resp);
  const parsed = parseLooseJsonObject(txt) || {};

  const semanticQuery = String(parsed.semantic_query || brief).trim() || String(brief || "").trim();
  const keywordRaw = Array.isArray(parsed.keyword_terms) ? parsed.keyword_terms : [];
  const keywordTermsFinal = Array.from(
    new Set(
      keywordRaw
        .map((x) => String(x || "").toLowerCase().trim())
        .filter((x) => x && x.length >= 3)
        .slice(0, 12),
    ),
  );
  const documentType = String(parsed.document_type || "cover_letter").toLowerCase().trim() || "cover_letter";
  const onsCode = String(parsed.ons_code || onsCodeHint || "").trim() || null;
  const intent = String(parsed.intent || "").trim() || null;

  return {
    semantic_query: semanticQuery,
    keyword_terms: keywordTermsFinal.length ? keywordTermsFinal : keywordTerms(brief, 10),
    document_type: documentType,
    ons_code: onsCode,
    intent,
    raw: parsed,
  };
}

async function fetchExampleLetters(client, parsedBrief, k, embeddingModel) {
  const semanticQuery = String(parsedBrief?.semantic_query || "").trim();
  const onsCode = String(parsedBrief?.ons_code || "").trim();
  const keywords = Array.isArray(parsedBrief?.keyword_terms) ? parsedBrief.keyword_terms : [];
  const emb = await openaiEmbeddings(semanticQuery, embeddingModel);
  const vec = vectorLiteral(emb);
  const terms = keywords.length ? keywords : keywordTerms(semanticQuery, 10);

  const params = [vec, Math.max(1, Math.min(20, Number(k || 4)))];
  const onsClause = onsCode ? "AND d.lpa_code = $3" : "";
  if (onsCode) params.push(onsCode);

  let kwClause = "";
  if (terms.length > 0) {
    const start = params.length + 1;
    const sql = terms.map((_, i) => `d.full_text ILIKE '%' || $${start + i} || '%'`).join(" OR ");
    kwClause = `AND (${sql})`;
    params.push(...terms);
  }

  const { rows } = await client.query(
    `
      SELECT
        d.id,
        d.application_ref,
        d.title,
        d.full_text,
        d.meta->>'address' AS address,
        d.meta->>'application_type' AS application_type,
        d.meta->>'status' AS status,
        d.meta->>'decision' AS decision,
        d.meta->>'source_doc_url' AS source_doc_url,
        (d.doc_vec <=> $1::vector) AS distance
      FROM public.documents d
      WHERE d.document_type = 'cover_letter'
        AND d.doc_vec IS NOT NULL
        AND d.full_text IS NOT NULL
        ${onsClause}
        ${kwClause}
      ORDER BY d.doc_vec <=> $1::vector ASC, d.created_at DESC
      LIMIT $2
    `,
    params,
  );
  return rows;
}

function resolveCityPlanPath(preferredPath) {
  const preferred = String(preferredPath || "").trim();
  if (preferred && fs.existsSync(preferred)) return preferred;
  const fallback = "/opt/scraper/tmp/cityplan_merged.txt";
  if (fs.existsSync(fallback)) return fallback;
  throw new Error(`City Plan file not found at ${preferred || "(empty path)"} or ${fallback}`);
}

function splitCityPlanSections(cityPlanText) {
  const raw = String(cityPlanText || "");
  const parts = raw.split(/\n(?=##\s+)/g).map((x) => x.trim()).filter(Boolean);
  if (parts.length > 1) return parts;
  return raw.split(/\n(?===== PAGE \d+ =====)/g).map((x) => x.trim()).filter(Boolean);
}

function scoreSection(section, terms) {
  const s = section.toLowerCase();
  let score = 0;
  for (const t of terms) {
    if (!t) continue;
    const re = new RegExp(`\\b${t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "g");
    const matches = s.match(re);
    score += matches ? matches.length : 0;
  }
  return score;
}

function selectCityPlanContext(cityPlanText, brief, maxSections, maxChars) {
  const terms = keywordTerms(brief, 16);
  const sections = splitCityPlanSections(cityPlanText);
  const scored = sections
    .map((sec, i) => ({ idx: i, score: scoreSection(sec, terms), text: sec }))
    .sort((a, b) => b.score - a.score || a.idx - b.idx);

  const out = [];
  let chars = 0;
  for (const s of scored) {
    if (out.length >= Math.max(1, Number(maxSections || 8))) break;
    if (s.score <= 0 && out.length > 0) break;
    if (chars + s.text.length > Math.max(5000, Number(maxChars || 50000))) continue;
    out.push(s.text);
    chars += s.text.length;
  }
  if (out.length === 0 && sections.length > 0) {
    out.push(sections[0].slice(0, Math.max(2000, Math.min(12000, Number(maxChars || 50000)))));
  }
  return out;
}

function resolveAdditionalContext(req, maxChars) {
  const inline = String(req.additional_context_text || "").trim();
  if (inline) return inline.slice(0, Math.max(2000, Number(maxChars || 30000)));
  const p = String(req.additional_context_path || "").trim();
  if (!p) return "";
  if (!fs.existsSync(p)) throw new Error(`Additional context file not found at ${p}`);
  return fs.readFileSync(p, "utf8").slice(0, Math.max(2000, Number(maxChars || 30000)));
}

function buildRendererJsonPrompt(req, examples, cityPlanSections, additionalContextText, jobNumber) {
  const ex = examples.map((d, i) => {
    const full = String(d.full_text || "").trim().slice(0, Math.max(4000, Number(req.max_example_chars || 18000)));
    return [
      `EXAMPLE_${i + 1}_META`,
      `doc_id: ${d.id}`,
      `application_ref: ${d.application_ref || "n/a"}`,
      `title: ${d.title || "n/a"}`,
      `address: ${d.address || "n/a"}`,
      `application_type: ${d.application_type || "n/a"}`,
      `status: ${d.status || "n/a"}`,
      `decision: ${d.decision || "n/a"}`,
      `source_doc_url: ${d.source_doc_url || "n/a"}`,
      "",
      `EXAMPLE_${i + 1}_FULL_TEXT`,
      full,
    ].join("\n");
  });

  const city = cityPlanSections.map((s, i) => `CITY_PLAN_SECTION_${i + 1}\n${s}`).join("\n\n");
  const siteAddress = String(req.site_address || "").trim();
  const developmentDescription = String(req.development_description || "").trim();
  const keyInstructions = String(req.key_instructions || "").trim();

  return [
    "Return only strict JSON with this exact top-level shape:",
    "{",
    '  "meta": {',
    '    "address": string|null,',
    '    "your_ref": string|null,',
    '    "our_ref": string|null,',
    '    "heading": string|null,',
    '    "salutation": string|null,',
    '    "sign_off": string|null,',
    '    "email": string|null,',
    '    "phone_no": string|null',
    "  },",
    '  "content_blocks": [',
    "    {",
    '      "type": "heading"|"subheading"|"paragraph"|"bullet"|"quote",',
    '      "text": string',
    "    }",
    "  ]",
    "}",
    "",
    "Write a complete UK planning cover letter in Newmark style.",
    "Use the example letters for tone, structure and level of detail.",
    "Use the City Plan context for policy alignment.",
    "Do not invent policy references not present in the supplied context.",
    "Do not include the sign-off in content_blocks.",
    "If recipient details are unclear, use meta.address = null and a generic salutation like 'Dear Sir / Madam'.",
    `Set meta.our_ref to ${jobNumber || "null"} if possible.`,
    "",
    "PROJECT FACTS",
    `Job number: ${jobNumber || "n/a"}`,
    `Site address: ${siteAddress || "n/a"}`,
    `Development description: ${developmentDescription || "n/a"}`,
    `Key instructions: ${keyInstructions || "n/a"}`,
    "",
    "EXAMPLE COVER LETTERS",
    ex.join("\n\n---\n\n"),
    "",
    "CITY PLAN CONTEXT",
    city,
    "",
    "ADDITIONAL CONTEXT",
    additionalContextText || "(none)",
  ].join("\n");
}

function normalizeRendererResult(decoded, req) {
  const metaRaw = decoded && typeof decoded === "object" && decoded.meta && typeof decoded.meta === "object" ? decoded.meta : {};
  const blocksRaw = Array.isArray(decoded?.content_blocks) ? decoded.content_blocks : [];
  const contentBlocks = blocksRaw
    .filter((x) => x && typeof x === "object")
    .map((x) => ({
      type: ["heading", "subheading", "paragraph", "bullet", "quote"].includes(String(x.type || "").trim())
        ? String(x.type).trim()
        : "paragraph",
      text: String(x.text || "").trim(),
    }))
    .filter((x) => x.text);

  return {
    meta: {
      address: String(metaRaw.address || "").trim() || null,
      your_ref: String(metaRaw.your_ref || "").trim() || null,
      our_ref: String(metaRaw.our_ref || req.job_number || "").trim() || null,
      heading: String(metaRaw.heading || "").trim() || null,
      salutation: String(metaRaw.salutation || "").trim() || "Dear Sir / Madam",
      sign_off: String(metaRaw.sign_off || "").trim() || null,
      email: String(metaRaw.email || "").trim() || null,
      phone_no: String(metaRaw.phone_no || "").trim() || null,
    },
    content_blocks: contentBlocks,
  };
}

async function main() {
  const requestFile = loadRequestPayload(argv["request-json-path"]);
  const req = {
    brief: argv.brief || requestFile.brief || "",
    job_number: argv["job-number"] || requestFile.job_number || "",
    site_address: argv["site-address"] || requestFile.site_address || "",
    development_description: argv["development-description"] || requestFile.development_description || "",
    key_instructions: argv["key-instructions"] || requestFile.key_instructions || "",
    ons_code: argv["ons-code"] || requestFile.ons_code || "",
    additional_context_path: argv["additional-context-path"] || requestFile.additional_context_path || "",
    additional_context_text: argv["additional-context-text"] || requestFile.additional_context_text || "",
    model: argv.model,
    parser_model: argv["parser-model"],
    embedding_model: argv["embedding-model"],
    example_k: Number(argv["example-k"] || 4),
    cityplan_path: argv["cityplan-path"],
    cityplan_sections: Number(argv["cityplan-sections"] || 8),
    cityplan_mode: argv["cityplan-mode"],
    max_example_chars: Number(argv["max-example-chars"] || 18000),
    max_cityplan_chars: Number(argv["max-cityplan-chars"] || 50000),
    max_additional_context_chars: Number(argv["max-additional-context-chars"] || 30000),
  };

  if (!req.site_address || !req.development_description || !req.key_instructions || !req.job_number) {
    throw new Error("Missing required request fields: job_number, site_address, development_description, key_instructions");
  }

  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const inferredOns = !String(req.ons_code || "").trim()
      ? await inferOnsCodeFromSiteAddress(req.site_address)
      : { ons_code: String(req.ons_code).trim(), postcode: extractUkPostcode(req.site_address), lpa_name: null };

    const brief = buildBriefFromRequest(req);
    const parsedBrief = await parseBriefForRetrieval(brief, String(req.parser_model), inferredOns.ons_code || null);

    let examples = await fetchExampleLetters(client, parsedBrief, req.example_k, String(req.embedding_model));
    if (examples.length === 0 && parsedBrief.ons_code) {
      examples = await fetchExampleLetters(
        client,
        { ...parsedBrief, ons_code: null },
        req.example_k,
        String(req.embedding_model),
      );
    }
    if (examples.length === 0) {
      throw new Error("No cover-letter examples found for retrieval context");
    }

    const cityPlanPath = resolveCityPlanPath(String(req.cityplan_path));
    const cityPlanText = fs.readFileSync(cityPlanPath, "utf8");
    const cityPlanSections =
      String(req.cityplan_mode) === "full"
        ? [cityPlanText]
        : selectCityPlanContext(cityPlanText, brief, req.cityplan_sections, req.max_cityplan_chars);

    const additionalContextText = resolveAdditionalContext(req, req.max_additional_context_chars);
    const prompt = buildRendererJsonPrompt(req, examples, cityPlanSections, additionalContextText, req.job_number);

    const response = await openaiResponses({
      model: String(req.model),
      instructions: [
        "You are a senior planning consultant at Newmark.",
        "Write high-quality UK planning cover letters in structured JSON.",
        "Return only valid JSON in the requested schema.",
      ].join(" "),
      input: prompt,
      temperature: 0.2,
    });

    const decoded = parseLooseJsonObject(getResponseText(response));
    if (!decoded || typeof decoded !== "object") {
      throw new Error("Model did not return valid renderer JSON");
    }

    const result = normalizeRendererResult(decoded, req);
    if (!Array.isArray(result.content_blocks) || result.content_blocks.length === 0) {
      throw new Error("Renderer JSON contained no content_blocks");
    }

    const sources = examples.map((d) => ({
      doc_id: d.id,
      application_ref: d.application_ref,
      title: d.title,
      source_doc_url: d.source_doc_url,
      distance: d.distance == null ? null : Number(d.distance),
    }));

    process.stdout.write(
      `${JSON.stringify(
        {
          ok: true,
          model: String(req.model),
          parser_model: String(req.parser_model),
          retrieval_parse: parsedBrief,
          ons_resolution: inferredOns,
          cityplan_mode: String(req.cityplan_mode),
          cityplan_path: cityPlanPath,
          additional_context_chars: additionalContextText.length,
          examples_used: sources,
          cityplan_sections_used: cityPlanSections.length,
          result,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`fatal: ${msg}\n`);
  process.exit(1);
});
