#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("write-cover-letter-from-rag")
  .option("brief", {
    type: "string",
    demandOption: true,
    describe: "Instruction/brief for the target cover letter.",
  })
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS scope for example retrieval.",
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
    describe: "Max chars for included additional context file.",
  })
  .option("json", {
    type: "boolean",
    default: false,
    describe: "Output JSON (letter + sources) instead of plain text.",
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

function resolveOptionalContextPath(pathValue) {
  const p = String(pathValue || "").trim();
  if (!p) return null;
  if (fs.existsSync(p)) return p;
  throw new Error(`Additional context file not found at ${p}`);
}

function splitCityPlanSections(cityPlanText) {
  const raw = String(cityPlanText || "");
  // Split by markdown headings; keep heading with content.
  const parts = raw.split(/\n(?=##\s+)/g).map((x) => x.trim()).filter(Boolean);
  if (parts.length > 1) return parts;
  // fallback split by page marker
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
    const next = s.text;
    if (chars + next.length > Math.max(5000, Number(maxChars || 50000))) continue;
    out.push(next);
    chars += next.length;
  }
  // If zero scored, include first section as minimal policy context.
  if (out.length === 0 && sections.length > 0) {
    out.push(sections[0].slice(0, Math.max(2000, Math.min(12000, Number(maxChars || 50000)))));
  }
  return out;
}

function buildPrompt(brief, examples, cityPlanSections, maxExampleChars, additionalContextText) {
  const ex = examples.map((d, i) => {
    const full = String(d.full_text || "").trim();
    const clipped = full.slice(0, Math.max(4000, Number(maxExampleChars || 18000)));
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
      `distance: ${d.distance == null ? "n/a" : Number(d.distance).toFixed(6)}`,
      "",
      `EXAMPLE_${i + 1}_FULL_TEXT`,
      clipped,
    ].join("\n");
  });

  const city = cityPlanSections.map((s, i) => `CITY_PLAN_SECTION_${i + 1}\n${s}`).join("\n\n");
  const extra = String(additionalContextText || "").trim();

  return [
    `BRIEF`,
    brief,
    "",
    "TASK",
    "Draft a complete UK planning cover letter in Newmark style.",
    "Assume you are writing on behalf of Newmark as planning consultant.",
    "Mirror tone, structure and level of detail from the example cover letters.",
    "Have regard to relevant City Plan context included below.",
    "Do not invent policy references; only cite policy context present in provided City Plan sections.",
    "Use formal professional language and include practical planning justification.",
    "",
    "EXAMPLE COVER LETTERS (FULL TEXT)",
    ex.join("\n\n---\n\n"),
    "",
    "CITY PLAN CONTEXT",
    city,
    "",
    "ADDITIONAL CONTEXT",
    extra || "(none)",
  ].join("\n");
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const brief = String(argv.brief || "").trim();
    const onsCode = String(argv["ons-code"] || "").trim();
    const parsedBrief = await parseBriefForRetrieval(brief, String(argv["parser-model"]), onsCode);

    const examples = await fetchExampleLetters(
      client,
      parsedBrief,
      Number(argv["example-k"] || 4),
      String(argv["embedding-model"]),
    );
    if (examples.length === 0) {
      throw new Error("No cover-letter examples found for retrieval context");
    }

    const cityPlanPath = resolveCityPlanPath(String(argv["cityplan-path"]));
    const cityPlanText = fs.readFileSync(cityPlanPath, "utf8");
    const cityPlanSections =
      String(argv["cityplan-mode"]) === "full"
        ? [cityPlanText]
        : selectCityPlanContext(
            cityPlanText,
            brief,
            Number(argv["cityplan-sections"] || 8),
            Number(argv["max-cityplan-chars"] || 50000),
          );

    const additionalContextPath = resolveOptionalContextPath(String(argv["additional-context-path"]));
    const additionalContextText = additionalContextPath
      ? fs
          .readFileSync(additionalContextPath, "utf8")
          .slice(0, Math.max(2000, Number(argv["max-additional-context-chars"] || 30000)))
      : "";

    const input = buildPrompt(
      brief,
      examples,
      cityPlanSections,
      Number(argv["max-example-chars"] || 18000),
      additionalContextText,
    );

    const response = await openaiResponses({
      model: String(argv.model),
      instructions: [
        "You are a senior planning consultant at Newmark.",
        "Write high-quality UK planning cover letters in a clear, persuasive, professional style.",
        "Use the supplied example letters as style guides and City Plan context for policy alignment.",
      ].join(" "),
      input,
      temperature: 0.2,
    });

    const letter = getResponseText(response);
    const sources = examples.map((d) => ({
      doc_id: d.id,
      application_ref: d.application_ref,
      title: d.title,
      source_doc_url: d.source_doc_url,
      distance: d.distance == null ? null : Number(d.distance),
    }));

    if (argv.json) {
      process.stdout.write(
        `${JSON.stringify(
          {
            ok: true,
            model: String(argv.model),
            parser_model: String(argv["parser-model"]),
            retrieval_parse: parsedBrief,
            cityplan_mode: String(argv["cityplan-mode"]),
            cityplan_path: cityPlanPath,
            additional_context_path: additionalContextPath,
            additional_context_chars: additionalContextText.length,
            examples_used: sources,
            cityplan_sections_used: cityPlanSections.length,
            letter,
          },
          null,
          2,
        )}\n`,
      );
      return;
    }

    process.stdout.write(`${letter}\n\n`);
    process.stdout.write("----\nSources used:\n");
    for (const s of sources) {
      process.stdout.write(
        `- ref=${s.application_ref || "n/a"} doc_id=${s.doc_id} dist=${s.distance == null ? "n/a" : s.distance.toFixed(4)} url=${s.source_doc_url || "n/a"}\n`,
      );
    }
    process.stdout.write(
      `- cityplan_mode=${String(argv["cityplan-mode"])}\n`,
    );
    process.stdout.write(
      `- retrieval_parse=${JSON.stringify(
        {
          semantic_query: parsedBrief.semantic_query,
          keyword_terms: parsedBrief.keyword_terms,
          ons_code: parsedBrief.ons_code,
          intent: parsedBrief.intent,
        },
        null,
        0,
      )}\n`,
    );
    process.stdout.write(`- city_plan_path=${cityPlanPath} sections_used=${cityPlanSections.length}\n`);
    process.stdout.write(
      `- additional_context_path=${additionalContextPath || "n/a"} chars_used=${additionalContextText.length}\n`,
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
