#!/usr/bin/env node
import "../bootstrap.js";

import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("rag-cover-letter-search-responses")
  .option("query", {
    type: "string",
    demandOption: true,
    describe: "User query (e.g. 'cover letter for alterations to a shopfront').",
  })
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "Optional ONS scope. Empty string = all.",
  })
  .option("model", {
    type: "string",
    default: "gpt-4.1-mini",
    describe: "Responses API model.",
  })
  .option("embedding-model", {
    type: "string",
    default: "text-embedding-3-small",
    describe: "Embedding model for retrieval.",
  })
  .option("doc-k", {
    type: "number",
    default: 20,
    describe: "Max docs for doc-level retrieval.",
  })
  .option("chunk-k", {
    type: "number",
    default: 30,
    describe: "Max chunks for chunk-level retrieval.",
  })
  .option("find-only", {
    type: "boolean",
    default: true,
    describe: "Deterministic mode: only find/show matching cover letters and chunks (no final LLM synthesis).",
  })
  .option("show-chunks", {
    type: "boolean",
    default: true,
    describe: "When --find-only is enabled, also show top chunks for matched docs.",
  })
  .option("chunk-preview-chars", {
    type: "number",
    default: 280,
    describe: "Preview length for chunk text in --find-only mode.",
  })
  .option("json", {
    type: "boolean",
    default: false,
    describe: "Print raw final JSON instead of text answer.",
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
  if (!key || !String(key).trim()) {
    throw new Error("OPENAI_API_KEY is not set");
  }
  return String(key).trim();
}

function vectorLiteral(v) {
  return `[${v.map((x) => Number(x).toString()).join(",")}]`;
}

function parseLooseJsonObject(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
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

async function openaiEmbeddings(input, model) {
  const apiKey = getOpenAiApiKey();
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";

  const resp = await fetch(`${endpointBase}/embeddings`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
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
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Responses HTTP ${resp.status}: ${body.slice(0, 1200)}`);
  }
  return await resp.json();
}

async function toolSearchCoverLetterDocs(client, args) {
  const semanticQuery = String(args?.semantic_query || "").trim();
  const onsCode = String(args?.ons_code ?? argv["ons-code"] ?? "").trim();
  const topK = Math.max(1, Math.min(100, Number(args?.top_k ?? argv["doc-k"] ?? 20)));
  const keywordTerms = Array.isArray(args?.keyword_terms)
    ? args.keyword_terms.map((x) => String(x || "").trim()).filter(Boolean).slice(0, 12)
    : [];
  if (!semanticQuery) return { ok: false, error: "semantic_query is required" };

  const embedding = await openaiEmbeddings(semanticQuery, String(argv["embedding-model"]));
  const vec = vectorLiteral(embedding);

  const params = [vec, topK];
  const onsClause = onsCode ? "AND d.lpa_code = $3" : "";
  if (onsCode) params.push(onsCode);

  let keywordClause = "";
  if (keywordTerms.length > 0) {
    const start = params.length + 1;
    const termsSql = keywordTerms
      .map((_, i) => `d.full_text ILIKE '%' || $${start + i} || '%'`)
      .join(" OR ");
    keywordClause = `AND (${termsSql})`;
    params.push(...keywordTerms);
  }

  const { rows } = await client.query(
    `
      SELECT
        d.id,
        d.application_ref,
        d.title,
        d.document_type,
        d.document_date,
        d.lpa_code,
        d.meta->>'address' AS address,
        d.meta->>'application_type' AS application_type,
        d.meta->>'status' AS status,
        d.meta->>'decision' AS decision,
        d.meta->>'source_doc_url' AS source_doc_url,
        (d.doc_vec <=> $1::vector) AS distance
      FROM public.documents d
      WHERE d.document_type = 'cover_letter'
        AND d.doc_vec IS NOT NULL
        ${onsClause}
        ${keywordClause}
      ORDER BY d.doc_vec <=> $1::vector ASC, d.created_at DESC
      LIMIT $2
    `,
    params,
  );

  return {
    ok: true,
    semantic_query: semanticQuery,
    ons_code: onsCode || null,
    keyword_terms: keywordTerms,
    top_k: topK,
    results: rows.map((r) => ({
      doc_id: r.id,
      application_ref: r.application_ref,
      title: r.title,
      document_type: r.document_type,
      document_date: r.document_date,
      lpa_code: r.lpa_code,
      address: r.address,
      application_type: r.application_type,
      status: r.status,
      decision: r.decision,
      source_doc_url: r.source_doc_url,
      distance: r.distance == null ? null : Number(r.distance),
    })),
  };
}

async function toolSearchCoverLetterChunks(client, args) {
  const semanticQuery = String(args?.semantic_query || "").trim();
  const topK = Math.max(1, Math.min(120, Number(args?.top_k ?? argv["chunk-k"] ?? 30)));
  const docIds = Array.isArray(args?.doc_ids) ? args.doc_ids.map((x) => String(x || "").trim()).filter(Boolean) : [];
  if (!semanticQuery) return { ok: false, error: "semantic_query is required" };
  if (docIds.length === 0) return { ok: false, error: "doc_ids is required and must be non-empty" };

  const embedding = await openaiEmbeddings(semanticQuery, String(argv["embedding-model"]));
  const vec = vectorLiteral(embedding);

  const { rows } = await client.query(
    `
      SELECT
        c.doc_id,
        d.application_ref,
        d.title,
        c.natural_key,
        c.summary,
        c.text,
        c.position_json,
        (c.embedding <=> $1::vector) AS distance
      FROM public.chunks c
      JOIN public.documents d ON d.id = c.doc_id
      WHERE c.embedding IS NOT NULL
        AND c.doc_id = ANY($2::uuid[])
      ORDER BY c.embedding <=> $1::vector ASC
      LIMIT $3
    `,
    [vec, docIds, topK],
  );

  return {
    ok: true,
    semantic_query: semanticQuery,
    top_k: topK,
    doc_ids: docIds,
    results: rows.map((r) => ({
      doc_id: r.doc_id,
      application_ref: r.application_ref,
      title: r.title,
      natural_key: r.natural_key,
      summary: r.summary,
      text: r.text,
      position_json: r.position_json,
      distance: r.distance == null ? null : Number(r.distance),
    })),
  };
}

function getFunctionCalls(resp) {
  const out = Array.isArray(resp?.output) ? resp.output : [];
  return out.filter((x) => x?.type === "function_call");
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    if (argv["find-only"]) {
      const query = String(argv.query || "").trim();
      const keywordTerms = query
        .toLowerCase()
        .split(/[^a-z0-9]+/g)
        .map((x) => x.trim())
        .filter((x) => x.length >= 3)
        .slice(0, 10);

      const docs = await toolSearchCoverLetterDocs(client, {
        semantic_query: query,
        ons_code: String(argv["ons-code"] || "").trim(),
        keyword_terms: keywordTerms,
        top_k: Number(argv["doc-k"] || 20),
      });

      let chunks = null;
      if (argv["show-chunks"] && docs?.ok && Array.isArray(docs.results) && docs.results.length > 0) {
        const docIds = docs.results.map((d) => d.doc_id).filter(Boolean);
        chunks = await toolSearchCoverLetterChunks(client, {
          semantic_query: query,
          doc_ids: docIds,
          top_k: Number(argv["chunk-k"] || 30),
        });
      }

      if (argv.json) {
        process.stdout.write(`${JSON.stringify({ docs, chunks }, null, 2)}\n`);
        return;
      }

      if (!docs?.ok) {
        process.stdout.write(`Search failed: ${docs?.error || "unknown error"}\n`);
        return;
      }

      process.stdout.write(`Query: ${query}\n`);
      process.stdout.write(`Matched documents: ${docs.results.length}\n\n`);
      docs.results.forEach((d, i) => {
        process.stdout.write(
          `${i + 1}. ref=${d.application_ref || "n/a"} doc_id=${d.doc_id} dist=${d.distance?.toFixed?.(4) ?? d.distance}\n`,
        );
        process.stdout.write(`   title=${d.title || "n/a"}\n`);
        process.stdout.write(`   address=${d.address || "n/a"}\n`);
        process.stdout.write(`   app_type=${d.application_type || "n/a"} status=${d.status || "n/a"} decision=${d.decision || "n/a"}\n`);
        process.stdout.write(`   source_doc_url=${d.source_doc_url || "n/a"}\n\n`);
      });

      if (chunks?.ok && Array.isArray(chunks.results) && chunks.results.length > 0) {
        process.stdout.write(`Top chunks: ${chunks.results.length}\n\n`);
        const maxChars = Math.max(80, Number(argv["chunk-preview-chars"] || 280));
        chunks.results.forEach((c, i) => {
          const preview = String(c.text || "").replace(/\s+/g, " ").trim().slice(0, maxChars);
          process.stdout.write(
            `${i + 1}. ref=${c.application_ref || "n/a"} doc_id=${c.doc_id} key=${c.natural_key || "n/a"} dist=${c.distance?.toFixed?.(4) ?? c.distance}\n`,
          );
          process.stdout.write(`   ${preview}${String(c.text || "").length > maxChars ? "..." : ""}\n\n`);
        });
      }
      return;
    }

    const tools = [
      {
        type: "function",
        name: "search_cover_letter_docs",
        description:
          "Vector + keyword retrieval over public.documents for cover letters. Use first to get candidate doc_ids.",
        parameters: {
          type: "object",
          properties: {
            semantic_query: { type: "string" },
            ons_code: { type: "string" },
            keyword_terms: { type: "array", items: { type: "string" } },
            top_k: { type: "number" },
          },
          required: ["semantic_query"],
          additionalProperties: false,
        },
      },
      {
        type: "function",
        name: "search_cover_letter_chunks",
        description:
          "Chunk-level vector retrieval over public.chunks for a set of doc_ids from search_cover_letter_docs.",
        parameters: {
          type: "object",
          properties: {
            semantic_query: { type: "string" },
            doc_ids: { type: "array", items: { type: "string" } },
            top_k: { type: "number" },
          },
          required: ["semantic_query", "doc_ids"],
          additionalProperties: false,
        },
      },
    ];

    const instructions = [
      "You are a retrieval planner for planning cover letters.",
      "Always call search_cover_letter_docs first.",
      "Then call search_cover_letter_chunks using doc_ids from docs results.",
      "Prefer Westminster scope unless user specifies otherwise.",
      "Return a concise grounded answer with references to application_ref and doc_id.",
      "Do NOT invent examples, documents, or snippets.",
      "If retrieval is weak/empty, say that explicitly and ask the user to broaden query terms.",
      "Do not draft template letters unless explicitly asked.",
    ].join(" ");

    let response = await openaiResponses({
      model: String(argv.model),
      input: String(argv.query),
      instructions,
      tools,
      tool_choice: "auto",
      temperature: 0,
    });

    let guard = 0;
    while (guard < 8) {
      guard++;
      const calls = getFunctionCalls(response);
      if (calls.length === 0) break;

      const outputs = [];
      for (const call of calls) {
        const name = String(call?.name || "");
        const parsedArgs = parseLooseJsonObject(call?.arguments) || {};
        let out;
        if (name === "search_cover_letter_docs") {
          // eslint-disable-next-line no-await-in-loop
          out = await toolSearchCoverLetterDocs(client, parsedArgs);
        } else if (name === "search_cover_letter_chunks") {
          // eslint-disable-next-line no-await-in-loop
          out = await toolSearchCoverLetterChunks(client, parsedArgs);
        } else {
          out = { ok: false, error: `Unknown tool: ${name}` };
        }
        outputs.push({
          type: "function_call_output",
          call_id: call.call_id,
          output: JSON.stringify(out),
        });
      }

      response = await openaiResponses({
        model: String(argv.model),
        previous_response_id: response.id,
        input: outputs,
        temperature: 0,
      });
    }

    if (argv.json) {
      process.stdout.write(`${JSON.stringify(response, null, 2)}\n`);
      return;
    }

    const text = getResponseText(response);
    if (!text) {
      process.stdout.write("No final text response produced.\n");
      process.stdout.write(`${JSON.stringify(response, null, 2)}\n`);
      return;
    }
    process.stdout.write(`${text}\n`);
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`fatal: ${msg}\n`);
  process.exit(1);
});
