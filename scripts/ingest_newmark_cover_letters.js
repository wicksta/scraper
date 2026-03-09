#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("ingest-newmark-cover-letters")
  .option("artifacts-dir", {
    type: "string",
    default: "/mnt/HC_Volume_103054926/newmark_jobcode_uid_artifacts",
    describe: "Directory containing per-reference cover_letter.txt files.",
  })
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code scope for mapping metadata.",
  })
  .option("limit", {
    type: "number",
    default: 0,
    describe: "Max files to process (0 = all).",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into discovered files.",
  })
  .option("embed", {
    type: "boolean",
    default: true,
    describe: "Generate embeddings for chunks and doc_vec.",
  })
  .option("openai-model", {
    type: "string",
    default: "text-embedding-3-small",
    describe: "Embedding model.",
  })
  .option("embed-batch-size", {
    type: "number",
    default: 32,
    describe: "Chunk embedding batch size.",
  })
  .option("max-chars-per-chunk", {
    type: "number",
    default: 8000,
    describe: "Max chars sent to embedding API per chunk.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Write to DB. Default dry-run.",
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

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function sanitizeReference(reference) {
  return String(reference || "").replace(/[^A-Za-z0-9._-]/g, "_");
}

function getOpenAiApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

function tokenizeCount(text) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  return words.length;
}

function shortSummary(text, n = 20) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  return words.slice(0, n).join(" ");
}

function splitParagraphs(text) {
  return String(text || "")
    .split(/\n\s*\n/g)
    .map((x) => x.replace(/\s+/g, " ").trim())
    .filter(Boolean);
}

function packParagraphs(paragraphs, minWords = 180, maxWords = 420) {
  const chunks = [];
  let buf = [];
  let wc = 0;

  const flush = () => {
    if (!buf.length) return;
    const text = buf.join("\n\n");
    chunks.push(text);
    buf = [];
    wc = 0;
  };

  for (const p of paragraphs) {
    const pw = tokenizeCount(p);
    if (wc + pw > maxWords && wc >= minWords) flush();
    buf.push(p);
    wc += pw;
  }
  flush();
  return chunks;
}

function vectorLiteral(v) {
  return `[${v.map((x) => Number(x).toString()).join(",")}]`;
}

function meanVector(vectors) {
  if (!vectors.length) return null;
  const dim = vectors[0].length;
  const out = new Array(dim).fill(0);
  for (const v of vectors) {
    if (!Array.isArray(v) || v.length !== dim) continue;
    for (let i = 0; i < dim; i++) out[i] += Number(v[i]) || 0;
  }
  for (let i = 0; i < dim; i++) out[i] /= vectors.length;
  return out;
}

async function embedTexts(inputs, { model, timeoutMs, maxCharsPerChunk }) {
  if (!inputs.length) return [];
  const apiKey = getOpenAiApiKey();
  if (!apiKey) throw new Error("OPENAI_API_KEY is not set");

  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(5000, Number(timeoutMs) || 60000));
  try {
    const resp = await fetch(`${endpointBase}/embeddings`, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        input: inputs.map((x) => String(x || "").slice(0, Math.max(1000, Number(maxCharsPerChunk) || 8000))),
      }),
    });
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new Error(`OpenAI embeddings HTTP ${resp.status}: ${body.slice(0, 500)}`);
    }
    const payload = await resp.json();
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    return rows.map((r) => (Array.isArray(r?.embedding) ? r.embedding.map((x) => Number(x)) : null));
  } finally {
    clearTimeout(timeout);
  }
}

async function loadReferenceMeta(client, onsCode) {
  const { rows } = await client.query(
    `
      SELECT
        c.ons_code,
        c.reference,
        c.documents_url,
        c.source_doc_url,
        c.source_doc_description,
        a.address,
        a.lat,
        a.lon,
        a.ward,
        a.parish,
        a.application_type,
        a.status,
        a.decision,
        a.source_url,
        a.keyval
      FROM public.newmark_jobcode_candidates c
      LEFT JOIN public.applications a
        ON a.ons_code = c.ons_code
       AND a.reference = c.reference
      WHERE ($1::text = '' OR c.ons_code = $1)
    `,
    [String(onsCode || "").trim()],
  );

  const map = new Map();
  for (const r of rows) {
    const key = sanitizeReference(r.reference);
    if (!map.has(key)) map.set(key, r);
  }
  return map;
}

function discoverCoverLetterFiles(artifactsDir) {
  const root = path.resolve(String(artifactsDir || ""));
  if (!fs.existsSync(root)) {
    throw new Error(`artifacts dir not found: ${root}`);
  }
  const entries = fs.readdirSync(root, { withFileTypes: true });
  const out = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const txtPath = path.join(root, e.name, "cover_letter.txt");
    if (fs.existsSync(txtPath)) out.push({ folder: e.name, txt_path: txtPath });
  }
  out.sort((a, b) => a.folder.localeCompare(b.folder));
  return out;
}

async function upsertDocument(client, docRow) {
  const { rows } = await client.query(
    `
      INSERT INTO public.documents (
        source_file, sha256, bytes, mime_type, pages, title,
        application_ref, document_type, local_authority, originator,
        document_date, meta, provenance, full_text, doc_vec, token_count,
        lpa_code, original_filename, site_point
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10,
        $11::date, $12::jsonb, $13::jsonb, $14, $15::vector, $16,
        $17, $18, CASE
          WHEN $19::double precision IS NULL OR $20::double precision IS NULL THEN NULL
          ELSE ST_SetSRID(ST_MakePoint($19::double precision, $20::double precision), 4326)
        END
      )
      ON CONFLICT (sha256) WHERE sha256 IS NOT NULL
      DO UPDATE SET
        source_file = EXCLUDED.source_file,
        bytes = EXCLUDED.bytes,
        mime_type = EXCLUDED.mime_type,
        title = COALESCE(EXCLUDED.title, public.documents.title),
        application_ref = COALESCE(EXCLUDED.application_ref, public.documents.application_ref),
        document_type = COALESCE(EXCLUDED.document_type, public.documents.document_type),
        local_authority = COALESCE(EXCLUDED.local_authority, public.documents.local_authority),
        originator = COALESCE(EXCLUDED.originator, public.documents.originator),
        document_date = COALESCE(EXCLUDED.document_date, public.documents.document_date),
        meta = EXCLUDED.meta,
        provenance = EXCLUDED.provenance,
        full_text = EXCLUDED.full_text,
        doc_vec = COALESCE(EXCLUDED.doc_vec, public.documents.doc_vec),
        token_count = EXCLUDED.token_count,
        lpa_code = COALESCE(EXCLUDED.lpa_code, public.documents.lpa_code),
        original_filename = COALESCE(EXCLUDED.original_filename, public.documents.original_filename),
        site_point = COALESCE(public.documents.site_point, EXCLUDED.site_point),
        updated_at = now()
      RETURNING id
    `,
    [
      docRow.source_file,
      docRow.sha256,
      docRow.bytes,
      docRow.mime_type,
      docRow.pages,
      docRow.title,
      docRow.application_ref,
      docRow.document_type,
      docRow.local_authority,
      docRow.originator,
      docRow.document_date,
      JSON.stringify(docRow.meta || {}),
      JSON.stringify(docRow.provenance || []),
      docRow.full_text,
      docRow.doc_vec ? vectorLiteral(docRow.doc_vec) : null,
      docRow.token_count,
      docRow.lpa_code,
      docRow.original_filename,
      docRow.lon,
      docRow.lat,
    ],
  );
  return rows[0]?.id || null;
}

async function upsertChunk(client, chunkRow) {
  await client.query(
    `
      INSERT INTO public.chunks
        (doc_id, kind, page, position_json, summary, text, embedding, natural_key)
      VALUES
        ($1::uuid, $2, $3, $4::jsonb, $5, $6, $7::vector, $8)
      ON CONFLICT (doc_id, natural_key)
      DO UPDATE SET
        kind = EXCLUDED.kind,
        page = EXCLUDED.page,
        position_json = EXCLUDED.position_json,
        summary = EXCLUDED.summary,
        text = EXCLUDED.text,
        embedding = COALESCE(EXCLUDED.embedding, public.chunks.embedding)
    `,
    [
      chunkRow.doc_id,
      chunkRow.kind,
      chunkRow.page,
      JSON.stringify(chunkRow.position_json || {}),
      chunkRow.summary,
      chunkRow.text,
      chunkRow.embedding ? vectorLiteral(chunkRow.embedding) : null,
      chunkRow.natural_key,
    ],
  );
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const artifactsDir = path.resolve(String(argv["artifacts-dir"] || ""));
    const onsCode = String(argv["ons-code"] || "").trim();
    const all = discoverCoverLetterFiles(artifactsDir);
    const offset = Math.max(0, Number(argv.offset || 0));
    const limit = Math.max(0, Number(argv.limit || 0));
    const selected = all.slice(offset, limit > 0 ? offset + limit : undefined);
    const metaMap = await loadReferenceMeta(client, onsCode);

    logEvent("start", {
      mode: argv.apply ? "apply" : "dry-run",
      artifacts_dir: artifactsDir,
      discovered: all.length,
      selected: selected.length,
      ons_code: onsCode || "ALL",
      embed: Boolean(argv.embed),
      model: String(argv["openai-model"]),
    });

    let scanned = 0;
    let skipped = 0;
    let failed = 0;
    let ingested = 0;
    let chunksInserted = 0;

    for (const item of selected) {
      scanned++;
      const txtPath = item.txt_path;
      const raw = fs.readFileSync(txtPath, "utf8");
      const text = String(raw || "").trim();
      if (!text) {
        skipped++;
        logEvent("skip_empty", { folder: item.folder, txt_path: txtPath });
        continue;
      }

      const stat = fs.statSync(txtPath);
      const contentSha256 = crypto.createHash("sha256").update(Buffer.from(raw, "utf8")).digest("hex");
      const referenceMeta = metaMap.get(item.folder) || null;
      const reference = referenceMeta?.reference || null;
      const shaSeed = reference ? `${contentSha256}|${reference}` : contentSha256;
      const sha256 = crypto.createHash("sha256").update(shaSeed).digest("hex");
      const localAuthority = referenceMeta?.local_authority || "Westminster City Council";
      const lpaCode = referenceMeta?.ons_code || onsCode || null;
      const appLat = referenceMeta?.lat == null ? null : Number(referenceMeta.lat);
      const appLon = referenceMeta?.lon == null ? null : Number(referenceMeta.lon);
      const hasCoords = Number.isFinite(appLat) && Number.isFinite(appLon);
      const title = reference ? `Cover Letter - ${reference}` : `Cover Letter - ${item.folder}`;

      const paragraphs = splitParagraphs(text);
      const packedChunks = packParagraphs(paragraphs, 180, 420);
      const chunks = packedChunks.length ? packedChunks : [text];

      let chunkEmbeddings = new Array(chunks.length).fill(null);
      if (argv.embed) {
        const batchSize = Math.max(1, Number(argv["embed-batch-size"] || 32));
        const out = [];
        for (let i = 0; i < chunks.length; i += batchSize) {
          const batch = chunks.slice(i, i + batchSize);
          try {
            // eslint-disable-next-line no-await-in-loop
            const emb = await embedTexts(batch, {
              model: String(argv["openai-model"]),
              timeoutMs: 60000,
              maxCharsPerChunk: Number(argv["max-chars-per-chunk"] || 8000),
            });
            out.push(...emb);
          } catch (err) {
            failed++;
            logEvent("embed_failed", {
              folder: item.folder,
              reference,
              error: err instanceof Error ? err.message : String(err),
            });
            out.push(...new Array(batch.length).fill(null));
          }
        }
        chunkEmbeddings = out;
      }

      const validChunkEmbeddings = chunkEmbeddings.filter((v) => Array.isArray(v));
      const docVec = validChunkEmbeddings.length ? meanVector(validChunkEmbeddings) : null;

      const geocode = hasCoords
        ? {
            lat: appLat,
            long: appLon,
            lad25cd: lpaCode || null,
            postcode: null,
            lpa_name: localAuthority,
            _source: "public.applications",
          }
        : null;

      const docMeta = {
        pipeline: "newmark_jobcode_cover_letter_ingest",
        artifacts_folder: item.folder,
        source_txt_path: txtPath,
        derived_from_pdf: true,
        content_sha256: contentSha256,
        ingest_sha_seed: shaSeed,
        source_doc_url: referenceMeta?.source_doc_url || null,
        source_doc_description: referenceMeta?.source_doc_description || null,
        documents_url: referenceMeta?.documents_url || null,
        application_source_url: referenceMeta?.source_url || null,
        address: referenceMeta?.address || null,
        ward: referenceMeta?.ward || null,
        parish: referenceMeta?.parish || null,
        application_type: referenceMeta?.application_type || null,
        status: referenceMeta?.status || null,
        decision: referenceMeta?.decision || null,
        keyval: referenceMeta?.keyval || null,
        geocode,
        paragraph_count: paragraphs.length,
        chunk_count: chunks.length,
      };

      const provenance = [
        {
          via: "detect_newmark_jobcode_for_uid",
          artifacts_dir: artifactsDir,
          folder: item.folder,
          source_doc_url: referenceMeta?.source_doc_url || null,
          documents_url: referenceMeta?.documents_url || null,
        },
      ];

      const docRow = {
        source_file: txtPath,
        sha256,
        bytes: stat.size,
        mime_type: "text/plain",
        pages: null,
        title,
        application_ref: reference,
        document_type: "cover_letter",
        local_authority: localAuthority,
        originator: "Newmark",
        document_date: null,
        meta: docMeta,
        provenance,
        full_text: text,
        doc_vec: docVec,
        token_count: tokenizeCount(text),
        lpa_code: lpaCode,
        original_filename: "cover_letter.txt",
        lat: hasCoords ? appLat : null,
        lon: hasCoords ? appLon : null,
      };

      if (!argv.apply) {
        ingested++;
        chunksInserted += chunks.length;
        logEvent("dry_run_row", {
          folder: item.folder,
          reference,
          sha256,
          chunks: chunks.length,
          has_doc_vec: Boolean(docVec),
        });
        continue;
      }

      try {
        // eslint-disable-next-line no-await-in-loop
        const docId = await upsertDocument(client, docRow);
        if (!docId) {
          failed++;
          logEvent("document_upsert_failed", { folder: item.folder, reference, sha256 });
          continue;
        }

        for (let i = 0; i < chunks.length; i++) {
          const chunkText = chunks[i];
          const emb = Array.isArray(chunkEmbeddings[i]) ? chunkEmbeddings[i] : null;
          // eslint-disable-next-line no-await-in-loop
          await upsertChunk(client, {
            doc_id: docId,
            kind: "paragraph",
            page: null,
            position_json: { index: i + 1, source: "cover_letter_txt" },
            summary: shortSummary(chunkText),
            text: chunkText,
            embedding: emb,
            natural_key: `cover_letter:p:${i + 1}`,
          });
        }

        ingested++;
        chunksInserted += chunks.length;
        logEvent("ingested", {
          folder: item.folder,
          reference,
          doc_id: docId,
          chunks: chunks.length,
          has_doc_vec: Boolean(docVec),
        });
      } catch (err) {
        failed++;
        logEvent("ingest_failed", {
          folder: item.folder,
          reference,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    console.log(
      JSON.stringify({
        ok: true,
        mode: argv.apply ? "apply" : "dry-run",
        scanned,
        ingested,
        chunks_inserted: chunksInserted,
        skipped,
        failed,
      }),
    );
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  logEvent("fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
