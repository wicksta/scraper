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
  .scriptName("ingest-newmark-planning-statements")
  .option("artifacts-dir", {
    type: "string",
    default: "/mnt/HC_Volume_103054926/newmark_jobcode_uid_artifacts",
    describe: "Directory containing per-reference planning_statement.txt files.",
  })
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code scope for application lookup.",
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
    describe: "Generate embeddings for page chunks and doc_vec.",
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

function restoreReference(folderName) {
  const raw = String(folderName || "").trim();
  return raw.replace(/_/g, "/");
}

function getOpenAiApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

function tokenizeCount(text) {
  return String(text || "").trim().split(/\s+/).filter(Boolean).length;
}

function shortSummary(text, n = 20) {
  return String(text || "").trim().split(/\s+/).filter(Boolean).slice(0, n).join(" ");
}

function vectorLiteral(v) {
  return `[${v.map((x) => Number(x).toString()).join(",")}]`;
}

function meanVector(vectors) {
  if (!vectors.length) return null;
  const dim = vectors[0].length;
  const out = new Array(dim).fill(0);
  let count = 0;
  for (const v of vectors) {
    if (!Array.isArray(v) || v.length !== dim) continue;
    for (let i = 0; i < dim; i++) out[i] += Number(v[i]) || 0;
    count += 1;
  }
  if (!count) return null;
  for (let i = 0; i < dim; i++) out[i] /= count;
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

function splitPages(text) {
  const normalized = String(text || "").replace(/\r\n/g, "\n");
  const pages = normalized
    .split(/\f+/)
    .map((page) => page.trim())
    .filter(Boolean);
  return pages.length ? pages : [normalized.trim()].filter(Boolean);
}

async function loadApplicationMeta(client, onsCode) {
  const { rows } = await client.query(
    `
      SELECT
        ons_code,
        reference,
        keyval,
        address,
        ward,
        parish,
        application_type,
        status,
        decision,
        source_url,
        lat,
        lon
      FROM public.applications
      WHERE ($1::text = '' OR ons_code = $1)
    `,
    [String(onsCode || "").trim()],
  );

  const map = new Map();
  for (const row of rows) {
    map.set(String(row.reference || "").trim(), row);
    map.set(sanitizeReference(row.reference), row);
  }
  return map;
}

function discoverPlanningStatementFiles(artifactsDir) {
  const root = path.resolve(String(artifactsDir || ""));
  if (!fs.existsSync(root)) {
    throw new Error(`artifacts dir not found: ${root}`);
  }
  const entries = fs.readdirSync(root, { withFileTypes: true });
  const out = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const folderPath = path.join(root, entry.name);
    const folderEntries = fs.readdirSync(folderPath, { withFileTypes: true });
    const txtFile = folderEntries.find((child) => child.isFile() && /_planning_statement\.txt$/i.test(child.name));
    const pdfFile = folderEntries.find((child) => child.isFile() && /_planning_statement\.pdf$/i.test(child.name));
    if (txtFile) {
      out.push({
        folder: entry.name,
        txt_path: path.join(folderPath, txtFile.name),
        pdf_path: pdfFile ? path.join(folderPath, pdfFile.name) : null,
      });
    }
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
        pages = COALESCE(EXCLUDED.pages, public.documents.pages),
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
    const all = discoverPlanningStatementFiles(artifactsDir);
    const offset = Math.max(0, Number(argv.offset || 0));
    const limit = Math.max(0, Number(argv.limit || 0));
    const selected = all.slice(offset, limit > 0 ? offset + limit : undefined);
    const metaMap = await loadApplicationMeta(client, onsCode);

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
      const raw = fs.readFileSync(item.txt_path, "utf8");
      const text = String(raw || "").trim();
      if (!text) {
        skipped++;
        logEvent("skip_empty", { folder: item.folder, txt_path: item.txt_path });
        continue;
      }

      const reference = restoreReference(item.folder);
      const appMeta = metaMap.get(reference) || metaMap.get(item.folder) || null;
      if (!appMeta) {
        skipped++;
        logEvent("skip_missing_application", { folder: item.folder, reference });
        continue;
      }

      const stat = fs.statSync(item.txt_path);
      const contentSha256 = crypto.createHash("sha256").update(Buffer.from(raw, "utf8")).digest("hex");
      const shaSeed = `${contentSha256}|${reference}|planning_statement`;
      const sha256 = crypto.createHash("sha256").update(shaSeed).digest("hex");

      const pages = splitPages(text);
      let chunkEmbeddings = new Array(pages.length).fill(null);
      if (argv.embed) {
        const batchSize = Math.max(1, Number(argv["embed-batch-size"] || 32));
        const out = [];
        for (let i = 0; i < pages.length; i += batchSize) {
          const batch = pages.slice(i, i + batchSize);
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
      const lat = appMeta.lat == null ? null : Number(appMeta.lat);
      const lon = appMeta.lon == null ? null : Number(appMeta.lon);
      const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);
      const localAuthority = "Westminster City Council";
      const documentsUrl = appMeta.keyval
        ? `https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=${encodeURIComponent(String(appMeta.keyval))}`
        : null;
      const applicationSourceUrl = appMeta.keyval
        ? `https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=summary&keyVal=${encodeURIComponent(String(appMeta.keyval))}`
        : (appMeta.source_url || null);

      const geocode = hasCoords
        ? {
            lat,
            long: lon,
            _source: "public.applications",
            lad25cd: appMeta.ons_code || onsCode || null,
            lpa_name: localAuthority,
            postcode: null,
          }
        : null;

      const meta = {
        ward: appMeta.ward || null,
        keyval: appMeta.keyval || null,
        parish: appMeta.parish || null,
        status: appMeta.status || null,
        address: appMeta.address || null,
        geocode,
        decision: appMeta.decision || null,
        pipeline: "newmark_planning_statement_ingest",
        chunk_count: pages.length,
        documents_url: documentsUrl,
        content_sha256: contentSha256,
        source_doc_url: null,
        ingest_sha_seed: shaSeed,
        paragraph_count: null,
        source_txt_path: item.txt_path,
        application_type: appMeta.application_type || null,
        artifacts_folder: item.folder,
        derived_from_pdf: fs.existsSync(item.pdf_path),
        application_source_url: applicationSourceUrl,
        source_doc_description: "PLANNING STATEMENT",
      };

      const provenance = [
        {
          via: "detect_newmark_jobcode_for_uid",
          artifacts_dir: artifactsDir,
          folder: item.folder,
          source_txt_path: item.txt_path,
          source_pdf_path: fs.existsSync(item.pdf_path) ? item.pdf_path : null,
        },
      ];

      const docRow = {
        source_file: item.txt_path,
        sha256,
        bytes: stat.size,
        mime_type: "text/plain",
        pages: pages.length || null,
        title: `Planning Statement - ${reference}`,
        application_ref: reference,
        document_type: "planning_statement",
        local_authority: localAuthority,
        originator: "Newmark",
        document_date: null,
        meta,
        provenance,
        full_text: text,
        doc_vec: docVec,
        token_count: tokenizeCount(text),
        lpa_code: appMeta.ons_code || onsCode || null,
        original_filename: "planning_statement.txt",
        lat: hasCoords ? lat : null,
        lon: hasCoords ? lon : null,
      };

      if (!argv.apply) {
        ingested++;
        chunksInserted += pages.length;
        logEvent("dry_run_row", {
          folder: item.folder,
          reference,
          sha256,
          pages: pages.length,
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

        for (let i = 0; i < pages.length; i += 1) {
          const pageText = pages[i];
          const embedding = Array.isArray(chunkEmbeddings[i]) ? chunkEmbeddings[i] : null;
          // eslint-disable-next-line no-await-in-loop
          await upsertChunk(client, {
            doc_id: docId,
            kind: "page",
            page: i + 1,
            position_json: { page: i + 1, source: "planning_statement_txt" },
            summary: shortSummary(pageText),
            text: pageText,
            embedding,
            natural_key: `planning_statement:page:${i + 1}`,
          });
        }

        ingested++;
        chunksInserted += pages.length;
        logEvent("ingested", {
          folder: item.folder,
          reference,
          doc_id: docId,
          pages: pages.length,
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

    process.stdout.write(
      `${JSON.stringify({
        ok: true,
        mode: argv.apply ? "apply" : "dry-run",
        scanned,
        ingested,
        chunks_inserted: chunksInserted,
        skipped,
        failed,
      })}\n`,
    );
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  logEvent("fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
