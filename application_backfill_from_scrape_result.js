// application_backfill_from_scrape_result.js
//
// Retrospective helper:
// - Loads applications rows.
// - Finds a corresponding scrape_jobs row by reference/application_ref.
// - For each NULL application field, searches scrape_jobs.result recursively
//   for a matching key name and backfills when a coercible value is found.
//
// IMPORTANT: import ./bootstrap.js first to load .env.
import "./bootstrap.js";

import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("application-backfill-from-scrape-result")
  .option("limit", { type: "number", default: 200, describe: "Batch size for scanning applications." })
  .option("max-rows", { type: "number", default: 0, describe: "Stop after scanning this many rows (0 = no cap)." })
  .option("ons-code", { type: "string", default: "", describe: "Only process applications for this ONS code." })
  .option("heartbeat-seconds", {
    type: "number",
    default: 15,
    describe: "Emit progress heartbeat every N seconds (0 = disabled).",
  })
  .option("apply", { type: "boolean", default: false, describe: "Write updates to DB. Default is dry-run." })
  .strict()
  .help()
  .argv;

const SKIP_COLUMNS = new Set([
  "id",
  "ons_code",
  "reference",
  "date_added",
  "first_seen_at",
  "scraped_at",
  "last_look",
  "updated_at",
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

function normalizeLookupKey(key) {
  return String(key || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function quoteIdent(ident) {
  return `"${String(ident).replace(/"/g, "\"\"")}"`;
}

function isMeaningful(value) {
  if (value == null) return false;
  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return false;
    if (/^not available$/i.test(s)) return false;
    return true;
  }
  return true;
}

function buildJsonKeyIndex(root) {
  const out = new Map();
  const queue = [root];

  while (queue.length > 0) {
    const node = queue.shift();
    if (node == null) continue;

    if (Array.isArray(node)) {
      for (const item of node) queue.push(item);
      continue;
    }

    if (typeof node !== "object") continue;

    for (const [rawKey, rawVal] of Object.entries(node)) {
      const normalized = normalizeLookupKey(rawKey);
      if (!out.has(normalized) && isMeaningful(rawVal) && typeof rawVal !== "object") {
        out.set(normalized, rawVal);
      }
      if (rawVal && typeof rawVal === "object") queue.push(rawVal);
    }
  }

  return out;
}

function toDateOnly(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const m = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function coerceValue(value, col) {
  if (!isMeaningful(value)) return null;

  const dataType = String(col.data_type || "").toLowerCase();
  const udtName = String(col.udt_name || "").toLowerCase();

  if (dataType === "date") return toDateOnly(value);

  if (dataType.includes("timestamp")) {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  if (dataType === "boolean") {
    if (typeof value === "boolean") return value;
    const s = String(value).trim().toLowerCase();
    if (["true", "t", "1", "yes", "y"].includes(s)) return true;
    if (["false", "f", "0", "no", "n"].includes(s)) return false;
    return null;
  }

  if (["smallint", "integer", "bigint"].includes(dataType)) {
    if (typeof value === "number" && Number.isInteger(value)) return value;
    const n = Number(String(value).replace(/,/g, "").trim());
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  }

  if (["real", "double precision", "numeric", "decimal"].includes(dataType)) {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    const n = Number(String(value).replace(/,/g, "").trim());
    return Number.isFinite(n) ? n : null;
  }

  if (dataType === "json" || dataType === "jsonb" || udtName === "json" || udtName === "jsonb") {
    return null;
  }

  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

async function getApplicationColumns(client) {
  const { rows } = await client.query(
    `
      SELECT column_name, data_type, udt_name, is_nullable
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'applications'
      ORDER BY ordinal_position ASC
    `,
  );

  return rows.filter((r) => r.is_nullable === "YES" && !SKIP_COLUMNS.has(r.column_name));
}

async function fetchBatch(client, { lastId, limit, onsCode }) {
  const params = [lastId, limit];
  const onsClause = onsCode ? `AND a.ons_code = $3` : "";
  if (onsCode) params.push(onsCode);

  const { rows } = await client.query(
    `
      SELECT
        a.*,
        j.id AS matched_scrape_job_id,
        j.result AS scrape_result
      FROM public.applications a
      LEFT JOIN LATERAL (
        SELECT id, result
        FROM public.scrape_jobs
        WHERE application_ref = a.reference
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1
      ) j ON TRUE
      WHERE a.id > $1
      ${onsClause}
      ORDER BY a.id ASC
      LIMIT $2
    `,
    params,
  );

  return rows;
}

async function updateApplication(client, appId, updates) {
  const keys = Object.keys(updates);
  if (!keys.length) return;

  const setSql = keys.map((k, i) => `${quoteIdent(k)} = $${i + 1}`).join(", ");
  const values = keys.map((k) => updates[k]);
  values.push(appId);

  await client.query(
    `
      UPDATE public.applications
      SET ${setSql},
          updated_at = now()
      WHERE id = $${keys.length + 1}
    `,
    values,
  );
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();

  const nullableColumns = await getApplicationColumns(client);
  const colByName = new Map(nullableColumns.map((c) => [c.column_name, c]));
  const startMs = Date.now();

  let lastId = 0;
  let scanned = 0;
  let withMatch = 0;
  let withResult = 0;
  let updatedApps = 0;
  let updatedFields = 0;
  let heartbeat = null;

  try {
    const heartbeatSeconds = Number(argv["heartbeat-seconds"] || 0);
    console.error(
      `[application-backfill-from-scrape-result] starting mode=${argv.apply ? "apply" : "dry-run"} ` +
        `limit=${Number(argv.limit || 200)} ons_code=${String(argv["ons-code"] || "").trim() || "ALL"} ` +
        `heartbeat=${heartbeatSeconds > 0 ? `${heartbeatSeconds}s` : "off"}`,
    );
    if (heartbeatSeconds > 0) {
      heartbeat = setInterval(() => {
        const elapsedSec = Math.max(1, Math.floor((Date.now() - startMs) / 1000));
        const rowsPerSec = (scanned / elapsedSec).toFixed(1);
        console.error(
          `[application-backfill-from-scrape-result] heartbeat scanned=${scanned} ` +
            `with_match=${withMatch} with_result=${withResult} apps_with_updates=${updatedApps} ` +
            `fields_updated=${updatedFields} rps=${rowsPerSec}`,
        );
      }, heartbeatSeconds * 1000);
    }

    while (true) {
      const batch = await fetchBatch(client, {
        lastId,
        limit: Number(argv.limit || 200),
        onsCode: String(argv["ons-code"] || "").trim(),
      });
      if (!batch.length) break;

      for (const row of batch) {
        scanned++;
        lastId = Number(row.id);

        const result = row.scrape_result;
        if (row.matched_scrape_job_id) withMatch++;
        if (!result || typeof result !== "object") continue;
        withResult++;

        const index = buildJsonKeyIndex(result);
        const updates = {};

        for (const col of nullableColumns) {
          const colName = col.column_name;
          if (row[colName] != null) continue;

          const raw = index.get(normalizeLookupKey(colName));
          if (!isMeaningful(raw)) continue;

          const coerced = coerceValue(raw, colByName.get(colName));
          if (coerced == null) continue;
          updates[colName] = coerced;
        }

        const n = Object.keys(updates).length;
        if (!n) continue;

        if (argv.apply) {
          // eslint-disable-next-line no-await-in-loop
          await updateApplication(client, row.id, updates);
        }
        updatedApps++;
        updatedFields += n;
      }

      if (argv["max-rows"] > 0 && scanned >= Number(argv["max-rows"])) break;
    }
  } finally {
    if (heartbeat) clearInterval(heartbeat);
    await client.end();
  }

  console.log(
    JSON.stringify({
      ok: true,
      mode: argv.apply ? "apply" : "dry-run",
      scanned,
      with_matched_scrape_job: withMatch,
      with_scrape_result: withResult,
      apps_with_updates: updatedApps,
      fields_updated: updatedFields,
    }),
  );
}

main().catch((e) => {
  console.error("[application-backfill-from-scrape-result] fatal:", e);
  process.exit(1);
});
