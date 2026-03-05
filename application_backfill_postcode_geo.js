// application_backfill_postcode_geo.js
//
// Retrospective helper:
// - Scans completed scrape_jobs where postcode exists but lat/lng are missing.
// - Re-runs postcode geocoding via ONSPD (with retries).
// - Dry-run by default; use --apply to persist updates.
//
// IMPORTANT: import ./bootstrap.js first to load .env.
import "./bootstrap.js";

import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { resolvePostcodeViaOnspd } = require("./library.cjs");

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("application-backfill-postcode-geo")
  .option("limit", { type: "number", default: 500, describe: "Max rows to scan (0 = no cap)." })
  .option("batch-size", { type: "number", default: 50, describe: "Rows fetched per DB batch." })
  .option("ons-code", { type: "string", default: "", describe: "Only process this ONS code." })
  .option("sleep-ms", { type: "number", default: 200, describe: "Pause between postcode lookups." })
  .option("timeout-ms", { type: "number", default: 6000, describe: "ONSPD request timeout per attempt." })
  .option("attempts", { type: "number", default: 3, describe: "Lookup attempts per postcode." })
  .option("apply", { type: "boolean", default: false, describe: "Write updates. Default is dry-run." })
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchBatch(client, { lastId, batchSize, onsCode }) {
  const params = [lastId, batchSize];
  const onsClause = onsCode ? "AND j.ons_code = $3" : "";
  if (onsCode) params.push(onsCode);

  const { rows } = await client.query(
    `
      SELECT j.id, j.ons_code, j.application_ref, j.result
      FROM public.scrape_jobs j
      WHERE ($1::bigint IS NULL OR j.id < $1::bigint)
        AND j.status = 'completed'
        AND j.result->'planit'->'planit' IS NOT NULL
        AND NULLIF(BTRIM(j.result->'planit'->'planit'->>'postcode'), '') IS NOT NULL
        AND (j.result->'planit'->'planit'->>'lat') IS NULL
        AND (j.result->'planit'->'planit'->>'lng') IS NULL
        ${onsClause}
      ORDER BY j.id DESC
      LIMIT $2
    `,
    params,
  );

  return rows;
}

async function lookupWithRetry(postcode, { attempts, timeoutMs, sleepMs }) {
  let lastOut = { success: false, error: "lookup_not_attempted", postcode };
  const maxAttempts = Math.max(1, Number(attempts || 1));

  for (let i = 1; i <= maxAttempts; i++) {
    try {
      // noCache=true keeps retries honest (avoid pinning transient failures).
      // eslint-disable-next-line no-await-in-loop
      const out = await resolvePostcodeViaOnspd(postcode, { noCache: true, timeoutMs });
      if (out?.success) return out;
      lastOut = out || { success: false, error: "unknown_error", postcode };
    } catch (err) {
      lastOut = { success: false, error: err instanceof Error ? err.message : String(err), postcode };
    }

    if (i < maxAttempts) {
      const backoffMs = Math.max(0, Number(sleepMs || 0)) * i;
      if (backoffMs > 0) {
        // eslint-disable-next-line no-await-in-loop
        await sleep(backoffMs);
      }
    }
  }

  return lastOut;
}

function cloneJson(x) {
  return x == null ? x : JSON.parse(JSON.stringify(x));
}

async function updateScrapeJobResult(client, jobId, nextResult) {
  const { rowCount } = await client.query(
    `
      UPDATE public.scrape_jobs
      SET result = $2::jsonb,
          updated_at = now()
      WHERE id = $1
        AND (result->'planit'->'planit'->>'lat') IS NULL
        AND (result->'planit'->'planit'->>'lng') IS NULL
    `,
    [jobId, JSON.stringify(nextResult)],
  );

  return rowCount;
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();

  const sleepMs = Math.max(0, Number(argv["sleep-ms"] || 0));
  const timeoutMs = Math.max(1000, Number(argv["timeout-ms"] || 6000));
  const attempts = Math.max(1, Number(argv.attempts || 1));
  const batchSize = Math.max(1, Number(argv["batch-size"] || 50));
  const maxRows = Math.max(0, Number(argv.limit || 0));
  const onsCode = String(argv["ons-code"] || "").trim();

  let lastId = null;
  let scanned = 0;
  let lookupOk = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;
  const failByReason = new Map();

  console.error(
    `[application-backfill-postcode-geo] starting mode=${argv.apply ? "apply" : "dry-run"} ` +
      `batch_size=${batchSize} limit=${maxRows || "ALL"} ons_code=${onsCode || "ALL"} ` +
      `attempts=${attempts} timeout_ms=${timeoutMs} sleep_ms=${sleepMs}`,
  );

  try {
    while (true) {
      const batch = await fetchBatch(client, { lastId, batchSize, onsCode });
      if (!batch.length) break;

      for (const row of batch) {
        if (maxRows > 0 && scanned >= maxRows) break;

        scanned++;
        lastId = String(row.id);

        const planit = row?.result?.planit?.planit;
        const postcode = String(planit?.postcode || "").trim();
        if (!postcode) {
          skipped++;
          continue;
        }

        // eslint-disable-next-line no-await-in-loop
        const geo = await lookupWithRetry(postcode, { attempts, timeoutMs, sleepMs });
        if (!geo?.success) {
          failed++;
          const reason = String(geo?.error || "unknown_error");
          failByReason.set(reason, (failByReason.get(reason) || 0) + 1);
          continue;
        }

        lookupOk++;
        const lat = Number(geo.lat);
        const lng = Number(geo.long);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
          failed++;
          failByReason.set("non_numeric_coords", (failByReason.get("non_numeric_coords") || 0) + 1);
          continue;
        }

        if (!argv.apply) {
          updated++;
        } else {
          const nextResult = cloneJson(row.result) || {};
          nextResult.planit = nextResult.planit && typeof nextResult.planit === "object" ? nextResult.planit : {};
          nextResult.planit.planit =
            nextResult.planit.planit && typeof nextResult.planit.planit === "object" ? nextResult.planit.planit : {};

          nextResult.planit.planit.lat = lat;
          nextResult.planit.planit.lng = lng;
          if (nextResult.planit.planit.location_x == null && nextResult.planit.planit.location_y == null) {
            nextResult.planit.planit.location_x = lat;
            nextResult.planit.planit.location_y = lng;
          }

          // eslint-disable-next-line no-await-in-loop
          const rowCount = await updateScrapeJobResult(client, row.id, nextResult);
          if (rowCount > 0) updated++;
          else skipped++;
        }

        if (sleepMs > 0) {
          // eslint-disable-next-line no-await-in-loop
          await sleep(sleepMs);
        }
      }

      if (maxRows > 0 && scanned >= maxRows) break;
      lastId = String(batch[batch.length - 1].id);
    }
  } finally {
    await client.end();
  }

  const failReasons = {};
  for (const [k, v] of failByReason.entries()) failReasons[k] = v;

  console.log(
    JSON.stringify({
      ok: true,
      mode: argv.apply ? "apply" : "dry-run",
      scanned,
      lookup_ok: lookupOk,
      updated,
      skipped,
      failed,
      fail_reasons: failReasons,
      attempts,
      timeout_ms: timeoutMs,
      sleep_ms: sleepMs,
    }),
  );
}

main().catch((e) => {
  console.error("[application-backfill-postcode-geo] fatal:", e);
  process.exit(1);
});
