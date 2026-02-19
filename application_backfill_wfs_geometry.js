// application_backfill_wfs_geometry.js
//
// Backfill lat/lon + wfs_geom from Westminster WFS by keyval.
// - Scans applications where lat/lon is missing and keyval exists.
// - Works backwards by id (newest first).
// - Sleeps between requests (default 1000ms).
// - Dry-run by default; use --apply to write.
//
// IMPORTANT: import ./bootstrap.js first to load .env.
import "./bootstrap.js";

import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import wfsModule from "./wcc_wfs_geometry.cjs";

const { Client } = pg;
const { fetchGeometryByKeyVal } = wfsModule;

const argv = yargs(hideBin(process.argv))
  .scriptName("application-backfill-wfs-geometry")
  .option("limit", { type: "number", default: 100, describe: "Max applications to scan (0 = no cap)." })
  .option("batch-size", { type: "number", default: 20, describe: "Batch size per DB fetch." })
  .option("ons-code", { type: "string", default: "", describe: "Only process this ONS code." })
  .option("sleep-ms", { type: "number", default: 1000, describe: "Pause between WFS requests." })
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
  const onsClause = onsCode ? "AND ons_code = $3" : "";
  if (onsCode) params.push(onsCode);

  const { rows } = await client.query(
    `
      SELECT id, ons_code, reference, keyval, lat, lon
      FROM public.applications
      WHERE ($1::bigint IS NULL OR id < $1::bigint)
        AND (lat IS NULL OR lon IS NULL)
        AND NULLIF(BTRIM(keyval), '') IS NOT NULL
        ${onsClause}
      ORDER BY id DESC
      LIMIT $2
    `,
    params,
  );

  return rows;
}

async function updateApplicationGeometry(client, rowId, geom) {
  const srid = Number(geom?.srid || 27700);
  const wkt = typeof geom?.wkt === "string" ? geom.wkt : null;
  const centroidE = Number(geom?.centroid?.e);
  const centroidN = Number(geom?.centroid?.n);
  const hasCentroid = Number.isFinite(centroidE) && Number.isFinite(centroidN);

  await client.query(
    `
      UPDATE public.applications
      SET
        wfs_geom = CASE
          WHEN $2::text IS NULL THEN wfs_geom
          ELSE ST_GeomFromText($2::text, $3::int)
        END,
        wfs_geometry = $4::jsonb,
        lon = CASE
          WHEN $5::double precision IS NULL OR $6::double precision IS NULL THEN lon
          ELSE ST_X(ST_Transform(ST_SetSRID(ST_MakePoint($5::double precision, $6::double precision), $3::int), 4326))
        END,
        lat = CASE
          WHEN $5::double precision IS NULL OR $6::double precision IS NULL THEN lat
          ELSE ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint($5::double precision, $6::double precision), $3::int), 4326))
        END,
        updated_at = now()
      WHERE id = $1
    `,
    [rowId, wkt, srid, JSON.stringify(geom), hasCentroid ? centroidE : null, hasCentroid ? centroidN : null],
  );
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();

  const sleepMs = Math.max(0, Number(argv["sleep-ms"] || 0));
  const batchSize = Math.max(1, Number(argv["batch-size"] || 20));
  const maxRows = Math.max(0, Number(argv.limit || 0));
  const onsCode = String(argv["ons-code"] || "").trim();

  let lastId = null;
  let scanned = 0;
  let fetched = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;

  console.error(
    `[application-backfill-wfs-geometry] starting mode=${argv.apply ? "apply" : "dry-run"} ` +
      `batch_size=${batchSize} sleep_ms=${sleepMs} limit=${maxRows || "ALL"} ons_code=${onsCode || "ALL"}`,
  );

  try {
    while (true) {
      const batch = await fetchBatch(client, { lastId, batchSize, onsCode });
      if (!batch.length) break;

      for (const row of batch) {
        if (maxRows > 0 && scanned >= maxRows) break;
        scanned++;
        lastId = String(row.id);

        const keyVal = String(row.keyval || "").trim();
        if (!keyVal) {
          skipped++;
          continue;
        }

        let geom;
        try {
          // eslint-disable-next-line no-await-in-loop
          geom = await fetchGeometryByKeyVal(keyVal);
          fetched++;
        } catch (err) {
          failed++;
          console.error(
            `[application-backfill-wfs-geometry] fetch_failed id=${row.id} ref=${row.reference} keyval=${keyVal}:`,
            err instanceof Error ? err.message : String(err),
          );
          if (sleepMs > 0) {
            // eslint-disable-next-line no-await-in-loop
            await sleep(sleepMs);
          }
          continue;
        }

        const hasWkt = typeof geom?.wkt === "string" && geom.wkt.length > 0;
        const hasCentroid = Number.isFinite(Number(geom?.centroid?.e)) && Number.isFinite(Number(geom?.centroid?.n));
        if (!hasWkt && !hasCentroid) {
          skipped++;
          if (sleepMs > 0) {
            // eslint-disable-next-line no-await-in-loop
            await sleep(sleepMs);
          }
          continue;
        }

        if (argv.apply) {
          try {
            // eslint-disable-next-line no-await-in-loop
            await updateApplicationGeometry(client, row.id, geom);
            updated++;
          } catch (err) {
            failed++;
            console.error(
              `[application-backfill-wfs-geometry] update_failed id=${row.id} ref=${row.reference} keyval=${keyVal}:`,
              err instanceof Error ? err.message : String(err),
            );
          }
        } else {
          updated++;
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

  console.log(
    JSON.stringify({
      ok: true,
      mode: argv.apply ? "apply" : "dry-run",
      scanned,
      fetched,
      updated,
      skipped,
      failed,
      sleep_ms: sleepMs,
    }),
  );
}

main().catch((e) => {
  console.error("[application-backfill-wfs-geometry] fatal:", e);
  process.exit(1);
});
