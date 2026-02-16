// application_discover_idox_validated.js
//
// Long-running (or one-shot) discovery that walks backwards in time week-by-week
// using Idox advanced search "Date Validated" range, then enqueues scrape_jobs
// for per-application detail scraping.
//
// IMPORTANT: import ./bootstrap.js first to load .env.
import "./bootstrap.js";

import { spawn } from "node:child_process";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("idox-discover-validated")
  .option("ons-code", { type: "string", demandOption: true, describe: "ONS code, e.g. E09000033" })
  .option("cutoff-date", {
    type: "string",
    default: "2018-01-01",
    describe: "Stop backfill once window_end < cutoff-date (YYYY-MM-DD).",
  })
  .option("step-days", { type: "number", default: 7, describe: "Window size in days." })
  .option("loop", { type: "boolean", default: false, describe: "Run forever." })
  .option("sleep-ms", { type: "number", default: 60_000, describe: "Sleep between iterations when looping." })
  .option("steady-sleep-ms", {
    type: "number",
    default: 7 * 24 * 60 * 60 * 1000,
    describe: "Sleep between steady-state weekly scans when looping.",
  })
  .option("max-pages", { type: "number", default: 100, describe: "Max results pages to traverse per window." })
  .option("dry-run", { type: "boolean", default: false, describe: "Do not enqueue scrape_jobs." })
  .strict()
  .help()
  .argv;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseISODate(s) {
  const raw = String(s || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) throw new Error(`Expected YYYY-MM-DD. Got: ${raw}`);
  return raw;
}

function isoAddDays(iso, days) {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

function isoLessThan(a, b) {
  return a < b; // safe for YYYY-MM-DD
}

function parseEmittedJson(stdout, marker) {
  const matches = stdout.match(new RegExp(`^${marker}=(.+)$`, "gm"));
  if (!matches || matches.length === 0) return null;
  const last = matches[matches.length - 1];
  const jsonPart = last.slice(marker.length + 1);
  try {
    return JSON.parse(jsonPart);
  } catch {
    return null;
  }
}

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

async function getConfigForOns(client, onsCode) {
  const { rows } = await client.query(
    `
      SELECT ons_code, site_url, enabled
      FROM lpa_scrape_configs
      WHERE ons_code = $1 AND enabled = true
      LIMIT 1
    `,
    [onsCode],
  );
  return rows[0] || null;
}

async function ensureBackfillState(client, onsCode, cutoffDate) {
  const { rows } = await client.query(
    `
      SELECT ons_code, mode,
             cursor_end::text AS cursor_end,
             cutoff_date::text AS cutoff_date,
             updated_at
      FROM application_backfill_state
      WHERE ons_code = $1
      LIMIT 1
    `,
    [onsCode],
  );
  if (rows[0]) return rows[0];

  // Initialize cursor_end to DB current_date for determinism.
  const { rows: nowRows } = await client.query(`SELECT current_date::text AS d`);
  const cursorEnd = nowRows[0]?.d;
  if (!cursorEnd) throw new Error("Could not read current_date from DB.");

  const ins = await client.query(
    `
      INSERT INTO application_backfill_state (ons_code, mode, cursor_end, cutoff_date)
      VALUES ($1, 'backfill', $2::date, $3::date)
      RETURNING ons_code, mode,
                cursor_end::text AS cursor_end,
                cutoff_date::text AS cutoff_date,
                updated_at
    `,
    [onsCode, cursorEnd, cutoffDate],
  );
  return ins.rows[0];
}

async function createDiscoveryRun(client, onsCode, windowStart, windowEnd) {
  const { rows } = await client.query(
    `
      INSERT INTO application_discovery_runs (ons_code, window_start, window_end, status)
      VALUES ($1, $2::date, $3::date, 'running')
      RETURNING id
    `,
    [onsCode, windowStart, windowEnd],
  );
  return rows[0].id;
}

async function finishDiscoveryRun(client, runId, { status, nRefs, error }) {
  await client.query(
    `
      UPDATE application_discovery_runs
      SET status = $2,
          n_refs = $3,
          error = $4,
          updated_at = now()
      WHERE id = $1
    `,
    [runId, status, nRefs ?? 0, error ?? null],
  );
}

async function enqueueScrapeJobs(client, { onsCode, refs, windowStart, windowEnd, runId, dryRun }) {
  if (dryRun) return { inserted: 0 };

  let inserted = 0;
  for (const ref of refs) {
    const idempotencyKey = `${onsCode}:detail:v1:${ref}`;
    const params = {
      discovered_by: "validated_range",
      discovery_run_id: runId,
      window_start: windowStart,
      window_end: windowEnd,
    };

    const { rowCount } = await client.query(
      `
        INSERT INTO scrape_jobs (job_type, ons_code, application_ref, params, status, idempotency_key, requested_by)
        VALUES ('detail_scrape', $1, $2, $3::jsonb, 'queued', $4, 'validated_discovery')
        ON CONFLICT (idempotency_key) DO NOTHING
      `,
      [onsCode, ref, JSON.stringify(params), idempotencyKey],
    );
    inserted += rowCount || 0;
  }

  return { inserted };
}

function runValidatedRangeScraper({ startUrl, validatedStart, validatedEnd, maxPages }) {
  return new Promise((resolve) => {
    const script = path.resolve(process.cwd(), "scraper_idox_validated_range_refs.cjs");
    const args = [
      script,
      "--start-url",
      startUrl,
      "--validated-start",
      validatedStart,
      "--validated-end",
      validatedEnd,
      "--max-pages",
      String(maxPages),
      "--emit-json",
    ];

    const child = spawn(process.execPath, args, {
      cwd: process.cwd(),
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (c) => (stdout += c.toString()));
    child.stderr.on("data", (c) => (stderr += c.toString()));

    child.on("close", (code) => resolve({ code, stdout, stderr }));
  });
}

async function runOnce(client) {
  const onsCode = argv["ons-code"];
  const cutoffDate = parseISODate(argv["cutoff-date"]);
  const stepDays = Number(argv["step-days"] || 7);
  const maxPages = Number(argv["max-pages"] || 100);

  const cfg = await getConfigForOns(client, onsCode);
  if (!cfg) throw new Error(`No enabled lpa_scrape_configs row for ons_code ${onsCode}.`);

  const state = await ensureBackfillState(client, onsCode, cutoffDate);
  if (state.mode !== "backfill") {
    // Steady-state: only run at most once per 7 days (polite), even if scheduled hourly.
    const { rows: tsRows } = await client.query(`SELECT now() AS now_ts`);
    const nowTs = tsRows[0]?.now_ts ? new Date(tsRows[0].now_ts) : new Date();
    const last = state.updated_at ? new Date(state.updated_at) : null;
    if (last && nowTs.getTime() - last.getTime() < 7 * 24 * 60 * 60 * 1000) {
      return { mode: "steady_state", skipped: true, last_run_at: last.toISOString() };
    }

    // Steady-state: discover the most recent completed week and enqueue detail scrapes.
    const { rows: dateRows } = await client.query(`SELECT current_date::text AS d`);
    const today = String(dateRows[0].d);
    const windowEnd = isoAddDays(today, -1);
    const windowStart = isoAddDays(windowEnd, -(stepDays - 1));
    const runId = await createDiscoveryRun(client, onsCode, windowStart, windowEnd);

    try {
      const r = await runValidatedRangeScraper({
        startUrl: cfg.site_url,
        validatedStart: windowStart,
        validatedEnd: windowEnd,
        maxPages,
      });
      if (r.code !== 0) throw new Error(`validated-range scraper failed (exit ${r.code}). ${r.stderr.trim()}`);

      const payload = parseEmittedJson(r.stdout, "__IDOX_VALIDATED_RANGE_REFS__");
      if (!payload || !Array.isArray(payload.refs)) {
        throw new Error(`Could not parse __IDOX_VALIDATED_RANGE_REFS__ marker. stderr=${r.stderr.slice(0, 500)}`);
      }

      const refs = payload.refs;
      const { inserted } = await enqueueScrapeJobs(client, {
        onsCode,
        refs,
        windowStart,
        windowEnd,
        runId,
        dryRun: argv["dry-run"],
      });
      await finishDiscoveryRun(client, runId, { status: "completed", nRefs: refs.length, error: null });

      // Mark last steady-state run time.
      await client.query(
        `
          UPDATE application_backfill_state
          SET updated_at = now()
          WHERE ons_code = $1
        `,
        [onsCode],
      );

      return { mode: "steady_state", windowStart, windowEnd, refs: refs.length, inserted };
    } catch (e) {
      await finishDiscoveryRun(client, runId, { status: "failed", nRefs: 0, error: e instanceof Error ? e.message : String(e) });
      throw e;
    }
  }

  // Backfill mode: walk backwards week-by-week.
  const windowEnd = String(state.cursor_end);
  if (isoLessThan(windowEnd, cutoffDate)) {
    await client.query(
      `
        UPDATE application_backfill_state
        SET mode = 'steady_state',
            updated_at = now()
        WHERE ons_code = $1
      `,
      [onsCode],
    );
    return { mode: "steady_state", switched: true };
  }

  const windowStart = isoAddDays(windowEnd, -(stepDays - 1));
  const runId = await createDiscoveryRun(client, onsCode, windowStart, windowEnd);

  try {
    const r = await runValidatedRangeScraper({
      startUrl: cfg.site_url,
      validatedStart: windowStart,
      validatedEnd: windowEnd,
      maxPages,
    });

    if (r.code !== 0) throw new Error(`validated-range scraper failed (exit ${r.code}). ${r.stderr.trim()}`);

    const payload = parseEmittedJson(r.stdout, "__IDOX_VALIDATED_RANGE_REFS__");
    if (!payload || !Array.isArray(payload.refs)) {
      throw new Error(`Could not parse __IDOX_VALIDATED_RANGE_REFS__ marker. stderr=${r.stderr.slice(0, 500)}`);
    }

    const refs = payload.refs;
    const { inserted } = await enqueueScrapeJobs(client, {
      onsCode,
      refs,
      windowStart,
      windowEnd,
      runId,
      dryRun: argv["dry-run"],
    });

    await finishDiscoveryRun(client, runId, { status: "completed", nRefs: refs.length, error: null });

    // Move cursor backwards by one day before this window.
    const nextCursorEnd = isoAddDays(windowStart, -1);
    await client.query(
      `
        UPDATE application_backfill_state
        SET cursor_end = $2::date,
            updated_at = now()
        WHERE ons_code = $1
      `,
      [onsCode, nextCursorEnd],
    );

    return { mode: "backfill", windowStart, windowEnd, refs: refs.length, inserted, nextCursorEnd };
  } catch (e) {
    await finishDiscoveryRun(client, runId, { status: "failed", nRefs: 0, error: e instanceof Error ? e.message : String(e) });
    throw e;
  }
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    do {
      const r = await runOnce(client);
      console.log(JSON.stringify({ ok: true, ...r }));
      if (!argv.loop) break;
      const ms = r?.mode === "steady_state" ? Number(argv["steady-sleep-ms"]) : Number(argv["sleep-ms"] || 60_000);
      await sleep(ms);
    } while (true);
  } finally {
    await client.end();
  }
}

main().catch((e) => {
  console.error("[discovery] fatal:", e);
  process.exit(1);
});
