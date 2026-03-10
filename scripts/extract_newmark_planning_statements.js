#!/usr/bin/env node
import "../bootstrap.js";

import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const perAppScriptPath = path.join(rootDir, "scripts", "detect_newmark_jobcode_for_uid.js");

const argv = yargs(hideBin(process.argv))
  .scriptName("extract-newmark-planning-statements")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to process.",
  })
  .option("year", {
    type: "number",
    describe: "Restrict applications to this validated/received/date_added year.",
  })
  .option("limit", {
    type: "number",
    default: 50,
    describe: "Maximum applications to process.",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into the candidate list.",
  })
  .option("artifacts-dir", {
    type: "string",
    default: "/mnt/HC_Volume_103054926/newmark_jobcode_uid_artifacts",
    describe: "Directory for downloaded PDFs / extracted text.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 60000,
    describe: "Navigation/request timeout in milliseconds.",
  })
  .option("delay-ms", {
    type: "number",
    default: 60000,
    describe: "Base delay between per-application runs in milliseconds.",
  })
  .option("delay-jitter-ms", {
    type: "number",
    default: 10000,
    describe: "Random plus/minus jitter applied to the base delay between runs.",
  })
  .option("headed", {
    type: "boolean",
    default: false,
    describe: "Run browser headed for debugging.",
  })
  .option("debug-docs", {
    type: "boolean",
    default: false,
    describe: "Pass through document debug logging to the per-application worker.",
  })
  .strict()
  .help()
  .argv;

const CANDIDATE_SQL = `
  SELECT
    a.reference,
    a.keyval
  FROM public.applications a
  WHERE a.ons_code = $1
    AND NULLIF(BTRIM(a.keyval), '') IS NOT NULL
    AND (
      a.agent_company_name ILIKE '%newmark%'
      OR a.agent_company_name ILIKE '%gerald eve%'
    )
    AND (
      a.reference ILIKE '%/FULL'
      OR a.reference ILIKE '%/LBC'
    )
    AND (
      $2::int IS NULL
      OR EXTRACT(YEAR FROM COALESCE(a.application_validated, a.application_received, a.date_added, current_date)) = $2
    )
  ORDER BY COALESCE(a.application_validated, a.application_received, a.date_added, current_date) DESC, a.reference DESC
  LIMIT $3 OFFSET $4
`;

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

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function computeDelayMs() {
  const baseDelay = Math.max(0, Number(argv["delay-ms"]) || 0);
  const jitter = Math.max(0, Number(argv["delay-jitter-ms"]) || 0);
  if (jitter === 0) return baseDelay;
  const offset = Math.floor((Math.random() * ((jitter * 2) + 1)) - jitter);
  return Math.max(0, baseDelay + offset);
}

function runPerApplication({ reference, keyval }) {
  return new Promise((resolve, reject) => {
    const args = [
      perAppScriptPath,
      "--reference",
      reference,
      "--keyval",
      keyval,
      "--ons-code",
      String(argv["ons-code"]),
      "--artifacts-dir",
      String(argv["artifacts-dir"]),
      "--timeout-ms",
      String(argv["timeout-ms"]),
      "--skip-upsert",
    ];

    if (argv.headed) args.push("--headed");
    if (argv["debug-docs"]) args.push("--debug-docs");

    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: "inherit",
      env: process.env,
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`Per-application worker exited with code ${code} for ${reference}`));
    });
  });
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();

  try {
    logEvent("batch_start", {
      ons_code: argv["ons-code"],
      year: argv.year ?? null,
      limit: Number(argv.limit),
      offset: Number(argv.offset),
    });

    const res = await client.query(CANDIDATE_SQL, [
      String(argv["ons-code"]),
      Number.isFinite(Number(argv.year)) ? Number(argv.year) : null,
      Number(argv.limit),
      Number(argv.offset),
    ]);

    logEvent("batch_candidates", { count: res.rows.length });

    for (let index = 0; index < res.rows.length; index += 1) {
      const row = res.rows[index];
      const reference = String(row.reference || "").trim();
      const keyval = String(row.keyval || "").trim();
      if (!reference || !keyval) continue;

      logEvent("batch_item_start", {
        index: index + 1,
        total: res.rows.length,
        reference,
        keyval,
      });

      await runPerApplication({ reference, keyval });

      logEvent("batch_item_done", {
        index: index + 1,
        total: res.rows.length,
        reference,
      });

      if (index < res.rows.length - 1) {
        const delayMs = computeDelayMs();
        logEvent("batch_delay", {
          next_index: index + 2,
          total: res.rows.length,
          delay_ms: delayMs,
        });
        await sleep(delayMs);
      }
    }

    logEvent("batch_done", { processed: res.rows.length });
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  logEvent("batch_fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
