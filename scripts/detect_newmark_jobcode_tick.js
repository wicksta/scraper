#!/usr/bin/env node
import "./../bootstrap.js";

import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const uidWorkerPath = path.join(rootDir, "scripts", "detect_newmark_jobcode_for_uid.js");

const argv = yargs(hideBin(process.argv))
  .scriptName("detect-newmark-jobcode-tick")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to process.",
  })
  .option("year", {
    type: "number",
    default: 2026,
    describe: "Year to process (works backwards through this year).",
  })
  .option("include-existing", {
    type: "boolean",
    default: false,
    describe: "If true, allows reprocessing rows already in newmark_jobcode_candidates.",
  })
  .option("extract-applicant-with-openai", {
    type: "boolean",
    default: true,
    describe: "Pass OpenAI applicant extraction flag to UID worker.",
  })
  .option("openai-model", {
    type: "string",
    default: "gpt-4.1-mini",
    describe: "OpenAI model to pass through.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 60000,
    describe: "Timeout passed to UID worker.",
  })
  .strict()
  .help()
  .argv;

const NEXT_CANDIDATE_SQL = `
  SELECT
    a.ons_code,
    a.reference,
    a.application_validated,
    a.application_received,
    a.date_added
  FROM public.applications a
  WHERE a.ons_code = $1
    AND (
      a.agent_company_name ILIKE '%newmark%'
      OR a.agent_company_name ILIKE '%gerald eve%'
    )
    AND EXTRACT(YEAR FROM COALESCE(a.application_validated, a.application_received, a.date_added, current_date)) = $2
    AND (
      $3::boolean
      OR NOT EXISTS (
        SELECT 1
        FROM public.newmark_jobcode_candidates c
        WHERE c.ons_code = a.ons_code
          AND c.reference = a.reference
      )
    )
  ORDER BY COALESCE(a.application_validated, a.application_received, a.date_added, current_date) DESC, a.reference DESC
  LIMIT 1
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

function runUidWorker({ uid, onsCode, timeoutMs, openaiModel, extractApplicantWithOpenAi }) {
  return new Promise((resolve, reject) => {
    const args = [
      uidWorkerPath,
      "--uid",
      uid,
      "--ons-code",
      onsCode,
      "--timeout-ms",
      String(timeoutMs),
      "--openai-model",
      openaiModel,
    ];
    if (extractApplicantWithOpenAi) {
      args.push("--extract-applicant-with-openai");
    }

    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: "inherit",
      env: process.env,
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`UID worker exited with code ${code}`));
    });
  });
}

async function main() {
  const pgClient = new Client(getPgClientConfig());
  await pgClient.connect();
  try {
    logEvent("tick_start", {
      ons_code: argv["ons-code"],
      year: Number(argv.year),
      include_existing: Boolean(argv["include-existing"]),
      extract_applicant_with_openai: Boolean(argv["extract-applicant-with-openai"]),
    });

    const res = await pgClient.query(NEXT_CANDIDATE_SQL, [
      argv["ons-code"],
      Number(argv.year),
      Boolean(argv["include-existing"]),
    ]);

    if (!res.rows.length) {
      logEvent("tick_idle", {
        message: "No remaining candidate found for current criteria.",
      });
      return;
    }

    const row = res.rows[0];
    const reference = String(row.reference || "").trim();
    const onsCode = String(row.ons_code || "").trim();
    logEvent("tick_selected", {
      ons_code: onsCode,
      reference,
      application_validated: row.application_validated || null,
      application_received: row.application_received || null,
      date_added: row.date_added || null,
    });

    await runUidWorker({
      uid: reference,
      onsCode,
      timeoutMs: Number(argv["timeout-ms"]),
      openaiModel: String(argv["openai-model"]),
      extractApplicantWithOpenAi: Boolean(argv["extract-applicant-with-openai"]),
    });

    logEvent("tick_done", { ons_code: onsCode, reference });
  } finally {
    await pgClient.end();
  }
}

main().catch((err) => {
  logEvent("tick_fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
