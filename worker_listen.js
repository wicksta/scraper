// worker_listen.js
import "./bootstrap.js";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import os from "node:os";
import path from "node:path";
import pg from "pg";
const { Client } = pg;

const CHANNEL = "scrape_job_created";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}:${process.pid}`;
const SCRAPER_TIMEOUT_MS = Number(process.env.SCRAPER_TIMEOUT_MS || 15 * 60 * 1000);
const PROJECT_ROOT = process.cwd();
const ALLOWED_SCRAPER_ENTRYPOINTS = new Set([
  "scraper.cjs",
  "scraper_camden_northgate.cjs",
  "scraper_camden_accountforms.cjs",
  "scraper_camden_socrata.cjs",
]);

let client;
let draining = false;
let drainRequested = false;

function tail(value, maxLen = 4000) {
  if (!value) return "";
  return value.length > maxLen ? value.slice(-maxLen) : value;
}

async function claimNextQueuedJob() {
  const sql = `
    WITH candidate AS (
      SELECT id
      FROM scrape_jobs
      WHERE status = 'queued'
        AND ons_code IS NOT NULL
        AND application_ref IS NOT NULL
      ORDER BY created_at ASC, id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE scrape_jobs j
    SET status = 'running',
        locked_at = now(),
        locked_by = $1,
        attempts = j.attempts + 1
    FROM candidate
    WHERE j.id = candidate.id
    RETURNING j.*;
  `;
  const { rows } = await client.query(sql, [WORKER_ID]);
  return rows[0] || null;
}

async function getConfigForOns(onsCode) {
  const { rows } = await client.query(
    `
      SELECT ons_code, site_url, scraper_entrypoint, mapper_path, enabled
      FROM lpa_scrape_configs
      WHERE ons_code = $1
        AND enabled = true
      LIMIT 1
    `,
    [onsCode],
  );
  return rows[0] || null;
}

function runCommand(command, args, timeoutMs) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd: process.cwd(),
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("close", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal, stdout, stderr, timedOut });
    });
  });
}

async function tryReadJson(filepath) {
  try {
    const raw = await fs.readFile(filepath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function ensureSafeRepoRelativePath(inputPath, kind) {
  if (!inputPath || !String(inputPath).trim()) {
    throw new Error(`Missing ${kind} path in lpa_scrape_configs.`);
  }

  const rel = String(inputPath).trim();
  if (path.isAbsolute(rel)) {
    throw new Error(`${kind} path must be repo-relative: ${rel}`);
  }

  const normalized = path.posix.normalize(rel.replace(/\\/g, "/"));
  if (
    normalized.startsWith("../") ||
    normalized === ".." ||
    normalized.includes("/../") ||
    normalized === "."
  ) {
    throw new Error(`${kind} path escapes repo root: ${rel}`);
  }

  const resolved = path.resolve(PROJECT_ROOT, normalized);
  const relFromRoot = path.relative(PROJECT_ROOT, resolved);
  if (relFromRoot.startsWith("..") || path.isAbsolute(relFromRoot)) {
    throw new Error(`${kind} path escapes repo root after resolve: ${rel}`);
  }

  return { normalized, resolved };
}

function resolveScraperEntrypoint(rawPath) {
  const { normalized, resolved } = ensureSafeRepoRelativePath(rawPath, "scraper_entrypoint");
  if (!ALLOWED_SCRAPER_ENTRYPOINTS.has(normalized)) {
    throw new Error(
      `scraper_entrypoint not allowlisted: ${normalized}. Allowed: ${Array.from(ALLOWED_SCRAPER_ENTRYPOINTS).join(", ")}`,
    );
  }

  if (!fsSync.existsSync(resolved)) {
    throw new Error(`scraper_entrypoint file not found: ${normalized}`);
  }

  return { normalized, resolved };
}

function resolveMapperPath(rawPath) {
  const { normalized, resolved } = ensureSafeRepoRelativePath(rawPath, "mapper_path");
  if (!normalized.startsWith("mappers/") || !normalized.endsWith(".cjs")) {
    throw new Error(`mapper_path must be under mappers/*.cjs: ${normalized}`);
  }

  if (!fsSync.existsSync(resolved)) {
    throw new Error(`mapper_path file not found: ${normalized}`);
  }

  return { normalized, resolved };
}

async function executeScrape(job, cfg) {
  const scraper = resolveScraperEntrypoint(cfg.scraper_entrypoint);
  const mapper = resolveMapperPath(cfg.mapper_path);

  const args = [
    scraper.resolved,
    "--ref",
    job.application_ref,
    "--start-url",
    cfg.site_url,
    "--mapper",
    mapper.resolved,
    "--ons-code",
    job.ons_code,
  ];

  const startedAt = new Date().toISOString();
  const proc = await runCommand(process.execPath, args, SCRAPER_TIMEOUT_MS);
  const finishedAt = new Date().toISOString();

  if (proc.timedOut) {
    throw new Error(`Scraper timed out after ${SCRAPER_TIMEOUT_MS}ms`);
  }

  if (proc.code !== 0) {
    throw new Error(
      `Scraper exited with code ${proc.code}.\nstdout:\n${tail(proc.stdout)}\nstderr:\n${tail(proc.stderr)}`,
    );
  }

  const unifiedMatch = proc.stdout.match(/Unified JSON:\s+([^\s]+_UNIFIED\.json)/);
  const planitMatch = proc.stdout.match(/PlanIt-mapped JSON:\s+([^\s]+_PLANIT\.json)/);

  const unifiedFile = unifiedMatch ? path.resolve(process.cwd(), unifiedMatch[1]) : null;
  const planitFile = planitMatch ? path.resolve(process.cwd(), planitMatch[1]) : null;

  const unified = unifiedFile ? await tryReadJson(unifiedFile) : null;
  const planit = planitFile ? await tryReadJson(planitFile) : null;

  return {
    started_at: startedAt,
    finished_at: finishedAt,
    scraper_entrypoint: scraper.normalized,
    mapper_path: mapper.normalized,
    site_url: cfg.site_url,
    scraper_cmd: `${process.execPath} ${args.join(" ")}`,
    artifacts: {
      unified_file: unifiedFile,
      planit_file: planitFile,
    },
    unified,
    planit,
    stdout_tail: tail(proc.stdout),
    stderr_tail: tail(proc.stderr),
  };
}

async function markSuccess(job, result) {
  await client.query(
    `
      UPDATE scrape_jobs
      SET status = 'completed',
          result = $2::jsonb,
          error = NULL,
          logs = $3,
          mapper = $4,
          locked_at = NULL,
          locked_by = NULL
      WHERE id = $1
    `,
    [job.id, JSON.stringify(result), result.stdout_tail, result.mapper_path],
  );
}

async function markFailure(job, err) {
  const maxAttempts = Number(job.max_attempts || 1);
  const attempts = Number(job.attempts || 0);
  const retry = attempts < maxAttempts;
  const nextStatus = retry ? "queued" : "failed";

  const errorMsg = err instanceof Error ? err.message : String(err);
  const logs = tail(errorMsg, 8000);

  await client.query(
    `
      UPDATE scrape_jobs
      SET status = $2,
          error = $3,
          logs = $4,
          locked_at = NULL,
          locked_by = NULL
      WHERE id = $1
    `,
    [job.id, nextStatus, tail(errorMsg, 2000), logs],
  );
}

async function processOneJob(job) {
  if (!job.ons_code || !job.application_ref) {
    throw new Error("Missing ons_code or application_ref on scrape_jobs row.");
  }

  const cfg = await getConfigForOns(job.ons_code);
  if (!cfg) {
    throw new Error(`No enabled lpa_scrape_configs row for ons_code ${job.ons_code}.`);
  }

  const result = await executeScrape(job, cfg);
  await markSuccess(job, result);
}

async function drainQueue() {
  while (true) {
    const job = await claimNextQueuedJob();
    if (!job) return;

    console.log(`[worker] claimed job id=${job.id} ons=${job.ons_code} ref=${job.application_ref}`);
    try {
      await processOneJob(job);
      console.log(`[worker] completed job id=${job.id}`);
    } catch (err) {
      await markFailure(job, err);
      console.error(`[worker] failed job id=${job.id}:`, err);
    }
  }
}

async function scheduleDrain() {
  if (draining) {
    drainRequested = true;
    return;
  }

  draining = true;
  try {
    do {
      drainRequested = false;
      await drainQueue();
    } while (drainRequested);
  } finally {
    draining = false;
  }
}

async function connectAndListen() {
  const clientConfig = process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: process.env.PGHOST,
        port: process.env.PGPORT ? Number(process.env.PGPORT) : undefined,
        database: process.env.PGDATABASE,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
      };

  client = new Client(clientConfig);

  client.on("error", (err) => {
    console.error("[pg] client error:", err);
  });

  client.on("end", () => {
    console.error("[pg] connection ended; reconnecting in 2s...");
    setTimeout(connectAndListen, 2000);
  });

  await client.connect();
  console.log("[pg] connected");

  // Important: if connection drops, LISTEN is lost; hence reconnect logic above.
  await client.query(`LISTEN ${CHANNEL}`);
  console.log(`[pg] LISTEN ${CHANNEL}`);
  scheduleDrain().catch((err) => {
    console.error("[worker] initial drain failed:", err);
  });

  client.on("notification", (msg) => {
    if (msg.channel !== CHANNEL) return;
    console.log("[pg] notification:", msg.payload);
    scheduleDrain().catch((err) => {
      console.error("[worker] drain failed:", err);
    });
  });
}

connectAndListen().catch((e) => {
  console.error("[worker] fatal connect error:", e);
  process.exit(1);
});
