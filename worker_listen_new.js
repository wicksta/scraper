// worker_listen.js
import "./bootstrap.js";
import { spawn } from "node:child_process";
import fsSync from "node:fs";
import { createRequire } from "node:module";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import pg from "pg";
import wfsModule from "./wcc_wfs_geometry.cjs";
const { Client } = pg;
const { fetchGeometryByKeyVal } = wfsModule;
const require = createRequire(import.meta.url);
const { extractUkPostcode, resolvePostcodeViaOnspd } = require("./library.cjs");

const CHANNEL = "scrape_job_created";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}:${process.pid}`;
// In production this worker is usually started by systemd unit "idox-worker",
// which loads /etc/default/idox-worker via EnvironmentFile.
// bootstrap.js only fills unset vars from .env, so systemd-provided values take precedence.
const SCRAPER_TIMEOUT_MS = Number(process.env.SCRAPER_TIMEOUT_MS || 15 * 60 * 1000);
const WORKER_JOB_DELAY_MS = Number(process.env.WORKER_JOB_DELAY_MS || 0);
const WORKER_JOB_JITTER_MS = Number(process.env.WORKER_JOB_JITTER_MS || 0);
const WORKER_POLL_INTERVAL_MS = Number(process.env.WORKER_POLL_INTERVAL_MS || 60_000);
const PREFLIGHT_CONNECT_TIMEOUT_MS = Number(process.env.PREFLIGHT_CONNECT_TIMEOUT_MS || 5_000);
const RECOVERY_STAGE1_SUCCESSES = Number(process.env.RECOVERY_STAGE1_SUCCESSES || 2);
const RECOVERY_STAGE2_SUCCESSES = Number(process.env.RECOVERY_STAGE2_SUCCESSES || 5);
const RECOVERY_STAGE3_SUCCESSES = Number(process.env.RECOVERY_STAGE3_SUCCESSES || 10);
const RECOVERY_STAGE1_DELAY_MS = Number(process.env.RECOVERY_STAGE1_DELAY_MS || 15 * 60 * 1000);
const RECOVERY_STAGE2_DELAY_MS = Number(process.env.RECOVERY_STAGE2_DELAY_MS || 5 * 60 * 1000);
const RECOVERY_STAGE3_DELAY_MS = Number(process.env.RECOVERY_STAGE3_DELAY_MS || 60 * 1000);
const DAILY_PAUSE_ENABLED = !/^(0|false|no)$/i.test(String(process.env.DAILY_PAUSE_ENABLED || "1"));
const DAILY_PAUSE_INTERVAL_MS = Number(process.env.DAILY_PAUSE_INTERVAL_MS || 24 * 60 * 60 * 1000);
const DAILY_PAUSE_BASE_MS = Number(process.env.DAILY_PAUSE_BASE_MS || 60 * 60 * 1000);
const DAILY_PAUSE_JITTER_MS = Number(process.env.DAILY_PAUSE_JITTER_MS || 15 * 60 * 1000);
const PROJECT_ROOT = process.cwd();
const ALLOWED_SCRAPER_ENTRYPOINTS = new Set([
  "scraper.cjs",
  "scraper_camden_socrata.cjs",
]);
const PREFLIGHT_ERROR_PREFIX = "PREFLIGHT_CONNECTIVITY:";
const SCRAPER_PROXY_ENV = resolveScraperProxyEnv(process.env);
const WESTMINSTER_ONS_CODE = "E09000033";

let client;
let draining = false;
let drainRequested = false;
let recoveryMode = false;
let recoverySuccessCount = 0;
let lastDailyPauseAtMs = Date.now();

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function politeDelay() {
  const base = Number.isFinite(WORKER_JOB_DELAY_MS) ? WORKER_JOB_DELAY_MS : 0;
  const jitter = Number.isFinite(WORKER_JOB_JITTER_MS) ? WORKER_JOB_JITTER_MS : 0;
  const extra = jitter > 0 ? Math.floor(Math.random() * (jitter + 1)) : 0;
  const ms = Math.max(0, base + extra);
  if (ms > 0) await sleep(ms);
}

function tail(value, maxLen = 4000) {
  if (!value) return "";
  return value.length > maxLen ? value.slice(-maxLen) : value;
}

function markRecoveryFailure() {
  recoveryMode = true;
  recoverySuccessCount = 0;
}

function markRecoverySuccess() {
  if (!recoveryMode) return;
  recoverySuccessCount += 1;
  if (recoverySuccessCount >= RECOVERY_STAGE3_SUCCESSES) {
    recoveryMode = false;
    recoverySuccessCount = 0;
    console.log("[worker] recovery mode cleared; returning to baseline pacing.");
  }
}

function getRecoveryDelayMs() {
  if (!recoveryMode) return 0;
  if (recoverySuccessCount < RECOVERY_STAGE1_SUCCESSES) return Math.max(0, RECOVERY_STAGE1_DELAY_MS);
  if (recoverySuccessCount < RECOVERY_STAGE2_SUCCESSES) return Math.max(0, RECOVERY_STAGE2_DELAY_MS);
  if (recoverySuccessCount < RECOVERY_STAGE3_SUCCESSES) return Math.max(0, RECOVERY_STAGE3_DELAY_MS);
  return 0;
}

function shouldTakeDailyPause(nowMs) {
  if (!DAILY_PAUSE_ENABLED) return false;
  const interval = Math.max(60_000, DAILY_PAUSE_INTERVAL_MS);
  return nowMs - lastDailyPauseAtMs >= interval;
}

function computeDailyPauseMs() {
  const base = Math.max(60_000, DAILY_PAUSE_BASE_MS);
  const jitter = Math.max(0, DAILY_PAUSE_JITTER_MS);
  const delta = jitter > 0 ? Math.floor(Math.random() * (2 * jitter + 1)) - jitter : 0;
  return Math.max(60_000, base + delta);
}

async function maybeTakeDailyPause() {
  const now = Date.now();
  if (!shouldTakeDailyPause(now)) return;

  const pauseMs = computeDailyPauseMs();
  const minutes = Math.round(pauseMs / 60000);
  console.log(`[worker] scheduled cooldown pause starting now; duration=${minutes}m`);
  await sleep(pauseMs);
  lastDailyPauseAtMs = Date.now();

  // After a long pause, re-ramp traffic rather than jumping to normal throughput.
  recoveryMode = true;
  recoverySuccessCount = 0;
  console.log("[worker] cooldown pause ended; entering recovery pacing ramp.");
}

function computeConnectivityBackoffMs(attempts) {
  const n = Number(attempts || 0);
  if (n <= 1) return 60 * 60 * 1000; // 1 hour
  if (n === 2) return 6 * 60 * 60 * 1000; // 6 hours
  return 12 * 60 * 60 * 1000; // 12 hours thereafter
}

function isConnectivityPreflightError(err) {
  if (!err) return false;
  if (err.code === "PREFLIGHT_CONNECTIVITY") return true;
  const msg = err instanceof Error ? err.message : String(err);
  return msg.startsWith(PREFLIGHT_ERROR_PREFIX);
}

async function claimNextQueuedJob() {
  const sql = `
    WITH candidate AS (
      SELECT id
      FROM scrape_jobs
      WHERE status = 'queued'
        AND ons_code IS NOT NULL
        AND application_ref IS NOT NULL
        AND (
          error IS NULL
          OR left(error, length($2)) <> $2
          OR updated_at <= now() - (
            CASE
              WHEN attempts <= 1 THEN interval '1 hour'
              WHEN attempts = 2 THEN interval '6 hours'
              ELSE interval '12 hours'
            END
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM scrape_jobs block
          WHERE block.ons_code = scrape_jobs.ons_code
            AND block.error IS NOT NULL
            AND left(block.error, length($2)) = $2
            AND block.updated_at > now() - (
              CASE
                WHEN block.attempts <= 1 THEN interval '1 hour'
                WHEN block.attempts = 2 THEN interval '6 hours'
                ELSE interval '12 hours'
              END
            )
        )
      ORDER BY
        (job_type = 'live_scrape_request') DESC,
        created_at ASC,
        id ASC
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
  const { rows } = await client.query(sql, [WORKER_ID, PREFLIGHT_ERROR_PREFIX]);
  return rows[0] || null;
}

function tcpConnectProbe(host, port, timeoutMs) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port });
    let settled = false;

    const done = (result) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(result);
    };

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => done({ ok: true }));
    socket.once("timeout", () => done({ ok: false, reason: "timeout" }));
    socket.once("error", (err) => done({ ok: false, reason: err?.code || err?.message || "error" }));
  });
}

function parseProxyServer(raw) {
  if (!raw || !String(raw).trim()) return null;
  const trimmed = String(raw).trim();
  try {
    const withScheme = /^[a-z]+:\/\//i.test(trimmed) ? trimmed : `http://${trimmed}`;
    const u = new URL(withScheme);
    const port = Number(u.port || (u.protocol === "https:" ? 443 : 80));
    if (!u.hostname || !Number.isFinite(port) || port <= 0) return null;
    return { host: u.hostname, port };
  } catch {
    return null;
  }
}

function resolveScraperProxyEnv(env) {
  const out = {};
  const rawUrl = env.SCRAPER_PROXY_URL || "";
  const rawServer = env.SCRAPER_PROXY_SERVER || "";
  const rawUser = env.SCRAPER_PROXY_USERNAME || "";
  const rawPass = env.SCRAPER_PROXY_PASSWORD || "";

  let server = "";
  let username = "";
  let password = "";

  if (rawUrl) {
    try {
      const u = new URL(rawUrl);
      server = `${u.protocol}//${u.host}`;
      username = decodeURIComponent(u.username || "");
      password = decodeURIComponent(u.password || "");
    } catch {
      // If parse fails, fallback to SCRAPER_PROXY_SERVER/USERNAME/PASSWORD.
    }
  }

  if (!server && rawServer) server = rawServer;
  if (!username && rawUser) username = rawUser;
  if (!password && rawPass) password = rawPass;

  if (server) out.SCRAPER_PROXY_SERVER = server;
  if (username) out.SCRAPER_PROXY_USERNAME = username;
  if (password) out.SCRAPER_PROXY_PASSWORD = password;
  return out;
}

async function runConnectivityPreflight(job, cfg) {
  let host = "";
  let port = 443;
  let targetLabel = "";

  const parsedProxy = parseProxyServer(SCRAPER_PROXY_ENV.SCRAPER_PROXY_SERVER);
  if (parsedProxy) {
    host = parsedProxy.host;
    port = parsedProxy.port;
    targetLabel = `proxy=${SCRAPER_PROXY_ENV.SCRAPER_PROXY_SERVER}`;
  } else {
    try {
      const u = new URL(cfg.site_url);
      host = u.hostname;
      targetLabel = `site=${cfg.site_url}`;
    } catch {
      const err = new Error(`${PREFLIGHT_ERROR_PREFIX} invalid_site_url=${cfg.site_url}`);
      err.code = "PREFLIGHT_CONNECTIVITY";
      throw err;
    }
  }

  const probe = await tcpConnectProbe(host, port, PREFLIGHT_CONNECT_TIMEOUT_MS);
  if (probe.ok) {
    markRecoverySuccess();
    return;
  }

  const err = new Error(
    `${PREFLIGHT_ERROR_PREFIX} host=${host} port=${port} reason=${probe.reason} timeout_ms=${PREFLIGHT_CONNECT_TIMEOUT_MS} ons=${job.ons_code} ref=${job.application_ref} ${targetLabel}`,
  );
  err.code = "PREFLIGHT_CONNECTIVITY";
  throw err;
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

function runCommand(command, args, timeoutMs, envOverrides = {}) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd: process.cwd(),
      env: { ...process.env, ...envOverrides },
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

function parseEmittedJson(stdout, marker) {
  // Marker format: "__UNIFIED_JSON__=<json>" or "__PLANIT_JSON__=<json>"
  const re = new RegExp(`^${marker}=(.+)$`, "m");
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

function isWestminsterJob(job, cfg) {
  return (
    String(job?.ons_code || "").trim().toUpperCase() === WESTMINSTER_ONS_CODE &&
    String(cfg?.mapper_path || "").trim() === "mappers/westminster.cjs"
  );
}

function extractKeyValFromUnified(unified) {
  const direct = String(unified?.keyVal || "").trim();
  if (direct) return direct;

  const summaryUrl = String(unified?.tabs?.summary?.url || "").trim();
  if (!summaryUrl) return "";
  try {
    const u = new URL(summaryUrl);
    return String(u.searchParams.get("keyVal") || "").trim();
  } catch {
    const m = summaryUrl.match(/[?&]keyVal=([A-Za-z0-9]+)/);
    return m ? String(m[1] || "").trim() : "";
  }
}

function getPlanitInner(result) {
  const outer = result?.planit && typeof result.planit === "object" ? result.planit : null;
  const inner = outer?.planit && typeof outer.planit === "object" ? outer.planit : null;
  return inner;
}

function withPlanitInner(result, nextInner) {
  const nextPlanit = result?.planit && typeof result.planit === "object" ? { ...result.planit } : {};
  nextPlanit.planit = nextInner;
  return { ...result, planit: nextPlanit };
}

async function enrichWithPostcodeFallback(result, trigger) {
  const inner = getPlanitInner(result);
  if (!inner) {
    return {
      ...result,
      postcode_lookup: { ok: false, skipped: true, reason: "missing_planit_payload", trigger },
    };
  }

  if (inner.lat != null && inner.lng != null) return result;

  const postcode = extractUkPostcode(inner.postcode || inner.address || "");
  if (!postcode) {
    return {
      ...result,
      postcode_lookup: { ok: false, skipped: true, reason: "missing_postcode", trigger },
    };
  }

  try {
    const geo = await resolvePostcodeViaOnspd(postcode, {
      timeoutMs: Number(process.env.POSTCODE_LOOKUP_TIMEOUT_MS || 6000),
    });
    if (!geo?.success) {
      return {
        ...result,
        postcode_lookup: {
          ok: false,
          postcode,
          trigger,
          error: String(geo?.error || "lookup_failed"),
        },
      };
    }

    const nextInner = { ...inner };
    if (nextInner.lat == null) nextInner.lat = geo.lat;
    if (nextInner.lng == null) nextInner.lng = geo.long;
    if (nextInner.location_x == null && nextInner.location_y == null && nextInner.lat != null && nextInner.lng != null) {
      nextInner.location_x = nextInner.lat;
      nextInner.location_y = nextInner.lng;
    }

    return {
      ...withPlanitInner(result, nextInner),
      postcode_lookup: {
        ok: true,
        postcode,
        trigger,
        lat: geo.lat,
        lon: geo.long,
        lad25cd: geo.lad25cd ?? null,
        terminated: geo.terminated ?? null,
      },
    };
  } catch (err) {
    return {
      ...result,
      postcode_lookup: {
        ok: false,
        postcode,
        trigger,
        error: err instanceof Error ? err.message : String(err),
      },
    };
  }
}

async function transformPointToWgs84(easting, northing, srid) {
  const { rows } = await client.query(
    `
      SELECT
        ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), $3::int), 4326)) AS lat,
        ST_X(ST_Transform(ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), $3::int), 4326)) AS lon
    `,
    [easting, northing, srid],
  );
  const lat = Number(rows?.[0]?.lat);
  const lon = Number(rows?.[0]?.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { lat, lon };
}

async function enrichWithWestminsterGeometry(job, cfg, result) {
  if (!isWestminsterJob(job, cfg)) return result;

  const unified = result?.unified || null;
  const keyVal = extractKeyValFromUnified(unified);
  if (!keyVal) {
    const next = {
      ...result,
      wfs_lookup: { ok: false, skipped: true, reason: "missing_keyval" },
    };
    return await enrichWithPostcodeFallback(next, "missing_keyval");
  }

  try {
    const geom = await fetchGeometryByKeyVal(keyVal);
    const enriched = {
      ...result,
      wfs_lookup: {
        ok: true,
        keyval: keyVal,
        geometry: geom,
      },
    };

    const centroidE = Number(geom?.centroid?.e);
    const centroidN = Number(geom?.centroid?.n);
    const srid = Number(geom?.srid || 27700);
    const hasCentroid = Number.isFinite(centroidE) && Number.isFinite(centroidN);

    if (!hasCentroid) return await enrichWithPostcodeFallback(enriched, "wfs_no_centroid");

    const coords = await transformPointToWgs84(centroidE, centroidN, srid);
    if (!coords) return await enrichWithPostcodeFallback(enriched, "wfs_transform_failed");

    const inner = getPlanitInner(enriched);
    const nextInner = inner && typeof inner === "object" ? { ...inner } : {};

    // Preserve mapped coordinates when already present.
    if (nextInner.lat == null) nextInner.lat = coords.lat;
    if (nextInner.lng == null) nextInner.lng = coords.lon;
    if (
      nextInner.location_x == null &&
      nextInner.location_y == null &&
      nextInner.lat != null &&
      nextInner.lng != null
    ) {
      nextInner.location_x = nextInner.lat;
      nextInner.location_y = nextInner.lng;
    }
    if (nextInner.easting == null) nextInner.easting = centroidE;
    if (nextInner.northing == null) nextInner.northing = centroidN;

    return withPlanitInner(enriched, nextInner);
  } catch (err) {
    const next = {
      ...result,
      wfs_lookup: {
        ok: false,
        keyval: keyVal,
        error: err instanceof Error ? err.message : String(err),
      },
    };
    return await enrichWithPostcodeFallback(next, "wfs_error");
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
  await runConnectivityPreflight(job, cfg);

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
    "--emit-json",
  ];

  const startedAt = new Date().toISOString();
  const proc = await runCommand(process.execPath, args, SCRAPER_TIMEOUT_MS, SCRAPER_PROXY_ENV);
  const finishedAt = new Date().toISOString();

  if (proc.timedOut) {
    throw new Error(`Scraper timed out after ${SCRAPER_TIMEOUT_MS}ms`);
  }

  if (proc.code !== 0) {
    throw new Error(
      `Scraper exited with code ${proc.code}.\nstdout:\n${tail(proc.stdout)}\nstderr:\n${tail(proc.stderr)}`,
    );
  }

  const unified = parseEmittedJson(proc.stdout, "__UNIFIED_JSON__");
  const planit = parseEmittedJson(proc.stdout, "__PLANIT_JSON__");
  const baseResult = {
    started_at: startedAt,
    finished_at: finishedAt,
    scraper_entrypoint: scraper.normalized,
    mapper_path: mapper.normalized,
    site_url: cfg.site_url,
    scraper_cmd: `${process.execPath} ${args.join(" ")}`,
    artifacts: {
      unified_file: null,
      planit_file: null,
    },
    unified,
    planit,
    stdout_tail: tail(proc.stdout),
    stderr_tail: tail(proc.stderr),
  };

  return await enrichWithWestminsterGeometry(job, cfg, baseResult);
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
          updated_at = now(),
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
  const isPreflight = isConnectivityPreflightError(err);
  const retry = isPreflight || attempts < maxAttempts;
  const nextStatus = retry ? "queued" : "failed";

  const errorMsg = err instanceof Error ? err.message : String(err);
  const backoffMs = isPreflight ? computeConnectivityBackoffMs(attempts) : 0;
  const logs = isPreflight
    ? tail(`${errorMsg}\nnext_retry_after_ms=${backoffMs}`, 8000)
    : tail(errorMsg, 8000);

  await client.query(
    `
      UPDATE scrape_jobs
      SET status = $2,
          error = $3,
          logs = $4,
          updated_at = now(),
          locked_at = NULL,
          locked_by = NULL
      WHERE id = $1
    `,
    [job.id, nextStatus, tail(errorMsg, 2000), logs],
  );

  if (isPreflight) {
    markRecoveryFailure();
    const delayMinutes = Math.round(backoffMs / 60000);
    console.warn(
      `[worker] preflight connectivity failure id=${job.id} attempts=${attempts} next_retry_in=${delayMinutes}m`,
    );
  }
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
  await maybeTakeDailyPause();
  let job = await claimNextQueuedJob();
  if (!job) return;
  while (job) {
    console.log(`[worker] claimed job id=${job.id} ons=${job.ons_code} ref=${job.application_ref}`);
    try {
      await processOneJob(job);
      console.log(`[worker] completed job id=${job.id}`);
    } catch (err) {
      await markFailure(job, err);
      console.error(`[worker] failed job id=${job.id}:`, err);
    }

    await maybeTakeDailyPause();

    const nextJob = await claimNextQueuedJob();
    if (!nextJob) return;

    const recoveryDelay = getRecoveryDelayMs();
    if (recoveryDelay > 0) {
      const delaySeconds = Math.round(recoveryDelay / 1000);
      console.log(
        `[worker] recovery pacing delay ${delaySeconds}s before job id=${nextJob.id} successes=${recoverySuccessCount}`,
      );
      await sleep(recoveryDelay);
    }

    // Politeness throttle: avoid hammering the upstream site for background jobs.
    if (nextJob.job_type !== "live_scrape_request") {
      console.log(
        `[worker] delaying before background job id=${nextJob.id} type=${nextJob.job_type ?? "unknown"}`,
      );
      await politeDelay();
    }

    job = nextJob;
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

  // Periodic polling is needed for backoff-delayed queued jobs.
  setInterval(() => {
    scheduleDrain().catch((err) => {
      console.error("[worker] periodic drain failed:", err);
    });
  }, WORKER_POLL_INTERVAL_MS).unref();

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
