#!/usr/bin/env node
import '../bootstrap.js';
import { loadDotEnv } from '../bootstrap.js';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { parseArgs } from 'node:util';
import mysql from 'mysql2/promise';
import {
  ANNUAL_FILE, RANKING_FILE, METADATA_FILE, hash, json, buildDatasets,
  coverage, readOptionalJson, summarizeChanges, publishBundle, recoverPublication,
} from './appeals.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.dirname(HERE);
const PAGE = 'https://www.gov.uk/government/statistical-data-sets/live-tables-on-planning-application-statistics';
const DEFAULT_OUTPUT = '/mnt/ngist/public_html/dashboard/data';
loadDotEnv(path.join(ROOT, '.env'));

function help() {
  console.log(`Usage: node planning_data_import_tools/import_planning_quality.js [options]

  --source URL_OR_FILE   Explicit HTTPS ODS URL or local workbook (default: GOV.UK discovery)
  --output-dir DIR      JSON destination (default: ${DEFAULT_OUTPUT})
  --lpa-json FILE       Offline lookup array [{ons_code,lpa_name}] (default: MySQL lpa_codes)
  --dry-run             Validate and report changes; do not write any files
  --help                Show this help

Live publication requires the nGISt SSH mount. Backups and downloaded workbooks
are kept privately under planning_data_import_tools/.state/. No scheduling is installed.`);
}

function runPython(bytes, discover = false) {
  const result = spawnSync('python3', [path.join(HERE, 'read_ods.py'), ...(discover ? ['--discover'] : [])], {
    input: bytes, maxBuffer: 20 * 1024 * 1024, timeout: 120000,
  });
  if (result.error) throw new Error(`ODS helper failed: ${result.error.message}`);
  if (result.status !== 0) throw new Error(`ODS helper failed: ${result.stderr.toString().trim()}`);
  return JSON.parse(result.stdout.toString());
}

async function download(url) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      let next = url;
      for (let redirects = 0; redirects <= 5; redirects++) {
        if (new URL(next).protocol !== 'https:') throw new Error('Only HTTPS download URLs are supported');
        const response = await fetch(next, { redirect: 'manual', signal: AbortSignal.timeout(60000) });
        if ([301, 302, 303, 307, 308].includes(response.status)) {
          await response.body?.cancel();
          const location = response.headers.get('location');
          if (!location) throw new Error('Download redirect has no Location header');
          next = new URL(location, next).href;
          continue;
        }
        if (!response.ok) { await response.body?.cancel(); throw new Error(`Download HTTP ${response.status}`); }
        const chunks = [];
        let size = 0;
        for await (const chunk of response.body) {
          size += chunk.length;
          if (size > 20 * 1024 * 1024) throw new Error('Download exceeds 20 MiB');
          chunks.push(chunk);
        }
        return { bytes: Buffer.concat(chunks), url: next };
      }
      throw new Error('Too many download redirects');
    } catch (error) {
      if (attempt === 2) throw new Error(`Download failed: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
    }
  }
}

async function lookupLpas(file) {
  if (file) {
    const data = readOptionalJson(path.resolve(file));
    if (!Array.isArray(data)) throw new Error('--lpa-json must be an array of {ons_code,lpa_name}');
    return { rows: data, source: 'json' };
  }
  const env = process.env;
  const config = {
    host: env.MYSQL_HOST,
    port: Number(env.MYSQL_PORT || 3306),
    database: env.MYSQL_DATABASE || env.MYSQL_DB,
    user: env.MYSQL_USER,
    password: env.MYSQL_PASSWORD || env.MYSQL_PASS,
    connectTimeout: 15000,
  };
  if (!config.host || !config.database || !config.user || !config.password) {
    throw new Error('Set MYSQL_HOST, MYSQL_DATABASE (or MYSQL_DB), MYSQL_USER and MYSQL_PASSWORD (or MYSQL_PASS) in /opt/scraper/.env, or use --lpa-json');
  }
  let connection;
  try {
    connection = await mysql.createConnection(config);
    const [rows] = await connection.execute({ sql: "SELECT ons_code, lpa_name FROM lpa_codes WHERE ons_code LIKE 'E%' ORDER BY ons_code", timeout: 15000 });
    return { rows, source: 'mysql:lpa_codes' };
  } catch (error) {
    // MySQL error messages can include host/user details; report only the code.
    throw new Error(`LPA lookup failed (${error.code || 'database error'})`);
  } finally { if (connection) await connection.end(); }
}

function requireNgistMount(outputDir) {
  // Resolve existing ancestor directories, so symlinks into the mount are checked too.
  let ancestor = outputDir;
  while (!fs.existsSync(ancestor)) ancestor = path.dirname(ancestor);
  const resolved = fs.realpathSync(ancestor);
  const lexical = outputDir.startsWith('/mnt/ngist/') || outputDir === '/mnt/ngist'
    || outputDir.startsWith(path.join(ROOT, 'ngist') + '/');
  if (!lexical && resolved !== '/mnt/ngist' && !resolved.startsWith('/mnt/ngist/')) return;
  const check = spawnSync('findmnt', ['-T', '/mnt/ngist', '-n', '-o', 'TARGET,FSTYPE'], { encoding: 'utf8', timeout: 10000 });
  if (check.status !== 0 || !/^\/mnt\/ngist\s+fuse\.sshfs\s*$/m.test(check.stdout)) {
    throw new Error('nGISt SSH mount is unavailable; use a local --output-dir and publish in a mounted terminal session');
  }
}

function canonicalOutput(outputDir) {
  let ancestor = outputDir;
  while (!fs.existsSync(ancestor)) ancestor = path.dirname(ancestor);
  return path.resolve(fs.realpathSync(ancestor), path.relative(ancestor, outputDir));
}

function acquireLock(stateDir) {
  fs.mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  const lock = path.join(stateDir, 'run.lock');
  try {
    fs.writeFileSync(lock, json({ pid: process.pid }), { flag: 'wx', mode: 0o600 });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
    const previous = readOptionalJson(lock);
    if (!Number.isInteger(previous?.pid) || previous.pid <= 0) throw new Error(`Invalid lock; inspect ${lock}`);
    try { process.kill(previous.pid, 0); throw new Error(`Importer already running (PID ${previous.pid})`); }
    catch (probeError) {
      if (probeError.code !== 'ESRCH') throw probeError;
      fs.unlinkSync(lock);
      return acquireLock(stateDir);
    }
  }
  return () => fs.unlinkSync(lock);
}

async function main() {
  const { values, positionals } = parseArgs({ options: {
    source: { type: 'string' }, 'output-dir': { type: 'string' }, 'lpa-json': { type: 'string' },
    'dry-run': { type: 'boolean', default: false }, help: { type: 'boolean', default: false },
  }, allowPositionals: true });
  if (values.help) { help(); return; }
  if (positionals.length) throw new Error('Unexpected positional arguments; use --source FILE_OR_URL');
  const requestedOutput = path.resolve(values['output-dir'] || DEFAULT_OUTPUT);
  requireNgistMount(requestedOutput);
  const outputDir = canonicalOutput(requestedOutput);
  const stateDir = path.join(HERE, '.state', hash(outputDir).slice(0, 16));
  const backupRoot = path.join(stateDir, 'backups');
  let release;
  if (!values['dry-run']) {
    release = acquireLock(stateDir);
    try { recoverPublication(outputDir, backupRoot); }
    catch (error) { release(); throw error; }
  } else if (fs.existsSync(path.join(backupRoot, 'pending.json'))) {
    throw new Error('An interrupted publication needs recovery; run without --dry-run first');
  }
  try {
    let source = values.source;
    if (!source) source = runPython((await download(PAGE)).bytes, true).url;
    const workbook = /^https?:\/\//i.test(source)
      ? await download(source)
      : { bytes: fs.readFileSync(path.resolve(source)), url: null };
    const parsed = runPython(workbook.bytes);
    if (parsed.quarters[0] > '2020 Q1') throw new Error('Workbook must cover the ranking start quarter, 2020 Q1');
    const lpas = await lookupLpas(values['lpa-json']);
    const { annual, rankings } = buildDatasets(parsed, lpas.rows);
    const period = coverage(parsed);
    const previous = readOptionalJson(path.join(outputDir, METADATA_FILE));
    const oldAnnual = readOptionalJson(path.join(outputDir, ANNUAL_FILE));
    const oldRankings = readOptionalJson(path.join(outputDir, RANKING_FILE));
    if (previous?.coverage && (period.last_quarter < previous.coverage.last_quarter || period.first_quarter > previous.coverage.first_quarter)) {
      throw new Error('Source coverage regresses relative to the published metadata');
    }
    if (oldAnnual?.length) {
      const years = oldAnnual.map(row => row.Year).sort();
      if (period.first_quarter.slice(0, 4) > years[0] || period.last_quarter.slice(0, 4) < years.at(-1)) throw new Error('Source coverage regresses relative to the existing annual JSON');
    }
    const annualBytes = json(annual);
    const rankingBytes = json(rankings);
    const datasetHash = hash(annualBytes + rankingBytes);
    const sourceHash = hash(workbook.bytes);
    const lookupHash = hash(json([...lpas.rows].sort((a, b) => a.ons_code.localeCompare(b.ons_code))));
    const changed = !fs.existsSync(path.join(outputDir, ANNUAL_FILE)) || !fs.existsSync(path.join(outputDir, RANKING_FILE))
      || hash(fs.readFileSync(path.join(outputDir, ANNUAL_FILE))) !== hash(annualBytes)
      || hash(fs.readFileSync(path.join(outputDir, RANKING_FILE))) !== hash(rankingBytes);
    const now = new Date().toISOString();
    const generation = `${now.replace(/[^0-9]/g, '')}-${cryptoSuffix(datasetHash)}`;
    const metadata = {
      schema_version: 1, generation_id: generation, source_page: PAGE,
      source_url: workbook.url, source_file: workbook.url ? null : path.resolve(source),
      source_sha256: sourceHash, lookup_source: lpas.source, lookup_sha256: lookupHash,
      dataset_sha256: datasetHash, last_successful_check: now,
      last_data_update: changed ? now : (previous?.last_data_update ?? now),
      coverage: period, annual_records: annual.length,
      ranking_period: { first_quarter: '2020 Q1', last_quarter: period.last_quarter },
      ranking_population: { count: rankings.length, rule: 'LPA lookup intersected with authorities present in the latest quarter of both sheets' },
      methodology: {
        year_basis: 'Original application-decision quarter or non-determination appeal receipt quarter; not PINS decision year',
        annual_per_100_denominator: 'application_decisions', ranking_per_100_denominator: 'total_decisions_and_nondetermined',
        ranking_ties: 'Descending ordinal ranks; ties ordered by ONS code; undefined rates unranked',
      },
      files: { [ANNUAL_FILE]: hash(annualBytes), [RANKING_FILE]: hash(rankingBytes) },
    };
    console.log(json({
      dry_run: values['dry-run'], output_dir: outputDir,
      coverage: `${period.first_quarter}–${period.last_quarter}`, partial_years: period.partial_years,
      annual_records: annual.length, ranked_authorities: rankings.length, dataset_changed: changed,
      current_source_authorities: new Set(parsed.sheets.P152a.filter(row => row.quarter === period.last_quarter).map(row => row.code)).size,
      annual_changes: summarizeChanges(oldAnnual, annual, row => `${row.LADCD}/${row.Year}`),
      ranking_changes: summarizeChanges(oldRankings, rankings, row => row.ons_code),
    }).trim());
    if (values['dry-run']) return;
    const archiveDir = path.join(stateDir, 'sources');
    fs.mkdirSync(archiveDir, { recursive: true, mode: 0o700 });
    const archive = path.join(archiveDir, `${sourceHash}.ods`);
    if (!fs.existsSync(archive)) fs.writeFileSync(archive, workbook.bytes, { mode: 0o600, flag: 'wx' });
    const lookupArchive = path.join(archiveDir, `${lookupHash}.lpas.json`);
    if (!fs.existsSync(lookupArchive)) fs.writeFileSync(lookupArchive, json(lpas.rows), { mode: 0o600, flag: 'wx' });
    const files = changed ? { [ANNUAL_FILE]: annualBytes, [RANKING_FILE]: rankingBytes } : {};
    files[METADATA_FILE] = json(metadata);
    const backup = publishBundle(outputDir, backupRoot, files, generation);
    console.log(`Published ${changed ? 'annual data, rankings and metadata' : 'check metadata (dataset unchanged)'}. Backup: ${backup}`);
  } finally { if (release) release(); }
}

function cryptoSuffix(value) { return `${value.slice(0, 12)}-${process.pid}`; }

main().catch(error => { console.error(`Import failed: ${error.message}`); process.exitCode = 1; });
