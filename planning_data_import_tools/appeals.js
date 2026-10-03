import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const ANNUAL_FILE = '250419_appeals_by_year.json';
export const RANKING_FILE = '2020_appeal_rankings.json';
export const METADATA_FILE = 'appeals_metadata.json';
export const FIELDS = ['application_decisions', 'not_determined', 'total_decisions_and_nondetermined', 'appeal_decisions', 'overturned_at_appeal'];
export const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
export const json = value => `${JSON.stringify(value, null, 2)}\n`;
const byCode = (a, b) => a.ons_code.localeCompare(b.ons_code);
const ratio = (numerator, denominator, decimals) => denominator > 0 ? Number((100 * numerator / denominator).toFixed(decimals)) : null;
const empty = () => Object.fromEntries(FIELDS.map(field => [field, 0]));

export function buildDatasets(parsed, lpas) {
  const latest = parsed.quarters.at(-1);
  if (!latest || latest < '2020 Q1') throw new Error('Workbook has no ranking-period data from 2020 onward');
  const annualMap = new Map();
  for (const [sheet, kind] of [['P152a', 'major'], ['P154', 'non_major']]) {
    for (const row of parsed.sheets[sheet]) {
      const year = row.quarter.slice(0, 4);
      const key = `${row.code}/${year}`;
      if (!annualMap.has(key)) annualMap.set(key, { LADNM: row.name, LADCD: row.code, Year: year, major: empty(), non_major: empty() });
      const entry = annualMap.get(key);
      entry.LADNM = row.name;
      for (const field of FIELDS) {
        entry[kind][field] += row[field];
        if (!Number.isSafeInteger(entry[kind][field])) throw new Error(`Unsafe count for ${key}`);
      }
    }
  }
  const annual = [...annualMap.values()].sort((a, b) => a.Year.localeCompare(b.Year) || a.LADCD.localeCompare(b.LADCD));
  for (const entry of annual) {
    for (const kind of ['major', 'non_major']) {
      const values = entry[kind];
      values.overturn_rate_percent = ratio(values.overturned_at_appeal, values.appeal_decisions, 1);
      values.appeals_per_100_apps = ratio(values.appeal_decisions, values.application_decisions, 1);
      values.overturns_per_100_apps = ratio(values.overturned_at_appeal, values.application_decisions, 1);
    }
  }
  const current = ['P152a', 'P154'].map(sheet => new Set(parsed.sheets[sheet].filter(row => row.quarter === latest).map(row => row.code)));
  const names = new Map();
  for (const lpa of lpas) {
    if (!/^E\d{8}$/.test(lpa.ons_code ?? '') || !lpa.lpa_name) continue;
    if (names.has(lpa.ons_code)) throw new Error(`Duplicate ONS code in LPA lookup: ${lpa.ons_code}`);
    names.set(lpa.ons_code, lpa.lpa_name);
  }
  if (!names.size) throw new Error('LPA lookup has no valid English authority records');
  const rankings = [...names].filter(([code]) => current.every(codes => codes.has(code))).map(([code, name]) => {
    const entries = annual.filter(row => row.LADCD === code && Number(row.Year) >= 2020);
    const output = { lpa_name: name, ons_code: code, applications: {}, appeals: {} };
    for (const [kind, annualKind] of [['major', 'major'], ['nonmajor', 'non_major']]) {
      const sums = empty();
      for (const entry of entries) for (const field of FIELDS) sums[field] += entry[annualKind][field];
      output.applications[kind] = sums.total_decisions_and_nondetermined;
      output.appeals[kind] = {
        total: sums.appeal_decisions, overturned: sums.overturned_at_appeal,
        rate: ratio(sums.overturned_at_appeal, sums.appeal_decisions, 1),
        per_100_apps: ratio(sums.appeal_decisions, sums.total_decisions_and_nondetermined, 2),
        overturned_per_100_apps: ratio(sums.overturned_at_appeal, sums.total_decisions_and_nondetermined, 2),
        rate_rank: null, per_100_apps_rank: null, overturned_per_100_apps_rank: null,
      };
    }
    return output;
  }).sort(byCode);
  if (!rankings.length) throw new Error('No current workbook authorities match the LPA lookup');
  for (const kind of ['major', 'nonmajor']) {
    for (const [metric, target] of [['rate', 'rate_rank'], ['per_100_apps', 'per_100_apps_rank'], ['overturned_per_100_apps', 'overturned_per_100_apps_rank']]) {
      const ordered = rankings.filter(row => row.appeals[kind][metric] !== null)
        .sort((a, b) => b.appeals[kind][metric] - a.appeals[kind][metric] || byCode(a, b));
      ordered.forEach((row, index) => { row.appeals[kind][target] = index + 1; });
    }
  }
  return { annual, rankings };
}

export function coverage(parsed) {
  const years = [...new Set(parsed.quarters.map(q => q.slice(0, 4)))];
  return {
    first_quarter: parsed.quarters[0], last_quarter: parsed.quarters.at(-1),
    quarters_by_year: Object.fromEntries(years.map(year => [year, parsed.quarters.filter(q => q.startsWith(year))])),
    partial_years: years.filter(year => parsed.quarters.filter(q => q.startsWith(year)).length < 4),
    sheet_rows: Object.fromEntries(Object.entries(parsed.sheets).map(([name, rows]) => [name, rows.length])),
  };
}

export function readOptionalJson(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}

export function summarizeChanges(previous, next, key) {
  const old = new Map((previous ?? []).map(row => [key(row), row]));
  let added = 0, revised = 0;
  for (const row of next) {
    const id = key(row);
    if (!old.has(id)) added++;
    else if (JSON.stringify(old.get(id)) !== JSON.stringify(row)) revised++;
    old.delete(id);
  }
  return { added, revised, removed: old.size };
}

// Files are individually atomic. The journal allows rollback after a process crash;
// the legacy readers cannot provide a transaction across multiple HTTP requests.
export function recoverPublication(outputDir, backupRoot) {
  const journalPath = path.join(backupRoot, 'pending.json');
  const pending = readOptionalJson(journalPath);
  if (!pending) return;
  if (pending.output_dir !== path.resolve(outputDir)) throw new Error('Recovery journal belongs to a different output directory');
  const current = readOptionalJson(path.join(outputDir, METADATA_FILE));
  if (current?.generation_id !== pending.generation_id) {
    for (const file of pending.files) {
      const target = path.join(outputDir, file.name);
      if (file.existed) {
        const temporary = `${target}.restore-${pending.generation_id}`;
        fs.copyFileSync(path.join(pending.backup_dir, file.name), temporary);
        fs.chmodSync(temporary, file.mode);
        fs.renameSync(temporary, target);
      } else {
        fs.rmSync(target, { force: true });
      }
    }
  }
  fs.unlinkSync(journalPath);
}

export function publishBundle(outputDir, backupRoot, files, generationId) {
  if (JSON.parse(files[METADATA_FILE] ?? '{}').generation_id !== generationId) {
    throw new Error('Publication metadata must match the generation ID');
  }
  fs.mkdirSync(outputDir, { recursive: true });
  fs.mkdirSync(backupRoot, { recursive: true, mode: 0o700 });
  recoverPublication(outputDir, backupRoot);
  const backupDir = path.join(backupRoot, generationId);
  fs.mkdirSync(backupDir, { mode: 0o700 });
  const descriptors = Object.keys(files).map(name => {
    const file = path.join(outputDir, name);
    const existed = fs.existsSync(file);
    const mode = existed ? fs.statSync(file).mode & 0o777 : 0o644;
    if (existed) fs.copyFileSync(file, path.join(backupDir, name));
    return { name, existed, mode };
  });
  const journal = { output_dir: path.resolve(outputDir), generation_id: generationId, backup_dir: backupDir, files: descriptors };
  const journalPath = path.join(backupRoot, 'pending.json');
  const staged = [];
  try {
    for (const descriptor of descriptors) {
      const temporary = path.join(outputDir, `.${descriptor.name}.${generationId}.tmp`);
      const bytes = files[descriptor.name];
      fs.writeFileSync(temporary, bytes, { flag: 'wx', mode: descriptor.mode });
      staged.push(temporary);
      const actual = fs.readFileSync(temporary);
      JSON.parse(actual.toString('utf8'));
      if (hash(actual) !== hash(bytes)) throw new Error(`Staged file verification failed: ${descriptor.name}`);
    }
    fs.writeFileSync(`${journalPath}.tmp`, json(journal), { mode: 0o600 });
    fs.renameSync(`${journalPath}.tmp`, journalPath);
    for (const name of Object.keys(files)) {
      // Metadata is inserted last by the caller and is the completion marker.
      fs.renameSync(path.join(outputDir, `.${name}.${generationId}.tmp`), path.join(outputDir, name));
      if (hash(fs.readFileSync(path.join(outputDir, name))) !== hash(files[name])) throw new Error(`Published file verification failed: ${name}`);
    }
    fs.unlinkSync(journalPath);
  } catch (error) {
    if (fs.existsSync(journalPath)) {
      // Force rollback even if a failure happened after the metadata rename.
      const marker = path.join(outputDir, METADATA_FILE);
      if (readOptionalJson(marker)?.generation_id === generationId) fs.rmSync(marker);
      recoverPublication(outputDir, backupRoot);
    }
    throw error;
  } finally {
    for (const temporary of staged) fs.rmSync(temporary, { force: true });
  }
  return backupDir;
}
