import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { ANNUAL_FILE, RANKING_FILE, METADATA_FILE, buildDatasets, coverage, json, publishBundle, recoverPublication } from './appeals.js';

function row(code, quarter, decisions, nondetermined, appeals, overturns) {
  return { code, name: code, quarter, application_decisions: decisions, not_determined: nondetermined,
    total_decisions_and_nondetermined: decisions + nondetermined, appeal_decisions: appeals, overturned_at_appeal: overturns };
}

test('cohort sums, denominators, current lookup population, null rates and deterministic ties', () => {
  const codes = ['E09000001', 'E09000002', 'E09000003'];
  const rows = [row(codes[0], '2019 Q4', 100, 0, 99, 99), row(codes[0], '2020 Q1', 10, 2, 4, 1),
    row(codes[0], '2025 Q3', 10, 0, 4, 1), row(codes[1], '2025 Q3', 20, 2, 8, 2),
    row(codes[2], '2025 Q3', 0, 0, 0, 0), row('E07000001', '2020 Q1', 100, 0, 30, 20),
    row('E07000002', '2025 Q3', 100, 0, 30, 20)];
  const parsed = { quarters: ['2019 Q4', '2020 Q1', '2025 Q3'], sheets: { P152a: rows, P154: rows } };
  const lpas = [...codes, 'E07000001'].reverse().map(ons_code => ({ ons_code, lpa_name: ons_code }));
  const result = buildDatasets(parsed, lpas);
  assert.equal(result.rankings.length, 3);
  const first = result.rankings[0];
  assert.equal(first.applications.major, 22);
  assert.equal(first.appeals.major.total, 8);
  assert.equal(first.appeals.major.rate, 25);
  assert.equal(first.appeals.major.per_100_apps, 36.36);
  assert.equal(first.appeals.major.rate_rank, 1);
  assert.equal(result.rankings[1].appeals.major.rate_rank, 2);
  assert.equal(result.rankings[2].appeals.major.rate_rank, null);
  assert.equal(result.rankings[2].appeals.major.per_100_apps, null);
  assert.equal(result.annual.find(r => r.LADCD === codes[0] && r.Year === '2020').major.appeals_per_100_apps, 40);
  assert.deepEqual(coverage(parsed).partial_years, ['2019', '2020', '2025']);
  assert.deepEqual(buildDatasets(parsed, [...lpas].reverse()), result);
});

test('publication retains originals and rolls back if a rename fails', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'planning-publish-'));
  const out = path.join(temp, 'out');
  const backups = path.join(temp, 'backups');
  fs.mkdirSync(out);
  const old = { [ANNUAL_FILE]: json([{ old: true }]), [RANKING_FILE]: json([{ old: true }]), [METADATA_FILE]: json({ generation_id: 'old' }) };
  const next = { [ANNUAL_FILE]: json([{ next: true }]), [RANKING_FILE]: json([{ next: true }]), [METADATA_FILE]: json({ generation_id: 'new' }) };
  for (const [name, bytes] of Object.entries(old)) fs.writeFileSync(path.join(out, name), bytes);
  const rename = fs.renameSync;
  try {
    fs.renameSync = (source, target) => {
      if (String(source).endsWith('.tmp') && target === path.join(out, RANKING_FILE)) throw new Error('simulated disconnect');
      return rename(source, target);
    };
    assert.throws(() => publishBundle(out, backups, next, 'new'), /simulated disconnect/);
    for (const [name, bytes] of Object.entries(old)) assert.equal(fs.readFileSync(path.join(out, name), 'utf8'), bytes);
    assert.equal(fs.existsSync(path.join(backups, 'pending.json')), false);
    fs.renameSync = rename;
    next[METADATA_FILE] = json({ generation_id: 'successful' });
    const backup = publishBundle(out, backups, next, 'successful');
    for (const [name, bytes] of Object.entries(old)) assert.equal(fs.readFileSync(path.join(backup, name), 'utf8'), bytes);
    for (const [name, bytes] of Object.entries(next)) assert.equal(fs.readFileSync(path.join(out, name), 'utf8'), bytes);
  } finally { fs.renameSync = rename; fs.rmSync(temp, { recursive: true, force: true }); }
});

test('recovery restores an interrupted bundle but preserves a completed generation', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'planning-recover-'));
  const out = path.join(temp, 'out');
  const backups = path.join(temp, 'backups');
  const snapshot = path.join(backups, 'interrupted');
  fs.mkdirSync(out);
  fs.mkdirSync(snapshot, { recursive: true });
  try {
    fs.writeFileSync(path.join(snapshot, ANNUAL_FILE), json([{ original: true }]));
    fs.writeFileSync(path.join(out, ANNUAL_FILE), json([{ partial: true }]));
    const pending = { output_dir: out, generation_id: 'interrupted', backup_dir: snapshot,
      files: [{ name: ANNUAL_FILE, existed: true, mode: 0o644 }, { name: METADATA_FILE, existed: false, mode: 0o644 }] };
    fs.writeFileSync(path.join(backups, 'pending.json'), json(pending));
    recoverPublication(out, backups);
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(out, ANNUAL_FILE))), [{ original: true }]);
    fs.writeFileSync(path.join(backups, 'pending.json'), json(pending));
    fs.writeFileSync(path.join(out, METADATA_FILE), json({ generation_id: 'interrupted' }));
    fs.writeFileSync(path.join(out, ANNUAL_FILE), json([{ complete: true }]));
    recoverPublication(out, backups);
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(out, ANNUAL_FILE))), [{ complete: true }]);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});
