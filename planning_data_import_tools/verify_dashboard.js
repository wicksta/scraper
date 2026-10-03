#!/usr/bin/env node
// Runs the actual PHP readers and dashboard rendering functions, without login
// or changing dashboard code. DOM/canvas are mocked; this is not a browser test.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { parseArgs } from 'node:util';
import { ANNUAL_FILE, RANKING_FILE, METADATA_FILE, hash } from './appeals.js';

const { values } = parseArgs({ options: {
  'data-dir': { type: 'string', default: '/mnt/ngist/public_html/dashboard/data' },
  'dashboard-dir': { type: 'string', default: '/mnt/ngist/public_html/dashboard' },
} });
const dataDir = path.resolve(values['data-dir']);
const dashboardDir = path.resolve(values['dashboard-dir']);
const annualBytes = fs.readFileSync(path.join(dataDir, ANNUAL_FILE));
const rankingBytes = fs.readFileSync(path.join(dataDir, RANKING_FILE));
const annual = JSON.parse(annualBytes);
const rankings = JSON.parse(rankingBytes);
const metadata = JSON.parse(fs.readFileSync(path.join(dataDir, METADATA_FILE)));
assert.equal(hash(annualBytes), metadata.files[ANNUAL_FILE]);
assert.equal(hash(rankingBytes), metadata.files[RANKING_FILE]);
assert.equal(annual.length, metadata.annual_records);
assert.equal(rankings.length, metadata.ranking_population.count);

function presentation(metadataPath) {
  const result = spawnSync('php', ['-r', 'require $argv[1]; echo json_encode(dashboardAppealsDisplay($argv[2]));',
    path.join(dashboardDir, 'appeals_metadata.php'), metadataPath], { encoding: 'utf8', timeout: 30000 });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  return JSON.parse(result.stdout);
}
const display = presentation(path.join(dataDir, METADATA_FILE));
assert.ok(display.coveragePeriod);
assert.ok(display.rankingPeriod);
assert.ok(display.source.includes('Data updated:'));
assert.ok(display.source.includes('Last checked:'));
assert.ok(!display.source.includes('August 2025'));

// A later importer metadata generation must change dates without a PHP edit.
const fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), 'appeals-display-'));
try {
  const fixture = path.join(fixtureDir, 'metadata.json');
  fs.writeFileSync(fixture, JSON.stringify({
    coverage: { first_quarter: '2016 Q1', last_quarter: '2026 Q4' },
    ranking_period: { first_quarter: '2020 Q1', last_quarter: '2026 Q4' },
    last_data_update: '2027-01-02T12:34:00Z', last_successful_check: '2027-01-09T13:45:00Z',
  }));
  const future = presentation(fixture);
  assert.equal(future.coveragePeriod, 'January 2016 to December 2026');
  assert.equal(future.rankingPeriod, 'January 2020 to December 2026');
  assert.ok(future.source.includes('2 January 2027, 12:34 UTC'));
  assert.ok(future.source.includes('9 January 2027, 13:45 UTC'));
  fs.writeFileSync(fixture, '{invalid');
  assert.equal(presentation(fixture).coveragePeriod, null);
  assert.ok(presentation(path.join(fixtureDir, 'missing.json')).source.includes('Dataset period unavailable.'));
} finally { fs.rmSync(fixtureDir, { recursive: true, force: true }); }

function phpReader(filename, code, dataset) {
  const live = dataDir === path.join(dashboardDir, 'data');
  const codeToRun = live
    ? '$_GET["onsCode"]=$argv[1]; include $argv[2];'
    : '$_GET["onsCode"]=$argv[1]; $source=file_get_contents($argv[2]); $needle="__DIR__ . " . var_export("/../data/".$argv[4],true); '
      + '$source=str_replace($needle,var_export($argv[3],true),$source,$count); if($count!==1){throw new Exception("Expected one data path replacement");} eval("?>".$source);';
  const result = spawnSync('php', ['-r', codeToRun, code, path.join(dashboardDir, 'fetchers', filename), path.join(dataDir, dataset), dataset], { encoding: 'utf8', timeout: 30000 });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  const parsed = JSON.parse(result.stdout);
  assert.equal(parsed.error, undefined, JSON.stringify(parsed));
  return parsed;
}

const source = fs.readFileSync(path.join(dashboardDir, 'dashboard.php'), 'utf8').replace(/\r\n/g, '\n');
function functionSource(name) {
  const start = source.indexOf(`function ${name}(`);
  assert.ok(start >= 0, `Missing dashboard function ${name}`);
  const next = source.indexOf('\nfunction ', start + 1);
  assert.ok(next > start, `Cannot find end of ${name}`);
  return source.slice(start, next);
}

const results = [];
for (const code of ['E09000033', 'E09000007']) {
  const entry = rankings.find(row => row.ons_code === code);
  assert.ok(entry, `No ranking for ${code}`);
  const series = phpReader('appeal_data_fetcher.php', code, ANNUAL_FILE);
  for (const row of annual.filter(row => row.LADCD === code)) {
    for (const [kind, apiKind] of [['major', 'Major'], ['non_major', 'NonMajor']]) {
      assert.equal(series[row.Year][apiKind].Appeals, row[kind].appeal_decisions);
      assert.equal(series[row.Year][apiKind].Overturned, row[kind].overturned_at_appeal);
    }
  }
  const api = phpReader('loadAppealRankingsCard.php', code, RANKING_FILE);
  assert.equal(api.total_lpas_ranked, rankings.length);
  for (const [kind, apiKind] of [['major', 'major'], ['nonmajor', 'non_major']]) {
    assert.equal(api[apiKind].total_appeals, entry.appeals[kind].total);
    assert.equal(api[apiKind].overturned, entry.appeals[kind].overturned);
    assert.equal(api[apiKind].appeals_per_100_apps, entry.appeals[kind].per_100_apps);
  }
  const elements = new Map();
  const chartConfigs = [];
  const errors = [];
  const context = vm.createContext({
    source: display.source, appealsDisplay: display, stackedChartInstance: null, highlightLabelPlugin: {},
    getGradientColor: () => '#fff',
    bootstrap: { Tooltip: class {} },
    document: {
      getElementById: id => {
        if (!elements.has(id)) elements.set(id, { innerHTML: '', getContext: () => ({}), addEventListener: () => {} });
        return elements.get(id);
      },
      querySelectorAll: () => [],
    },
    fetch: async url => ({ json: async () => url.startsWith('data/') ? rankings : series }),
    Chart: class { constructor(_canvas, config) { chartConfigs.push(config); } destroy() {} },
    console: { error: (...args) => errors.push(args) },
  });
  for (const name of ['loadAppealRankingCard', 'loadAppealDataCard', 'loadAppealOutcomeStackedCard', 'renderAppealOutcomeStackedChart']) {
    vm.runInContext(functionSource(name), context);
  }
  context.loadAppealRankingCard(code);
  context.loadAppealDataCard(code);
  context.loadAppealOutcomeStackedCard(code);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(errors.length, 0, JSON.stringify(errors));
  assert.ok(elements.get('appeal-ranking-card-container')?.innerHTML.includes('Appeal Overturn Rates'));
  assert.ok(elements.get('appeal-rate-card-container')?.innerHTML.includes('Appeals and Overturns'));
  assert.ok(elements.get('appeal-card-container')?.innerHTML.includes(metadata.coverage.last_quarter.slice(0, 4)));
  for (const id of ['appeal-ranking-card-container', 'appeal-rate-card-container', 'appeal-card-container', 'appeal-outcome-chart-container']) {
    assert.ok(elements.get(id)?.innerHTML.includes(display.coveragePeriod), `Missing coverage in ${id}`);
    assert.ok(elements.get(id)?.innerHTML.includes('Last checked:'), `Missing freshness in ${id}`);
  }
  assert.ok(elements.get('appeal-ranking-card-container').innerHTML.includes(`Appeal Overturn Rates (${display.rankingPeriod})`));
  assert.ok(elements.get('appeal-outcome-chart-container').innerHTML.includes(`Appeal Volumes per 100 Applications (${display.rankingPeriod})`));
  for (const kind of ['major', 'nonmajor']) {
    context.renderAppealOutcomeStackedChart(rankings, code, kind);
    const config = chartConfigs.at(-1);
    assert.ok(config.data.labels.length > 0);
    for (const dataset of config.data.datasets) {
      assert.equal(dataset.data.length, config.data.labels.length);
      assert.ok(dataset.data.every(value => Number.isFinite(value) && value >= 0));
    }
    assert.ok(config.options.plugins.highlightLPA.allOnsCodes.includes(code));
  }
  results.push({ ons_code: code, name: entry.lpa_name, annual_years: Object.keys(series).length,
    major_appeals: entry.appeals.major.total, nonmajor_appeals: entry.appeals.nonmajor.total });
}
const missing = phpReader('appeal_data_fetcher.php', 'E00000000', ANNUAL_FILE);
assert.equal(Object.keys(missing).length, 0);
console.log(JSON.stringify({ status: 'passed', coverage: metadata.coverage, ranked_authorities: rankings.length,
  checks: 'Existing PHP readers, metadata-driven dates and freshness (including future/missing metadata), annual/ranking/chart card renderers, and major/non-major chart configurations (mock DOM/canvas)', authorities: results }, null, 2));
