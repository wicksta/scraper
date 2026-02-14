const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .scriptName('idox-scrape')
  .option('ref', { type: 'string', describe: 'Application reference', demandOption: true })
  .option('start-url', { type: 'string', describe: 'Idox advanced search URL', demandOption: true })
  .option('mapper', { type: 'string', describe: 'Path to mapping module', demandOption: false })
  .option('area-name', { type: 'string', describe: 'LPA name', demandOption: false })
  .option('ons-code', { type: 'string', describe: 'ONS code', demandOption: false })
  .option('headed', { type: 'boolean', describe: 'Run with browser visible', default: false })
  .option('artifacts', { type: 'boolean', describe: 'Write HTML/PNG/JSON artifacts to disk', default: false })
  .option('emit-json', { type: 'boolean', describe: 'Emit UNIFIED/PLANIT JSON to stdout (worker-friendly)', default: true })
  .strict()
  .help()
  .argv;

// then:
const q = argv.ref;
const startUrl = argv['start-url'];
const mapperPath = argv.mapper;
const areaName = argv['area-name'] || null;
const onsCode = argv['ons-code'] || null;
const headed = argv.headed;

function stamp() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function safeFilename(s) {
  return String(s)
    .replace(/[^a-z0-9._-]+/gi, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 120);
}

// ---- Date normalisation -------------------------------------------------

// Accepts: "Mon 14 Jul 2025" | "14 Jul 2025" | "2025-07-14" | "Not Available" | ""
function normaliseIdoxDateToISO(s) {
  if (!s) return null;
  const raw = String(s).trim();
  if (!raw) return null;
  if (/^not available$/i.test(raw)) return null;

  // Already ISO?
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;

  // Strip weekday if present
  // "Mon 14 Jul 2025" -> "14 Jul 2025"
  const parts = raw.split(/\s+/);
  const maybeWeekday = parts[0];
  let tokens = parts;

  if (/^(mon|tue|wed|thu|fri|sat|sun)$/i.test(maybeWeekday)) {
    tokens = parts.slice(1);
  }

  if (tokens.length < 3) return null;

  const [ddStr, monStr, yyyyStr] = tokens;
  const dd = parseInt(ddStr, 10);
  const yyyy = parseInt(yyyyStr, 10);
  if (!Number.isFinite(dd) || !Number.isFinite(yyyy)) return null;

  const monMap = {
    jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
    jul: 7, aug: 8, sep: 9, sept: 9, oct: 10, nov: 11, dec: 12
  };
  const mm = monMap[String(monStr).slice(0, 4).toLowerCase()] ?? monMap[String(monStr).slice(0, 3).toLowerCase()];
  if (!mm) return null;

  const pad2 = (n) => String(n).padStart(2, '0');
  return `${yyyy}-${pad2(mm)}-${pad2(dd)}`;
}

function compactWhitespace(s) {
  if (s == null) return null;
  return String(s).replace(/\s+/g, ' ').trim();
}

function pickFirst(...vals) {
  for (const v of vals) {
    if (v == null) continue;
    const s = String(v).trim();
    if (s && !/^not available$/i.test(s)) return s;
  }
  return null;
}

// ---- Extract lots of “label/value” shapes from a page -------------------

async function extractKV(page) {
  return await page.evaluate(() => {
    const normKey = (k) =>
      k.trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^\w]/g, '');

    const out = {
      headline: {},
      tables: {},
      dl: {},
      raw: {},
    };

    const ref = document.querySelector('.caseNumber')?.textContent?.trim();
    if (ref) out.headline.reference = ref;

    const address = document.querySelector('.address')?.textContent?.trim();
    if (address) out.headline.address = address;

    const decision = document.querySelector('.badge-decided')?.textContent?.trim();
    if (decision) out.headline.decision_badge = decision;

    const description = document.querySelector('.description')?.textContent?.trim();
    if (description) out.headline.description = description;

    const stepper = {};
    document.querySelectorAll('.stepper-item').forEach(step => {
      const id = step.getAttribute('id') || '';
      const status = step.querySelector('.status')?.textContent?.trim() || '';
      const date = step.querySelector('.date')?.textContent?.trim() || '';
      if (id || status) stepper[id || status] = { status, date };
    });
    if (Object.keys(stepper).length) out.raw.stepper = stepper;

    const tables = Array.from(document.querySelectorAll('table'));
    tables.forEach((table, i) => {
      const id = table.id ? table.id : `table_${i + 1}`;
      const kv = {};
      table.querySelectorAll('tr').forEach(tr => {
        const th = tr.querySelector('th');
        const td = tr.querySelector('td');
        if (!th || !td) return;
        const k = normKey(th.textContent || '');
        const v = (td.textContent || '')
          .trim()
          .replace(/\s+\n/g, '\n')
          .replace(/[ \t]+/g, ' ');
        if (k) kv[k] = v;
      });
      if (Object.keys(kv).length) out.tables[id] = kv;
    });

    document.querySelectorAll('dl').forEach((dl, i) => {
      const id = dl.id ? dl.id : `dl_${i + 1}`;
      const kv = {};
      const dts = dl.querySelectorAll('dt');
      dts.forEach(dt => {
        const dd = dt.nextElementSibling;
        if (!dd || dd.tagName.toLowerCase() !== 'dd') return;
        const k = normKey(dt.textContent || '');
        const v = (dd.textContent || '').trim().replace(/[ \t]+/g, ' ');
        if (k) kv[k] = v;
      });
      if (Object.keys(kv).length) out.dl[id] = kv;
    });

    return out;
  });
}

async function getKeyValFromPage(page) {
  const href = await page.evaluate(() => {
    const a =
      document.querySelector('a[href*="applicationDetails.do"][href*="keyVal="]') ||
      document.querySelector('a[href*="keyVal="]');
    return a ? a.getAttribute('href') : null;
  });

  if (href) {
    const u = new URL(href, page.url());
    const kv = u.searchParams.get('keyVal');
    if (kv) return kv;
  }

  const html = await page.content();
  const m = html.match(/keyVal=([A-Za-z0-9]+)/);
  if (m) return m[1];

  return null;
}

async function ensureOnDetailsOrGetKeyVal(page) {
  const resultLink = page
    .locator('a[href*="applicationDetails.do"][href*="keyVal="]')
    .first();

  if (await resultLink.count()) {
    const url = page.url();
    if (!url.includes('applicationDetails.do')) {
      await Promise.all([
        page.waitForLoadState('domcontentloaded', { timeout: 60000 }),
        resultLink.click(),
      ]);
      await page.waitForTimeout(800);
    }
  }

  const kv = await getKeyValFromPage(page);
  if (!kv) throw new Error('Could not find keyVal in page HTML/links.');
  return kv;
}

// ---- Minimal CLI args ---------------------------------------------------

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  if (i === -1) return null;
  return process.argv[i + 1] ?? null;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function requireMapper(mapperPath) {
  if (!mapperPath) return null;
  const abs = path.isAbsolute(mapperPath) ? mapperPath : path.resolve(process.cwd(), mapperPath);
  // eslint-disable-next-line import/no-dynamic-require, global-require
  const mod = require(abs);
  if (!mod || typeof mod.mapToPlanit !== 'function') {
    throw new Error(`Mapper must export { mapToPlanit(unified, ctx) }. Got: ${abs}`);
  }
  return mod;
}

// ---- Run ---------------------------------------------------------------

(async () => {
  const ts = stamp();
  const base = `idox_${ts}`;

  const q = argValue('--ref') || process.argv[2] || '25/04808/ADFULL';
  const mapperPath = argValue('--mapper');          // e.g. ./mappers/westminster.js
  const areaName = argValue('--area-name') || null; // e.g. "City of Westminster"
  const onsCode = argValue('--ons-code') || null;   // e.g. E09000033
  const startUrl =
    argValue('--start-url') ||
    'https://idoxpa.westminster.gov.uk/online-applications/search.do?action=advanced&searchType=Application';

  const mapper = requireMapper(mapperPath);

  const browser = await chromium.launch({ headless: !hasFlag('--headed') });
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-GB,en;q=0.9' });

  console.log('Going to:', startUrl);
  await page.goto(startUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(500);

  const refSelector = 'input[name="searchCriteria.reference"]';
  const refInput = page.locator(refSelector);

  if (!(await refInput.count())) {
    if (hasFlag('--artifacts')) {
      await page.screenshot({ path: `${base}_NO_REF_INPUT.png`, fullPage: true });
      fs.writeFileSync(`${base}_NO_REF_INPUT.html`, await page.content(), 'utf8');
    }
    throw new Error(`Could not find reference input: ${refSelector}`);
  }

  await refInput.fill(q);

  await Promise.all([
    page.waitForLoadState('domcontentloaded', { timeout: 60000 }),
    page.evaluate((sel) => {
      const el = document.querySelector(sel);
      const form = el?.closest('form');
      if (form) form.submit();
      else {
        const btn = document.querySelector('input[type="submit"], button[type="submit"]');
        if (btn) btn.click();
      }
    }, refSelector),
  ]);

  await page.waitForURL(/(simpleSearchResults\.do|applicationDetails\.do|advancedSearchResults\.do|searchResults\.do)/, {
    timeout: 60000
  });

  await page.waitForLoadState('domcontentloaded');
  await page.waitForTimeout(250);

  console.log('After submit - URL:', page.url());

  const keyVal = await ensureOnDetailsOrGetKeyVal(page);
  console.log('✅ keyVal:', keyVal);

  const origin = new URL(startUrl).origin;
  const basePath = '/online-applications/applicationDetails.do';

  const tabUrls = {
    summary: `${origin}${basePath}?activeTab=summary&keyVal=${encodeURIComponent(keyVal)}`,
    further_information: `${origin}${basePath}?activeTab=details&keyVal=${encodeURIComponent(keyVal)}`,
    important_dates: `${origin}${basePath}?activeTab=dates&keyVal=${encodeURIComponent(keyVal)}`,
  };

  const unified = {
    query: q,
    keyVal,
    fetched_at: new Date().toISOString(),
    start_url: startUrl,
    tabs: {},
  };

  for (const [tabName, tabUrl] of Object.entries(tabUrls)) {
    console.log(`\n--- Fetching tab: ${tabName}`);
    console.log(tabUrl);

    await page.goto(tabUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForTimeout(800);

    const title = await page.title();
    const finalUrl = page.url();
    const extracted = await extractKV(page);

    unified.tabs[tabName] = { title, url: finalUrl, extracted };

    if (hasFlag('--artifacts')) {
      const tag = safeFilename(`${base}_${tabName}`);
      fs.writeFileSync(`${tag}.html`, await page.content(), 'utf8');
      await page.screenshot({ path: `${tag}.png`, fullPage: true });
      console.log(`Saved artefacts: ${tag}.html / ${tag}.png`);
    }
  }

  if (hasFlag('--artifacts')) {
    fs.writeFileSync(`${base}_UNIFIED.json`, JSON.stringify(unified, null, 2), 'utf8');
    console.log(`\n✅ Unified JSON: ${base}_UNIFIED.json`);
  }

  if (!process.argv.includes('--no-emit-json')) {
    // Single-line JSON marker for worker parsing.
    console.log(`__UNIFIED_JSON__=${JSON.stringify(unified)}`);
  }

  // ---- Mapping step -----------------------------------------------------

  if (mapper) {
    const ctx = {
      origin,
      area_name: areaName,
      ons_code: onsCode,
      scraper_name: mapper.scraperName || safeFilename(origin),
      normaliseIdoxDateToISO,
      compactWhitespace,
      pickFirst,
    };

    const mapped = await mapper.mapToPlanit(unified, ctx);

    // Ensure minimum canonical fields
    const planit = mapped?.planit || {};
    planit.scraper_name = planit.scraper_name || ctx.scraper_name;
    planit.source_url = planit.source_url || unified.tabs.summary?.url || unified.start_url;
    planit.url = planit.url || planit.source_url;

    if (hasFlag('--artifacts')) {
      fs.writeFileSync(`${base}_PLANIT.json`, JSON.stringify(mapped, null, 2), 'utf8');
      console.log(`✅ PlanIt-mapped JSON: ${base}_PLANIT.json`);
    }

    if (!process.argv.includes('--no-emit-json')) {
      console.log(`__PLANIT_JSON__=${JSON.stringify(mapped)}`);
    }
  } else {
    console.log('ℹ️ No mapper provided (--mapper). Skipping PlanIt mapping step.');
  }

  await browser.close();
})().catch((e) => {
  console.error('❌ Scrape failed:', e);
  process.exitCode = 1;
});
