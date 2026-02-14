#!/usr/bin/env node

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .scriptName('camden-northgate-scrape')
  .option('ref', { type: 'string', describe: 'Application reference', demandOption: true })
  .option('start-url', {
    type: 'string',
    describe: 'Northgate General Search URL',
    demandOption: true,
  })
  .option('mapper', { type: 'string', describe: 'Path to mapping module', demandOption: false })
  .option('area-name', { type: 'string', describe: 'LPA name', demandOption: false })
  .option('ons-code', { type: 'string', describe: 'ONS code', demandOption: false })
  .option('headed', { type: 'boolean', describe: 'Run with browser visible', default: false })
  .strict()
  .help()
  .argv;

const q = argv.ref;
const startUrl = argv['start-url'];
const mapperPath = argv.mapper;
const areaName = argv['area-name'] || null;
const onsCode = argv['ons-code'] || null;
const headed = argv.headed;

function canRunHeaded() {
  return Boolean(process.env.DISPLAY || process.env.WAYLAND_DISPLAY);
}

const WAF_WAIT_MS = Number(process.env.WAF_WAIT_MS || 45000);
const WAF_CHECK_INTERVAL_MS = Number(process.env.WAF_CHECK_INTERVAL_MS || 1500);
const INPUT_WAIT_MS = Number(process.env.INPUT_WAIT_MS || 15000);

function stamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function safeFilename(s) {
  return String(s)
    .replace(/[^a-z0-9._-]+/gi, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 120);
}

function normaliseDateToISO(s) {
  if (!s) return null;
  const raw = String(s).trim();
  if (!raw || /^not available$/i.test(raw)) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;

  const slash = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slash) {
    const dd = Number(slash[1]);
    const mm = Number(slash[2]);
    const yyyy = Number(slash[3]);
    return `${yyyy}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
  }

  const parts = raw.split(/\s+/);
  const maybeWeekday = parts[0];
  const tokens = /^(mon|tue|wed|thu|fri|sat|sun)$/i.test(maybeWeekday) ? parts.slice(1) : parts;
  if (tokens.length < 3) return null;

  const dd = Number(tokens[0]);
  const yyyy = Number(tokens[2]);
  if (!Number.isFinite(dd) || !Number.isFinite(yyyy)) return null;

  const monMap = {
    jan: 1,
    feb: 2,
    mar: 3,
    apr: 4,
    may: 5,
    jun: 6,
    jul: 7,
    aug: 8,
    sep: 9,
    sept: 9,
    oct: 10,
    nov: 11,
    dec: 12,
  };
  const mm = monMap[String(tokens[1]).slice(0, 4).toLowerCase()] ?? monMap[String(tokens[1]).slice(0, 3).toLowerCase()];
  if (!mm) return null;

  return `${yyyy}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
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

function requireMapper(mapperPathArg) {
  if (!mapperPathArg) return null;
  const abs = path.isAbsolute(mapperPathArg) ? mapperPathArg : path.resolve(process.cwd(), mapperPathArg);
  // eslint-disable-next-line import/no-dynamic-require, global-require
  const mod = require(abs);
  if (!mod || typeof mod.mapToPlanit !== 'function') {
    throw new Error(`Mapper must export { mapToPlanit(unified, ctx) }. Got: ${abs}`);
  }
  return mod;
}

async function detectWafOrChallenge(page) {
  const title = (await page.title()) || '';
  const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 5000) || '');
  const haystack = `${title}\n${bodyText}`.toLowerCase();
  const wafMarkers = [
    'cloudflare',
    'checking your browser',
    'attention required',
    'verify you are human',
    'access denied',
  ];

  return wafMarkers.some((m) => haystack.includes(m));
}

async function saveWafEvidence(page, base, stage) {
  const tag = safeFilename(`${base}_WAF_${stage}`);
  try {
    fs.writeFileSync(`${tag}.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${tag}.png`, fullPage: true });
    console.error(`Saved WAF evidence: ${tag}.html / ${tag}.png`);
  } catch (err) {
    console.error('Failed to save WAF evidence:', err);
  }
}

async function waitForChallengeClear(page, stage) {
  const started = Date.now();
  while (Date.now() - started < WAF_WAIT_MS) {
    if (!(await detectWafOrChallenge(page))) {
      console.log(`[waf] challenge cleared at stage=${stage} after ${Date.now() - started}ms`);
      return true;
    }
    await page.waitForTimeout(WAF_CHECK_INTERVAL_MS);
  }
  return false;
}

async function waitForApplicationInput(page) {
  const started = Date.now();
  while (Date.now() - started < INPUT_WAIT_MS) {
    const input = await findApplicationInput(page);
    if (input) return input;
    await page.waitForTimeout(500);
  }
  return null;
}

async function extractKV(page, ref) {
  return page.evaluate((targetRef) => {
    const normKey = (k) =>
      String(k || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^\w]/g, '');

    const out = {
      headline: {},
      tables: {},
      dl: {},
      raw: {},
    };

    const title = document.querySelector('h1, h2, .page-title, .title')?.textContent?.trim();
    if (title) out.headline.title = title;

    const pageText = (document.body?.innerText || '').replace(/\s+/g, ' ').trim();
    if (targetRef && pageText.includes(targetRef)) out.headline.reference = targetRef;

    const addressByLabel = Array.from(document.querySelectorAll('th, dt, .label')).find((el) =>
      /address/i.test(el.textContent || ''),
    );
    if (addressByLabel) {
      const val =
        addressByLabel.closest('tr')?.querySelector('td')?.textContent ||
        addressByLabel.nextElementSibling?.textContent ||
        '';
      if (val.trim()) out.headline.address = val.trim();
    }

    const descByLabel = Array.from(document.querySelectorAll('th, dt, .label')).find((el) =>
      /(description|proposal|development)/i.test(el.textContent || ''),
    );
    if (descByLabel) {
      const val =
        descByLabel.closest('tr')?.querySelector('td')?.textContent ||
        descByLabel.nextElementSibling?.textContent ||
        '';
      if (val.trim()) out.headline.description = val.trim();
    }

    const tables = Array.from(document.querySelectorAll('table'));
    tables.forEach((table, idx) => {
      const id = table.id || `table_${idx + 1}`;
      const kv = {};
      table.querySelectorAll('tr').forEach((tr) => {
        const th = tr.querySelector('th');
        const td = tr.querySelector('td');
        if (!th || !td) return;
        const k = normKey(th.textContent || '');
        const v = (td.textContent || '').trim().replace(/[ \t]+/g, ' ');
        if (k) kv[k] = v;
      });
      if (Object.keys(kv).length) out.tables[id] = kv;
    });

    const dls = Array.from(document.querySelectorAll('dl'));
    dls.forEach((dl, idx) => {
      const id = dl.id || `dl_${idx + 1}`;
      const kv = {};
      dl.querySelectorAll('dt').forEach((dt) => {
        const dd = dt.nextElementSibling;
        if (!dd || dd.tagName.toLowerCase() !== 'dd') return;
        const k = normKey(dt.textContent || '');
        const v = (dd.textContent || '').trim().replace(/[ \t]+/g, ' ');
        if (k) kv[k] = v;
      });
      if (Object.keys(kv).length) out.dl[id] = kv;
    });

    const labelValues = {};
    Array.from(document.querySelectorAll('.row, .form-group, .field')).forEach((row) => {
      const label = row.querySelector('label, .label, .field-label')?.textContent?.trim();
      const value = row.querySelector('.value, .field-value, span, div')?.textContent?.trim();
      if (!label || !value) return;
      const k = normKey(label);
      if (k && !labelValues[k]) labelValues[k] = value;
    });
    if (Object.keys(labelValues).length) out.raw.label_values = labelValues;

    return out;
  }, ref);
}

async function findApplicationInput(page) {
  const selectors = [
    'input[name*="ApplicationNumber"]',
    'input[id*="ApplicationNumber"]',
    'input[name*="CaseNo"]',
    'input[id*="CaseNo"]',
    'input[name*="txtApplication"]',
    'input[id*="txtApplication"]',
  ];

  for (const sel of selectors) {
    const loc = page.locator(sel).first();
    if (await loc.count()) return loc;
  }

  const labelled = page.getByLabel(/Application\s*Number/i).first();
  if (await labelled.count()) return labelled;

  const idFromLabel = await page.evaluate(() => {
    const labels = Array.from(document.querySelectorAll('label'));
    const target = labels.find((l) => /application\s*number/i.test(l.textContent || ''));
    if (!target) return null;
    const forId = target.getAttribute('for');
    if (forId) return `#${forId}`;
    const input = target.parentElement?.querySelector('input[type="text"], input:not([type])');
    if (input?.id) return `#${input.id}`;
    if (input?.name) return `input[name="${input.name}"]`;
    return null;
  });

  if (idFromLabel) {
    const loc = page.locator(idFromLabel).first();
    if (await loc.count()) return loc;
  }

  return null;
}

async function submitSearch(page) {
  const buttonSelectors = [
    'input[type="submit"][value*="Search"]',
    'button:has-text("Search")',
    'input[id*="Search"]',
    'button[id*="Search"]',
  ];

  for (const sel of buttonSelectors) {
    const btn = page.locator(sel).first();
    if (await btn.count()) {
      await Promise.all([
        page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => {}),
        btn.click(),
      ]);
      return;
    }
  }

  await page.evaluate(() => {
    const form = document.querySelector('form');
    if (form) form.submit();
  });
  await page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => {});
}

async function clickResultByReference(page, ref) {
  const exact = page.locator('a', { hasText: ref }).first();
  if (await exact.count()) {
    await Promise.all([
      page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => {}),
      exact.click(),
    ]);
    await page.waitForTimeout(500);
    return true;
  }

  const href = await page.evaluate((targetRef) => {
    const links = Array.from(document.querySelectorAll('a'));
    const target = links.find((a) => (a.textContent || '').replace(/\s+/g, ' ').includes(targetRef));
    return target ? target.getAttribute('href') : null;
  }, ref);

  if (!href) return false;

  await page.goto(new URL(href, page.url()).toString(), { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(500);
  return true;
}

async function tryOpenTab(page, labels) {
  for (const label of labels) {
    const link = page.getByRole('link', { name: new RegExp(label, 'i') }).first();
    if (await link.count()) {
      await Promise.all([
        page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => {}),
        link.click(),
      ]);
      await page.waitForTimeout(350);
      return true;
    }
  }
  return false;
}

async function captureTab(page, base, tabName, ref) {
  const title = await page.title();
  const url = page.url();
  const extracted = await extractKV(page, ref);

  const tag = safeFilename(`${base}_${tabName}`);
  fs.writeFileSync(`${tag}.html`, await page.content(), 'utf8');
  await page.screenshot({ path: `${tag}.png`, fullPage: true });

  return { title, url, extracted };
}

(async () => {
  const ts = stamp();
  const base = `camden_${ts}`;

  const mapper = requireMapper(mapperPath);

  const useHeaded = headed && canRunHeaded();
  if (headed && !useHeaded) {
    console.warn('⚠️ --headed requested but no display detected; falling back to headless mode.');
  }

  const browser = await chromium.launch({ headless: !useHeaded });
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-GB,en;q=0.9' });

  console.log('Going to:', startUrl);
  await page.goto(startUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(600);

  if (await detectWafOrChallenge(page)) {
    const cleared = await waitForChallengeClear(page, 'landing');
    if (cleared) {
      console.log('[waf] continuing after landing challenge clear');
    } else {
    await saveWafEvidence(page, base, 'landing');
    throw new Error('blocked_by_waf: challenge page detected on search landing page.');
    }
  }

  const refInput = await waitForApplicationInput(page);
  if (!refInput) {
    fs.writeFileSync(`${base}_NO_REF_INPUT.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${base}_NO_REF_INPUT.png`, fullPage: true });
    throw new Error('Could not find Application Number input on Camden search page.');
  }

  await refInput.fill(q);
  await submitSearch(page);
  await page.waitForTimeout(600);

  if (await detectWafOrChallenge(page)) {
    const cleared = await waitForChallengeClear(page, 'after_search');
    if (cleared) {
      console.log('[waf] continuing after post-search challenge clear');
    } else {
    await saveWafEvidence(page, base, 'after_search');
    throw new Error('blocked_by_waf: challenge page detected after search submit.');
    }
  }

  const clicked = await clickResultByReference(page, q);
  if (!clicked) {
    fs.writeFileSync(`${base}_NO_MATCHING_RESULT.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${base}_NO_MATCHING_RESULT.png`, fullPage: true });
    throw new Error(`No matching result found for application reference ${q}.`);
  }

  if (await detectWafOrChallenge(page)) {
    const cleared = await waitForChallengeClear(page, 'details');
    if (cleared) {
      console.log('[waf] continuing after details challenge clear');
    } else {
    await saveWafEvidence(page, base, 'details');
    throw new Error('blocked_by_waf: challenge page detected on application details page.');
    }
  }

  const unified = {
    query: q,
    keyVal: null,
    fetched_at: new Date().toISOString(),
    start_url: startUrl,
    tabs: {},
    raw: {
      navigation: {
        details_url: page.url(),
      },
    },
  };

  unified.tabs.summary = await captureTab(page, base, 'summary', q);

  const openedDetails = await tryOpenTab(page, ['Further Information', 'Application Details', 'Details']);
  if (openedDetails) {
    unified.tabs.further_information = await captureTab(page, base, 'further_information', q);
  } else {
    unified.tabs.further_information = unified.tabs.summary;
  }

  const openedDates = await tryOpenTab(page, ['Important Dates', 'Dates', 'Key Dates']);
  if (openedDates) {
    unified.tabs.important_dates = await captureTab(page, base, 'important_dates', q);
  } else {
    unified.tabs.important_dates = unified.tabs.summary;
  }

  fs.writeFileSync(`${base}_UNIFIED.json`, JSON.stringify(unified, null, 2), 'utf8');
  console.log(`\n✅ Unified JSON: ${base}_UNIFIED.json`);

  if (mapper) {
    const ctx = {
      area_name: areaName,
      ons_code: onsCode,
      scraper_name: mapper.scraperName || safeFilename(startUrl),
      normaliseIdoxDateToISO: normaliseDateToISO,
      compactWhitespace,
      pickFirst,
    };

    const mapped = await mapper.mapToPlanit(unified, ctx);
    const planit = mapped?.planit || {};
    planit.scraper_name = planit.scraper_name || ctx.scraper_name;
    planit.source_url = planit.source_url || unified.tabs.summary?.url || unified.start_url;
    planit.url = planit.url || planit.source_url;

    fs.writeFileSync(`${base}_PLANIT.json`, JSON.stringify(mapped, null, 2), 'utf8');
    console.log(`✅ PlanIt-mapped JSON: ${base}_PLANIT.json`);
  } else {
    console.log('ℹ️ No mapper provided (--mapper). Skipping PlanIt mapping step.');
  }

  await browser.close();
})().catch((e) => {
  console.error('❌ Scrape failed:', e);
  process.exitCode = 1;
});
