#!/usr/bin/env node

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .scriptName('camden-accountforms-scrape')
  .option('ref', { type: 'string', describe: 'Application reference', demandOption: true })
  .option('start-url', {
    type: 'string',
    describe: 'Camden planning search URL (accountforms) e.g. https://accountforms.camden.gov.uk/planning-search/',
    demandOption: true,
  })
  .option('mapper', { type: 'string', describe: 'Path to mapping module (.cjs)', demandOption: false })
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

function normaliseMonthDateToISO(s) {
  if (!s) return null;
  const raw = String(s).trim();
  if (!raw || /^not available$/i.test(raw)) return null;
  // e.g. "Feb 12 2026" or "Feb 12 2026)" (we strip non-word)
  const cleaned = raw.replace(/[()]/g, '').trim();
  const m = cleaned.match(/^([A-Za-z]{3,})\s+(\d{1,2})\s+(\d{4})$/);
  if (!m) return null;
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
  const mm = monMap[m[1].slice(0, 4).toLowerCase()] ?? monMap[m[1].slice(0, 3).toLowerCase()];
  const dd = Number(m[2]);
  const yyyy = Number(m[3]);
  if (!mm || !dd || !yyyy) return null;
  return `${yyyy}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
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

async function extractKV(page) {
  return page.evaluate(() => {
    const normKey = (k) =>
      String(k || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^\w]/g, '');

    const out = { headline: {}, tables: {}, dl: {}, raw: {} };

    const title = document.querySelector('h1, h2, .page-title, .title')?.textContent?.trim();
    if (title) out.headline.title = title;

    const tables = Array.from(document.querySelectorAll('table'));
    tables.forEach((table, i) => {
      const id = table.id || `table_${i + 1}`;
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

    document.querySelectorAll('dl').forEach((dl, i) => {
      const id = dl.id || `dl_${i + 1}`;
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

    out.raw.text = (document.body?.innerText || '').slice(0, 20000);
    return out;
  });
}

async function capture(page, base, tabName) {
  const title = await page.title();
  const url = page.url();
  const extracted = await extractKV(page);

  const tag = safeFilename(`${base}_${tabName}`);
  fs.writeFileSync(`${tag}.html`, await page.content(), 'utf8');
  await page.screenshot({ path: `${tag}.png`, fullPage: true });

  return { title, url, extracted };
}

async function findMatchingResult(page, ref) {
  // Match "(... (REF))" pattern.
  const needle = `(${ref})`;

  const item = page.locator('.planning-application-item').filter({
    has: page.locator('a', { hasText: needle }),
  }).first();

  if (await item.count()) return item;

  // Fallback: scan anchors.
  const href = await page.evaluate((needleText) => {
    const links = Array.from(document.querySelectorAll('.planning-application-item a'));
    const link = links.find((a) => (a.textContent || '').includes(needleText));
    return link ? link.getAttribute('href') : null;
  }, needle);

  if (!href) return null;

  // Return a synthetic handle via selector by href.
  const loc = page.locator(`.planning-application-item a[href="${href.replace(/"/g, '\\"')}"]`).first();
  if (await loc.count()) {
    return loc.locator('xpath=ancestor::div[contains(@class,"planning-application-item")]').first();
  }

  return null;
}

async function extractResultItemFields(item) {
  const data = await item.evaluate((el) => {
    const a = el.querySelector('p.font-bold a');
    const href = a ? a.href : null;
    const title = a ? (a.textContent || '').replace(/\s+/g, ' ').trim() : null;

    const status = el.querySelector('p span.font-bold')?.textContent?.trim() || null;
    const meta = el.querySelectorAll('p')[1]?.querySelectorAll('span')[1]?.textContent || null;
    const metaText = meta ? meta.replace(/\s+/g, ' ').trim() : null;

    const desc = el.querySelectorAll('p')[2]?.textContent?.trim() || null;

    return { href, title, status, metaText, desc };
  });

  // Parse "Address (REF)" from title.
  const title = data.title || '';
  const m = title.match(/^(.*)\s+\(([^)]+)\)\s*$/);
  const address = m ? m[1].trim() : title || null;
  const ref = m ? m[2].trim() : null;

  // Parse "(Feb 12 2026) - Type" from metaText.
  let decisionDateRaw = null;
  let applicationType = null;
  if (data.metaText) {
    const mm = data.metaText.match(/^\(([^)]+)\)\s*-\s*(.+)$/);
    if (mm) {
      decisionDateRaw = mm[1].trim();
      applicationType = mm[2].trim();
    }
  }

  return {
    href: data.href,
    title: data.title,
    status: data.status,
    metaText: data.metaText,
    description: data.desc,
    address,
    reference: ref,
    decision_date_raw: decisionDateRaw,
    application_type: applicationType,
  };
}

(async () => {
  const ts = stamp();
  const base = `camden_af_${ts}`;

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
  await page.waitForTimeout(800);

  const input = page.locator('#searchForm\\:searchTermInput\\:textField').first();
  const btn = page.locator('#searchForm\\:SubmitButton\\:button').first();

  if (!(await input.count())) {
    fs.writeFileSync(`${base}_NO_SEARCH_INPUT.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${base}_NO_SEARCH_INPUT.png`, fullPage: true });
    throw new Error('Could not find search input on accountforms page.');
  }

  await input.fill(q);

  await Promise.all([
    page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => {}),
    btn.click(),
  ]);

  await page.waitForTimeout(1500);

  // Ensure results list exists.
  await page.waitForSelector('.planning-application-item', { timeout: 60000 });

  const match = await findMatchingResult(page, q);
  if (!match) {
    fs.writeFileSync(`${base}_NO_MATCH.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${base}_NO_MATCH.png`, fullPage: true });
    throw new Error(`No exact match found in results for reference ${q}.`);
  }

  const fields = await extractResultItemFields(match);
  if (!fields.reference || fields.reference !== q) {
    fs.writeFileSync(`${base}_NO_EXACT_MATCH.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${base}_NO_EXACT_MATCH.png`, fullPage: true });
    throw new Error(`Found a result but could not confirm exact reference match ${q}.`);
  }

  const unified = {
    query: q,
    keyVal: null,
    fetched_at: new Date().toISOString(),
    start_url: startUrl,
    tabs: {},
  };

  // Summary from results page.
  unified.tabs.summary = {
    title: await page.title(),
    url: page.url(),
    extracted: {
      headline: {
        reference: fields.reference,
        address: fields.address,
        description: fields.description,
      },
      tables: {
        simpleDetailsTable: {
          reference: fields.reference,
          address: fields.address,
          proposal: fields.description,
          status: fields.status,
          application_type: fields.application_type,
          decision_date: fields.decision_date_raw,
        },
      },
      dl: {},
      raw: {
        result_item: fields,
      },
    },
  };

  // Best-effort: follow the result link and capture whatever it shows.
  if (fields.href) {
    try {
      await page.goto(fields.href, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(1200);
      unified.tabs.further_information = await capture(page, base, 'further_information');
      unified.tabs.important_dates = unified.tabs.further_information;
    } catch (err) {
      unified.tabs.further_information = unified.tabs.summary;
      unified.tabs.important_dates = unified.tabs.summary;
      unified.tabs.summary.extracted.raw.follow_error = String(err && err.message ? err.message : err);
    }
  } else {
    unified.tabs.further_information = unified.tabs.summary;
    unified.tabs.important_dates = unified.tabs.summary;
  }

  fs.writeFileSync(`${base}_UNIFIED.json`, JSON.stringify(unified, null, 2), 'utf8');
  console.log(`\n✅ Unified JSON: ${base}_UNIFIED.json`);

  if (mapper) {
    const ctx = {
      area_name: areaName,
      ons_code: onsCode,
      scraper_name: mapper.scraperName || safeFilename(startUrl),
      normaliseIdoxDateToISO: normaliseMonthDateToISO,
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
