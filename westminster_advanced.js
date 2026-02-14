const { chromium } = require('playwright');
const fs = require('fs');

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

// Extract lots of “label/value” shapes from a page
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

    // headline bits (common on details pages)
    const ref = document.querySelector('.caseNumber')?.textContent?.trim();
    if (ref) out.headline.reference = ref;

    const address = document.querySelector('.address')?.textContent?.trim();
    if (address) out.headline.address = address;

    const decision = document.querySelector('.badge-decided')?.textContent?.trim();
    if (decision) out.headline.decision_badge = decision;

    const description = document.querySelector('.description')?.textContent?.trim();
    if (description) out.headline.description = description;

    // stepper / timeline if present
    const stepper = {};
    document.querySelectorAll('.stepper-item').forEach(step => {
      const id = step.getAttribute('id') || '';
      const status = step.querySelector('.status')?.textContent?.trim() || '';
      const date = step.querySelector('.date')?.textContent?.trim() || '';
      if (id || status) stepper[id || status] = { status, date };
    });
    if (Object.keys(stepper).length) out.raw.stepper = stepper;

    // tables with th/td
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

    // dl dt/dd fallback
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
  // Prefer link extraction
  const href = await page.evaluate(() => {
    const a =
      document.querySelector('a[href*="applicationDetails.do"][href*="keyVal="]') ||
      document.querySelector('a[href*="keyVal="]');
    return a ? a.getAttribute('href') : null;
  });

  if (href) {
    const u = new URL(href, page.url()); // base resolved in Node
    const kv = u.searchParams.get('keyVal');
    if (kv) return kv;
  }

  // Fallback: regex over HTML
  const html = await page.content();
  const m = html.match(/keyVal=([A-Za-z0-9]+)/);
  if (m) return m[1];

  return null;
}

async function ensureOnDetailsOrGetKeyVal(page) {
  // If we're on a results list, click the first result (still safe even if 1 row)
  const resultLink = page
    .locator('a[href*="applicationDetails.do"][href*="keyVal="]')
    .first();

  if (await resultLink.count()) {
    // If the current page isn't already applicationDetails.do, click through.
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

(async () => {
  const ts = stamp();
  const base = `westminster_${ts}`;

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-GB,en;q=0.9' });

  const startUrl =
    'https://idoxpa.westminster.gov.uk/online-applications/search.do?action=advanced&searchType=Application';
  const q = process.argv[2] || '25/04808/ADFULL';

  console.log('Going to:', startUrl);
  await page.goto(startUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(500);

  const refSelector = 'input[name="searchCriteria.reference"]';

  // Ensure the reference box exists
  const refInput = page.locator(refSelector);
  if (!(await refInput.count())) {
    await page.screenshot({ path: `${base}_NO_REF_INPUT.png`, fullPage: true });
    fs.writeFileSync(`${base}_NO_REF_INPUT.html`, await page.content(), 'utf8');
    throw new Error(`Could not find reference input: ${refSelector}`);
  }

  await refInput.fill(q);

  // Submit: Idox sometimes has multiple submits; safest is submit the form that owns the input.
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

// Wait until we’ve actually landed somewhere meaningful
await page.waitForURL(/(simpleSearchResults\.do|applicationDetails\.do|advancedSearchResults\.do|searchResults\.do)/, {
  timeout: 60000
});

// Let the DOM settle (some Idox pages do an extra quick redirect/render)
await page.waitForLoadState('domcontentloaded');
await page.waitForTimeout(250);

const landedUrl = page.url();

// Title can still race with redirects on some installs, so retry once
let landedTitle = '';
try {
  landedTitle = await page.title();
} catch (e) {
  await page.waitForLoadState('domcontentloaded');
  landedTitle = await page.title();
}

console.log('After submit - Title:', landedTitle);
console.log('After submit - URL:', landedUrl);

  const keyVal = await ensureOnDetailsOrGetKeyVal(page);
  console.log('✅ keyVal:', keyVal);

  // Force canonical details URL tabs
  const origin = new URL('https://idoxpa.westminster.gov.uk').origin;
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

    const tag = safeFilename(`${base}_${tabName}`);
    fs.writeFileSync(`${tag}.html`, await page.content(), 'utf8');
    await page.screenshot({ path: `${tag}.png`, fullPage: true });

    console.log(`Saved artefacts: ${tag}.html / ${tag}.png`);
  }

  fs.writeFileSync(`${base}_UNIFIED.json`, JSON.stringify(unified, null, 2), 'utf8');
  console.log(`\n✅ Unified JSON: ${base}_UNIFIED.json`);

  await browser.close();
})();