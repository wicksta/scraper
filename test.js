const { chromium } = require('playwright');
const fs = require('fs');

function looksBlocked(html, title) {
  const t = (title || '').toLowerCase();
  const h = (html || '').toLowerCase();

  // Cloudflare / WAF / true challenge signals
  if (h.includes('cf-ray')) return true;
  if (h.includes('cloudflare')) return true;
  if (t.includes('attention required')) return true;
  if (t.includes('access denied')) return true;
  if (h.includes('checking your browser before accessing')) return true;

  // Actual CAPTCHA/challenge markup (not just a class name)
  // (g-recaptcha div or typical challenge text)
  if (h.includes('class="g-recaptcha"')) return true;
  if (h.includes('data-sitekey')) return true; // common on real captcha widgets
  if (h.includes('please verify you are human')) return true;

  return false;
}


function stamp() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

(async () => {
  const ts = stamp();
  const base = `westminster_${ts}`;

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  // “Good hygiene” headers. Not magic, just reduces needless weirdness.
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-GB,en;q=0.9' });

  const url = 'https://idoxpa.westminster.gov.uk/online-applications/';
  console.log('Going to:', url);

  const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(1500);

  const status = resp ? resp.status() : null;
  const title = await page.title();
  const html = await page.content();

  console.log('HTTP status:', status);
  console.log('Title:', title);
  console.log('Final URL:', page.url());

  fs.writeFileSync(`${base}_landing.html`, html);
  await page.screenshot({ path: `${base}_landing.png`, fullPage: true });

  if (looksBlocked(html, title)) {
    console.log('⚠️ Block/challenge detected on landing page. Saved artefacts.');
    await browser.close();
    process.exit(2);
  } else {
    console.log('✅ Landing page looks normal.');
  }

  // Click "Search" (Idox usually has this link)
  // We try a few likely selectors, safely.
  const searchSelectors = [
    'a:has-text("Search")',
    'a:has-text("Simple Search")',
    'a:has-text("Advanced Search")',
    'a[href*="search"]',
    'a[href*="simpleSearch"]',
  ];

  let clicked = false;
  for (const sel of searchSelectors) {
    const loc = page.locator(sel).first();
    if (await loc.count()) {
      try {
        await Promise.all([
          page.waitForLoadState('domcontentloaded', { timeout: 30000 }),
          loc.click({ timeout: 5000 })
        ]);
        clicked = true;
        console.log('Clicked:', sel);
        break;
      } catch (_) { /* try next */ }
    }
  }

  // If click didn't work, try going directly to known Idox path
  if (!clicked) {
    console.log('Could not click a Search link; trying direct simpleSearch path…');
    await page.goto(url.replace(/\/+$/, '') + '/search.do?action=simple', {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });
  }

  await page.waitForTimeout(1500);

  const title2 = await page.title();
  const html2 = await page.content();

  console.log('After navigation - Title:', title2);
  console.log('After navigation - URL:', page.url());

  fs.writeFileSync(`${base}_search.html`, html2);
  await page.screenshot({ path: `${base}_search.png`, fullPage: true });

  if (looksBlocked(html2, title2)) {
    console.log('⚠️ Block/challenge detected after navigating to search. Saved artefacts.');
    await browser.close();
    process.exit(3);
  } else {
    console.log('✅ Search page looks normal. Artefacts saved.');
  }

  await browser.close();
})();