const { chromium } = require('playwright');
const fs = require('fs');

function stamp() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function detectChallenge(html, title) {
  const t = (title || '').toLowerCase();
  const h = (html || '').toLowerCase();

  if (h.includes('cf-ray') || h.includes('cloudflare')) return 'cloudflare';
  if (t.includes('attention required') || t.includes('access denied')) return 'access_denied';
  if (h.includes('checking your browser before accessing')) return 'browser_check';

  // real captcha widget markers (not just a css class)
  if (h.includes('class="g-recaptcha"') || h.includes('data-sitekey')) return 'captcha_widget';
  if (h.includes('please verify you are human')) return 'verify_human';

  return null;
}

(async () => {
  const ts = stamp();
  const base = `westminster_${ts}`;

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-GB,en;q=0.9' });

  const url = 'https://idoxpa.westminster.gov.uk/online-applications/';
  console.log('Going to:', url);

  const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(1200);

  console.log('HTTP status:', resp ? resp.status() : null);
  console.log('Title:', await page.title());
  console.log('URL:', page.url());

  const html0 = await page.content();
  fs.writeFileSync(`${base}_before.html`, html0);
  await page.screenshot({ path: `${base}_before.png`, fullPage: true });

  const ch0 = detectChallenge(html0, await page.title());
  if (ch0) {
    console.log('⚠️ Challenge detected BEFORE submit:', ch0);
    await browser.close();
    process.exit(2);
  }

  const q = 'TEST-123';

  // Fill the actual Westminster simple search box
  await page.locator('#simpleSearchString').fill(q);

  // Click Search submit (first submit input is fine here)
  await Promise.all([
    page.waitForLoadState('domcontentloaded', { timeout: 60000 }),
    page.locator('input[type="submit"][value="Search"], input[type="submit"]').first().click()
  ]);

  await page.waitForTimeout(1500);

  console.log('After submit - Title:', await page.title());
  console.log('After submit - URL:', page.url());

  const html1 = await page.content();
  fs.writeFileSync(`${base}_after.html`, html1);
  await page.screenshot({ path: `${base}_after.png`, fullPage: true });

  const ch1 = detectChallenge(html1, await page.title());
  if (ch1) {
    console.log('⚠️ Challenge detected AFTER submit:', ch1);
    await browser.close();
    process.exit(3);
  }

  // quick hint if we got results page
  const h = html1.toLowerCase();
  const looksResults =
    h.includes('search results') ||
    h.includes('application search results') ||
    h.includes('no results') ||
    h.includes('your search returned') ||
    h.includes('result') && h.includes('application');

  console.log(looksResults ? '✅ Looks like a normal results page (possibly 0 results).' : '✅ No obvious challenge; check artefacts.');

  await browser.close();
})();