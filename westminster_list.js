const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-GB,en;q=0.9' });

  const url = 'https://idoxpa.westminster.gov.uk/online-applications/';
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(1200);

  const inputs = await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll('input, select, textarea'));
    return els.map(el => ({
      tag: el.tagName.toLowerCase(),
      type: (el.getAttribute('type') || '').toLowerCase(),
      id: el.id || null,
      name: el.getAttribute('name') || null,
      placeholder: el.getAttribute('placeholder') || null,
      ariaLabel: el.getAttribute('aria-label') || null,
      label: (() => {
        if (el.id) {
          const lab = document.querySelector(`label[for="${CSS.escape(el.id)}"]`);
          if (lab) return lab.textContent.trim();
        }
        const wrap = el.closest('label');
        if (wrap) return wrap.textContent.trim().replace(/\s+/g, ' ').slice(0, 80);
        return null;
      })()
    }));
  });

  fs.writeFileSync('westminster_inputs.json', JSON.stringify(inputs, null, 2));
  console.log(`Found ${inputs.length} form controls. Wrote westminster_inputs.json`);

  // Also save HTML + screenshot so we can correlate with the list
  fs.writeFileSync('westminster_simple.html', await page.content());
  await page.screenshot({ path: 'westminster_simple.png', fullPage: true });

  await browser.close();
})();