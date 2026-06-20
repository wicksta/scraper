#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { chromium } from "playwright";

const argv = yargs(hideBin(process.argv))
  .scriptName("historic-england-list-entry-extract")
  .option("ref", {
    type: "string",
    demandOption: true,
    describe: "Historic England list entry reference, for example 1336361.",
  })
  .option("section", {
    type: "string",
    default: "official-list-entry",
    describe: "Section query parameter to load.",
  })
  .option("html-file", {
    type: "string",
    default: "",
    describe: "Optional saved HTML file to parse instead of fetching the page live.",
  })
  .option("headed", {
    type: "boolean",
    default: false,
    describe: "Run Playwright headed.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 45000,
    describe: "Navigation and selector timeout in milliseconds.",
  })
  .option("user-data-dir", {
    type: "string",
    default: "/tmp/he-playwright-profile",
    describe: "Persistent browser profile directory.",
  })
  .option("json", {
    type: "boolean",
    default: true,
    describe: "Emit JSON output.",
  })
  .strict()
  .help()
  .parse();

function normaliseText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function pairsToObject(rows) {
  const out = {};
  for (const row of rows) {
    const key = normaliseText(row?.key);
    if (!key) continue;
    out[key] = normaliseText(row?.value);
  }
  return out;
}

const ref = String(argv.ref || "").trim();
if (!/^\d+$/.test(ref)) {
  throw new Error("--ref must be a numeric Historic England list entry reference.");
}

const section = String(argv.section || "official-list-entry").trim() || "official-list-entry";
const url = `https://historicengland.org.uk/listing/the-list/list-entry/${encodeURIComponent(ref)}?section=${encodeURIComponent(section)}`;
const htmlFile = String(argv["html-file"] || "").trim();
const userDataDir = path.resolve(String(argv["user-data-dir"] || "/tmp/he-playwright-profile"));

let context;

try {
  context = await chromium.launchPersistentContext(userDataDir, {
    headless: !argv.headed,
    viewport: { width: 1440, height: 2200 },
    locale: "en-GB",
    timezoneId: "Europe/London",
    colorScheme: "light",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-first-run",
      "--disable-dev-shm-usage",
    ],
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "platform", { get: () => "Win32" });
    Object.defineProperty(navigator, "language", { get: () => "en-GB" });
    Object.defineProperty(navigator, "languages", { get: () => ["en-GB", "en"] });
    Object.defineProperty(navigator, "hardwareConcurrency", { get: () => 8 });
    Object.defineProperty(navigator, "deviceMemory", { get: () => 8 });
    Object.defineProperty(window, "chrome", {
      get: () => ({ runtime: {} }),
    });
  });

  const page = context.pages()[0] || await context.newPage();
  page.setDefaultTimeout(Number(argv["timeout-ms"]));
  await page.route("**/*", async (route) => {
    const req = route.request();
    const type = req.resourceType();
    if (type === "image" || type === "font" || type === "media") {
      await route.continue();
      return;
    }
    await route.continue();
  });

  if (htmlFile) {
    const html = await fs.readFile(htmlFile, "utf8");
    await page.setContent(html, {
      waitUntil: "domcontentloaded",
      timeout: Number(argv["timeout-ms"]),
    });
  } else {
    await page.goto(url, {
      waitUntil: "networkidle",
      timeout: Number(argv["timeout-ms"]),
    });
    await page.waitForTimeout(1200);
    await page.mouse.move(220, 180, { steps: 12 });
    await page.waitForTimeout(350);
    await page.mouse.wheel(0, 640);
    await page.waitForTimeout(600);
    await page.mouse.wheel(0, -240);
  }

  await page.waitForSelector("#nhle-entry", {
    timeout: Number(argv["timeout-ms"]),
  });

  const extracted = await page.evaluate(() => {
    const text = (selector, root = document) =>
      root.querySelector(selector)?.textContent?.replace(/\s+/g, " ").trim() || "";

    const collectDlRows = (scope) => {
      const root = document.querySelector(scope);
      if (!root) return [];
      return Array.from(root.querySelectorAll("div")).map((row) => ({
        key: row.querySelector("dt")?.textContent || "",
        value: row.querySelector("dd")?.textContent || "",
      }));
    };

    const officialSection = document.querySelector("#official-list-entry");
    const overviewSection = document.querySelector("#overview");

    const officialDetailsHeading = officialSection
      ? Array.from(officialSection.querySelectorAll("h3")).find((el) => el.textContent?.trim() === "Details")
      : null;
    let officialDetails = "";
    if (officialDetailsHeading) {
      const nextBlock = officialDetailsHeading.nextElementSibling;
      officialDetails = nextBlock?.textContent?.replace(/\s+/g, " ").trim() || "";
    }

    const legalHeading = officialSection
      ? Array.from(officialSection.querySelectorAll("h3")).find((el) => el.textContent?.trim() === "Legal")
      : null;
    let legalText = "";
    if (legalHeading) {
      const nextBlock = legalHeading.nextElementSibling;
      legalText = nextBlock?.textContent?.replace(/\s+/g, " ").trim() || "";
    }

    const mapImage = officialSection?.querySelector(".nhle-entry__official-map-image");
    const mapPdfLink = officialSection?.querySelector(".nhle-entry__official-map-caption-download-button");

    return {
      page_title: document.title || "",
      canonical_url: document.querySelector('link[rel="canonical"]')?.getAttribute("href") || "",
      entry_title: text(".nhle-entry__title"),
      statutory_location: text(".nhle-entry__statutory-location"),
      description: text(".nhle-entry__description"),
      overview: {
        summary: pairsToObject(collectDlRows("#overview .nhle-entry__overview-info")),
      },
      official_list_entry: {
        key_info: pairsToObject(collectDlRows("#official-list-entry .key-info")),
        location: pairsToObject(collectDlRows("#official-list-entry .nhle__location, #official-list-entry .nhle__location-info")),
        details_text: officialDetails,
        legacy: pairsToObject(collectDlRows("#official-list-entry .nhle-legacy")),
        legal_text: legalText,
        map: {
          image_alt: mapImage?.getAttribute("alt") || "",
          image_srcset: mapImage?.getAttribute("srcset") || "",
          pdf_url: mapPdfLink?.getAttribute("href") || "",
        },
      },
    };

    function pairsToObject(rows) {
      const out = {};
      for (const row of rows) {
        const key = String(row?.key || "").replace(/\s+/g, " ").trim();
        if (!key) continue;
        out[key] = String(row?.value || "").replace(/\s+/g, " ").trim();
      }
      return out;
    }
  });

  const payload = {
    ok: true,
    ref,
    url,
    source_mode: htmlFile ? "html-file" : "live-fetch",
    html_file: htmlFile || null,
    extracted_at_utc: new Date().toISOString(),
    ...extracted,
  };

  if (argv.json) {
    console.log(JSON.stringify(payload, null, 2));
  } else {
    console.log(payload);
  }
} finally {
  if (context) await context.close();
}
