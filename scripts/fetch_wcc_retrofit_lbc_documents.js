#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { chromium } from "playwright";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("fetch-wcc-retrofit-lbc-documents")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to process. Default is Westminster.",
  })
  .option("limit", {
    type: "number",
    default: 100,
    describe: "Maximum matched applications to process per run.",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into the matched application set.",
  })
  .option("headed", {
    type: "boolean",
    default: false,
    describe: "Run Playwright headed for debugging.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 60000,
    describe: "Navigation timeout in milliseconds.",
  })
  .option("only-pending", {
    type: "boolean",
    default: true,
    describe: "When true, only process matched applications whose document inventory has not yet been scraped successfully.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/wcc_retrofit_lbc_document_inventory_summary.json",
    describe: "Path for a summary JSON artifact.",
  })
  .strict()
  .help()
  .argv;

const FIND_APPS_SQL = `
  SELECT
    id,
    ons_code,
    reference,
    keyval,
    documents_url,
    documents_scrape_status
  FROM public.wcc_retrofit_lbc_applications
  WHERE ons_code = $1
    AND is_candidate = true
    AND analysis_status = 'classified'
    AND NULLIF(BTRIM(keyval), '') IS NOT NULL
    AND (
      $2::boolean = false
      OR documents_scrape_status IN ('not_started', 'failed')
    )
  ORDER BY last_classified_at DESC NULLS LAST, reference ASC
  LIMIT $3 OFFSET $4
`;

const UPSERT_DOC_SQL = `
  INSERT INTO public.wcc_retrofit_lbc_document_inventory (
    application_analysis_id,
    ons_code,
    reference,
    documents_url,
    document_url,
    raw_href,
    description,
    document_type,
    published_date_raw,
    published_date,
    normalized_class,
    raw_source_json,
    scraped_at,
    updated_at
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::date, $11, $12::jsonb, now(), now()
  )
  ON CONFLICT (reference, document_url) DO UPDATE SET
    application_analysis_id = EXCLUDED.application_analysis_id,
    ons_code = EXCLUDED.ons_code,
    documents_url = EXCLUDED.documents_url,
    raw_href = EXCLUDED.raw_href,
    description = EXCLUDED.description,
    document_type = EXCLUDED.document_type,
    published_date_raw = EXCLUDED.published_date_raw,
    published_date = EXCLUDED.published_date,
    normalized_class = EXCLUDED.normalized_class,
    raw_source_json = EXCLUDED.raw_source_json,
    scraped_at = now(),
    updated_at = now()
`;

function getPgClientConfig() {
  return process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: process.env.PGHOST,
        port: process.env.PGPORT ? Number(process.env.PGPORT) : undefined,
        database: process.env.PGDATABASE,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
      };
}

function mkdirpForFile(filePath) {
  fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
}

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function normalizeText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function buildDocumentsUrl(keyval) {
  const clean = normalizeText(keyval);
  if (!clean) return null;
  return `https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=${encodeURIComponent(clean)}`;
}

function toAbsoluteUrl(baseUrl, href) {
  try {
    return new URL(href, baseUrl).toString();
  } catch {
    return null;
  }
}

function parseBritishDate(value) {
  const raw = normalizeText(value);
  if (!raw) return null;
  const match = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) return null;
  const [, d, m, y] = match;
  return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
}

function normalizeDocumentClass(doc) {
  const hay = `${normalizeText(doc.document_type)} ${normalizeText(doc.description)}`.toLowerCase();
  if (!hay) return "other";
  if (hay.includes("heritage statement")) return "heritage_statement";
  if (hay.includes("planning statement")) return "planning_statement";
  if (hay.includes("design and access")) return "design_access_statement";
  if (hay.includes("energy statement") || hay.includes("energy strategy")) return "energy_strategy";
  if (hay.includes("sustainability")) return "sustainability_statement";
  if (hay.includes("statement of significance")) return "statement_of_significance";
  if (hay.includes("cover letter") || hay.includes("covering letter")) return "cover_letter";
  if (hay.includes("application form")) return "application_form";
  if (hay.includes("existing") && hay.includes("plan")) return "existing_drawings";
  if (hay.includes("proposed") && hay.includes("plan")) return "proposed_drawings";
  if (hay.includes("drawing") || hay.includes("elevation") || hay.includes("section") || hay.includes("plan")) return "drawings";
  if (hay.includes("window") || hay.includes("glazing")) return "window_details";
  if (hay.includes("insulation")) return "insulation_details";
  if (hay.includes("heat pump") || hay.includes("solar") || hay.includes("pv")) return "retrofit_technical";
  return "other";
}

async function parseDocumentsTable(page) {
  return page.$eval("#Documents", (table) => {
    const docs = [];
    const headerCells = Array.from(table.querySelectorAll("thead th"));
    const normalizedHeaders = headerCells.map((cell) => (cell.textContent || "").trim().toLowerCase());
    const findHeaderIndex = (patterns) =>
      normalizedHeaders.findIndex((header) => patterns.some((pattern) => pattern.test(header)));

    const dateIdx = findHeaderIndex([/\bdate\b/, /\bpublished\b/]);
    const docTypeIdx = findHeaderIndex([/\bdocument\s*type\b/, /\btype\b/]);
    const descriptionIdx = findHeaderIndex([/\bdescription\b/, /\btitle\b/]);
    const rows = Array.from(table.querySelectorAll("tbody tr"));

    for (const row of rows) {
      const cells = row.querySelectorAll("td");
      if (!cells || cells.length < 4) continue;

      const cellAt = (headerIndex, fallbackIndexFromEnd) => {
        if (headerIndex >= 0 && headerIndex < cells.length) return cells[headerIndex];
        const idx = cells.length - fallbackIndexFromEnd;
        return idx >= 0 ? cells[idx] : null;
      };

      const viewCell = cellAt(-1, 1);
      const descriptionCell = cellAt(descriptionIdx, 2);
      const docTypeCell = cellAt(docTypeIdx, 3);
      const dateCell = cellAt(dateIdx, 4);

      const viewLink = viewCell?.querySelector("a")?.getAttribute("href") || null;
      if (!viewLink) continue;
      docs.push({
        date_published: dateCell?.textContent?.trim() || null,
        document_type: docTypeCell?.textContent?.trim() || null,
        description: descriptionCell?.textContent?.trim() || null,
        href: viewLink,
      });
    }
    return docs;
  });
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  const outputPath = path.resolve(String(argv["output-json"]));
  mkdirpForFile(outputPath);

  let browser = null;
  const summary = {
    ons_code: String(argv["ons-code"]),
    processed: 0,
    succeeded: 0,
    failed: 0,
    total_documents: 0,
    references: [],
  };

  try {
    const res = await client.query(FIND_APPS_SQL, [
      String(argv["ons-code"]),
      Boolean(argv["only-pending"]),
      Math.max(1, Number(argv.limit) || 100),
      Math.max(0, Number(argv.offset) || 0),
    ]);

    browser = await chromium.launch({ headless: !argv.headed });
    const context = await browser.newContext({
      extraHTTPHeaders: { "Accept-Language": "en-GB,en;q=0.9" },
    });

    for (const row of res.rows) {
      summary.processed += 1;
      const documentsUrl = row.documents_url || buildDocumentsUrl(row.keyval);
      const page = await context.newPage();
      try {
        if (!documentsUrl) {
          throw new Error("Missing documents URL / keyval");
        }
        logEvent("documents_start", { reference: row.reference, documents_url: documentsUrl });
        await page.goto(documentsUrl, { waitUntil: "domcontentloaded", timeout: Number(argv["timeout-ms"]) });
        await page.waitForSelector("#Documents tbody tr, table#Documents tbody tr", {
          timeout: Math.min(Number(argv["timeout-ms"]), 15000),
        });
        const docs = await parseDocumentsTable(page);
        const absDocs = docs
          .map((doc) => ({
            ...doc,
            document_url: toAbsoluteUrl(page.url(), doc.href),
            normalized_class: normalizeDocumentClass(doc),
            published_date: parseBritishDate(doc.date_published),
          }))
          .filter((doc) => doc.document_url);

        await client.query("BEGIN");
        try {
          for (const doc of absDocs) {
            await client.query(UPSERT_DOC_SQL, [
              row.id,
              row.ons_code,
              row.reference,
              documentsUrl,
              doc.document_url,
              doc.href,
              doc.description || null,
              doc.document_type || null,
              doc.date_published || null,
              doc.published_date,
              doc.normalized_class,
              JSON.stringify(doc),
            ]);
          }

          const urls = absDocs.map((doc) => doc.document_url);
          if (urls.length > 0) {
            await client.query(
              `DELETE FROM public.wcc_retrofit_lbc_document_inventory
               WHERE application_analysis_id = $1
                 AND document_url <> ALL($2::text[])`,
              [row.id, urls],
            );
          } else {
            await client.query(
              "DELETE FROM public.wcc_retrofit_lbc_document_inventory WHERE application_analysis_id = $1",
              [row.id],
            );
          }

          await client.query(
            `UPDATE public.wcc_retrofit_lbc_applications
             SET documents_url = $2,
                 documents_scrape_status = 'ok',
                 documents_scraped_at = now(),
                 documents_error = null,
                 document_count = $3,
                 updated_at = now()
             WHERE id = $1`,
            [row.id, documentsUrl, absDocs.length],
          );
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK");
          throw err;
        }

        summary.succeeded += 1;
        summary.total_documents += absDocs.length;
        summary.references.push({ reference: row.reference, document_count: absDocs.length });
        logEvent("documents_done", { reference: row.reference, document_count: absDocs.length });
      } catch (err) {
        summary.failed += 1;
        const message = err instanceof Error ? err.message : String(err);
        await client.query(
          `UPDATE public.wcc_retrofit_lbc_applications
           SET documents_scrape_status = 'failed',
               documents_scraped_at = now(),
               documents_error = $2,
               updated_at = now()
           WHERE id = $1`,
          [row.id, message],
        );
        logEvent("documents_failed", { reference: row.reference, error: message });
      } finally {
        await page.close().catch(() => {});
      }
    }

    await context.close().catch(() => {});
    fs.writeFileSync(outputPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    logEvent("done", { processed: summary.processed, succeeded: summary.succeeded, failed: summary.failed });
  } finally {
    if (browser) await browser.close().catch(() => {});
    await client.end().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack || err.message : String(err));
  process.exit(1);
});
