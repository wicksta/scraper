#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("summarize-wcc-retrofit-lbc-document-packs")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to summarise. Default is Westminster.",
  })
  .option("themes", {
    type: "string",
    default: "solar_pv,heat_pump,windows_glazing,insulation",
    describe: "Comma-separated retrofit themes to include.",
  })
  .option("output-json", {
    type: "string",
    default: "./tmp/wcc_retrofit_lbc_document_pack_summary.json",
    describe: "Path for the summary JSON artifact.",
  })
  .option("output-md", {
    type: "string",
    default: "./tmp/wcc_retrofit_lbc_document_pack_summary.md",
    describe: "Path for a Markdown summary artifact.",
  })
  .strict()
  .help()
  .argv;

const THEME_SQL = `
  WITH theme_apps AS (
    SELECT
      a.id,
      a.reference,
      t.theme
    FROM public.wcc_retrofit_lbc_applications a
    CROSS JOIN LATERAL jsonb_array_elements_text(a.retrofit_themes) AS t(theme)
    WHERE a.ons_code = $1
      AND a.is_candidate = true
      AND a.analysis_status = 'classified'
      AND t.theme = ANY($2::text[])
  ),
  filtered_docs AS (
    SELECT
      ta.theme,
      ta.reference,
      p.normalized_pack_class,
      p.normalized_pack_label,
      d.description
    FROM theme_apps ta
    JOIN public.wcc_retrofit_lbc_document_pack_analysis p
      ON p.application_analysis_id = ta.id
    JOIN public.wcc_retrofit_lbc_document_inventory d
      ON d.id = p.inventory_id
    WHERE p.analysis_status = 'classified'
      AND p.include_in_pack = true
      AND p.normalized_pack_class IS NOT NULL
  ),
  examples AS (
    SELECT
      theme,
      normalized_pack_class,
      ARRAY(
        SELECT DISTINCT NULLIF(BTRIM(f2.description), '')
        FROM filtered_docs f2
        WHERE f2.theme = f1.theme
          AND f2.normalized_pack_class = f1.normalized_pack_class
        ORDER BY NULLIF(BTRIM(f2.description), '')
        LIMIT 5
      ) AS example_titles
    FROM filtered_docs f1
    GROUP BY theme, normalized_pack_class
  )
  SELECT
    fd.theme,
    fd.normalized_pack_class,
    MIN(fd.normalized_pack_label) AS normalized_pack_label,
    COUNT(*)::int AS document_count,
    COUNT(DISTINCT fd.reference)::int AS application_count,
    ta_total.total_applications,
    ROUND((COUNT(DISTINCT fd.reference)::numeric / NULLIF(ta_total.total_applications, 0)::numeric) * 100.0, 1) AS application_pct,
    ex.example_titles
  FROM filtered_docs fd
  JOIN (
    SELECT theme, COUNT(DISTINCT reference)::int AS total_applications
    FROM theme_apps
    GROUP BY theme
  ) ta_total
    ON ta_total.theme = fd.theme
  JOIN examples ex
    ON ex.theme = fd.theme
   AND ex.normalized_pack_class = fd.normalized_pack_class
  GROUP BY
    fd.theme,
    fd.normalized_pack_class,
    ta_total.total_applications,
    ex.example_titles
  ORDER BY fd.theme, application_count DESC, document_count DESC, fd.normalized_pack_class ASC
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

function normalizeText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function splitThemes(raw) {
  return String(raw || "")
    .split(",")
    .map((item) => normalizeText(item))
    .filter(Boolean);
}

function titleForTheme(theme) {
  return ({
    solar_pv: "Solar / PV",
    heat_pump: "Heat pumps",
    windows_glazing: "Windows / glazing",
    insulation: "Insulation",
  })[theme] || theme;
}

function classifyBucket(normalizedPackClass) {
  const core = new Set([
    "application_form",
    "location_site_plan",
    "existing_drawings",
    "proposed_drawings",
    "design_access_statement",
    "heritage_statement",
    "planning_statement",
    "energy_sustainability_statement",
    "cover_letter",
  ]);
  return core.has(normalizedPackClass) ? "common_core_documents" : "extra_supporting_documents";
}

function renderMarkdown(summary) {
  const lines = [];
  lines.push("# Westminster Retrofit LBC Document Packs");
  lines.push("");
  lines.push(`Date: ${new Date().toISOString().slice(0, 10)}`);
  lines.push("");
  lines.push("This summary shows the applicant-side supporting documents most commonly submitted for each key Westminster retrofit theme. Procedural and council-generated case-management documents have been excluded.");
  lines.push("");

  for (const theme of summary.themes) {
    lines.push(`## ${titleForTheme(theme.theme)}`);
    lines.push("");
    lines.push(`Candidate applications in theme: ${theme.total_applications}`);
    lines.push("");
    lines.push("| Bucket | Document class | Documents | Applications | % of theme applications | Example titles |");
    lines.push("|---|---|---:|---:|---:|---|");
    for (const row of theme.rows) {
      lines.push(
        `| ${row.bucket === "common_core_documents" ? "Common core" : "Extra supporting"} | ${row.normalized_pack_label} | ${row.document_count} | ${row.application_count} | ${row.application_pct}% | ${row.example_titles.join("; ")} |`
      );
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  const themes = splitThemes(argv.themes);
  if (themes.length === 0) {
    throw new Error("No themes supplied.");
  }

  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const outputJson = path.resolve(String(argv["output-json"]));
    const outputMd = path.resolve(String(argv["output-md"]));
    mkdirpForFile(outputJson);
    mkdirpForFile(outputMd);

    const res = await client.query(THEME_SQL, [String(argv["ons-code"]), themes]);
    const byTheme = new Map();
    for (const theme of themes) {
      byTheme.set(theme, { theme, total_applications: 0, rows: [] });
    }

    for (const row of res.rows) {
      const themeEntry = byTheme.get(row.theme) || { theme: row.theme, total_applications: 0, rows: [] };
      themeEntry.total_applications = Number(row.total_applications) || themeEntry.total_applications;
      themeEntry.rows.push({
        normalized_pack_class: row.normalized_pack_class,
        normalized_pack_label: row.normalized_pack_label,
        document_count: Number(row.document_count) || 0,
        application_count: Number(row.application_count) || 0,
        application_pct: Number(row.application_pct) || 0,
        example_titles: Array.isArray(row.example_titles) ? row.example_titles.filter(Boolean).map((x) => normalizeText(x)) : [],
        bucket: classifyBucket(row.normalized_pack_class),
      });
      byTheme.set(row.theme, themeEntry);
    }

    const summary = {
      ons_code: String(argv["ons-code"]),
      themes: Array.from(byTheme.values()),
    };

    fs.writeFileSync(outputJson, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    fs.writeFileSync(outputMd, renderMarkdown(summary), "utf8");
    process.stdout.write(`${JSON.stringify({ ok: true, output_json: outputJson, output_md: outputMd, themes: themes.length })}\n`);
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  process.stderr.write(`${error?.stack || error?.message || String(error)}\n`);
  process.exit(1);
});
