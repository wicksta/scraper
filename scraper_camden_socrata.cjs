#!/usr/bin/env node

// Camden planning applications via Socrata (no browser automation).

const fs = require('fs');
const path = require('path');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .scriptName('camden-socrata-scrape')
  .option('ref', { type: 'string', describe: 'Application reference', demandOption: true })
  .option('start-url', {
    type: 'string',
    describe: 'Optional base Socrata URL (defaults to opendata.camden.gov.uk).',
    demandOption: false,
  })
  .option('dataset', {
    type: 'string',
    describe: 'Socrata dataset id',
    default: '2eiu-s2cw',
  })
  .option('mapper', { type: 'string', describe: 'Path to mapping module (.cjs)', demandOption: false })
  .option('area-name', { type: 'string', describe: 'LPA name', demandOption: false })
  .option('ons-code', { type: 'string', describe: 'ONS code', demandOption: false })
  .option('output', { type: 'string', describe: 'Optional output file path for UNIFIED JSON', demandOption: false })
  .strict()
  .help()
  .argv;

const q = argv.ref;

function parseSocrataBaseAndDataset(startUrl, datasetArg) {
  const fallbackBase = 'https://opendata.camden.gov.uk';
  const raw = String(startUrl || fallbackBase).trim();

  try {
    const u = new URL(raw);
    const m = u.pathname.match(/^\/resource\/([a-z0-9]{4}-[a-z0-9]{4})\.json$/i);
    if (m) {
      // If caller passes full resource URL, extract origin + dataset id.
      return { baseUrl: u.origin, datasetId: datasetArg || m[1] };
    }
    // Otherwise treat as base host.
    return { baseUrl: u.origin, datasetId: datasetArg || '2eiu-s2cw' };
  } catch {
    // Non-URL input; treat as base host string.
    return { baseUrl: raw.replace(/\/$/, ''), datasetId: datasetArg || '2eiu-s2cw' };
  }
}

const { baseUrl, datasetId } = parseSocrataBaseAndDataset(argv['start-url'], argv.dataset);
const mapperPath = argv.mapper;
const areaName = argv['area-name'] || null;
const onsCode = argv['ons-code'] || null;

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

function normaliseSocrataDateToISO(s) {
  if (!s) return null;
  const raw = String(s).trim();
  if (!raw || /^not available$/i.test(raw)) return null;
  // Common Socrata datetime: 2011-11-10T00:00:00.000
  const m = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
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

function soqlEscapeLiteral(value) {
  // SoQL string literal escaping: single quotes doubled.
  return String(value).replace(/'/g, "''");
}

async function fetchJson(url) {
  const headers = {
    Accept: 'application/json',
  };

  // Optional app token for higher rate limits.
  if (process.env.SOCRATA_APP_TOKEN) {
    headers['X-App-Token'] = process.env.SOCRATA_APP_TOKEN;
  }

  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} from Socrata. ${text.slice(0, 500)}`);
  }
  return await res.json();
}

(async () => {
  const ts = stamp();
  const base = `camden_socrata_${ts}`;

  const mapper = requireMapper(mapperPath);

  // Exact match by application_number.
  const where = `application_number='${soqlEscapeLiteral(q)}'`;
  const select = [
    'pk',
    'application_number',
    'development_address',
    'development_description',
    'decision_type',
    'valid_from_date',
    'registered_date',
    'earliest_decision_date',
    'decision_date',
    'decison_level',
    'system_status',
    'system_status_change_date',
    'applicant_name',
    'ward',
    'case_officer',
    'case_officer_team',
    'responsibility_type',
    'application_type',
    'easting',
    'northing',
    'longitude',
    'latitude',
    'spatial_accuracy',
  ].join(',');

  const queryUrl = `${baseUrl}/resource/${datasetId}.json?$select=${encodeURIComponent(select)}&$where=${encodeURIComponent(where)}&$limit=2`;
  console.log('Querying:', queryUrl);

  const rows = await fetchJson(queryUrl);
  if (!Array.isArray(rows)) {
    throw new Error(`Unexpected response from Socrata (expected array). Got: ${typeof rows}`);
  }

  if (rows.length === 0) {
    throw new Error(`No rows found for application_number='${q}' in dataset ${datasetId}.`);
  }

  if (rows.length > 1) {
    // This should be rare; keep deterministic but note it.
    console.warn(`⚠️ Multiple rows returned for ${q}; using the first.`);
  }

  const r = rows[0];

  const registeredDate = normaliseSocrataDateToISO(r.registered_date);
  const validatedDate = normaliseSocrataDateToISO(r.valid_from_date);
  const decidedDate = normaliseSocrataDateToISO(r.decision_date);
  const targetDate = normaliseSocrataDateToISO(r.earliest_decision_date);

  const detailsUrl = r.pk
    ? `https://planningrecords.camden.gov.uk/Northgate/Redirection/redirect.aspx?linkid=EXDC&PARAM0=${encodeURIComponent(r.pk)}`
    : null;

  const unified = {
    query: q,
    keyVal: r.pk || null,
    fetched_at: new Date().toISOString(),
    start_url: queryUrl,
    tabs: {
      summary: {
        title: `Camden ${q}`,
        url: detailsUrl || queryUrl,
        extracted: {
          headline: {
            reference: r.application_number || q,
            address: r.development_address || null,
            description: r.development_description || null,
          },
          tables: {
            simpleDetailsTable: {
              reference: r.application_number || q,
              address: r.development_address || null,
              proposal: r.development_description || null,
              status: r.system_status || null,
              decision: r.decision_type || null,
              application_received: registeredDate,
              application_validated: validatedDate,
              decision_issued_date: decidedDate,
            },
          },
          dl: {},
          raw: {
            socrata_row: r,
          },
        },
      },
      further_information: {
        title: `Camden ${q} (details)` ,
        url: detailsUrl || queryUrl,
        extracted: {
          headline: {
            reference: r.application_number || q,
            address: r.development_address || null,
            description: r.development_description || null,
          },
          tables: {
            applicationDetails: {
              application_type: r.application_type || null,
              expected_decision_level: r.decison_level || null,
              case_officer: r.case_officer || null,
              ward: r.ward || null,
              applicant_name: r.applicant_name || null,
            },
          },
          dl: {},
          raw: {
            case_officer_team: r.case_officer_team || null,
            responsibility_type: r.responsibility_type || null,
            spatial: {
              easting: r.easting || null,
              northing: r.northing || null,
              latitude: r.latitude || null,
              longitude: r.longitude || null,
              spatial_accuracy: r.spatial_accuracy || null,
            },
          },
        },
      },
      important_dates: {
        title: `Camden ${q} (dates)` ,
        url: detailsUrl || queryUrl,
        extracted: {
          headline: {
            reference: r.application_number || q,
            address: r.development_address || null,
            description: r.development_description || null,
          },
          tables: {
            simpleDetailsTable: {
              application_received_date: registeredDate,
              application_validated_date: validatedDate,
              decision_made_date: decidedDate,
              determination_deadline: targetDate,
              system_status_change_date: normaliseSocrataDateToISO(r.system_status_change_date),
            },
          },
          dl: {},
          raw: {},
        },
      },
    },
  };

  const unifiedPath = argv.output
    ? (path.isAbsolute(argv.output) ? argv.output : path.resolve(process.cwd(), argv.output))
    : path.resolve(process.cwd(), `${base}_UNIFIED.json`);

  fs.writeFileSync(unifiedPath, JSON.stringify(unified, null, 2), 'utf8');
  console.log(`\n✅ Unified JSON: ${unifiedPath}`);

  if (mapper) {
    const ctx = {
      area_name: areaName,
      ons_code: onsCode,
      scraper_name: mapper.scraperName || safeFilename(baseUrl),
      normaliseIdoxDateToISO: normaliseSocrataDateToISO,
      compactWhitespace,
      pickFirst,
    };

    const mapped = await mapper.mapToPlanit(unified, ctx);
    const planit = mapped?.planit || {};
    planit.scraper_name = planit.scraper_name || ctx.scraper_name;
    planit.source_url = planit.source_url || unified.tabs.summary?.url || unified.start_url;
    planit.url = planit.url || planit.source_url;

    const planitPath = path.resolve(process.cwd(), `${base}_PLANIT.json`);
    fs.writeFileSync(planitPath, JSON.stringify(mapped, null, 2), 'utf8');
    console.log(`✅ PlanIt-mapped JSON: ${planitPath}`);
  } else {
    console.log('ℹ️ No mapper provided (--mapper). Skipping PlanIt mapping step.');
  }
})();
