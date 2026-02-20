// application_materialize.js
//
// Consumes completed scrape_jobs and upserts into public.applications, while
// maintaining a simple idempotency log (application_ingest_log).
//
// IMPORTANT: import ./bootstrap.js first to load .env.
import "./bootstrap.js";

import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("applications-materialize")
  .option("limit", { type: "number", default: 50, describe: "Max jobs to ingest per batch." })
  .option("loop", { type: "boolean", default: false, describe: "Run forever." })
  .option("all", { type: "boolean", default: false, describe: "Process all pending completed jobs, then exit." })
  .option("sleep-ms", { type: "number", default: 30_000, describe: "Sleep between batches when looping." })
  .strict()
  .help()
  .argv;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

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

function pick(...vals) {
  for (const v of vals) {
    if (v == null) continue;
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) continue;
      if (/^not available$/i.test(s)) continue;
      return s;
    }
    return v;
  }
  return null;
}

function isoToDateOrNull(iso) {
  if (!iso) return null;
  const raw = String(iso).trim();
  if (!raw) return null;

  // ISO-ish fast path (YYYY-MM-DD or timestamp prefix)
  const isoMatch = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1];

  // Strip optional label prefix, e.g. "Committee: Tue 30 Sep 2025"
  const stripped = raw.includes(":") ? raw.split(":").slice(1).join(":").trim() : raw;

  // Idox-style day-prefixed date, e.g. "Tue 30 Sep 2025"
  const dayPrefixed = stripped.match(/^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})$/);
  if (dayPrefixed) {
    const day = Number(dayPrefixed[1]);
    const mon = dayPrefixed[2].toLowerCase();
    const year = Number(dayPrefixed[3]);
    const mmByMon = {
      jan: "01",
      feb: "02",
      mar: "03",
      apr: "04",
      may: "05",
      jun: "06",
      jul: "07",
      aug: "08",
      sep: "09",
      oct: "10",
      nov: "11",
      dec: "12",
    };
    const mm = mmByMon[mon];
    if (Number.isInteger(day) && day >= 1 && day <= 31 && Number.isInteger(year) && mm) {
      return `${year}-${mm}-${String(day).padStart(2, "0")}`;
    }
  }

  // Also accept "30 Sep 2025" without weekday.
  const shortDate = stripped.match(/^(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})$/);
  if (shortDate) {
    const day = Number(shortDate[1]);
    const mon = shortDate[2].toLowerCase();
    const year = Number(shortDate[3]);
    const mmByMon = {
      jan: "01",
      feb: "02",
      mar: "03",
      apr: "04",
      may: "05",
      jun: "06",
      jul: "07",
      aug: "08",
      sep: "09",
      oct: "10",
      nov: "11",
      dec: "12",
    };
    const mm = mmByMon[mon];
    if (Number.isInteger(day) && day >= 1 && day <= 31 && Number.isInteger(year) && mm) {
      return `${year}-${mm}-${String(day).padStart(2, "0")}`;
    }
  }

  return null;
}

function numOrNull(x) {
  if (x == null || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function normalizeLookupKey(key) {
  return String(key || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function hasMeaningfulValue(value) {
  if (value == null) return false;
  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return false;
    if (/^not available$/i.test(s)) return false;
    return true;
  }
  return true;
}

function findFirstValueByKeys(input, wantedKeys) {
  const wanted = new Set(wantedKeys.map(normalizeLookupKey));
  const queue = [input];

  while (queue.length > 0) {
    const node = queue.shift();
    if (node == null) continue;

    if (Array.isArray(node)) {
      for (const item of node) queue.push(item);
      continue;
    }

    if (typeof node !== "object") continue;

    for (const [rawKey, rawVal] of Object.entries(node)) {
      const normalizedKey = normalizeLookupKey(rawKey);
      if (wanted.has(normalizedKey) && hasMeaningfulValue(rawVal) && typeof rawVal !== "object") {
        return rawVal;
      }
      if (rawVal && typeof rawVal === "object") queue.push(rawVal);
    }
  }

  return null;
}

function planitFromJobResult(result) {
  // result.planit = { planit, warnings, unmapped }
  const p = result?.planit?.planit || null;
  return p && typeof p === "object" ? p : null;
}

function unifiedFromJobResult(result) {
  const u = result?.unified || null;
  return u && typeof u === "object" ? u : null;
}

function extractAlternativeReference(planit, unified) {
  // Legacy has "Alternative Reference". Prefer Idox alt ref, else planning portal id.
  const sTable = unified?.tabs?.summary?.extracted?.tables?.simpleDetailsTable || {};
  return pick(sTable.alternative_reference, planit?.planning_portal_id);
}

function extractDistrictReference(unified) {
  // Not consistently exposed; keep null unless present in unified tables.
  const sTable = unified?.tabs?.summary?.extracted?.tables?.simpleDetailsTable || {};
  return pick(sTable.district_reference, null);
}

async function ingestOne(client, job) {
  const result = job.result || {};
  const planit = planitFromJobResult(result);
  const unified = unifiedFromJobResult(result);
  const fallbackDetails = result?.planit?.unmapped?.source?.applicationDetails || null;
  const fallbackSource = result?.planit?.unmapped?.source || null;

  const reference = String(pick(job.application_ref, planit?.uid) || "").trim();
  if (!reference) throw new Error("Missing reference/application_ref.");

  const onsCode = String(job.ons_code || "").trim();
  if (!onsCode) throw new Error("Missing ons_code.");

  const alternativeReference = extractAlternativeReference(planit, unified);
  const districtReference = extractDistrictReference(unified);

  const applicationReceived = isoToDateOrNull(pick(planit?.date_received, planit?.dateReceived));
  const applicationValidated = isoToDateOrNull(pick(planit?.date_validated, planit?.start_date));

  const address = pick(planit?.address);
  const proposal = pick(planit?.description);

  const status = pick(planit?.app_state, planit?.status);
  const decision = pick(planit?.decision, planit?.status);

  const appealStatus = pick(planit?.appeal_status);
  const appealDecision = pick(planit?.appeal_decision);

  const applicationType = pick(planit?.app_type);
  const expectedDecisionLevel = pick(
    planit?.expected_decision_level,
    findFirstValueByKeys(fallbackDetails, ["expected_decision_level"]),
    findFirstValueByKeys(fallbackSource, ["expected_decision_level"]),
  );
  const actualDecisionLevel = pick(
    planit?.actual_decision_level,
    findFirstValueByKeys(fallbackDetails, ["actual_decision_level"]),
    findFirstValueByKeys(fallbackSource, ["actual_decision_level"]),
  );

  const caseOfficer = pick(planit?.case_officer);
  const parish = pick(planit?.parish);
  const ward = pick(planit?.ward_name);
  const amenitySociety = pick(planit?.amenity_society);

  const applicantName = pick(planit?.applicant_name);
  const applicantAddress = pick(planit?.applicant_address);

  const agentName = pick(planit?.agent_name);
  const agentCompanyName = pick(planit?.agent_company);
  const agentAddress = pick(planit?.agent_address);

  const environmentalAssessmentRequested = pick(planit?.environmental_assessment_requested);

  const actualCommitteeDate = isoToDateOrNull(
    pick(
      planit?.committee_date,
      planit?.actual_committee_date,
      findFirstValueByKeys(fallbackDetails, ["actual_committee_date"]),
      findFirstValueByKeys(fallbackSource, ["actual_committee_date"]),
    ),
  );
  const agreedExpiryDate = isoToDateOrNull(pick(planit?.agreed_expiry_date));
  const lastAdvertisedInPressDate = isoToDateOrNull(pick(planit?.last_advertised_in_press_date));
  const latestAdvertisementExpiryDate = isoToDateOrNull(pick(planit?.latest_advertisement_expiry_date));
  const lastSiteNoticePostedDate = isoToDateOrNull(pick(planit?.last_site_notice_posted_date));
  const latestSiteNoticeExpiryDate = isoToDateOrNull(pick(planit?.latest_site_notice_expiry_date));
  const decisionMadeDate = isoToDateOrNull(pick(planit?.decision_made_date, planit?.decided_date));
  const decisionIssuedDate = isoToDateOrNull(pick(planit?.decision_issued_date, planit?.decided_date));
  const targetDate = isoToDateOrNull(pick(planit?.target_decision_date));
  const temporaryPermissionExpiryDate = isoToDateOrNull(pick(planit?.temporary_permission_expiry_date));

  const lat = numOrNull(pick(planit?.lat));
  const lon = numOrNull(pick(planit?.lng));

  const major = pick(planit?.major);
  const spare2 = numOrNull(pick(planit?.spare2));

  const keyval = pick(unified?.keyVal);
  const sourceUrl = pick(planit?.source_url, planit?.url);

  // Upsert, preserving date_added + first_seen_at.
  await client.query(
    `
      INSERT INTO public.applications (
        ons_code, reference,
        alternative_reference, district_reference,
        application_received, application_validated,
        address, proposal,
        status, decision, appeal_status, appeal_decision,
        application_type, expected_decision_level, actual_decision_level,
        case_officer, parish, ward, amenity_society,
        applicant_name, applicant_address,
        agent_name, agent_company_name, agent_address,
        environmental_assessment_requested,
        actual_committee_date, agreed_expiry_date,
        last_advertised_in_press_date, latest_advertisement_expiry_date,
        last_site_notice_posted_date, latest_site_notice_expiry_date,
        decision_made_date, decision_issued_date,
        target_date, temporary_permission_expiry_date,
        lat, lon,
        major, spare2,
        keyval,
        source_url, unified_json, planit_json, scrape_job_id,
        last_look, scraped_at, updated_at
      ) VALUES (
        $1, $2,
        $3, $4,
        $5::date, $6::date,
        $7, $8,
        $9, $10, $11, $12,
        $13, $14, $15,
        $16, $17, $18, $19,
        $20, $21,
        $22, $23, $24,
        $25,
        $26::date, $27::date,
        $28::date, $29::date,
        $30::date, $31::date,
        $32::date, $33::date,
        $34::date, $35::date,
        $36, $37,
        $38, $39,
        $40,
        $41, $42::jsonb, $43::jsonb, $44,
        current_date, now(), now()
      )
      ON CONFLICT (ons_code, reference) DO UPDATE SET
        alternative_reference = COALESCE(EXCLUDED.alternative_reference, applications.alternative_reference),
        district_reference = COALESCE(EXCLUDED.district_reference, applications.district_reference),
        application_received = COALESCE(EXCLUDED.application_received, applications.application_received),
        application_validated = COALESCE(EXCLUDED.application_validated, applications.application_validated),
        address = COALESCE(EXCLUDED.address, applications.address),
        proposal = COALESCE(EXCLUDED.proposal, applications.proposal),
        status = COALESCE(EXCLUDED.status, applications.status),
        decision = COALESCE(EXCLUDED.decision, applications.decision),
        appeal_status = COALESCE(EXCLUDED.appeal_status, applications.appeal_status),
        appeal_decision = COALESCE(EXCLUDED.appeal_decision, applications.appeal_decision),
        application_type = COALESCE(EXCLUDED.application_type, applications.application_type),
        expected_decision_level = COALESCE(EXCLUDED.expected_decision_level, applications.expected_decision_level),
        actual_decision_level = COALESCE(EXCLUDED.actual_decision_level, applications.actual_decision_level),
        case_officer = COALESCE(EXCLUDED.case_officer, applications.case_officer),
        parish = COALESCE(EXCLUDED.parish, applications.parish),
        ward = COALESCE(EXCLUDED.ward, applications.ward),
        amenity_society = COALESCE(EXCLUDED.amenity_society, applications.amenity_society),
        applicant_name = COALESCE(EXCLUDED.applicant_name, applications.applicant_name),
        applicant_address = COALESCE(EXCLUDED.applicant_address, applications.applicant_address),
        agent_name = COALESCE(EXCLUDED.agent_name, applications.agent_name),
        agent_company_name = COALESCE(EXCLUDED.agent_company_name, applications.agent_company_name),
        agent_address = COALESCE(EXCLUDED.agent_address, applications.agent_address),
        environmental_assessment_requested = COALESCE(EXCLUDED.environmental_assessment_requested, applications.environmental_assessment_requested),
        actual_committee_date = COALESCE(EXCLUDED.actual_committee_date, applications.actual_committee_date),
        agreed_expiry_date = COALESCE(EXCLUDED.agreed_expiry_date, applications.agreed_expiry_date),
        last_advertised_in_press_date = COALESCE(EXCLUDED.last_advertised_in_press_date, applications.last_advertised_in_press_date),
        latest_advertisement_expiry_date = COALESCE(EXCLUDED.latest_advertisement_expiry_date, applications.latest_advertisement_expiry_date),
        last_site_notice_posted_date = COALESCE(EXCLUDED.last_site_notice_posted_date, applications.last_site_notice_posted_date),
        latest_site_notice_expiry_date = COALESCE(EXCLUDED.latest_site_notice_expiry_date, applications.latest_site_notice_expiry_date),
        decision_made_date = COALESCE(EXCLUDED.decision_made_date, applications.decision_made_date),
        decision_issued_date = COALESCE(EXCLUDED.decision_issued_date, applications.decision_issued_date),
        target_date = COALESCE(EXCLUDED.target_date, applications.target_date),
        temporary_permission_expiry_date = COALESCE(EXCLUDED.temporary_permission_expiry_date, applications.temporary_permission_expiry_date),
        lat = COALESCE(EXCLUDED.lat, applications.lat),
        lon = COALESCE(EXCLUDED.lon, applications.lon),
        major = COALESCE(EXCLUDED.major, applications.major),
        spare2 = COALESCE(EXCLUDED.spare2, applications.spare2),
        keyval = COALESCE(EXCLUDED.keyval, applications.keyval),
        source_url = COALESCE(EXCLUDED.source_url, applications.source_url),
        unified_json = COALESCE(EXCLUDED.unified_json, applications.unified_json),
        planit_json = COALESCE(EXCLUDED.planit_json, applications.planit_json),
        scrape_job_id = GREATEST(COALESCE(applications.scrape_job_id, 0), COALESCE(EXCLUDED.scrape_job_id, 0)),
        last_look = current_date,
        scraped_at = now(),
        updated_at = now()
    `,
    [
      onsCode,
      reference,
      alternativeReference,
      districtReference,
      applicationReceived,
      applicationValidated,
      address,
      proposal,
      status,
      decision,
      appealStatus,
      appealDecision,
      applicationType,
      expectedDecisionLevel,
      actualDecisionLevel,
      caseOfficer,
      parish,
      ward,
      amenitySociety,
      applicantName,
      applicantAddress,
      agentName,
      agentCompanyName,
      agentAddress,
      environmentalAssessmentRequested,
      actualCommitteeDate,
      agreedExpiryDate,
      lastAdvertisedInPressDate,
      latestAdvertisementExpiryDate,
      lastSiteNoticePostedDate,
      latestSiteNoticeExpiryDate,
      decisionMadeDate,
      decisionIssuedDate,
      targetDate,
      temporaryPermissionExpiryDate,
      lat,
      lon,
      major,
      spare2,
      keyval,
      sourceUrl,
      JSON.stringify(unified),
      JSON.stringify(result?.planit || null),
      job.id,
    ],
  );

  await client.query(
    `
      INSERT INTO public.application_ingest_log (scrape_job_id, ons_code, application_ref, error)
      VALUES ($1, $2, $3, NULL)
      ON CONFLICT (scrape_job_id) DO UPDATE SET ingested_at = now(), error = NULL
    `,
    [job.id, onsCode, reference],
  );

  return { reference };
}

async function ingestBatch(client, limit) {
  const { rows } = await client.query(
    `
      SELECT j.id, j.ons_code, j.application_ref, j.result
      FROM public.scrape_jobs j
      LEFT JOIN public.application_ingest_log l
        ON l.scrape_job_id = j.id
      WHERE j.status = 'completed'
        AND j.ons_code IS NOT NULL
        AND j.application_ref IS NOT NULL
        AND l.scrape_job_id IS NULL
      ORDER BY j.updated_at ASC, j.id ASC
      LIMIT $1
    `,
    [limit],
  );

  let ok = 0;
  let failed = 0;

  for (const job of rows) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await ingestOne(client, job);
      ok++;
    } catch (e) {
      failed++;
      const msg = e instanceof Error ? e.message : String(e);
      await client.query(
        `
          INSERT INTO public.application_ingest_log (scrape_job_id, ons_code, application_ref, error)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (scrape_job_id) DO UPDATE SET ingested_at = now(), error = EXCLUDED.error
        `,
        [job.id, job.ons_code, job.application_ref, msg],
      );
    }
  }

  return { scanned: rows.length, ok, failed };
}

async function main() {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    if (argv.loop) {
      do {
        const r = await ingestBatch(client, Number(argv.limit || 50));
        console.log(JSON.stringify({ ok: true, ...r }));
        await sleep(Number(argv["sleep-ms"] || 30_000));
      } while (true);
    } else if (argv.all) {
      while (true) {
        const r = await ingestBatch(client, Number(argv.limit || 50));
        console.log(JSON.stringify({ ok: true, ...r }));
        if (!r.scanned) break;
      }
    } else {
      const r = await ingestBatch(client, Number(argv.limit || 50));
      console.log(JSON.stringify({ ok: true, ...r }));
    }
  } finally {
    await client.end();
  }
}

main().catch((e) => {
  console.error("[materialize] fatal:", e);
  process.exit(1);
});
