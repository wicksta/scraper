#!/usr/bin/env node
import "../bootstrap.js";

import mysql from "mysql2/promise";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("materialize-newmark-apps-to-mysql")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code scope.",
  })
  .option("limit", {
    type: "number",
    default: 300,
    describe: "Max Newmark applications to process.",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into candidate rows.",
  })
  .option("submitted-by-user-id", {
    type: "number",
    default: null,
    describe: "Fallback submitted_by for app_metadata when team user IDs are missing.",
  })
  .option("dry-run", {
    type: "boolean",
    default: false,
    describe: "If true, read and log only; no MySQL writes.",
  })
  .option("continue-on-error", {
    type: "boolean",
    default: true,
    describe: "Continue processing remaining rows on row-level errors.",
  })
  .strict()
  .help()
  .argv;

const PG_SOURCE_SQL = `
  SELECT
    a.id,
    a.ons_code,
    a.reference,
    a.alternative_reference,
    a.application_received,
    a.application_validated,
    a.address,
    a.proposal,
    a.status,
    a.decision,
    a.application_type,
    a.case_officer,
    a.applicant_name,
    a.applicant_address,
    a.agent_name,
    a.agent_company_name,
    a.agent_address,
    a.lat,
    a.lon,
    a.target_date,
    a.decision_made_date,
    a.decision_issued_date,
    a.last_site_notice_posted_date,
    a.keyval,
    a.source_url,
    a.planit_json,
    a.scraped_at,
    a.updated_at,
    c.job_codes_found,
    c.job_code_parts
  FROM public.applications a
  LEFT JOIN public.newmark_jobcode_candidates c
    ON c.ons_code = a.ons_code
   AND c.reference = a.reference
  WHERE ($1::text IS NULL OR a.ons_code = $1)
    AND a.application_validated >= DATE '2026-01-01'
    AND a.application_validated < DATE '2027-01-01'
    AND (
      COALESCE(a.agent_company_name, '') ILIKE '%newmark%'
      OR COALESCE(a.agent_company_name, '') ILIKE '%gerald eve%'
      OR COALESCE(a.agent_name, '') ILIKE '%newmark%'
      OR COALESCE(a.agent_name, '') ILIKE '%gerald eve%'
      OR COALESCE(a.applicant_name, '') ILIKE '%newmark%'
      OR COALESCE(a.applicant_name, '') ILIKE '%gerald eve%'
      OR COALESCE(to_jsonb(a) ->> 'applicant_company_name', '') ILIKE '%newmark%'
      OR COALESCE(to_jsonb(a) ->> 'applicant_company_name', '') ILIKE '%gerald eve%'
      OR COALESCE(a.planit_json #>> '{planit,agent_company}', '') ILIKE '%newmark%'
      OR COALESCE(a.planit_json #>> '{planit,agent_company}', '') ILIKE '%gerald eve%'
      OR COALESCE(a.planit_json #>> '{planit,applicant_name}', '') ILIKE '%newmark%'
      OR COALESCE(a.planit_json #>> '{planit,applicant_name}', '') ILIKE '%gerald eve%'
    )
  ORDER BY COALESCE(a.application_validated, a.application_received, a.date_added) ASC NULLS LAST, a.reference ASC
  LIMIT $2 OFFSET $3
`;

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function requireEnv(name) {
  const value = process.env[name];
  if (value == null || String(value).trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return String(value);
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

function parseJson(value) {
  if (value == null) return null;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function dateOnly(input, fallback = null) {
  if (!input) return fallback;
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return fallback;
  return d.toISOString().slice(0, 10);
}

function dateTime(input, fallback = null) {
  if (!input) return fallback;
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return fallback;
  const iso = d.toISOString().slice(0, 19);
  return iso.replace("T", " ");
}

function intOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function cleanText(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

function normalizeInitialToken(token) {
  const cleaned = String(token || "")
    .toUpperCase()
    .replace(/[^A-Z]/g, "");
  if (!cleaned) return null;
  if (cleaned.length < 2 || cleaned.length > 6) return null;
  return cleaned;
}

function parseArrayJson(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function buildDocsUrl(keyval) {
  const k = cleanText(keyval);
  if (!k) return null;
  return `https://idoxpa.westminster.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=${encodeURIComponent(k)}`;
}

function extractJobCode(jobCodePartsRaw, jobCodesFoundRaw) {
  const parts = parseArrayJson(jobCodePartsRaw);
  for (const p of parts) {
    const jobNumber = cleanText(p?.job_number);
    if (jobNumber) return jobNumber;
  }
  const found = parseArrayJson(jobCodesFoundRaw);
  for (const code of found) {
    const s = String(code || "");
    const m = s.match(/\b([UJ][0-9]{3,8})\b/i);
    if (m) return m[1].toUpperCase();
  }
  return null;
}

function extractInitialTokens(jobCodePartsRaw) {
  const parts = parseArrayJson(jobCodePartsRaw);
  const out = [];
  const seen = new Set();
  for (const p of parts) {
    const partner = normalizeInitialToken(p?.partner_initials);
    if (partner && !seen.has(partner)) {
      seen.add(partner);
      out.push(partner);
    }
    const assistants = Array.isArray(p?.assistant_initials) ? p.assistant_initials : [];
    for (const a of assistants) {
      const tok = normalizeInitialToken(a);
      if (tok && !seen.has(tok)) {
        seen.add(tok);
        out.push(tok);
      }
    }
  }
  return out;
}

function pickPlanitObj(appRow) {
  const pj = parseJson(appRow.planit_json);
  if (!pj || typeof pj !== "object") return {};
  if (pj.planit && typeof pj.planit === "object") return pj.planit;
  return pj;
}

function mapToPlanitFields(appRow) {
  const planit = pickPlanitObj(appRow);
  const uid = cleanText(appRow.reference);
  const name = cleanText(planit.name) || `${cleanText(appRow.ons_code)}/${uid}`;
  const lat = numOrNull(appRow.lat);
  const lng = numOrNull(appRow.lon);
  const startDate = dateOnly(appRow.application_validated, dateOnly(appRow.application_received, new Date().toISOString().slice(0, 10)));
  const docsUrl = cleanText(planit.docs_url) || buildDocsUrl(appRow.keyval);
  const srcUrl = cleanText(appRow.source_url) || cleanText(planit.source_url) || cleanText(planit.url);

  return {
    uid,
    name,
    address: cleanText(appRow.address),
    postcode: cleanText(planit.postcode),
    ward_name: cleanText(planit.ward_name),
    area_id: intOrNull(planit.area_id),
    area_name: cleanText(planit.area_name),
    ons_code: cleanText(appRow.ons_code),
    app_type: cleanText(appRow.application_type),
    app_size: cleanText(planit.app_size),
    app_state: cleanText(appRow.status),
    application_type: cleanText(appRow.application_type),
    status: cleanText(appRow.decision) || cleanText(appRow.status),
    associated_id: cleanText(appRow.alternative_reference),
    description: cleanText(appRow.proposal),
    link: cleanText(planit.link) || srcUrl,
    source_url: srcUrl,
    docs_url: docsUrl,
    comment_url: cleanText(planit.comment_url),
    map_url: cleanText(planit.map_url),
    planning_portal_id: cleanText(planit.planning_portal_id) || cleanText(appRow.alternative_reference),
    agent_name: cleanText(appRow.agent_name),
    agent_company: cleanText(appRow.agent_company_name),
    agent_address: cleanText(appRow.agent_address),
    applicant_name: cleanText(appRow.applicant_name),
    case_officer: cleanText(appRow.case_officer),
    location_x: numOrNull(planit.location_x) ?? lng,
    location_y: numOrNull(planit.location_y) ?? lat,
    easting: intOrNull(planit.easting),
    northing: intOrNull(planit.northing),
    lat,
    lng,
    n_documents: intOrNull(planit.n_documents),
    n_comments: intOrNull(planit.n_comments),
    n_statutory_days: intOrNull(planit.n_statutory_days),
    date_received: dateOnly(appRow.application_received),
    date_validated: dateOnly(appRow.application_validated),
    start_date: startDate,
    target_decision_date: dateOnly(appRow.target_date),
    decided_date: dateOnly(appRow.decision_issued_date, dateOnly(appRow.decision_made_date)),
    consulted_date: dateOnly(appRow.last_site_notice_posted_date),
    last_changed: dateTime(appRow.updated_at),
    last_different: dateTime(appRow.updated_at),
    last_scraped: dateTime(appRow.scraped_at),
    scraper_name: "pg_newmark_materializer",
    url: srcUrl,
    last_planit_api_check: null,
    cannot_find: 0,
  };
}

function mapToNmrkFields(appRow, planitFields, jobCode) {
  const appType = cleanText(appRow.application_type) || "Unknown";
  const devDesc = cleanText(appRow.proposal) || appType;
  return {
    ref_num: cleanText(appRow.reference),
    application_type: appType,
    app_description: cleanText(appRow.proposal),
    submission_date: dateOnly(appRow.application_received, dateOnly(appRow.application_validated)),
    site_address: cleanText(appRow.address),
    applicant_org: cleanText(appRow.applicant_name),
    agent_org: cleanText(appRow.agent_company_name) || cleanText(appRow.agent_name),
    dev_description: devDesc,
    upload_date: dateTime(appRow.scraped_at),
    job_code: cleanText(jobCode),
    lpa_code: cleanText(planitFields.ons_code),
    lpa_name: cleanText(planitFields.area_name),
    ons_code: cleanText(appRow.ons_code),
    parent_permission: cleanText(appRow.alternative_reference),
    added_by: "pg_newmark_materializer",
    lat: numOrNull(planitFields.lat),
    lng: numOrNull(planitFields.lng),
  };
}

async function getDefaultSubmittedBy(mysqlConn, explicitUserId) {
  if (explicitUserId != null) return Number(explicitUserId);
  const [rows] = await mysqlConn.query(
    `
      SELECT id
      FROM users
      WHERE is_admin = 1
      ORDER BY is_verified DESC, updated_at DESC, id DESC
      LIMIT 1
    `,
  );
  if (!rows.length) return null;
  return Number(rows[0].id);
}

async function loadResolverUserMap(mysqlConn) {
  const [rows] = await mysqlConn.query(
    `
      SELECT initial_token, user_id
      FROM newmark_initial_resolver
      WHERE user_id IS NOT NULL
    `,
  );
  const out = new Map();
  for (const row of rows) {
    const token = normalizeInitialToken(row.initial_token);
    if (!token) continue;
    out.set(token, Number(row.user_id));
  }
  return out;
}

async function upsertPlanit(mysqlConn, p) {
  const [existingRows] = await mysqlConn.query(
    "SELECT id FROM planit_applications WHERE uid = ? AND ons_code = ? LIMIT 1",
    [p.uid, p.ons_code],
  );

  if (existingRows.length) {
    const id = Number(existingRows[0].id);
    await mysqlConn.query(
      `
        UPDATE planit_applications
        SET
          name = ?,
          address = ?,
          postcode = ?,
          ward_name = ?,
          area_id = ?,
          area_name = ?,
          app_type = ?,
          app_size = ?,
          app_state = ?,
          application_type = ?,
          status = ?,
          associated_id = ?,
          description = ?,
          link = ?,
          source_url = ?,
          docs_url = ?,
          comment_url = ?,
          map_url = ?,
          planning_portal_id = ?,
          agent_name = ?,
          agent_company = ?,
          agent_address = ?,
          applicant_name = ?,
          case_officer = ?,
          location_x = ?,
          location_y = ?,
          easting = ?,
          northing = ?,
          lat = ?,
          lng = ?,
          n_documents = ?,
          n_comments = ?,
          n_statutory_days = ?,
          date_received = ?,
          date_validated = ?,
          start_date = ?,
          target_decision_date = ?,
          decided_date = ?,
          consulted_date = ?,
          last_changed = ?,
          last_different = ?,
          last_scraped = ?,
          scraper_name = ?,
          url = ?,
          last_planit_api_check = ?,
          cannot_find = ?
        WHERE id = ?
      `,
      [
        p.name,
        p.address,
        p.postcode,
        p.ward_name,
        p.area_id,
        p.area_name,
        p.app_type,
        p.app_size,
        p.app_state,
        p.application_type,
        p.status,
        p.associated_id,
        p.description,
        p.link,
        p.source_url,
        p.docs_url,
        p.comment_url,
        p.map_url,
        p.planning_portal_id,
        p.agent_name,
        p.agent_company,
        p.agent_address,
        p.applicant_name,
        p.case_officer,
        p.location_x,
        p.location_y,
        p.easting,
        p.northing,
        p.lat,
        p.lng,
        p.n_documents,
        p.n_comments,
        p.n_statutory_days,
        p.date_received,
        p.date_validated,
        p.start_date,
        p.target_decision_date,
        p.decided_date,
        p.consulted_date,
        p.last_changed,
        p.last_different,
        p.last_scraped,
        p.scraper_name,
        p.url,
        p.last_planit_api_check,
        p.cannot_find,
        id,
      ],
    );
    return { id, action: "update" };
  }

  const [result] = await mysqlConn.query(
    `
      INSERT INTO planit_applications (
        uid, name, address, postcode, ward_name, area_id, area_name, ons_code,
        app_type, app_size, app_state, application_type, status, associated_id,
        description, link, source_url, docs_url, comment_url, map_url, planning_portal_id,
        agent_name, agent_company, agent_address, applicant_name, case_officer,
        location_x, location_y, easting, northing, lat, lng, n_documents, n_comments, n_statutory_days,
        date_received, date_validated, start_date, target_decision_date, decided_date, consulted_date,
        last_changed, last_different, last_scraped, scraper_name, url, last_planit_api_check, cannot_find
      ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?
      )
    `,
    [
      p.uid,
      p.name,
      p.address,
      p.postcode,
      p.ward_name,
      p.area_id,
      p.area_name,
      p.ons_code,
      p.app_type,
      p.app_size,
      p.app_state,
      p.application_type,
      p.status,
      p.associated_id,
      p.description,
      p.link,
      p.source_url,
      p.docs_url,
      p.comment_url,
      p.map_url,
      p.planning_portal_id,
      p.agent_name,
      p.agent_company,
      p.agent_address,
      p.applicant_name,
      p.case_officer,
      p.location_x,
      p.location_y,
      p.easting,
      p.northing,
      p.lat,
      p.lng,
      p.n_documents,
      p.n_comments,
      p.n_statutory_days,
      p.date_received,
      p.date_validated,
      p.start_date,
      p.target_decision_date,
      p.decided_date,
      p.consulted_date,
      p.last_changed,
      p.last_different,
      p.last_scraped,
      p.scraper_name,
      p.url,
      p.last_planit_api_check,
      p.cannot_find,
    ],
  );
  return { id: Number(result.insertId), action: "insert" };
}

async function upsertNmrk(mysqlConn, n) {
  const [existingRows] = await mysqlConn.query(
    "SELECT id FROM nmrk_apps WHERE ref_num = ? AND ons_code = ? LIMIT 1",
    [n.ref_num, n.ons_code],
  );

  if (existingRows.length) {
    const id = Number(existingRows[0].id);
    await mysqlConn.query(
      `
        UPDATE nmrk_apps
        SET
          application_type = ?,
          app_description = ?,
          submission_date = ?,
          site_address = ?,
          applicant_org = ?,
          agent_org = ?,
          dev_description = ?,
          upload_date = ?,
          job_code = ?,
          lpa_code = ?,
          lpa_name = ?,
          parent_permission = ?,
          added_by = ?,
          location = IF(? IS NOT NULL AND ? IS NOT NULL, ST_SRID(POINT(?, ?), 4326), NULL),
          upload_time = NOW()
        WHERE id = ?
      `,
      [
        n.application_type,
        n.app_description,
        n.submission_date,
        n.site_address,
        n.applicant_org,
        n.agent_org,
        n.dev_description,
        n.upload_date,
        n.job_code,
        n.lpa_code,
        n.lpa_name,
        n.parent_permission,
        n.added_by,
        n.lng,
        n.lat,
        n.lng,
        n.lat,
        id,
      ],
    );
    return { id, action: "update" };
  }

  const [result] = await mysqlConn.query(
    `
      INSERT INTO nmrk_apps (
        ref_num, application_type, app_description, submission_date, site_address,
        applicant_org, agent_org, dev_description, upload_date, job_code,
        lpa_code, lpa_name, ons_code, parent_permission, added_by, location
      ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, IF(? IS NOT NULL AND ? IS NOT NULL, ST_SRID(POINT(?, ?), 4326), NULL)
      )
    `,
    [
      n.ref_num,
      n.application_type,
      n.app_description,
      n.submission_date,
      n.site_address,
      n.applicant_org,
      n.agent_org,
      n.dev_description,
      n.upload_date,
      n.job_code,
      n.lpa_code,
      n.lpa_name,
      n.ons_code,
      n.parent_permission,
      n.added_by,
      n.lng,
      n.lat,
      n.lng,
      n.lat,
    ],
  );
  return { id: Number(result.insertId), action: "insert" };
}

async function upsertMetadata(mysqlConn, payload) {
  const {
    jobCode,
    submittedBy,
    teamMembers,
    planitUid,
    planitId,
    nmrkAppId,
    planningPortalRef,
  } = payload;

  const [existingRows] = await mysqlConn.query(
    `
      SELECT id
      FROM app_metadata
      WHERE (nmrk_app_id = ? AND nmrk_app_id IS NOT NULL)
         OR (planit_uid = ? AND planit_uid IS NOT NULL)
      LIMIT 1
    `,
    [nmrkAppId, planitUid],
  );

  if (existingRows.length) {
    const id = Number(existingRows[0].id);
    await mysqlConn.query(
      `
        UPDATE app_metadata
        SET
          job_code = ?,
          submitted_by = ?,
          team_members = CAST(? AS JSON),
          planit_uid = ?,
          planit_id = ?,
          nmrk_app_id = ?,
          planning_portal_ref = ?,
          last_updated = CURDATE()
        WHERE id = ?
      `,
      [jobCode, submittedBy, JSON.stringify(teamMembers), planitUid, planitId, nmrkAppId, planningPortalRef, id],
    );
    return { id, action: "update" };
  }

  const [result] = await mysqlConn.query(
    `
      INSERT INTO app_metadata (
        job_code,
        submitted_by,
        team_members,
        planit_uid,
        planit_id,
        nmrk_app_id,
        planning_portal_ref,
        last_updated
      ) VALUES (
        ?, ?, CAST(? AS JSON), ?, ?, ?, ?, CURDATE()
      )
    `,
    [jobCode, submittedBy, JSON.stringify(teamMembers), planitUid, planitId, nmrkAppId, planningPortalRef],
  );
  return { id: Number(result.insertId), action: "insert" };
}

async function main() {
  const mysqlHost = requireEnv("MYSQL_HOST");
  const mysqlUser = requireEnv("MYSQL_USER");
  const mysqlPassword = requireEnv("MYSQL_PASSWORD");
  const mysqlDatabase = requireEnv("MYSQL_DATABASE");

  const pgClient = new Client(getPgClientConfig());
  const mysqlConn = await mysql.createConnection({
    host: mysqlHost,
    port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
    user: mysqlUser,
    password: mysqlPassword,
    database: mysqlDatabase,
    connectTimeout: process.env.MYSQL_TIMEOUT_MS ? Number(process.env.MYSQL_TIMEOUT_MS) : 10000,
  });

  await pgClient.connect();
  try {
    logEvent("start", {
      ons_code: argv["ons-code"],
      limit: Number(argv.limit),
      offset: Number(argv.offset),
      dry_run: Boolean(argv["dry-run"]),
    });

    const resolverMap = await loadResolverUserMap(mysqlConn);
    const fallbackSubmittedBy = await getDefaultSubmittedBy(mysqlConn, argv["submitted-by-user-id"]);
    logEvent("resolver_loaded", {
      resolver_user_ids: resolverMap.size,
      fallback_submitted_by: fallbackSubmittedBy,
    });

    const pgRes = await pgClient.query(PG_SOURCE_SQL, [
      argv["ons-code"] ? String(argv["ons-code"]).trim() : null,
      Number(argv.limit),
      Number(argv.offset),
    ]);
    const rows = pgRes.rows || [];

    const summary = {
      scanned: rows.length,
      planit_inserted: 0,
      planit_updated: 0,
      nmrk_inserted: 0,
      nmrk_updated: 0,
      metadata_inserted: 0,
      metadata_updated: 0,
      metadata_skipped_no_job_code: 0,
      metadata_skipped_no_submitted_by: 0,
      errors: 0,
    };

    for (const row of rows) {
      const reference = cleanText(row.reference);
      const onsCode = cleanText(row.ons_code);
      const jobCode = extractJobCode(row.job_code_parts, row.job_codes_found);
      const initials = extractInitialTokens(row.job_code_parts);
      const teamUserIds = Array.from(
        new Set(
          initials
            .map((tok) => resolverMap.get(tok))
            .filter((id) => Number.isFinite(id)),
        ),
      );
      const submittedBy = teamUserIds[0] || fallbackSubmittedBy || null;

      try {
        const planit = mapToPlanitFields(row);
        const nmrk = mapToNmrkFields(row, planit, jobCode);

        let planitRes = { id: null, action: "skip" };
        let nmrkRes = { id: null, action: "skip" };
        let metaRes = { id: null, action: "skip" };

        await mysqlConn.query("START TRANSACTION");
        try {
          planitRes = await upsertPlanit(mysqlConn, planit);
          if (planitRes.action === "insert") summary.planit_inserted += 1;
          if (planitRes.action === "update") summary.planit_updated += 1;

          nmrkRes = await upsertNmrk(mysqlConn, nmrk);
          if (nmrkRes.action === "insert") summary.nmrk_inserted += 1;
          if (nmrkRes.action === "update") summary.nmrk_updated += 1;

          if (!jobCode) {
            summary.metadata_skipped_no_job_code += 1;
          } else if (!submittedBy) {
            summary.metadata_skipped_no_submitted_by += 1;
          } else {
            const planningPortalRef = cleanText(planit.planning_portal_id) || cleanText(row.alternative_reference) || null;
            const members = teamUserIds.length ? teamUserIds : [submittedBy];
            metaRes = await upsertMetadata(mysqlConn, {
              jobCode,
              submittedBy,
              teamMembers: members,
              planitUid: planit.uid,
              planitId: planitRes.id,
              nmrkAppId: nmrkRes.id,
              planningPortalRef,
            });
            if (metaRes.action === "insert") summary.metadata_inserted += 1;
            if (metaRes.action === "update") summary.metadata_updated += 1;
          }

          if (!argv["dry-run"]) await mysqlConn.query("COMMIT");
          else await mysqlConn.query("ROLLBACK");
        } catch (rowErr) {
          await mysqlConn.query("ROLLBACK");
          throw rowErr;
        }

        logEvent("row_done", {
          ons_code: onsCode,
          reference,
          job_code: jobCode,
          team_user_ids: teamUserIds,
          submitted_by: submittedBy,
          planit_action: planitRes.action,
          nmrk_action: nmrkRes.action,
          metadata_action: metaRes.action,
          dry_run: Boolean(argv["dry-run"]),
        });
      } catch (err) {
        summary.errors += 1;
        logEvent("row_error", {
          ons_code: onsCode,
          reference,
          error: err instanceof Error ? err.message : String(err),
        });
        if (!argv["continue-on-error"]) throw err;
      }
    }

    logEvent("done", summary);
  } finally {
    await Promise.allSettled([pgClient.end(), mysqlConn.end()]);
  }
}

main().catch((err) => {
  logEvent("fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
