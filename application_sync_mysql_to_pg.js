#!/usr/bin/env node
import "./bootstrap.js";

import mysql from "mysql2/promise";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("sync-mysql-applications-to-pg")
  .option("ons-code", {
    type: "string",
    describe: "Override ONS code for all imported rows (required if source rows do not contain ons_code).",
  })
  .option("batch-size", {
    type: "number",
    default: 500,
    describe: "Rows fetched from MySQL per batch.",
  })
  .option("max-rows", {
    type: "number",
    describe: "Stop after processing this many rows.",
  })
  .option("dry-run", {
    type: "boolean",
    default: false,
    describe: "Read/transform rows but do not write to Postgres.",
  })
  .strict()
  .help()
  .argv;

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

function requireEnv(name) {
  const v = process.env[name];
  if (v == null || String(v).trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return v;
}

function toTextOrNull(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function toDateOrNull(v) {
  if (v == null) return null;

  if (v instanceof Date && Number.isFinite(v.getTime())) {
    const iso = v.toISOString().slice(0, 10);
    return iso === "1970-01-01" ? null : iso;
  }

  const s = String(v).trim();
  if (
    !s ||
    s === "0000-00-00" ||
    s === "0000-00-00 00:00:00" ||
    s === "1970-01-01" ||
    s === "1970-01-01 00:00:00"
  ) {
    return null;
  }

  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m) return m[1] === "1970-01-01" ? null : m[1];

  const d = new Date(s);
  if (Number.isFinite(d.getTime())) {
    const iso = d.toISOString().slice(0, 10);
    return iso === "1970-01-01" ? null : iso;
  }

  return null;
}

function toNumericOrNull(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function toSmallIntOrNull(v) {
  const n = toNumericOrNull(v);
  if (n == null) return null;
  const i = Math.trunc(n);
  if (i < -32768 || i > 32767) return null;
  return i;
}

function firstNonEmpty(row, keys) {
  for (const k of keys) {
    if (!(k in row)) continue;
    const val = row[k];
    if (val == null) continue;
    if (typeof val === "string" && val.trim() === "") continue;
    return val;
  }
  return null;
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function mapMysqlRowToPg(row, onsCodeOverride) {
  const reference = toTextOrNull(firstNonEmpty(row, ["Reference", "reference"]));
  const onsCode = toTextOrNull(onsCodeOverride || firstNonEmpty(row, ["ons_code", "ONS Code", "onsCode"]));

  const applicationReceived = toDateOrNull(firstNonEmpty(row, ["Application Received Date", "Application Received"]));
  const applicationValidated = toDateOrNull(firstNonEmpty(row, ["Application Validated Date", "Application Validated"]));

  const dateAdded =
    toDateOrNull(firstNonEmpty(row, ["date_added", "Date Added"])) ||
    applicationValidated ||
    applicationReceived ||
    todayIsoDate();

  return {
    ons_code: onsCode,
    reference,
    alternative_reference: toTextOrNull(firstNonEmpty(row, ["Alternative Reference"])),
    district_reference: toTextOrNull(firstNonEmpty(row, ["District Reference"])),
    application_received: applicationReceived,
    application_validated: applicationValidated,
    address: toTextOrNull(firstNonEmpty(row, ["Address"])),
    proposal: toTextOrNull(firstNonEmpty(row, ["Proposal"])),
    status: toTextOrNull(firstNonEmpty(row, ["Status"])),
    decision: toTextOrNull(firstNonEmpty(row, ["Decision"])),
    appeal_status: toTextOrNull(firstNonEmpty(row, ["Appeal Status"])),
    appeal_decision: toTextOrNull(firstNonEmpty(row, ["Appeal Decision"])),
    application_type: toTextOrNull(firstNonEmpty(row, ["Application Type"])),
    expected_decision_level: toTextOrNull(firstNonEmpty(row, ["Expected Decision Level"])),
    actual_decision_level: toTextOrNull(firstNonEmpty(row, ["Actual Decision Level"])),
    case_officer: toTextOrNull(firstNonEmpty(row, ["Case Officer"])),
    parish: toTextOrNull(firstNonEmpty(row, ["Parish"])),
    ward: toTextOrNull(firstNonEmpty(row, ["Ward"])),
    amenity_society: toTextOrNull(firstNonEmpty(row, ["Amenity Society"])),
    applicant_name: toTextOrNull(firstNonEmpty(row, ["Applicant Name"])),
    applicant_address: toTextOrNull(firstNonEmpty(row, ["Applicant Address"])),
    agent_name: toTextOrNull(firstNonEmpty(row, ["Agent Name"])),
    agent_company_name: toTextOrNull(firstNonEmpty(row, ["Agent Company Name"])),
    agent_address: toTextOrNull(firstNonEmpty(row, ["Agent Address"])),
    environmental_assessment_requested: toTextOrNull(firstNonEmpty(row, ["Environmental Assessment Requested"])),
    actual_committee_date: toDateOrNull(firstNonEmpty(row, ["Actual Committee Date"])),
    agreed_expiry_date: toDateOrNull(firstNonEmpty(row, ["Agreed Expiry Date"])),
    last_advertised_in_press_date: toDateOrNull(firstNonEmpty(row, ["Last Advertised In Press Date"])),
    latest_advertisement_expiry_date: toDateOrNull(firstNonEmpty(row, ["Latest Advertisement Expiry Date"])),
    last_site_notice_posted_date: toDateOrNull(firstNonEmpty(row, ["Last Site Notice Posted Date"])),
    latest_site_notice_expiry_date: toDateOrNull(firstNonEmpty(row, ["Latest Site Notice Expiry Date"])),
    decision_made_date: toDateOrNull(firstNonEmpty(row, ["Decision Made Date"])),
    decision_issued_date: toDateOrNull(firstNonEmpty(row, ["Decision Issued Date"])),
    target_date: toDateOrNull(firstNonEmpty(row, ["Target Date"])),
    temporary_permission_expiry_date: toDateOrNull(firstNonEmpty(row, ["Temporary Permission Expiry Date"])),
    major: toTextOrNull(firstNonEmpty(row, ["Major"])),
    removed: toDateOrNull(firstNonEmpty(row, ["Removed"])),
    lat: toNumericOrNull(firstNonEmpty(row, ["lat", "Lat", "Latitude"])),
    lon: toNumericOrNull(firstNonEmpty(row, ["lon", "Lon", "Longitude"])),
    date_added: dateAdded,
    spare2: toSmallIntOrNull(firstNonEmpty(row, ["spare2", "Spare2"])),
    keyval: toTextOrNull(firstNonEmpty(row, ["keyval", "KeyVal"])),
    last_look: toDateOrNull(firstNonEmpty(row, ["last_look", "Last Look"])),
  };
}

const UPSERT_SQL = `
  INSERT INTO public.applications (
    ons_code,
    reference,
    alternative_reference,
    district_reference,
    application_received,
    application_validated,
    address,
    proposal,
    status,
    decision,
    appeal_status,
    appeal_decision,
    application_type,
    expected_decision_level,
    actual_decision_level,
    case_officer,
    parish,
    ward,
    amenity_society,
    applicant_name,
    applicant_address,
    agent_name,
    agent_company_name,
    agent_address,
    environmental_assessment_requested,
    actual_committee_date,
    agreed_expiry_date,
    last_advertised_in_press_date,
    latest_advertisement_expiry_date,
    last_site_notice_posted_date,
    latest_site_notice_expiry_date,
    decision_made_date,
    decision_issued_date,
    target_date,
    temporary_permission_expiry_date,
    major,
    removed,
    lat,
    lon,
    date_added,
    spare2,
    keyval,
    last_look,
    scraped_at,
    updated_at
  ) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5::date,
    $6::date,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26::date,
    $27::date,
    $28::date,
    $29::date,
    $30::date,
    $31::date,
    $32::date,
    $33::date,
    $34::date,
    $35::date,
    $36,
    $37::date,
    $38,
    $39,
    $40::date,
    $41,
    $42,
    $43::date,
    now(),
    now()
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
    major = COALESCE(EXCLUDED.major, applications.major),
    removed = COALESCE(EXCLUDED.removed, applications.removed),
    lat = COALESCE(EXCLUDED.lat, applications.lat),
    lon = COALESCE(EXCLUDED.lon, applications.lon),
    spare2 = COALESCE(EXCLUDED.spare2, applications.spare2),
    keyval = COALESCE(EXCLUDED.keyval, applications.keyval),
    last_look = COALESCE(EXCLUDED.last_look, applications.last_look),
    scraped_at = now(),
    updated_at = now()
  RETURNING (xmax = 0) AS inserted
`;

function toUpsertValues(r) {
  return [
    r.ons_code,
    r.reference,
    r.alternative_reference,
    r.district_reference,
    r.application_received,
    r.application_validated,
    r.address,
    r.proposal,
    r.status,
    r.decision,
    r.appeal_status,
    r.appeal_decision,
    r.application_type,
    r.expected_decision_level,
    r.actual_decision_level,
    r.case_officer,
    r.parish,
    r.ward,
    r.amenity_society,
    r.applicant_name,
    r.applicant_address,
    r.agent_name,
    r.agent_company_name,
    r.agent_address,
    r.environmental_assessment_requested,
    r.actual_committee_date,
    r.agreed_expiry_date,
    r.last_advertised_in_press_date,
    r.latest_advertisement_expiry_date,
    r.last_site_notice_posted_date,
    r.latest_site_notice_expiry_date,
    r.decision_made_date,
    r.decision_issued_date,
    r.target_date,
    r.temporary_permission_expiry_date,
    r.major,
    r.removed,
    r.lat,
    r.lon,
    r.date_added,
    r.spare2,
    r.keyval,
    r.last_look,
  ];
}

async function main() {
  const mysqlHost = requireEnv("MYSQL_HOST");
  const mysqlUser = requireEnv("MYSQL_USER");
  const mysqlPassword = requireEnv("MYSQL_PASSWORD");
  const mysqlDatabase = requireEnv("MYSQL_DATABASE");

  const mysqlConn = await mysql.createConnection({
    host: mysqlHost,
    port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
    user: mysqlUser,
    password: mysqlPassword,
    database: mysqlDatabase,
    connectTimeout: process.env.MYSQL_TIMEOUT_MS ? Number(process.env.MYSQL_TIMEOUT_MS) : 10_000,
  });

  const pgClient = new Client(getPgClientConfig());
  await pgClient.connect();

  const startedAt = Date.now();
  const batchSize = Number(argv["batch-size"] || 500);
  const maxRows = argv["max-rows"] != null ? Number(argv["max-rows"]) : null;

  const summary = {
    dryRun: Boolean(argv["dry-run"]),
    batchSize,
    maxRows,
    scanned: 0,
    inserted: 0,
    updated: 0,
    skipped: 0,
    failed: 0,
  };

  try {
    const [sourceColsRows] = await mysqlConn.query(
      `SELECT COLUMN_NAME
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'applications'`,
      [mysqlDatabase],
    );
    const sourceColumns = new Set(sourceColsRows.map((r) => String(r.COLUMN_NAME)));
    const orderClause = sourceColumns.has("Reference") ? "ORDER BY `Reference`" : "";
    const refColumn = sourceColumns.has("Reference")
      ? "`Reference`"
      : sourceColumns.has("reference")
        ? "`reference`"
        : null;
    const whereClause = refColumn ? `WHERE TRIM(COALESCE(${refColumn}, '')) <> ''` : "";

    const [countRows] = await mysqlConn.query(
      `SELECT COUNT(*) AS c FROM applications ${whereClause}`,
    );
    const sourceTotal = Number(countRows[0]?.c || 0);

    console.log(
      JSON.stringify({
        phase: "start",
        sourceTotal,
        batchSize,
        dryRun: summary.dryRun,
        onsCodeOverride: argv["ons-code"] || null,
      }),
    );

    let offset = 0;
    let batchNo = 0;

    while (true) {
      if (maxRows != null && summary.scanned >= maxRows) break;

      const remainingCap = maxRows == null ? batchSize : Math.max(0, maxRows - summary.scanned);
      if (remainingCap <= 0) break;
      const currentBatchSize = Math.min(batchSize, remainingCap);

      // mysql2 uses ? placeholders for LIMIT/OFFSET values.
      // eslint-disable-next-line no-await-in-loop
      const [rows] = await mysqlConn.query(
        `SELECT * FROM applications ${whereClause} ${orderClause} LIMIT ? OFFSET ?`,
        [currentBatchSize, offset],
      );

      if (!rows.length) break;

      batchNo += 1;
      let batchInserted = 0;
      let batchUpdated = 0;
      let batchSkipped = 0;
      let batchFailed = 0;

      for (const row of rows) {
        const mapped = mapMysqlRowToPg(row, argv["ons-code"] || null);
        summary.scanned += 1;

        if (!mapped.reference) {
          summary.skipped += 1;
          batchSkipped += 1;
          continue;
        }

        if (!mapped.ons_code) {
          summary.failed += 1;
          batchFailed += 1;
          console.error(`Missing ons_code for reference=${mapped.reference}. Supply --ons-code or include ons_code in source.`);
          continue;
        }

        if (summary.dryRun) {
          continue;
        }

        try {
          // eslint-disable-next-line no-await-in-loop
          const res = await pgClient.query(UPSERT_SQL, toUpsertValues(mapped));
          const inserted = Boolean(res.rows?.[0]?.inserted);
          if (inserted) {
            summary.inserted += 1;
            batchInserted += 1;
          } else {
            summary.updated += 1;
            batchUpdated += 1;
          }
        } catch (err) {
          summary.failed += 1;
          batchFailed += 1;
          const msg = err instanceof Error ? err.message : String(err);
          console.error(`Upsert failed for reference=${mapped.reference}: ${msg}`);
        }
      }

      offset += rows.length;

      console.log(
        JSON.stringify({
          phase: "batch",
          batchNo,
          fetched: rows.length,
          scannedSoFar: summary.scanned,
          insertedSoFar: summary.inserted,
          updatedSoFar: summary.updated,
          skippedSoFar: summary.skipped,
          failedSoFar: summary.failed,
          batchInserted,
          batchUpdated,
          batchSkipped,
          batchFailed,
        }),
      );

      if (rows.length < currentBatchSize) break;
    }

    const finishedAt = Date.now();
    console.log(
      JSON.stringify({
        phase: "done",
        durationMs: finishedAt - startedAt,
        ...summary,
      }),
    );
  } finally {
    await Promise.allSettled([mysqlConn.end(), pgClient.end()]);
  }
}

main().catch((err) => {
  console.error("[sync_mysql_to_pg] fatal:", err);
  process.exit(1);
});
