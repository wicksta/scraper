#!/usr/bin/env node
// Refresh status-only fields for already-linked, pending MySQL applications.
// This intentionally does not create applications or modify job-code linkage.
import "../bootstrap.js";

import mysql from "mysql2/promise";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;
const argv = yargs(hideBin(process.argv))
  .scriptName("refresh-pending-mysql-applications-from-pg")
  .option("ons-codes", {
    type: "string",
    default: "E09000001,E09000033",
    describe: "Comma-separated City/Westminster ONS codes to refresh.",
  })
  .option("states", {
    type: "string",
    default: "Pending,Undecided,Under Consideration",
    describe: "Comma-separated app_combined_nmrk_planit app_state values eligible for refresh.",
  })
  .option("apply", {
    type: "boolean",
    default: false,
    describe: "Persist updates. Omit for a read-only preview.",
  })
  .option("limit", {
    type: "number",
    default: null,
    describe: "Maximum number of tracked pending applications to process.",
  })
  .strict()
  .help()
  .argv;

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

function pgConfig() {
  return process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: requiredEnv("PGHOST"),
        port: Number(requiredEnv("PGPORT")),
        database: requiredEnv("PGDATABASE"),
        user: requiredEnv("PGUSER"),
        password: requiredEnv("PGPASSWORD"),
      };
}

function list(value) {
  return String(value || "").split(",").map((item) => item.trim()).filter(Boolean);
}

function dateOnly(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
}

function dateTime(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 19).replace("T", " ");
}

async function main() {
  const onsCodes = list(argv["ons-codes"]);
  const states = list(argv.states);
  if (!onsCodes.length || !states.length) throw new Error("--ons-codes and --states must each contain at least one value");
  const limit = argv.limit == null ? null : Number(argv.limit);
  if (limit !== null && (!Number.isInteger(limit) || limit < 1)) throw new Error("--limit must be a positive integer");

  const mysqlConn = await mysql.createConnection({
    host: requiredEnv("MYSQL_HOST"),
    port: Number(process.env.MYSQL_PORT || 3306),
    database: requiredEnv("MYSQL_DATABASE"),
    user: requiredEnv("MYSQL_USER"),
    password: requiredEnv("MYSQL_PASSWORD"),
    connectTimeout: Number(process.env.MYSQL_TIMEOUT_MS || 10000),
  });
  const pgClient = new Client(pgConfig());
  await pgClient.connect();

  try {
    const onsPlaceholders = onsCodes.map(() => "?").join(",");
    const statePlaceholders = states.map(() => "?").join(",");
    const [trackedRows] = await mysqlConn.query(
      `
        SELECT DISTINCT
          a.ons_code,
          COALESCE(NULLIF(TRIM(a.uid), ''), NULLIF(TRIM(a.ref_num), '')) AS reference
        FROM app_combined_nmrk_planit a
        WHERE a.ons_code IN (${onsPlaceholders})
          AND a.app_state IN (${statePlaceholders})
          AND COALESCE(NULLIF(TRIM(a.uid), ''), NULLIF(TRIM(a.ref_num), '')) IS NOT NULL
        ORDER BY a.ons_code, reference
        ${limit !== null ? "LIMIT ?" : ""}
      `,
      limit !== null ? [...onsCodes, ...states, limit] : [...onsCodes, ...states],
    );

    const keys = trackedRows.map((row) => ({ onsCode: String(row.ons_code), reference: String(row.reference) }));
    if (!keys.length) {
      console.log(JSON.stringify({ event: "done", apply: Boolean(argv.apply), tracked: 0, matched_in_pg: 0, updated: 0 }));
      return;
    }

    const { rows: pgRows } = await pgClient.query(
      `
        WITH requested(ons_code, reference) AS (
          SELECT * FROM UNNEST($1::text[], $2::text[])
        )
        SELECT
          a.ons_code, a.reference, a.status, a.decision, a.target_date,
          a.decision_made_date, a.decision_issued_date, a.updated_at, a.scraped_at
        FROM requested r
        JOIN public.applications a
          ON a.ons_code = r.ons_code
         AND a.reference = r.reference
      `,
      [keys.map((key) => key.onsCode), keys.map((key) => key.reference)],
    );

    let updated = 0;
    if (argv.apply) {
      for (const row of pgRows) {
        const [result] = await mysqlConn.query(
          `
            UPDATE planit_applications
            SET
              app_state = COALESCE(?, app_state),
              status = COALESCE(?, status),
              target_decision_date = COALESCE(?, target_decision_date),
              decided_date = COALESCE(?, decided_date),
              last_changed = COALESCE(?, last_changed),
              last_different = COALESCE(?, last_different),
              last_scraped = COALESCE(?, last_scraped)
            WHERE ons_code = ? AND uid = ?
          `,
          [
            row.status || null,
            row.decision || null,
            dateOnly(row.target_date),
            dateOnly(row.decision_issued_date) || dateOnly(row.decision_made_date),
            dateTime(row.updated_at),
            dateTime(row.updated_at),
            dateTime(row.scraped_at),
            row.ons_code,
            row.reference,
          ],
        );
        updated += Number(result.affectedRows || 0);
      }
    }

    console.log(JSON.stringify({
      event: "done",
      apply: Boolean(argv.apply),
      ons_codes: onsCodes,
      states,
      tracked: keys.length,
      matched_in_pg: pgRows.length,
      unmatched_in_pg: keys.length - pgRows.length,
      updated,
    }));
  } finally {
    await Promise.allSettled([mysqlConn.end(), pgClient.end()]);
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ event: "fatal", error: error instanceof Error ? error.message : String(error) }));
  process.exit(1);
});
