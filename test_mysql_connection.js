#!/usr/bin/env node
import "./bootstrap.js";
import mysql from "mysql2/promise";

const host = process.env.MYSQL_HOST || "35.214.51.175";
const port = Number(process.env.MYSQL_PORT || 3306);
const timeoutMs = Number(process.env.MYSQL_TIMEOUT_MS || 5000);
const user = process.env.MYSQL_USER;
const password = process.env.MYSQL_PASSWORD;
const database = process.env.MYSQL_DATABASE || "wickhams_monitor";

if (!user || !password) {
  console.error("Missing MYSQL_USER or MYSQL_PASSWORD in environment.");
  process.exit(1);
}

let conn;
try {
  conn = await mysql.createConnection({
    host,
    port,
    user,
    password,
    database,
    connectTimeout: timeoutMs,
    enableKeepAlive: false,
  });

  console.log(`Authenticated connection OK: ${host}:${port} (db=${database})`);

  await conn.query("SELECT 1 FROM wickhams_monitor.applications LIMIT 1");
  console.log("SELECT on wickhams_monitor.applications succeeded.");

  process.exit(0);
} catch (err) {
  console.error("MySQL connectivity/auth query failed.");
  console.error(err.message);
  process.exit(1);
} finally {
  if (conn) {
    await conn.end();
  }
}
