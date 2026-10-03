#!/usr/bin/env node
import "../bootstrap.js";
import fs from "node:fs/promises";
import mysql from "mysql2/promise";

const filePath = process.argv[2];

if (!filePath) {
  console.error("Usage: node scripts/apply_mysql_sql_file.js <sql-file>");
  process.exit(1);
}

if (!process.env.MYSQL_USER) {
  console.error("Missing MYSQL_USER in environment.");
  process.exit(1);
}

const sql = await fs.readFile(filePath, "utf8");
const conn = await mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : undefined,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS,
  database: process.env.MYSQL_DATABASE,
  multipleStatements: true,
});

try {
  await conn.query(sql);
  console.log(`Applied ${filePath}`);
} finally {
  await conn.end();
}
