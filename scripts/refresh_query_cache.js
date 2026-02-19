#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadDotEnv } from "../bootstrap.js";
import pg from "pg";

const { Client } = pg;
const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SCRIPT_DIR, "..");
loadDotEnv(path.join(REPO_ROOT, ".env"));

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

async function getCacheSqlFiles() {
  const queriesDir = path.resolve(REPO_ROOT, "queries");
  const entries = await fs.readdir(queriesDir, { withFileTypes: true });

  const files = entries
    .filter((entry) => entry.isFile() && /^cache_.*\.sql$/i.test(entry.name))
    .map((entry) => path.join(queriesDir, entry.name))
    .sort((a, b) => a.localeCompare(b));

  return files;
}

async function run() {
  const startedAt = Date.now();
  const files = await getCacheSqlFiles();

  if (!files.length) {
    throw new Error("No cache SQL files found in queries/ matching cache_*.sql");
  }

  const client = new Client(getPgClientConfig());
  await client.connect();

  try {
    console.log(`[cache-refresh] starting file_count=${files.length}`);

    for (const filePath of files) {
      const fileStart = Date.now();
      const sql = await fs.readFile(filePath, "utf8");
      const fileName = path.basename(filePath);

      console.log(`[cache-refresh] running file=${fileName}`);
      // Each SQL file is self-contained and upserts its own cache key.
      // eslint-disable-next-line no-await-in-loop
      await client.query(sql);

      const elapsedMs = Date.now() - fileStart;
      console.log(`[cache-refresh] ok file=${fileName} elapsed_ms=${elapsedMs}`);
    }
  } finally {
    await client.end();
  }

  const totalMs = Date.now() - startedAt;
  console.log(`[cache-refresh] complete file_count=${files.length} total_ms=${totalMs}`);
}

run().catch((err) => {
  console.error("[cache-refresh] failed:", err);
  process.exit(1);
});
