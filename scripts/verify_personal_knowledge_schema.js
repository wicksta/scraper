#!/usr/bin/env node
import "../bootstrap.js";
import fs from "node:fs/promises";
import mysql from "mysql2/promise";

const fixturePath = new URL("../fixtures/personal_knowledge_dev_fixture.sql", import.meta.url);

if (!process.env.MYSQL_USER) {
  console.error("Missing MYSQL_USER in environment.");
  process.exit(1);
}

const conn = await mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : undefined,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS,
  database: process.env.MYSQL_DATABASE,
  multipleStatements: true,
});

async function scalar(sql, params = []) {
  const [rows] = await conn.query(sql, params);
  return rows[0];
}

try {
  await conn.beginTransaction();
  const fixtureSql = (await fs.readFile(fixturePath, "utf8"))
    .replace("START TRANSACTION;", "")
    .replace("COMMIT;", "");
  await conn.query(fixtureSql);

  const tableCount = await scalar(`
    SELECT COUNT(*) AS n
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name LIKE 'personal\\_%'
  `);
  const fkCount = await scalar(`
    SELECT COUNT(*) AS n
    FROM information_schema.referential_constraints
    WHERE constraint_schema = DATABASE()
      AND constraint_name LIKE 'fk_personal\\_%'
  `);
  const indexCount = await scalar(`
    SELECT COUNT(DISTINCT CONCAT(table_name, ':', index_name)) AS n
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name LIKE 'personal\\_%'
  `);

  let fkRejected = false;
  try {
    await conn.query("INSERT INTO personal_document_pages (document_id, page_number) VALUES (999999999, 99)");
  } catch (err) {
    fkRejected = err.code === "ER_NO_REFERENCED_ROW_2";
  }

  const provenance = await scalar(`
    SELECT t.task_ref, t.title AS task_title, e.event_type, e.status AS event_status,
           p.page_number, d.original_filename, d.storage_ref
    FROM personal_tasks t
    JOIN personal_event_links el
      ON el.entity_type = 'task' AND el.entity_id = t.id AND el.link_type = 'created'
    JOIN personal_events e ON e.id = el.event_id
    JOIN personal_document_pages p ON p.id = t.source_page_id
    JOIN personal_source_documents d ON d.id = p.document_id
    WHERE t.task_ref = 'T184'
    LIMIT 1
  `);

  const completionHistory = await scalar(`
    SELECT t.task_ref, t.completed_at, e.event_type, e.applied_at,
           JSON_UNQUOTE(JSON_EXTRACT(e.payload_json, '$.evidence_text')) AS evidence_text,
           p.page_number
    FROM personal_tasks t
    JOIN personal_event_links el
      ON el.entity_type = 'task' AND el.entity_id = t.id AND el.link_type = 'completed'
    JOIN personal_events e ON e.id = el.event_id
    LEFT JOIN personal_document_pages p ON p.id = e.source_page_id
    WHERE t.task_ref = 'T184'
    LIMIT 1
  `);

  console.log(JSON.stringify({
    database: process.env.MYSQL_DATABASE,
    tables_created: tableCount.n,
    foreign_keys: fkCount.n,
    indexes: indexCount.n,
    foreign_key_rejected_bad_page_insert: fkRejected,
    fixture_rows_rolled_back: true,
    provenance,
    completion_history: completionHistory,
  }, null, 2));
} finally {
  await conn.rollback();
  await conn.end();
}
