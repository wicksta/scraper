#!/usr/bin/env node
import '../bootstrap.js';
import { readFileSync, writeFileSync } from 'node:fs';
import mysql from 'mysql2/promise';

// Dry run by default. --apply adds the column and populates existing borough rows.
// Rollback: ALTER TABLE sm_update_cleaned DROP COLUMN londonPlan2026Target;
const source = JSON.parse(readFileSync(new URL('../data/london_plan/2026_table_3_1_targets.json', import.meta.url)));
const targets = source.authorities.filter(row => row.ons_code);
if (targets.length !== 33 || new Set(targets.map(row => row.ons_code)).size !== 33 ||
    source.authorities.reduce((sum, row) => sum + row.total_housing_target, 0) !== source.london_total) {
  throw new Error('Invalid London Plan source totals or authority mapping');
}
const connection = await mysql.createConnection({
  host: process.env.MYSQL_HOST, port: Number(process.env.MYSQL_PORT || 3306),
  database: process.env.MYSQL_DATABASE || process.env.MYSQL_DB,
  user: process.env.MYSQL_USER, password: process.env.MYSQL_PASSWORD || process.env.MYSQL_PASS,
});
try {
  const [before] = await connection.execute('SELECT * FROM sm_update_cleaned ORDER BY `ONS Code`');
  const existing = new Set(before.map(row => row['ONS Code']));
  for (const row of targets) {
    if (!existing.has(row.ons_code)) throw new Error(`Missing authority: ${row.lpa} (${row.ons_code})`);
  }
  const [names] = await connection.execute("SELECT ons_code, lpa_name FROM lpa_codes WHERE ons_code LIKE 'E09%'");
  for (const target of targets) {
    const match = names.find(row => row.ons_code === target.ons_code);
    if (!match || match.lpa_name.toLowerCase().replace(/[^a-z]/g, '') !== target.lpa.toLowerCase().replace(/[^a-z]/g, '')) {
      throw new Error(`Authority name mismatch: ${target.lpa}: ${match?.lpa_name}`);
    }
  }
  console.log(`Matched ${targets.length} boroughs; ten-year total ${targets.reduce((sum, row) => sum + row.total_housing_target, 0)}. OPDC ${source.authorities.find(row => row.lpa === 'OPDC').total_housing_target} retained in source file; no borough ONS code.`);
  if (process.argv.includes('--apply')) {
    const backupPath = `/tmp/sm_update_cleaned_before_london_plan_2026_${Date.now()}.json`;
    writeFileSync(backupPath, JSON.stringify(before, null, 2), { mode: 0o600 });
    const [columns] = await connection.execute("SHOW COLUMNS FROM sm_update_cleaned LIKE 'londonPlan2026Target'");
    if (!columns.length) {
      await connection.query("ALTER TABLE sm_update_cleaned ADD COLUMN londonPlan2026Target INT UNSIGNED NULL DEFAULT NULL COMMENT 'Draft London Plan 2026 Table 3.1 ten-year total 2027/28-2036/37'");
    }
    await connection.beginTransaction();
    try {
      for (const row of targets) {
        await connection.execute('UPDATE sm_update_cleaned SET londonPlan2026Target = ? WHERE `ONS Code` = ?', [row.total_housing_target, row.ons_code]);
      }
      const [after] = await connection.execute('SELECT * FROM sm_update_cleaned ORDER BY `ONS Code`');
      if (after.length !== before.length) throw new Error('Row count changed');
      for (let i = 0; i < before.length; i++) {
        for (const key of Object.keys(before[i]).filter(key => key !== 'londonPlan2026Target')) {
          if (before[i][key] !== after[i][key]) throw new Error(`Existing value changed: ${key}`);
        }
        const target = targets.find(row => row.ons_code === after[i]['ONS Code']);
        if (target && after[i].londonPlan2026Target !== target.total_housing_target) throw new Error('Target verification failed');
        if (!target && after[i].londonPlan2026Target !== (before[i].londonPlan2026Target ?? null)) throw new Error('Non-London value changed');
      }
      await connection.commit();
      console.log(`Verified all ${targets.length} targets; all existing fields unchanged. Backup: ${backupPath}`);
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  }
} finally {
  await connection.end();
}
