#!/usr/bin/env node
import "../bootstrap.js";

import mysql from "mysql2/promise";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("build-newmark-initial-resolver")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code scope for candidate extraction.",
  })
  .option("limit", {
    type: "number",
    default: 5000,
    describe: "Max candidate rows to scan from Postgres.",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into candidate rows.",
  })
  .option("mysql-table", {
    type: "string",
    default: "newmark_initial_resolver",
    describe: "Target table name in MySQL.",
  })
  .option("min-confidence", {
    type: "string",
    default: "high",
    choices: ["high", "medium", "low"],
    describe: "Minimum confidence threshold to write resolver rows.",
  })
  .option("include-locked", {
    type: "boolean",
    default: false,
    describe: "If true, locked rows can be overwritten by this run.",
  })
  .option("write-null-user", {
    type: "boolean",
    default: true,
    describe: "If false, skip resolver rows where no users.id can be resolved.",
  })
  .option("dry-run", {
    type: "boolean",
    default: false,
    describe: "If true, do not write MySQL updates.",
  })
  .strict()
  .help()
  .argv;

const PG_SQL = `
  SELECT
    id,
    ons_code,
    reference,
    job_code_parts
  FROM public.newmark_jobcode_candidates
  WHERE status = 'matched'
    AND ($1::text IS NULL OR ons_code = $1)
    AND jsonb_typeof(job_code_parts) = 'array'
    AND jsonb_array_length(job_code_parts) > 0
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(job_code_parts) elem
      WHERE jsonb_typeof(elem->'team_responsible_names') = 'array'
        AND jsonb_array_length(elem->'team_responsible_names') > 0
    )
  ORDER BY detected_at DESC, id DESC
  LIMIT $2 OFFSET $3
`;

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
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

function requireEnv(name) {
  const value = process.env[name];
  if (value == null || String(value).trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return String(value);
}

function normalizeInitialToken(token) {
  const cleaned = String(token || "")
    .toUpperCase()
    .replace(/[^A-Z]/g, "");
  if (!cleaned) return null;
  if (cleaned.length < 2 || cleaned.length > 6) return null;
  return cleaned;
}

function normalizeName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z\s'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseNameParts(personName) {
  const cleaned = String(personName || "").trim().replace(/\s+/g, " ");
  if (!cleaned) return { first_name: null, last_name: null };
  const bits = cleaned.split(" ").filter(Boolean);
  if (!bits.length) return { first_name: null, last_name: null };
  if (bits.length === 1) return { first_name: bits[0], last_name: null };
  return {
    first_name: bits.slice(0, -1).join(" "),
    last_name: bits[bits.length - 1],
  };
}

function commonSenseNameFitScore(initialToken, personName) {
  const token = normalizeInitialToken(initialToken);
  const parts = parseNameParts(personName);
  const first = String(parts.first_name || "").trim();
  const last = String(parts.last_name || "").trim();
  if (!token || !first || !last) return 0;

  const firstInitial = first[0]?.toUpperCase() || "";
  const lastInitial = last[0]?.toUpperCase() || "";
  const last2 = last.slice(0, 2).toUpperCase();
  const last3 = last.slice(0, 3).toUpperCase();

  let score = 0;
  if (token.startsWith(firstInitial)) score += 0.62;
  else score -= 0.72;

  if (token.includes(lastInitial)) score += 0.16;
  else score -= 0.14;

  if (last2.length === 2 && token.includes(last2)) score += 0.48;
  if (last3.length === 3 && token.includes(last3)) score += 0.24;

  if (token.length === 2 && token === `${firstInitial}${lastInitial}`) score += 0.34;

  const firstWords = first.split(/\s+/).filter(Boolean);
  if (firstWords.length > 1) {
    for (let i = 1; i < firstWords.length; i += 1) {
      const midInitial = firstWords[i][0]?.toUpperCase() || "";
      if (!midInitial) continue;
      if (token.includes(midInitial)) score += 0.07;
    }
  }

  return Math.max(-1.6, Math.min(1.8, score));
}

function parseJobCodeParts(value) {
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

function pushEvidence(aggMap, payload) {
  const { token, name, reference, mode, weight } = payload;
  const initialToken = normalizeInitialToken(token);
  const personName = String(name || "").trim().replace(/\s+/g, " ");
  const personNameNorm = normalizeName(personName);
  if (!initialToken || !personNameNorm) return;

  const key = `${initialToken}||${personNameNorm}`;
  let row = aggMap.get(key);
  if (!row) {
    const nameParts = parseNameParts(personName);
    row = {
      initial_token: initialToken,
      person_name: personName,
      person_name_norm: personNameNorm,
      first_name: nameParts.first_name,
      last_name: nameParts.last_name,
      score: 0,
      evidence_count: 0,
      exact_count: 0,
      mismatch_count: 0,
      source_refs: new Map(),
    };
    aggMap.set(key, row);
  }

  row.score += Number(weight) || 0;
  row.evidence_count += 1;
  if (mode === "exact_positional") row.exact_count += 1;
  else row.mismatch_count += 1;

  if (!row.source_refs.has(reference)) {
    row.source_refs.set(reference, { observations: 0, modes: new Set() });
  }
  const src = row.source_refs.get(reference);
  src.observations += 1;
  src.modes.add(mode);
}

function mapExactPositional(initials, names, reference, aggMap) {
  for (let i = 0; i < initials.length; i += 1) {
    pushEvidence(aggMap, {
      token: initials[i],
      name: names[i],
      reference,
      mode: "exact_positional",
      weight: 1.3,
    });
  }
}

function mapMismatchCandidates(initials, names, reference, aggMap) {
  const nInitials = initials.length;
  const nNames = names.length;
  if (!nInitials || !nNames) return;

  const tailStart = Math.max(0, nInitials - nNames);
  for (let nameIdx = 0; nameIdx < nNames; nameIdx += 1) {
    for (let initIdx = 0; initIdx < nInitials; initIdx += 1) {
      let weight = 0.15;

      const isAssistantSide = initIdx > 0;
      const isTailAligned = initIdx >= tailStart;
      const isPrimaryGuess = initIdx === Math.min(nInitials - 1, tailStart + nameIdx);

      if (isAssistantSide) weight += 0.08;
      if (isTailAligned) weight += 0.14;
      if (isPrimaryGuess) weight += 0.18;
      if (!isAssistantSide) weight -= 0.06;
      if (nNames > nInitials && initIdx === nameIdx) weight += 0.08;

      if (weight < 0.03) weight = 0.03;
      pushEvidence(aggMap, {
        token: initials[initIdx],
        name: names[nameIdx],
        reference,
        mode: "mismatch_candidate",
        weight,
      });
    }
  }
}

function scoreToConfidence(entry) {
  const distinctRefs = entry.source_refs.size;
  if (entry.exact_count >= 2 && distinctRefs >= 2 && entry.score >= 2.2) return "high";
  if ((entry.exact_count >= 1 && entry.score >= 1.3) || (distinctRefs >= 2 && entry.score >= 1.8)) return "medium";
  return "low";
}

function confidenceRank(level) {
  if (level === "high") return 3;
  if (level === "medium") return 2;
  return 1;
}

function keepByMinConfidence(level, minLevel) {
  return confidenceRank(level) >= confidenceRank(minLevel);
}

function downgradeConfidence(level) {
  if (level === "high") return "medium";
  if (level === "medium") return "low";
  return "low";
}

function buildSourcesJson(entry) {
  const refs = Array.from(entry.source_refs.entries()).map(([reference, meta]) => ({
    reference,
    observations: meta.observations,
    modes: Array.from(meta.modes),
  }));
  refs.sort((a, b) => b.observations - a.observations || a.reference.localeCompare(b.reference));

  return {
    exact_count: entry.exact_count,
    mismatch_count: entry.mismatch_count,
    distinct_references: entry.source_refs.size,
    sample_references: refs.slice(0, 15),
  };
}

function choosePrimaryConfidence(primary, secondBest) {
  const distinctRefs = primary.source_refs.size;
  let out = "low";
  if (primary.score >= 2.4 && distinctRefs >= 2) out = "high";
  else if (primary.score >= 1.2 && distinctRefs >= 1) out = "medium";

  if (secondBest && Number(primary.score) - Number(secondBest.score) < 0.22) {
    out = downgradeConfidence(out);
  }
  return out;
}

function collapseToSingleMappingPerInitial(entries) {
  const byToken = new Map();
  for (const entry of entries) {
    const token = entry.initial_token;
    if (!byToken.has(token)) byToken.set(token, []);
    byToken.get(token).push(entry);
  }

  const out = [];
  for (const [token, candidates] of byToken.entries()) {
    for (const c of candidates) {
      const fit = commonSenseNameFitScore(c.initial_token, c.person_name);
      const scale = 0.8 + Math.min(1.2, c.evidence_count * 0.15);
      c.common_sense_fit = fit;
      c.score = Number((c.score + fit * scale).toFixed(4));
    }

    candidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (b.exact_count !== a.exact_count) return b.exact_count - a.exact_count;
      if (b.source_refs.size !== a.source_refs.size) return b.source_refs.size - a.source_refs.size;
      if (b.evidence_count !== a.evidence_count) return b.evidence_count - a.evidence_count;
      return a.person_name.localeCompare(b.person_name);
    });

    const primary = candidates[0];
    const second = candidates[1] || null;
    const confidence = choosePrimaryConfidence(primary, second);
    const alternatives = candidates.slice(1, 8).map((c) => ({
      person_name: c.person_name,
      person_name_norm: c.person_name_norm,
      user_id: c.user_id,
      score: Number(c.score.toFixed(4)),
      exact_count: c.exact_count,
      mismatch_count: c.mismatch_count,
      common_sense_fit: Number((c.common_sense_fit || 0).toFixed(4)),
      distinct_references: c.source_refs.size,
    }));

    out.push({
      ...primary,
      confidence,
      sources_json: {
        ...buildSourcesJson(primary),
        common_sense_fit: Number((primary.common_sense_fit || 0).toFixed(4)),
        alternatives,
      },
    });
  }

  return out;
}

function normalizeUserRecordName(row) {
  const rawFull = String(row.name || "").trim();
  if (rawFull) return rawFull;
  const full = `${String(row.first_name || "").trim()} ${String(row.last_name || "").trim()}`.trim();
  return full;
}

function buildUserIndex(usersRows) {
  const byNorm = new Map();
  for (const row of usersRows) {
    const candidates = new Set();
    const fullPrimary = normalizeUserRecordName(row);
    const fullFallback = `${String(row.first_name || "").trim()} ${String(row.last_name || "").trim()}`.trim();
    if (fullPrimary) candidates.add(normalizeName(fullPrimary));
    if (fullFallback) candidates.add(normalizeName(fullFallback));

    for (const key of candidates) {
      if (!key) continue;
      if (!byNorm.has(key)) byNorm.set(key, []);
      byNorm.get(key).push(row);
    }
  }
  return byNorm;
}

function chooseBestUser(rows) {
  if (!rows || !rows.length) return null;
  const sorted = [...rows].sort((a, b) => {
    const av = Number(Boolean(a.is_verified));
    const bv = Number(Boolean(b.is_verified));
    if (bv !== av) return bv - av;

    const aUpdated = a.updated_at ? new Date(a.updated_at).getTime() : 0;
    const bUpdated = b.updated_at ? new Date(b.updated_at).getTime() : 0;
    if (bUpdated !== aUpdated) return bUpdated - aUpdated;

    const aCreated = a.created_at ? new Date(a.created_at).getTime() : 0;
    const bCreated = b.created_at ? new Date(b.created_at).getTime() : 0;
    if (bCreated !== aCreated) return bCreated - aCreated;

    return Number(b.id) - Number(a.id);
  });
  return sorted[0];
}

function applyUserResolution(entries, userIndex) {
  for (const entry of entries) {
    const matches = userIndex.get(entry.person_name_norm) || [];
    const picked = chooseBestUser(matches);
    entry.user_id = picked ? Number(picked.id) : null;
  }
}

async function loadLockedKeys(mysqlConn, tableName) {
  const [rows] = await mysqlConn.query(`SELECT initial_token FROM \`${tableName}\` WHERE is_locked = 1`);
  const out = new Set();
  for (const row of rows) {
    out.add(String(row.initial_token));
  }
  return out;
}

async function upsertResolverRows(mysqlConn, tableName, rows) {
  const sql = `
    INSERT INTO \`${tableName}\` (
      initial_token,
      person_name,
      person_name_norm,
      first_name,
      last_name,
      user_id,
      confidence,
      score,
      evidence_count,
      sources_json,
      last_seen_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), NOW())
    ON DUPLICATE KEY UPDATE
      person_name = VALUES(person_name),
      first_name = VALUES(first_name),
      last_name = VALUES(last_name),
      user_id = VALUES(user_id),
      confidence = VALUES(confidence),
      score = VALUES(score),
      evidence_count = VALUES(evidence_count),
      sources_json = VALUES(sources_json),
      last_seen_at = NOW(),
      updated_at = NOW()
  `;

  for (const row of rows) {
    await mysqlConn.query(sql, [
      row.initial_token,
      row.person_name,
      row.person_name_norm,
      row.first_name,
      row.last_name,
      row.user_id,
      row.confidence,
      Number(row.score.toFixed(4)),
      row.evidence_count,
      JSON.stringify(row.sources_json),
    ]);
  }
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
    const tableName = String(argv["mysql-table"]).trim();
    logEvent("start", {
      ons_code: argv["ons-code"],
      limit: Number(argv.limit),
      offset: Number(argv.offset),
      mysql_table: tableName,
      min_confidence: argv["min-confidence"],
      include_locked: Boolean(argv["include-locked"]),
      write_null_user: Boolean(argv["write-null-user"]),
      dry_run: Boolean(argv["dry-run"]),
    });

    const res = await pgClient.query(PG_SQL, [
      argv["ons-code"] ? String(argv["ons-code"]).trim() : null,
      Number(argv.limit),
      Number(argv.offset),
    ]);
    const pgRows = res.rows || [];

    const evidenceMap = new Map();
    let parsedParts = 0;
    for (const row of pgRows) {
      const reference = String(row.reference || "").trim();
      const parts = parseJobCodeParts(row.job_code_parts);
      for (const part of parts) {
        if (!part || typeof part !== "object") continue;
        const partner = part.partner_initials ? [part.partner_initials] : [];
        const assistants = Array.isArray(part.assistant_initials) ? part.assistant_initials : [];
        const initials = [...partner, ...assistants].map((s) => normalizeInitialToken(s)).filter(Boolean);
        const names = Array.isArray(part.team_responsible_names)
          ? part.team_responsible_names.map((n) => String(n || "").trim()).filter(Boolean)
          : [];
        if (!initials.length || !names.length) continue;

        parsedParts += 1;
        if (initials.length === names.length) {
          mapExactPositional(initials, names, reference, evidenceMap);
        } else {
          mapMismatchCandidates(initials, names, reference, evidenceMap);
        }
      }
    }

    const [usersRows] = await mysqlConn.query(
      "SELECT id, first_name, last_name, name, is_verified, created_at, updated_at FROM users",
    );
    const userIndex = buildUserIndex(usersRows);

    const allEntries = [];
    for (const entry of evidenceMap.values()) {
      entry.confidence = scoreToConfidence(entry);
      allEntries.push(entry);
    }

    applyUserResolution(allEntries, userIndex);
    const onePerInitial = collapseToSingleMappingPerInitial(allEntries);

    const filtered = onePerInitial.filter((entry) => {
      if (!keepByMinConfidence(entry.confidence, argv["min-confidence"])) return false;
      if (!argv["write-null-user"] && entry.user_id == null) return false;
      return true;
    });

    let lockedSkipped = 0;
    let writableRows = filtered;
    if (!argv["include-locked"]) {
      const locked = await loadLockedKeys(mysqlConn, tableName);
      writableRows = filtered.filter((entry) => {
        const isLocked = locked.has(entry.initial_token);
        if (isLocked) lockedSkipped += 1;
        return !isLocked;
      });
    }

    if (!argv["dry-run"] && writableRows.length) {
      await upsertResolverRows(mysqlConn, tableName, writableRows);
    }

    logEvent("done", {
      pg_rows_scanned: pgRows.length,
      parsed_parts: parsedParts,
      aggregated_pairs: allEntries.length,
      collapsed_initials: onePerInitial.length,
      matched_user_rows: allEntries.filter((r) => r.user_id != null).length,
      qualified_rows: filtered.length,
      locked_skipped: lockedSkipped,
      upserted_rows: argv["dry-run"] ? 0 : writableRows.length,
      would_upsert_rows: writableRows.length,
    });
  } finally {
    await Promise.allSettled([pgClient.end(), mysqlConn.end()]);
  }
}

main().catch((err) => {
  logEvent("fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
