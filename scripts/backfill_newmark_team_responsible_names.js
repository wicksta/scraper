#!/usr/bin/env node
import "../bootstrap.js";

import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("backfill-newmark-team-responsible-names")
  .option("ons-code", {
    type: "string",
    default: "E09000033",
    describe: "ONS code to scope the backfill.",
  })
  .option("limit", {
    type: "number",
    default: 100,
    describe: "Max matched records to attempt in this run.",
  })
  .option("offset", {
    type: "number",
    default: 0,
    describe: "Offset into candidate records.",
  })
  .option("artifacts-dir", {
    type: "string",
    default: "/mnt/HC_Volume_103054926/newmark_jobcode_uid_artifacts",
    describe: "Directory containing cached cover_letter.txt files.",
  })
  .option("openai-model", {
    type: "string",
    default: "gpt-4.1-nano",
    describe: "OpenAI model for team-name extraction.",
  })
  .option("openai-max-chars", {
    type: "number",
    default: 24000,
    describe: "Max text chars to send to OpenAI.",
  })
  .option("openai-last-words", {
    type: "number",
    default: 1000,
    describe: "Only send the last N words of the cached cover letter text.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 60000,
    describe: "OpenAI request timeout in milliseconds.",
  })
  .option("dry-run", {
    type: "boolean",
    default: false,
    describe: "If true, do not write DB updates.",
  })
  .strict()
  .help()
  .argv;

const SELECT_SQL = `
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
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(job_code_parts) elem
      WHERE jsonb_typeof(elem->'team_responsible_names') = 'array'
        AND jsonb_array_length(elem->'team_responsible_names') > 0
    )
  ORDER BY detected_at DESC, id DESC
  LIMIT $2 OFFSET $3
`;

const UPDATE_SQL = `
  UPDATE public.newmark_jobcode_candidates
  SET job_code_parts = $2::jsonb,
      updated_at = now()
  WHERE id = $1
`;

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

function logEvent(event, payload = {}) {
  process.stdout.write(`${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`);
}

function getOpenAiApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

function parseJsonObjectLoose(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");
    if (first >= 0 && last > first) {
      const maybe = raw.slice(first, last + 1);
      try {
        return JSON.parse(maybe);
      } catch {
        return null;
      }
    }
    return null;
  }
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

function normalizedReferenceDir(reference) {
  return String(reference || "").replace(/[^A-Za-z0-9._-]/g, "_");
}

function readCachedCoverLetterText(artifactsDir, reference) {
  const refDir = normalizedReferenceDir(reference);
  const txtPath = path.join(path.resolve(artifactsDir), refDir, "cover_letter.txt");
  if (!fs.existsSync(txtPath)) return { txtPath, text: null };
  const text = fs.readFileSync(txtPath, "utf8");
  return { txtPath, text: String(text || "") };
}

function lastWords(text, count) {
  const n = Math.max(1, Number(count) || 1000);
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  if (words.length <= n) return words.join(" ");
  return words.slice(words.length - n).join(" ");
}

async function extractTeamNamesWithOpenAi(text, model, maxChars, timeoutMs, lastWordCount) {
  const apiKey = getOpenAiApiKey();
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not set");
  }

  const tailExcerpt = lastWords(text, lastWordCount);
  const excerpt = String(tailExcerpt || "").slice(0, Math.max(1000, Number(maxChars) || 24000));
  const endpointBase = process.env.OPENAI_BASE_URL
    ? String(process.env.OPENAI_BASE_URL).replace(/\/+$/, "")
    : "https://api.openai.com/v1";
  const url = `${endpointBase}/chat/completions`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(5000, Number(timeoutMs) || 60000));
  try {
    const resp = await fetch(url, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "Extract Newmark team contact names from UK planning cover letters. Return strict JSON only.",
          },
          {
            role: "user",
            content: `From this cover letter text, find the Newmark team contact names responsible.
These are often in the closing paragraph, e.g. "If you have any queries, please contact X or Y."

Return JSON with keys:
- team_contact_names (array of strings; empty array if none)
- evidence_quote (short exact phrase from the text, or null)
- confidence ("high" | "medium" | "low")

TEXT:
${excerpt}`,
          },
        ],
      }),
    });

    if (!resp.ok) {
      const errBody = await resp.text().catch(() => "");
      throw new Error(`OpenAI HTTP ${resp.status}: ${errBody.slice(0, 500)}`);
    }
    const payload = await resp.json();
    const content = payload?.choices?.[0]?.message?.content;
    const parsed = parseJsonObjectLoose(content);
    if (!parsed || typeof parsed !== "object") {
      throw new Error("OpenAI response did not contain parseable JSON object");
    }

    const teamContactNames = Array.isArray(parsed.team_contact_names)
      ? parsed.team_contact_names
          .map((name) => String(name == null ? "" : name).trim())
          .filter(Boolean)
      : [];

    return {
      team_contact_names: teamContactNames,
      evidence_quote: parsed.evidence_quote == null ? null : String(parsed.evidence_quote).trim() || null,
      confidence: parsed.confidence == null ? null : String(parsed.confidence).toLowerCase().trim() || null,
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function main() {
  const pgClient = new Client(getPgClientConfig());
  await pgClient.connect();
  try {
    logEvent("start", {
      ons_code: argv["ons-code"],
      limit: Number(argv.limit),
      offset: Number(argv.offset),
      model: String(argv["openai-model"]),
      dry_run: Boolean(argv["dry-run"]),
    });

    const res = await pgClient.query(SELECT_SQL, [
      argv["ons-code"] ? String(argv["ons-code"]).trim() : null,
      Number(argv.limit),
      Number(argv.offset),
    ]);

    const rows = res.rows || [];
    let scanned = 0;
    let missingText = 0;
    let openaiFailed = 0;
    let updated = 0;

    for (const row of rows) {
      scanned += 1;
      const reference = String(row.reference || "").trim();
      const currentParts = parseJobCodeParts(row.job_code_parts);
      if (!currentParts.length) continue;

      const { txtPath, text } = readCachedCoverLetterText(argv["artifacts-dir"], reference);
      if (!text || !text.trim()) {
        missingText += 1;
        logEvent("missing_cached_text", {
          reference,
          txt_path: txtPath,
        });
        continue;
      }

      let names = [];
      try {
        const team = await extractTeamNamesWithOpenAi(
          text,
          String(argv["openai-model"]),
          Number(argv["openai-max-chars"]),
          Number(argv["timeout-ms"]),
          Number(argv["openai-last-words"]),
        );
        names = team.team_contact_names || [];
      } catch (err) {
        openaiFailed += 1;
        logEvent("openai_extract_failed", {
          reference,
          error: err instanceof Error ? err.message : String(err),
        });
        continue;
      }

      const updatedParts = currentParts.map((part) => ({
        ...(part && typeof part === "object" ? part : {}),
        team_responsible_names: names,
      }));

      if (!argv["dry-run"]) {
        await pgClient.query(UPDATE_SQL, [row.id, JSON.stringify(updatedParts)]);
      }
      updated += 1;
      logEvent("row_backfilled", {
        reference,
        team_name_count: names.length,
        dry_run: Boolean(argv["dry-run"]),
      });
    }

    logEvent("done", {
      scanned,
      updated,
      missing_cached_text: missingText,
      openai_failed: openaiFailed,
    });
  } finally {
    await pgClient.end();
  }
}

main().catch((err) => {
  logEvent("fatal", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
