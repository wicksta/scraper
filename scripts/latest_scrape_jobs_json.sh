#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

if [[ -f "${ENV_FILE}" ]]; then
  DATABASE_URL="$(awk -F= '/^DATABASE_URL=/{print substr($0, index($0,$2))}' "${ENV_FILE}" | tail -n 1 | tr -d '\r' | sed -e 's/^"//' -e 's/"$//')"
else
  DATABASE_URL="${DATABASE_URL:-}"
fi

SQL=$(cat <<'SQL'
WITH latest_completed AS (
  SELECT
    id,
    status,
    job_type,
    ons_code,
    application_ref,
    source,
    requested_by,
    mapper,
    attempts,
    max_attempts,
    created_at,
    updated_at,
    locked_at,
    locked_by,
    result->>'started_at' AS scraper_started_at,
    result->>'finished_at' AS scraper_finished_at,
    COALESCE(
      result->'planit'->'planit'->>'date_received',
      result->'unified'->'tabs'->'datesTable'->>'application_received_date',
      result->'unified'->'tabs'->'simpleDetailsTable'->>'application_received'
    ) AS submission_date,
    COALESCE(
      result->'planit'->'planit'->>'date_validated',
      result->'unified'->'tabs'->'datesTable'->>'application_validated_date',
      result->'unified'->'tabs'->'simpleDetailsTable'->>'application_validated'
    ) AS validation_date,
    COALESCE(
      result->'planit'->'planit'->>'decided_date',
      result->'unified'->'tabs'->'datesTable'->>'decision_issued_date',
      result->'unified'->'tabs'->'datesTable'->>'decision_made_date'
    ) AS determination_date
  FROM public.scrape_jobs
  WHERE status = 'completed'
  ORDER BY COALESCE(locked_at, updated_at, created_at) DESC, id DESC
  LIMIT 1
),
latest_non_completed AS (
  SELECT
    id,
    status,
    job_type,
    ons_code,
    application_ref,
    source,
    requested_by,
    mapper,
    attempts,
    max_attempts,
    created_at,
    updated_at,
    locked_at,
    locked_by,
    result->>'started_at' AS scraper_started_at,
    result->>'finished_at' AS scraper_finished_at,
    COALESCE(
      result->'planit'->'planit'->>'date_received',
      result->'unified'->'tabs'->'datesTable'->>'application_received_date',
      result->'unified'->'tabs'->'simpleDetailsTable'->>'application_received'
    ) AS submission_date,
    COALESCE(
      result->'planit'->'planit'->>'date_validated',
      result->'unified'->'tabs'->'datesTable'->>'application_validated_date',
      result->'unified'->'tabs'->'simpleDetailsTable'->>'application_validated'
    ) AS validation_date,
    COALESCE(
      result->'planit'->'planit'->>'decided_date',
      result->'unified'->'tabs'->'datesTable'->>'decision_issued_date',
      result->'unified'->'tabs'->'datesTable'->>'decision_made_date'
    ) AS determination_date
  FROM public.scrape_jobs
  WHERE status <> 'completed'
  ORDER BY COALESCE(locked_at, updated_at, created_at) DESC, id DESC
  LIMIT 1
)
SELECT json_build_object(
  'generated_at', now(),
  'last_completed', (SELECT row_to_json(latest_completed) FROM latest_completed),
  'most_recent_non_completed', (SELECT row_to_json(latest_non_completed) FROM latest_non_completed)
);
SQL
)

PSQL_ARGS=(-X -q -t -A)

if [[ -n "${DATABASE_URL:-}" ]]; then
  PSQL_ARGS+=("${DATABASE_URL}")
else
  PSQL_ARGS+=(-d "${PGDATABASE:-docs_db}")
fi

psql "${PSQL_ARGS[@]}" -c "${SQL}"
