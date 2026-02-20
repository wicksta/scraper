WITH committee_base AS (
  SELECT
    EXTRACT(YEAR FROM actual_committee_date)::int AS yr,
    application_received::date AS received_date,
    application_validated::date AS validated_date,
    actual_committee_date::date AS committee_date,
    decision_issued_date::date AS issued_date,
    UPPER(BTRIM(COALESCE(decision, ''))) AS decision_norm
  FROM public.applications
  WHERE actual_decision_level IN ('Committee Decision', 'Full Committee', 'Sub-Committee')
    AND actual_committee_date IS NOT NULL
),
receipt_to_validation AS (
  SELECT
    yr,
    ROUND(AVG((validated_date - received_date)::numeric) / 7.0, 1) AS avg_weeks
  FROM committee_base
  WHERE received_date IS NOT NULL
    AND validated_date IS NOT NULL
    AND validated_date >= received_date
  GROUP BY yr
),
validation_to_committee AS (
  SELECT
    yr,
    ROUND(AVG((committee_date - validated_date)::numeric) / 7.0, 1) AS avg_weeks
  FROM committee_base
  WHERE validated_date IS NOT NULL
    AND committee_date >= validated_date
  GROUP BY yr
),
committee_to_permission_issue AS (
  SELECT
    yr,
    ROUND(AVG((issued_date - committee_date)::numeric) / 7.0, 1) AS avg_weeks
  FROM committee_base
  WHERE issued_date IS NOT NULL
    AND committee_date IS NOT NULL
    AND issued_date >= committee_date
    AND decision_norm IN ('APPLICATION PERMITTED', 'PERMITTED', 'GRANTED', 'APPROVED')
  GROUP BY yr
),
validation_to_decision_issue AS (
  SELECT
    yr,
    ROUND(AVG((issued_date - validated_date)::numeric) / 7.0, 1) AS avg_weeks
  FROM committee_base
  WHERE issued_date IS NOT NULL
    AND validated_date IS NOT NULL
    AND issued_date >= validated_date
  GROUP BY yr
),
bounds AS (
  SELECT
    COALESCE(MIN(yr), 2005) AS min_year,
    COALESCE(MAX(yr), 2005) AS max_year
  FROM committee_base
),
years AS (
  SELECT generate_series(
    (SELECT min_year FROM bounds),
    (SELECT max_year FROM bounds)
  )::int AS yr
),
stage_values AS (
  SELECT 'Receipt to validation (weeks)' AS type, yr, avg_weeks FROM receipt_to_validation
  UNION ALL
  SELECT 'Validation to committee (weeks)' AS type, yr, avg_weeks FROM validation_to_committee
  UNION ALL
  SELECT 'Committee to permission issue (weeks)' AS type, yr, avg_weeks FROM committee_to_permission_issue
  UNION ALL
  SELECT 'Validation to decision issue (weeks)' AS type, yr, avg_weeks FROM validation_to_decision_issue
),
stages AS (
  SELECT * FROM (VALUES
    ('Receipt to validation (weeks)', 1),
    ('Validation to committee (weeks)', 2),
    ('Committee to permission issue (weeks)', 3),
    ('Validation to decision issue (weeks)', 4)
  ) AS s(type, sort_key)
),
grid AS (
  SELECT
    s.type,
    s.sort_key,
    y.yr,
    sv.avg_weeks
  FROM stages s
  CROSS JOIN years y
  LEFT JOIN stage_values sv
    ON sv.type = s.type
   AND sv.yr = y.yr
),
row_json AS (
  SELECT
    type,
    sort_key,
    (
      jsonb_build_object('type', type)
      ||
      jsonb_object_agg(('y' || yr)::text, to_jsonb(avg_weeks) ORDER BY yr)
    ) AS row_obj
  FROM grid
  GROUP BY type, sort_key
),
payload AS (
  SELECT COALESCE(jsonb_agg(row_obj ORDER BY sort_key, type), '[]'::jsonb) AS j
  FROM row_json
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'committee_timing_stages_by_year', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
