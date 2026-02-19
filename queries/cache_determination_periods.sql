WITH typed AS (
  SELECT
    reference,
    application_validated::date AS validated_date,
    decision_issued_date::date  AS decision_date,
    major,
    CASE
      WHEN reference ~* '/ADFULL$' THEN 'ADFULL'
      WHEN reference ~* '/ADLBC$'  THEN 'ADLBC'
      WHEN reference ~* '/FULL$'   THEN 'FULL'
      WHEN reference ~* '/LBC$'    THEN 'LBC'
      WHEN reference ~* '/ADV$'    THEN 'ADV'
      ELSE NULL
    END AS base_type
  FROM public.applications
  WHERE application_validated IS NOT NULL
    AND decision_issued_date IS NOT NULL
),
classified AS (
  SELECT
    CASE
      WHEN base_type = 'FULL' AND major = 'Major' THEN 'FULL (Major)'
      WHEN base_type = 'FULL'                    THEN 'FULL (Non-Major)'
      ELSE base_type
    END AS app_type,
    EXTRACT(YEAR FROM decision_date)::int AS yr,
    (decision_date - validated_date)      AS days_to_decision
  FROM typed
  WHERE base_type IS NOT NULL
    AND decision_date >= validated_date
),
agg AS (
  SELECT
    app_type,
    yr,
    ROUND(AVG(days_to_decision) / 7.0)::int AS avg_weeks
  FROM classified
  GROUP BY app_type, yr
),
bounds AS (
  SELECT GREATEST(2005, COALESCE(MIN(yr), 2005)) AS min_year,
         GREATEST(2005, COALESCE(MAX(yr), 2005)) AS max_year
  FROM agg
),
years AS (
  SELECT generate_series((SELECT min_year FROM bounds),
                         (SELECT max_year FROM bounds))::int AS yr
),
types AS (
  SELECT DISTINCT app_type
  FROM agg
),
grid AS (
  SELECT
    t.app_type,
    y.yr,
    COALESCE(a.avg_weeks, 0) AS avg_weeks,
    CASE
      WHEN t.app_type = 'FULL (Major)'      THEN 1
      WHEN t.app_type = 'FULL (Non-Major)'  THEN 2
      WHEN t.app_type = 'LBC'               THEN 3
      WHEN t.app_type = 'ADFULL'            THEN 4
      WHEN t.app_type = 'ADLBC'             THEN 5
      WHEN t.app_type = 'ADV'               THEN 6
      ELSE 99
    END AS sort_key
  FROM types t
  CROSS JOIN years y
  LEFT JOIN agg a
    ON a.app_type = t.app_type AND a.yr = y.yr
),
row_json AS (
  SELECT
    app_type,
    sort_key,
    (
      jsonb_build_object('type', app_type)
      ||
      jsonb_object_agg(('y' || yr)::text, to_jsonb(avg_weeks) ORDER BY yr)
    ) AS row_obj
  FROM grid
  GROUP BY app_type, sort_key
),
payload AS (
  SELECT COALESCE(jsonb_agg(row_obj ORDER BY sort_key, app_type), '[]'::jsonb) AS j
  FROM row_json
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'determination_times_by_type', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
