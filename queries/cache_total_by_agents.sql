WITH app_text AS (
  SELECT
    ons_code,
    reference,
    application_validated,
    lower(concat_ws(
      ' ',
      COALESCE(agent_company_name, ''),
      COALESCE(agent_name, ''),
      COALESCE(agent_address, '')
    )) AS txt
  FROM public.applications
),
tagged AS (
  SELECT
    ons_code,
    reference,
    application_validated,
    CASE
      WHEN txt ~* '\m(dp9 limited|dp9)\M' THEN 'DP9'
      WHEN txt ~* '\m(turley associates|turley)\M' THEN 'Turley'
      WHEN txt ~* '\m(newmark gerald eve llp|gerald eve llp|gerald eve|newmark)\M' THEN 'Gerald Eve'
      WHEN txt ~* '\m(savills)\M' THEN 'Savills'
      WHEN txt ~* '\m(rolfe judd)\M' THEN 'Rolfe Judd'
      WHEN txt ~* '\m(montagu evans llp|montagu evans)\M' THEN 'Montagu Evans'
      WHEN txt ~* '\m(cbre)\M' THEN 'CBRE'
      ELSE NULL
    END AS canonical_agent
  FROM app_text
),
base AS (
  SELECT
    canonical_agent,
    EXTRACT(YEAR FROM application_validated)::int AS yr,
    COUNT(DISTINCT (ons_code, reference)) AS cnt
  FROM tagged
  WHERE canonical_agent IS NOT NULL
    AND application_validated IS NOT NULL
  GROUP BY canonical_agent, EXTRACT(YEAR FROM application_validated)
),
bounds AS (
  SELECT
    2005 AS min_year,
    GREATEST(2005, COALESCE(MAX(yr), 2005)) AS max_year
  FROM base
),
years AS (
  SELECT generate_series((SELECT min_year FROM bounds),
                         (SELECT max_year FROM bounds))::int AS yr
),
agents AS (
  SELECT DISTINCT canonical_agent AS agent
  FROM base
),
grid AS (
  SELECT
    a.agent,
    y.yr,
    COALESCE(b.cnt, 0) AS cnt
  FROM agents a
  CROSS JOIN years y
  LEFT JOIN base b
    ON b.canonical_agent = a.agent
   AND b.yr = y.yr
),
rows_by_agent AS (
  SELECT
    agent,
    0 AS is_total,
    (
      jsonb_build_object('agent', agent)
      ||
      jsonb_object_agg(('y' || yr)::text, to_jsonb(cnt) ORDER BY yr)
      ||
      jsonb_build_object('total', SUM(cnt))
    ) AS row_obj
  FROM grid
  GROUP BY agent
),
total_row_collapsed AS (
  SELECT
    'TOTAL (All applications)' AS agent,
    1 AS is_total,
    (
      jsonb_build_object('agent', 'TOTAL (All applications)')
      ||
      jsonb_object_agg(key, val ORDER BY key)
      ||
      jsonb_build_object('total', total_sum)
    ) AS row_obj
  FROM (
    SELECT
      ('y' || yr)::text AS key,
      to_jsonb(SUM(cnt)) AS val
    FROM grid
    GROUP BY yr
  ) y
  CROSS JOIN (
    SELECT SUM(cnt) AS total_sum FROM grid
  ) t
  GROUP BY total_sum
),
all_rows AS (
  SELECT agent, is_total, row_obj FROM rows_by_agent
  UNION ALL
  SELECT agent, is_total, row_obj FROM total_row_collapsed
),
payload AS (
  SELECT COALESCE(
    jsonb_agg(row_obj ORDER BY is_total DESC, (row_obj->>'total')::int DESC, agent),
    '[]'::jsonb
  ) AS j
  FROM all_rows
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'agents_by_year', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;