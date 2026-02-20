WITH app_text AS (
  SELECT
    ons_code,
    reference,
    application_validated,
    major,
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
    major,
    CASE
      WHEN txt ~* '\m(dp9 limited|dp9)\M' THEN 'DP9'
      WHEN txt ~* '\m(turley associates|turley)\M' THEN 'Turley'
      WHEN txt ~* '\m(newmark gerald eve llp|gerald eve llp|gerald eve|newmark)\M' THEN 'Newmark (inc Gerald Eve)'
      WHEN txt ~* '\m(savills)\M' THEN 'Savills'
      WHEN txt ~* '\m(rolfe judd)\M' THEN 'Rolfe Judd'
      WHEN txt ~* '\m(montagu evans llp|montagu evans)\M' THEN 'Montagu Evans'
      WHEN txt ~* '\m(cb richard ellis|cbre)\M' THEN 'CBRE'
      WHEN txt ~* '\m(howard de walden management ltd|howard de walden|howard de/walden|howard de\\/walden)\M' THEN 'Howard de Walden'
      WHEN txt ~* '\m(jones lang lasalle ltd|jones lang lasalle|jll)\M' THEN 'JLL'
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
    AND major = 'Major'
  GROUP BY canonical_agent, EXTRACT(YEAR FROM application_validated)
),
majors_all_year AS (
  SELECT
    EXTRACT(YEAR FROM application_validated)::int AS yr,
    COUNT(DISTINCT (ons_code, reference)) AS cnt
  FROM tagged
  WHERE application_validated IS NOT NULL
    AND major = 'Major'
  GROUP BY EXTRACT(YEAR FROM application_validated)
),
bounds AS (
  SELECT
    2005 AS min_year,
    GREATEST(
      2005,
      COALESCE((SELECT MAX(yr) FROM base), 2005),
      COALESCE((SELECT MAX(yr) FROM majors_all_year), 2005)
    ) AS max_year
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
    ) AS row_obj
  FROM grid
  GROUP BY agent
),
total_row_collapsed AS (
  SELECT
    'TOTAL (All majors)' AS agent,
    1 AS is_total,
    (
      jsonb_build_object('agent', 'TOTAL (All majors)')
      ||
      jsonb_object_agg(key, val ORDER BY key)
    ) AS row_obj
  FROM (
    SELECT
      ('y' || y.yr)::text AS key,
      to_jsonb(COALESCE(m.cnt, 0)) AS val
    FROM years y
    LEFT JOIN majors_all_year m
      ON m.yr = y.yr
  ) y
),
all_rows AS (
  SELECT agent, is_total, row_obj FROM rows_by_agent
  UNION ALL
  SELECT agent, is_total, row_obj FROM total_row_collapsed
),
payload AS (
  SELECT COALESCE(
    jsonb_agg(
      row_obj
      ORDER BY
        is_total ASC,
        CASE WHEN agent = 'Newmark (inc Gerald Eve)' THEN 0 ELSE 1 END,
        agent
    ),
    '[]'::jsonb
  ) AS j
  FROM all_rows
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'agents_by_year_majors', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
