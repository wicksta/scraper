WITH typed AS (
  SELECT
    CASE
      WHEN reference ~* '/ADFULL$' THEN 'ADFULL'
      WHEN reference ~* '/ADLBC$'  THEN 'ADLBC'
      WHEN reference ~* '/FULL$'   AND major = 'Major' THEN 'FULL (Major)'
      WHEN reference ~* '/FULL$'   THEN 'FULL (Non-Major)'
      WHEN reference ~* '/LBC$'    THEN 'LBC'
      WHEN reference ~* '/ADV$'    THEN 'ADV'
      ELSE NULL
    END AS app_type,
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
    END AS canonical_agent,
    application_validated::date AS validated_date,
    decision_issued_date::date  AS issued_date
  FROM public.applications
  CROSS JOIN LATERAL (
    SELECT lower(concat_ws(' ', COALESCE(agent_company_name, ''), COALESCE(agent_name, ''), COALESCE(agent_address, ''))) AS txt
  ) t
),
timed AS (
  SELECT
    app_type,
    canonical_agent,
    (issued_date - validated_date) / 7.0 AS weeks_to_decision
  FROM typed
  WHERE app_type IS NOT NULL
    AND validated_date IS NOT NULL
    AND issued_date IS NOT NULL
    AND issued_date >= validated_date
),
newmark AS (
  SELECT
    app_type,
    ROUND(AVG(weeks_to_decision), 1) AS newmark_avg_weeks,
    COUNT(*) AS newmark_n
  FROM timed
  WHERE canonical_agent = 'Newmark (inc Gerald Eve)'
  GROUP BY app_type
),
overall AS (
  SELECT
    app_type,
    ROUND(AVG(weeks_to_decision), 1) AS overall_avg_weeks,
    COUNT(*) AS overall_n
  FROM timed
  GROUP BY app_type
),
types AS (
  SELECT * FROM (VALUES
    ('FULL (Major)', 1),
    ('FULL (Non-Major)', 2),
    ('LBC', 3),
    ('ADFULL', 4),
    ('ADLBC', 5),
    ('ADV', 6)
  ) AS t(app_type, sort_order)
),
rows AS (
  SELECT
    t.app_type,
    n.newmark_avg_weeks,
    COALESCE(n.newmark_n, 0) AS newmark_n,
    o.overall_avg_weeks,
    COALESCE(o.overall_n, 0) AS overall_n,
    t.sort_order
  FROM types t
  LEFT JOIN newmark n USING (app_type)
  LEFT JOIN overall o USING (app_type)
),
payload AS (
  SELECT COALESCE(
    jsonb_agg(
      to_jsonb(rows) - 'sort_order'
      ORDER BY sort_order
    ),
    '[]'::jsonb
  ) AS j
  FROM rows
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'wcc_agent_compare_baseline_timing', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
