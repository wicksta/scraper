WITH typed AS (
  SELECT
    CASE
      WHEN reference ~* '/FULL$'   THEN 'FULL'
      WHEN reference ~* '/FULMAJ$' THEN 'FULMAJ'
      WHEN reference ~* '/FULEIA$' THEN 'FULEIA'
      WHEN reference ~* '/LBC$'    THEN 'LBC'
      WHEN reference ~* '/ADVT$'   THEN 'ADVT'
      WHEN reference ~* '/MDC$'    THEN 'MDC'
      WHEN reference ~* '/LDC$'    THEN 'LDC'
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
      WHEN txt ~* '\m(avison young)\M' THEN 'Avison Young'
      WHEN txt ~* '\m(daniel watney)\M' THEN 'Daniel Watney'
      WHEN txt ~* '\m(iceni projects|iceni)\M' THEN 'Iceni Projects'
      WHEN txt ~* '\m(jones lang lasalle ltd|jones lang lasalle|jll)\M' THEN 'JLL'
      ELSE NULL
    END AS canonical_agent,
    application_validated::date AS validated_date,
    decision_issued_date::date  AS issued_date
  FROM public.applications
  CROSS JOIN LATERAL (
    SELECT lower(concat_ws(' ', COALESCE(agent_company_name, ''), COALESCE(agent_name, ''), COALESCE(agent_address, ''))) AS txt
  ) t
  WHERE ons_code = 'E09000001'
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
    ROUND(AVG(weeks_to_decision), 1) AS newmark_mean_weeks,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY weeks_to_decision))::numeric, 1) AS newmark_p75_weeks,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY weeks_to_decision))::numeric, 1) AS newmark_p90_weeks
  FROM timed
  WHERE canonical_agent = 'Newmark (inc Gerald Eve)'
  GROUP BY app_type
),
overall AS (
  SELECT
    app_type,
    ROUND(AVG(weeks_to_decision), 1) AS overall_mean_weeks,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY weeks_to_decision))::numeric, 1) AS overall_p75_weeks,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY weeks_to_decision))::numeric, 1) AS overall_p90_weeks
  FROM timed
  GROUP BY app_type
),
types AS (
  SELECT * FROM (VALUES
    ('FULL', 1),
    ('FULMAJ', 2),
    ('FULEIA', 3),
    ('LBC', 4),
    ('ADVT', 5),
    ('MDC', 6),
    ('LDC', 7)
  ) AS t(app_type, sort_order)
),
rows AS (
  SELECT
    t.app_type,
    n.newmark_mean_weeks,
    n.newmark_p75_weeks,
    n.newmark_p90_weeks,
    o.overall_mean_weeks,
    o.overall_p75_weeks,
    o.overall_p90_weeks,
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
SELECT 'city_agent_compare_baseline_timing_percentiles', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
