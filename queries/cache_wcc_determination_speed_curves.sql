WITH scopes AS (
  SELECT *
  FROM (VALUES
    ('wcc_determination_speed_curves', false),
    ('wcc_determination_speed_curves_5y', true)
  ) AS s(cache_key, use_five_years)
),
typed_base AS (
  SELECT
    s.cache_key,
    CASE
      WHEN a.reference ~* '/FULL$'   AND a.major = 'Major' THEN 'MAJOR'
      WHEN a.reference ~* '/FULL$'   THEN 'NON-MAJOR'
      WHEN a.reference ~* '/LBC$'    THEN 'LBC'
      WHEN a.reference ~* '/ADFULL$' THEN 'ADFULL'
      WHEN a.reference ~* '/ADLBC$'  THEN 'ADLBC'
      WHEN a.reference ~* '/ADV$'    THEN 'ADV'
      ELSE NULL
    END AS app_type,
    CASE
      WHEN t.txt ~* '\m(dp9 limited|dp9)\M' THEN 'DP9'
      WHEN t.txt ~* '\m(turley associates|turley)\M' THEN 'Turley'
      WHEN t.txt ~* '\m(newmark gerald eve llp|gerald eve llp|gerald eve|newmark)\M' THEN 'Newmark (inc Gerald Eve)'
      WHEN t.txt ~* '\m(savills)\M' THEN 'Savills'
      WHEN t.txt ~* '\m(rolfe judd)\M' THEN 'Rolfe Judd'
      WHEN t.txt ~* '\m(montagu evans llp|montagu evans)\M' THEN 'Montagu Evans'
      WHEN t.txt ~* '\m(cb richard ellis|cbre)\M' THEN 'CBRE'
      WHEN t.txt ~* '\m(howard de walden management ltd|howard de walden|howard de/walden|howard de\\/walden)\M' THEN 'Howard de Walden'
      WHEN t.txt ~* '\m(jones lang lasalle ltd|jones lang lasalle|jll)\M' THEN 'JLL'
      ELSE NULL
    END AS canonical_agent,
    a.actual_decision_level,
    GREATEST(
      0,
      LEAST(
        156,
        CEIL((a.decision_issued_date::date - a.application_validated::date) / 7.0)::int
      )
    ) AS week_bin
  FROM public.applications a
  CROSS JOIN scopes s
  CROSS JOIN LATERAL (
    SELECT lower(concat_ws(' ', COALESCE(a.agent_company_name, ''), COALESCE(a.agent_name, ''), COALESCE(a.agent_address, ''))) AS txt
  ) t
  WHERE a.application_validated IS NOT NULL
    AND a.decision_issued_date IS NOT NULL
    AND a.decision_issued_date::date >= a.application_validated::date
    AND (
      NOT s.use_five_years
      OR a.application_validated::date >= (CURRENT_DATE - INTERVAL '5 years')::date
    )
),
typed AS (
  SELECT cache_key, app_type, canonical_agent, week_bin
  FROM typed_base
  WHERE app_type IS NOT NULL

  UNION ALL

  SELECT cache_key, 'COMMITTEE' AS app_type, canonical_agent, week_bin
  FROM typed_base
  WHERE lower(btrim(COALESCE(actual_decision_level, ''))) IN (
    'committee decision',
    'full committee',
    'sub-committee'
  )
),
base AS (
  SELECT cache_key, app_type, canonical_agent, week_bin
  FROM typed
),
types AS (
  SELECT *
  FROM (VALUES
    ('MAJOR', 1),
    ('NON-MAJOR', 2),
    ('LBC', 3),
    ('ADFULL', 4),
    ('ADLBC', 5),
    ('ADV', 6),
    ('COMMITTEE', 7)
  ) AS t(app_type, sort_order)
),
bounds AS (
  SELECT cache_key, GREATEST(1, COALESCE(MAX(week_bin), 1)) AS max_week
  FROM base
  GROUP BY cache_key
),
weeks AS (
  SELECT b.cache_key, generate_series(0, b.max_week)::int AS week_no
  FROM bounds b
),
market_week_counts AS (
  SELECT cache_key, app_type, week_bin AS week_no, COUNT(*)::int AS cnt
  FROM base
  GROUP BY cache_key, app_type, week_bin
),
newmark_week_counts AS (
  SELECT cache_key, app_type, week_bin AS week_no, COUNT(*)::int AS cnt
  FROM base
  WHERE canonical_agent = 'Newmark (inc Gerald Eve)'
  GROUP BY cache_key, app_type, week_bin
),
market_total AS (
  SELECT cache_key, app_type, COUNT(*)::int AS total_n
  FROM base
  GROUP BY cache_key, app_type
),
newmark_total AS (
  SELECT cache_key, app_type, COUNT(*)::int AS total_n
  FROM base
  WHERE canonical_agent = 'Newmark (inc Gerald Eve)'
  GROUP BY cache_key, app_type
),
market_grid AS (
  SELECT
    w.cache_key,
    t.app_type,
    w.week_no,
    COALESCE(m.cnt, 0) AS week_cnt
  FROM weeks w
  CROSS JOIN types t
  LEFT JOIN market_week_counts m
    ON m.cache_key = w.cache_key
   AND m.app_type = t.app_type
   AND m.week_no = w.week_no
),
newmark_grid AS (
  SELECT
    w.cache_key,
    t.app_type,
    w.week_no,
    COALESCE(n.cnt, 0) AS week_cnt
  FROM weeks w
  CROSS JOIN types t
  LEFT JOIN newmark_week_counts n
    ON n.cache_key = w.cache_key
   AND n.app_type = t.app_type
   AND n.week_no = w.week_no
),
market_cum AS (
  SELECT
    cache_key,
    app_type,
    week_no,
    SUM(week_cnt) OVER (
      PARTITION BY cache_key, app_type
      ORDER BY week_no
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_cnt
  FROM market_grid
),
newmark_cum AS (
  SELECT
    cache_key,
    app_type,
    week_no,
    SUM(week_cnt) OVER (
      PARTITION BY cache_key, app_type
      ORDER BY week_no
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_cnt
  FROM newmark_grid
),
points AS (
  SELECT
    w.cache_key,
    t.app_type,
    t.sort_order,
    w.week_no,
    CASE
      WHEN nt.total_n > 0 THEN ROUND((100.0 * n.cum_cnt / nt.total_n)::numeric, 2)
      ELSE NULL
    END AS newmark_pct,
    CASE
      WHEN mt.total_n > 0 THEN ROUND((100.0 * m.cum_cnt / mt.total_n)::numeric, 2)
      ELSE NULL
    END AS market_pct,
    COALESCE(nt.total_n, 0) AS newmark_n,
    COALESCE(mt.total_n, 0) AS market_n
  FROM weeks w
  CROSS JOIN types t
  LEFT JOIN newmark_cum n
    ON n.cache_key = w.cache_key
   AND n.app_type = t.app_type
   AND n.week_no = w.week_no
  LEFT JOIN market_cum m
    ON m.cache_key = w.cache_key
   AND m.app_type = t.app_type
   AND m.week_no = w.week_no
  LEFT JOIN newmark_total nt
    ON nt.cache_key = w.cache_key
   AND nt.app_type = t.app_type
  LEFT JOIN market_total mt
    ON mt.cache_key = w.cache_key
   AND mt.app_type = t.app_type
),
agent_list AS (
  SELECT DISTINCT cache_key, canonical_agent AS agent
  FROM base
  WHERE canonical_agent IS NOT NULL
),
agent_week_counts AS (
  SELECT cache_key, app_type, canonical_agent AS agent, week_bin AS week_no, COUNT(*)::int AS cnt
  FROM base
  WHERE canonical_agent IS NOT NULL
  GROUP BY cache_key, app_type, canonical_agent, week_bin
),
agent_totals AS (
  SELECT cache_key, app_type, canonical_agent AS agent, COUNT(*)::int AS total_n
  FROM base
  WHERE canonical_agent IS NOT NULL
  GROUP BY cache_key, app_type, canonical_agent
),
agent_grid AS (
  SELECT
    a.cache_key,
    t.app_type,
    a.agent,
    w.week_no,
    COALESCE(awc.cnt, 0) AS week_cnt
  FROM agent_list a
  CROSS JOIN types t
  JOIN weeks w
    ON w.cache_key = a.cache_key
  LEFT JOIN agent_week_counts awc
    ON awc.cache_key = a.cache_key
   AND awc.app_type = t.app_type
   AND awc.agent = a.agent
   AND awc.week_no = w.week_no
),
agent_cum AS (
  SELECT
    cache_key,
    app_type,
    agent,
    week_no,
    SUM(week_cnt) OVER (
      PARTITION BY cache_key, app_type, agent
      ORDER BY week_no
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_cnt
  FROM agent_grid
),
agent_points AS (
  SELECT
    ac.cache_key,
    ac.app_type,
    ac.agent,
    at.total_n,
    jsonb_agg(
      jsonb_build_object(
        'week', ac.week_no,
        'pct', CASE
          WHEN at.total_n > 0 THEN ROUND((100.0 * ac.cum_cnt / at.total_n)::numeric, 2)
          ELSE NULL
        END
      )
      ORDER BY ac.week_no
    ) AS points
  FROM agent_cum ac
  JOIN agent_totals at
    ON at.cache_key = ac.cache_key
   AND at.app_type = ac.app_type
   AND at.agent = ac.agent
  GROUP BY ac.cache_key, ac.app_type, ac.agent, at.total_n
),
agent_curves_by_type AS (
  SELECT
    cache_key,
    app_type,
    jsonb_agg(
      jsonb_build_object(
        'agent', agent,
        'n', total_n,
        'points', points
      )
      ORDER BY
        CASE WHEN agent = 'Newmark (inc Gerald Eve)' THEN 0 ELSE 1 END,
        agent
    ) AS curves
  FROM agent_points
  GROUP BY cache_key, app_type
),
rows AS (
  SELECT
    p.cache_key,
    p.app_type,
    p.sort_order,
    MAX(p.newmark_n) AS newmark_n,
    MAX(p.market_n) AS market_n,
    jsonb_agg(
      jsonb_build_object(
        'week', p.week_no,
        'newmark_pct', p.newmark_pct,
        'market_pct', p.market_pct
      )
      ORDER BY p.week_no
    ) AS points,
    COALESCE(ac.curves, '[]'::jsonb) AS agent_curves
  FROM points p
  LEFT JOIN agent_curves_by_type ac
    ON ac.cache_key = p.cache_key
   AND ac.app_type = p.app_type
  GROUP BY p.cache_key, p.app_type, p.sort_order, ac.curves
),
payload AS (
  SELECT
    s.cache_key,
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'type', r.app_type,
          'target_weeks', CASE WHEN r.app_type IN ('MAJOR', 'COMMITTEE') THEN 13 ELSE 8 END,
          'newmark_n', r.newmark_n,
          'market_n', r.market_n,
          'points', r.points,
          'agent_curves', r.agent_curves
        )
        ORDER BY r.sort_order
      ) FILTER (WHERE r.app_type IS NOT NULL),
      '[]'::jsonb
    ) AS j
  FROM scopes s
  LEFT JOIN rows r
    ON r.cache_key = s.cache_key
  GROUP BY s.cache_key
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT cache_key, now(), 86400, j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
