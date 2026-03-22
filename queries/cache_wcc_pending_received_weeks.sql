WITH params AS (
  SELECT
    date_trunc('week', CURRENT_DATE)::date AS current_week_start,
    date_trunc('week', CURRENT_DATE - INTERVAL '51 weeks')::date AS window_start,
    (CURRENT_DATE - INTERVAL '8 weeks')::date AS cutoff_date
),
weeks AS (
  SELECT generate_series(
    (SELECT window_start FROM params),
    (SELECT current_week_start FROM params),
    INTERVAL '1 week'
  )::date AS week_start
),
received_all AS (
  SELECT
    a.application_received::date AS received_date,
    date_trunc('week', a.application_received::date)::date AS week_start
  FROM public.applications a
  CROSS JOIN params p
  WHERE a.ons_code = 'E09000033'
    AND a.application_received IS NOT NULL
    AND a.application_received::date <= CURRENT_DATE
    AND a.application_received::date >= p.window_start
),
pending AS (
  SELECT
    a.application_received::date AS received_date,
    date_trunc('week', a.application_received::date)::date AS week_start
  FROM public.applications a
  CROSS JOIN params p
  WHERE a.ons_code = 'E09000033'
    AND a.status = 'Pending'
    AND a.application_received IS NOT NULL
    AND a.application_received::date <= CURRENT_DATE
    AND a.application_received::date >= p.window_start
),
weekly_received AS (
  SELECT
    r.week_start,
    COUNT(*) FILTER (WHERE r.received_date > (SELECT cutoff_date FROM params))::int AS total_under_8_weeks,
    COUNT(*) FILTER (WHERE r.received_date <= (SELECT cutoff_date FROM params))::int AS total_over_8_weeks
  FROM received_all r
  GROUP BY r.week_start
),
weekly AS (
  SELECT
    p.week_start,
    COUNT(*) FILTER (WHERE p.received_date > (SELECT cutoff_date FROM params))::int AS pending_under_8_weeks,
    COUNT(*) FILTER (WHERE p.received_date <= (SELECT cutoff_date FROM params))::int AS pending_over_8_weeks
  FROM pending p
  GROUP BY p.week_start
),
rows AS (
  SELECT
    w.week_start,
    COALESCE(weekly.pending_under_8_weeks, 0) AS pending_under_8_weeks,
    COALESCE(weekly.pending_over_8_weeks, 0) AS pending_over_8_weeks,
    GREATEST(COALESCE(weekly_received.total_under_8_weeks, 0) - COALESCE(weekly.pending_under_8_weeks, 0), 0) AS other_under_8_weeks,
    GREATEST(COALESCE(weekly_received.total_over_8_weeks, 0) - COALESCE(weekly.pending_over_8_weeks, 0), 0) AS other_over_8_weeks,
    COALESCE(weekly_received.total_under_8_weeks, 0) + COALESCE(weekly_received.total_over_8_weeks, 0) AS total_received,
    COALESCE(weekly.pending_under_8_weeks, 0) + COALESCE(weekly.pending_over_8_weeks, 0) AS total_pending
  FROM weeks w
  LEFT JOIN weekly ON weekly.week_start = w.week_start
  LEFT JOIN weekly_received ON weekly_received.week_start = w.week_start
  ORDER BY w.week_start
),
payload AS (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'week_start', to_char(week_start, 'YYYY-MM-DD'),
        'pending_under_8_weeks', pending_under_8_weeks,
        'pending_over_8_weeks', pending_over_8_weeks,
        'other_under_8_weeks', other_under_8_weeks,
        'other_over_8_weeks', other_over_8_weeks,
        'total_received', total_received,
        'total_pending', total_pending
      )
      ORDER BY week_start
    ),
    '[]'::jsonb
  ) AS j
  FROM rows
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'wcc_pending_received_weeks', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
