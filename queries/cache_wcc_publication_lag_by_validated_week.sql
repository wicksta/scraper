WITH params AS (
  SELECT
    date_trunc('week', CURRENT_DATE)::date AS this_week_start,
    (date_trunc('week', CURRENT_DATE) - INTERVAL '1 week')::date AS last_week_start
),
base AS (
  SELECT
    date_trunc('week', a.application_validated::date)::date AS validated_week,
    a.application_validated::date AS validated_date,
    a.first_seen_at::date AS first_seen_date
  FROM public.applications a
  CROSS JOIN params p
  WHERE a.ons_code = 'E09000033'
    AND a.application_received IS NOT NULL
    AND a.application_received::date <= CURRENT_DATE
    AND a.application_validated IS NOT NULL
    AND a.first_seen_at IS NOT NULL
    AND a.application_validated::date >= p.last_week_start
    AND a.first_seen_at::date >= a.application_validated::date
),
periods AS (
  SELECT 'This week'::text AS period_label, this_week_start AS week_start, 1 AS sort_order
  FROM params
  UNION ALL
  SELECT 'Last week'::text AS period_label, last_week_start AS week_start, 2 AS sort_order
  FROM params
),
summary AS (
  SELECT
    p.period_label,
    p.week_start,
    p.sort_order,
    COUNT(b.*)::int AS apps,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY (b.first_seen_date - b.validated_date))::numeric(10,2) AS median_validated_to_seen_days,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY (b.first_seen_date - b.validated_date))::numeric(10,2) AS p90_validated_to_seen_days
  FROM periods p
  LEFT JOIN base b
    ON b.validated_week = p.week_start
  GROUP BY p.period_label, p.week_start, p.sort_order
),
payload AS (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'period_label', period_label,
        'validated_week', to_char(week_start, 'YYYY-MM-DD'),
        'apps', apps,
        'median_validated_to_seen_days', median_validated_to_seen_days,
        'p90_validated_to_seen_days', p90_validated_to_seen_days
      )
      ORDER BY sort_order
    ),
    '[]'::jsonb
  ) AS j
  FROM summary
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'wcc_publication_lag_by_validated_week', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
