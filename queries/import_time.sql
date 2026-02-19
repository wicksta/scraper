WITH filtered AS (
  SELECT
    id,
    updated_at,
    params,
    date_trunc('month', (params->>'window_start')::date) AS month_bucket
  FROM public.scrape_jobs
  WHERE requested_by ILIKE '%validated_discovery%'
    AND params ? 'window_start'
),
gaps AS (
  SELECT
    id,
    month_bucket,
    updated_at,
    LEAD(updated_at) OVER (ORDER BY updated_at, id) AS next_updated_at
  FROM filtered
),
monthly AS (
  SELECT
    month_bucket::date AS month,
    COUNT(*) FILTER (WHERE next_updated_at IS NOT NULL) AS gap_count,
    AVG(next_updated_at - updated_at) FILTER (WHERE next_updated_at IS NOT NULL) AS avg_gap_interval,
    AVG(EXTRACT(EPOCH FROM (next_updated_at - updated_at))) FILTER (WHERE next_updated_at IS NOT NULL) AS avg_gap_seconds,
    SUM(EXTRACT(EPOCH FROM (next_updated_at - updated_at))) FILTER (WHERE next_updated_at IS NOT NULL) / 3600.0 AS approx_total_hours
  FROM gaps
  GROUP BY month_bucket
),
payload AS (
  SELECT
    COALESCE(
      jsonb_agg(to_jsonb(monthly) ORDER BY month),
      '[]'::jsonb
    ) AS j
  FROM monthly
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'import_time', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
