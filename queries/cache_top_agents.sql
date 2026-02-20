WITH src AS (
  SELECT
    ons_code,
    reference,
    NULLIF(btrim(agent_company_name), '') AS company,
    NULLIF(btrim(agent_name), '')         AS person,
    lower(concat_ws(
      ' ',
      COALESCE(agent_company_name, ''),
      COALESCE(agent_name, '')
    )) AS txt
  FROM public.applications
),
tagged AS (
  SELECT
    ons_code,
    reference,
    company,
    person,
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
  FROM src
),
normalised AS (
  SELECT
    ons_code,
    reference,
    CASE
      WHEN canonical_agent IS NOT NULL THEN canonical_agent
      WHEN company IS NOT NULL THEN company
      ELSE person
    END AS agent
  FROM tagged
),
ranked AS (
  SELECT
    agent,
    COUNT(DISTINCT (ons_code, reference)) AS apps
  FROM normalised
  WHERE agent IS NOT NULL
  GROUP BY agent
  ORDER BY
    CASE WHEN agent = 'Newmark (inc Gerald Eve)' THEN 0 ELSE 1 END,
    apps DESC,
    agent
  LIMIT 50
),
payload AS (
  SELECT COALESCE(
    jsonb_agg(
      to_jsonb(ranked)
      ORDER BY
        CASE WHEN agent = 'Newmark (inc Gerald Eve)' THEN 0 ELSE 1 END,
        apps DESC,
        agent
    ),
    '[]'::jsonb
  ) AS j
  FROM ranked
)
INSERT INTO public.query_cache (cache_key, generated_at, ttl_seconds, payload)
SELECT 'top_agents', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
