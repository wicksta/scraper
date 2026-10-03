WITH typed AS (
  SELECT
    CASE
  WHEN lower(btrim(application_type)) = 'full planning permission' THEN 'FULL'
  WHEN lower(btrim(application_type)) = 'listed building consent (alt/ext)' THEN 'LBC'
  WHEN lower(btrim(application_type)) = 'consent to display an advertisement' THEN 'ADV'
  WHEN lower(btrim(application_type)) = 'approval of details reserved by a condition' THEN 'ADDETAILS'
  WHEN lower(btrim(application_type)) = 'non-material amendment' THEN 'NMA'
  WHEN lower(btrim(application_type)) = 'section 106 obligation discharge' THEN 'S106'
  WHEN lower(btrim(application_type)) = 'lawful development: proposed use' THEN 'LDC_PROPOSED'
  WHEN lower(btrim(application_type)) = 'demolition in a conservation area' THEN 'DEMOLITION'
  WHEN lower(btrim(application_type)) = 'removal/variation of conditions' THEN 'VARCOND'
  WHEN lower(btrim(application_type)) = 'deed of variation' THEN 'DEED_VARIATION'
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
  WHEN txt ~* '\m(iceni projects|iceni)\M' THEN 'Iceni Projects'
  WHEN txt ~* '\m(jones lang lasalle ltd|jones lang lasalle|jll)\M' THEN 'JLL'
  WHEN txt ~* '\m(cushman.*wakefield)\M' THEN 'Cushman & Wakefield'
  WHEN txt ~* '\m(bidwells)\M' THEN 'Bidwells'
  WHEN txt ~* '\m(firstplan)\M' THEN 'Firstplan'
  WHEN txt ~* '\m(drew planning)\M' THEN 'Drew Planning & Development'
  WHEN txt ~* '\m(ferris.*sloane)\M' THEN 'Ferris & Sloane'
  WHEN txt ~* '\m(walsingham planning)\M' THEN 'Walsingham Planning'
  ELSE NULL
END AS canonical_agent,
    NULLIF(btrim(COALESCE(
      decision,
      unified_json #>> '{tabs,further_information,extracted,tables,applicationDetails,decision}',
      unified_json #>> '{tabs,summary,extracted,tables,simpleDetailsTable,decision}',
      planit_json #>> '{planit,decision}'
    )), '') AS decision_outcome
  FROM public.applications
  CROSS JOIN LATERAL (
    SELECT lower(concat_ws(' ', COALESCE(agent_company_name, ''), COALESCE(agent_name, ''), COALESCE(agent_address, ''))) AS txt
  ) t
  WHERE ons_code = 'E51000008'
),
decided AS (
  SELECT
    app_type,
    canonical_agent,
    lower(decision_outcome) AS decision_outcome
  FROM typed
  WHERE app_type IS NOT NULL
    AND decision_outcome IS NOT NULL
    AND lower(decision_outcome) NOT IN ('under consideration', 'under consultation', 'new application', 'pending')
),
newmark AS (
  SELECT
    app_type,
    ROUND(
      100.0 * COUNT(*) FILTER (
        WHERE decision_outcome IN ('application permitted', 'permitted', 'granted', 'approved')
      ) / NULLIF(COUNT(*), 0),
      1
    ) AS newmark_approval_pct,
    COUNT(*) AS newmark_n
  FROM decided
  WHERE canonical_agent = 'Newmark (inc Gerald Eve)'
  GROUP BY app_type
),
overall AS (
  SELECT
    app_type,
    ROUND(
      100.0 * COUNT(*) FILTER (
        WHERE decision_outcome IN ('application permitted', 'permitted', 'granted', 'approved')
      ) / NULLIF(COUNT(*), 0),
      1
    ) AS overall_approval_pct,
    COUNT(*) AS overall_n
  FROM decided
  GROUP BY app_type
),
types AS (
  SELECT * FROM (VALUES
    ('FULL', 1),
    ('LBC', 2),
    ('ADV', 3),
    ('ADDETAILS', 4),
    ('NMA', 5),
    ('S106', 6),
    ('LDC_PROPOSED', 7),
    ('DEMOLITION', 8),
    ('VARCOND', 9),
    ('DEED_VARIATION', 10)
  ) AS t(app_type, sort_order)
),
rows AS (
  SELECT
    t.app_type,
    n.newmark_approval_pct,
    COALESCE(n.newmark_n, 0) AS newmark_n,
    o.overall_approval_pct,
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
SELECT 'osdc_agent_compare_baseline_approval', now(), 86400, payload.j
FROM payload
ON CONFLICT (cache_key)
DO UPDATE SET generated_at = EXCLUDED.generated_at,
              ttl_seconds  = EXCLUDED.ttl_seconds,
              payload      = EXCLUDED.payload;
