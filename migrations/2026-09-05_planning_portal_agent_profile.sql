-- Shared agent identity used to prepare Planning Portal submission packs.
-- Rollback: DROP TABLE IF EXISTS public.planning_portal_agent_profiles;
BEGIN;
CREATE TABLE IF NOT EXISTS public.planning_portal_agent_profiles (
  profile_key text PRIMARY KEY,
  profile_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_by integer,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.planning_portal_agent_profiles (profile_key, profile_json)
VALUES ('shared_team', '{"title":"–","first_name":"–","surname":"–","address_line_1":"c/o Agent","address_line_2":"–","address_line_3":"–","town_city":"–","country":"United Kingdom","postcode":"W1T 3JJ"}'::jsonb)
ON CONFLICT (profile_key) DO NOTHING;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.planning_portal_agent_profiles TO webapp;
COMMIT;
