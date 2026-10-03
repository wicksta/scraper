-- Planning Portal submission answers captured alongside a condition-discharge workspace.
--
-- Rollback:
--   ALTER TABLE public.condition_discharge_workspaces
--     DROP COLUMN IF EXISTS portal_submission_json;

BEGIN;

ALTER TABLE public.condition_discharge_workspaces
  ADD COLUMN IF NOT EXISTS portal_submission_json jsonb NOT NULL DEFAULT '{}'::jsonb;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.condition_discharge_workspaces TO webapp;

COMMIT;
