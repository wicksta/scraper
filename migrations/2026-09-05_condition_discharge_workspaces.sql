-- Persistent condition-discharge drafting workspaces.
--
-- Rollback notes:
--   DROP TABLE IF EXISTS public.condition_discharge_workspace_documents;
--   DROP TABLE IF EXISTS public.condition_discharge_workspaces;

BEGIN;

CREATE TABLE IF NOT EXISTS public.condition_discharge_workspaces (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_number text NOT NULL,
  condition_tracker_id bigint NOT NULL,
  decision_notice_doc_id uuid NOT NULL REFERENCES public.documents(id) ON DELETE RESTRICT,
  selected_condition_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  title text,
  site_address text,
  client_name text,
  local_authority text,
  development_description text,
  recipient_name text,
  recipient_address text,
  specific_instructions text,
  status text NOT NULL DEFAULT 'not_started',
  draft_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  draft_text text,
  generation_job_id bigint,
  generation_meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS condition_discharge_workspaces_job_number_idx
  ON public.condition_discharge_workspaces (job_number, updated_at DESC);
CREATE INDEX IF NOT EXISTS condition_discharge_workspaces_tracker_idx
  ON public.condition_discharge_workspaces (condition_tracker_id);
CREATE INDEX IF NOT EXISTS condition_discharge_workspaces_decision_notice_idx
  ON public.condition_discharge_workspaces (decision_notice_doc_id);

CREATE TABLE IF NOT EXISTS public.condition_discharge_workspace_documents (
  workspace_id uuid NOT NULL REFERENCES public.condition_discharge_workspaces(id) ON DELETE CASCADE,
  doc_id uuid NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
  display_title text,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, doc_id)
);

CREATE INDEX IF NOT EXISTS condition_discharge_workspace_documents_doc_idx
  ON public.condition_discharge_workspace_documents (doc_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.condition_discharge_workspaces TO webapp;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.condition_discharge_workspace_documents TO webapp;

COMMIT;
