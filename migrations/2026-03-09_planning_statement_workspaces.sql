-- Planning statement drafting workspace tables.
-- Step 1 only: schema for workspaces, sections, and document linkage.
--
-- Rollback notes:
--   DROP TABLE IF EXISTS public.planning_statement_section_documents;
--   DROP TABLE IF EXISTS public.planning_statement_workspace_documents;
--   DROP TABLE IF EXISTS public.planning_statement_sections;
--   DROP TABLE IF EXISTS public.planning_statement_workspaces;

BEGIN;

CREATE TABLE IF NOT EXISTS public.planning_statement_workspaces (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_number text NOT NULL,
  application_ref text NOT NULL,
  application_id bigint,
  synthetic_id text,
  title text,
  site_address text,
  client_name text,
  local_authority text,
  facts_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  style_guidance_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS planning_statement_workspaces_job_number_idx
  ON public.planning_statement_workspaces (job_number);

CREATE INDEX IF NOT EXISTS planning_statement_workspaces_application_ref_idx
  ON public.planning_statement_workspaces (application_ref);

CREATE INDEX IF NOT EXISTS planning_statement_workspaces_application_id_idx
  ON public.planning_statement_workspaces (application_id);

CREATE INDEX IF NOT EXISTS planning_statement_workspaces_synthetic_id_idx
  ON public.planning_statement_workspaces (synthetic_id);

CREATE INDEX IF NOT EXISTS planning_statement_workspaces_facts_gin
  ON public.planning_statement_workspaces
  USING gin (facts_json);

CREATE INDEX IF NOT EXISTS planning_statement_workspaces_style_guidance_gin
  ON public.planning_statement_workspaces
  USING gin (style_guidance_json);

CREATE TABLE IF NOT EXISTS public.planning_statement_sections (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id uuid NOT NULL REFERENCES public.planning_statement_workspaces(id) ON DELETE CASCADE,
  section_key text NOT NULL,
  title text NOT NULL,
  position integer NOT NULL,
  is_selected boolean NOT NULL DEFAULT true,
  status text NOT NULL DEFAULT 'not_started',
  draft_text text,
  draft_summary text,
  prompt_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  generation_meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_drafted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT planning_statement_sections_workspace_key_uq
    UNIQUE (workspace_id, section_key)
);

CREATE INDEX IF NOT EXISTS planning_statement_sections_workspace_position_idx
  ON public.planning_statement_sections (workspace_id, position);

CREATE INDEX IF NOT EXISTS planning_statement_sections_workspace_status_idx
  ON public.planning_statement_sections (workspace_id, status);

CREATE INDEX IF NOT EXISTS planning_statement_sections_prompt_context_gin
  ON public.planning_statement_sections
  USING gin (prompt_context);

CREATE INDEX IF NOT EXISTS planning_statement_sections_generation_meta_gin
  ON public.planning_statement_sections
  USING gin (generation_meta);

CREATE TABLE IF NOT EXISTS public.planning_statement_workspace_documents (
  workspace_id uuid NOT NULL REFERENCES public.planning_statement_workspaces(id) ON DELETE CASCADE,
  doc_id uuid NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
  display_title text,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, doc_id)
);

CREATE INDEX IF NOT EXISTS planning_statement_workspace_documents_doc_idx
  ON public.planning_statement_workspace_documents (doc_id);

CREATE TABLE IF NOT EXISTS public.planning_statement_section_documents (
  section_id uuid NOT NULL REFERENCES public.planning_statement_sections(id) ON DELETE CASCADE,
  doc_id uuid NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
  relevance_note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (section_id, doc_id)
);

CREATE INDEX IF NOT EXISTS planning_statement_section_documents_doc_idx
  ON public.planning_statement_section_documents (doc_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.planning_statement_workspaces TO webapp;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.planning_statement_sections TO webapp;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.planning_statement_workspace_documents TO webapp;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.planning_statement_section_documents TO webapp;

COMMIT;
