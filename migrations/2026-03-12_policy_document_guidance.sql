-- Rollback:
--   DROP INDEX IF EXISTS public.policy_document_guidance_updated_at_idx;
--   DROP TABLE IF EXISTS public.policy_document_guidance;

CREATE TABLE IF NOT EXISTS public.policy_document_guidance (
  doc_id uuid PRIMARY KEY REFERENCES public.documents(id) ON DELETE CASCADE,
  explanation_text text NOT NULL DEFAULT '',
  custom_prompt_text text NOT NULL DEFAULT '',
  test_page_start integer,
  test_page_end integer,
  last_test_job_id bigint,
  last_execute_job_id bigint,
  created_by integer,
  updated_by integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS policy_document_guidance_updated_at_idx
  ON public.policy_document_guidance (updated_at DESC);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.policy_document_guidance TO webapp;
