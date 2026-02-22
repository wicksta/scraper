-- Stores per-application Newmark job-code detection output for review/enrichment.
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS public.newmark_jobcode_candidates (
  id bigserial PRIMARY KEY,
  ons_code text NOT NULL,
  reference text NOT NULL,
  keyval text,
  agent_company_name text,
  is_newmark boolean NOT NULL DEFAULT false,
  documents_url text,
  source_doc_url text,
  source_doc_description text,
  cover_docs_considered jsonb NOT NULL DEFAULT '[]'::jsonb,
  job_codes_found jsonb NOT NULL DEFAULT '[]'::jsonb,
  job_code_parts jsonb NOT NULL DEFAULT '[]'::jsonb,
  match_confidence text NOT NULL DEFAULT 'none',
  notes jsonb NOT NULL DEFAULT '[]'::jsonb,
  error text,
  status text NOT NULL DEFAULT 'new',
  detected_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS newmark_jobcode_candidates_ref_uq
  ON public.newmark_jobcode_candidates (ons_code, reference);

CREATE INDEX IF NOT EXISTS newmark_jobcode_candidates_status_idx
  ON public.newmark_jobcode_candidates (status, detected_at DESC);

