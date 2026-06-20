-- Westminster retrofit-related listed building consent analysis cache.
-- Rollback:
--   DROP TABLE IF EXISTS public.wcc_retrofit_lbc_document_inventory;
--   DROP TABLE IF EXISTS public.wcc_retrofit_lbc_applications;

CREATE TABLE IF NOT EXISTS public.wcc_retrofit_lbc_applications (
  id bigserial PRIMARY KEY,
  ons_code text NOT NULL,
  reference text NOT NULL,
  keyval text,
  application_received date,
  application_validated date,
  decision_made_date date,
  decision_issued_date date,
  decision text,
  actual_decision_level text,
  proposal text,
  address text,
  application_type text,
  source_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  prefilter_status text NOT NULL DEFAULT 'new',
  prefilter_keywords jsonb NOT NULL DEFAULT '[]'::jsonb,
  analysis_status text NOT NULL DEFAULT 'new',
  is_candidate boolean NOT NULL DEFAULT false,
  confidence text,
  reasoning text,
  retrofit_themes jsonb NOT NULL DEFAULT '[]'::jsonb,
  model text,
  prompt_version text,
  raw_model_json jsonb,
  determination_days integer,
  approval_status text,
  decision_route text,
  documents_url text,
  documents_scrape_status text NOT NULL DEFAULT 'not_started',
  documents_scraped_at timestamptz,
  documents_error text,
  document_count integer,
  first_classified_at timestamptz,
  last_classified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT wcc_retrofit_lbc_applications_ref_uq UNIQUE (ons_code, reference)
);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_applications_candidate_idx
  ON public.wcc_retrofit_lbc_applications (ons_code, is_candidate, analysis_status);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_applications_doc_status_idx
  ON public.wcc_retrofit_lbc_applications (documents_scrape_status, last_classified_at DESC);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_applications_themes_gin
  ON public.wcc_retrofit_lbc_applications
  USING gin (retrofit_themes);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_applications_snapshot_gin
  ON public.wcc_retrofit_lbc_applications
  USING gin (source_snapshot_json);

CREATE TABLE IF NOT EXISTS public.wcc_retrofit_lbc_document_inventory (
  id bigserial PRIMARY KEY,
  application_analysis_id bigint NOT NULL REFERENCES public.wcc_retrofit_lbc_applications(id) ON DELETE CASCADE,
  ons_code text NOT NULL,
  reference text NOT NULL,
  documents_url text,
  document_url text NOT NULL,
  raw_href text,
  description text,
  document_type text,
  published_date_raw text,
  published_date date,
  normalized_class text,
  raw_source_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  scraped_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT wcc_retrofit_lbc_document_inventory_doc_uq UNIQUE (reference, document_url)
);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_inventory_app_idx
  ON public.wcc_retrofit_lbc_document_inventory (application_analysis_id, normalized_class);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_inventory_ref_idx
  ON public.wcc_retrofit_lbc_document_inventory (ons_code, reference);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_inventory_raw_gin
  ON public.wcc_retrofit_lbc_document_inventory
  USING gin (raw_source_json);
