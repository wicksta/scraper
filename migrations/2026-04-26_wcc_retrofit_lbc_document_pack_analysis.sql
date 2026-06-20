-- Westminster retrofit-related listed building consent document-pack analysis cache.
-- Rollback:
--   DROP TABLE IF EXISTS public.wcc_retrofit_lbc_document_pack_analysis;

CREATE TABLE IF NOT EXISTS public.wcc_retrofit_lbc_document_pack_analysis (
  inventory_id bigint PRIMARY KEY REFERENCES public.wcc_retrofit_lbc_document_inventory(id) ON DELETE CASCADE,
  application_analysis_id bigint NOT NULL REFERENCES public.wcc_retrofit_lbc_applications(id) ON DELETE CASCADE,
  ons_code text NOT NULL,
  reference text NOT NULL,
  analysis_status text NOT NULL DEFAULT 'new',
  include_in_pack boolean,
  exclusion_reason text,
  normalized_pack_class text,
  normalized_pack_label text,
  classifier_stage text,
  model_confidence text,
  model_reasoning text,
  prompt_version text,
  model text,
  raw_model_json jsonb,
  analyzed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_pack_analysis_ref_idx
  ON public.wcc_retrofit_lbc_document_pack_analysis (ons_code, reference);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_pack_analysis_status_idx
  ON public.wcc_retrofit_lbc_document_pack_analysis (analysis_status, include_in_pack, normalized_pack_class);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_pack_analysis_app_idx
  ON public.wcc_retrofit_lbc_document_pack_analysis (application_analysis_id, normalized_pack_class);

CREATE INDEX IF NOT EXISTS wcc_retrofit_lbc_document_pack_analysis_raw_gin
  ON public.wcc_retrofit_lbc_document_pack_analysis
  USING gin (raw_model_json);
