-- Structured policy units extracted from policy documents.
--
-- Rollback notes:
--   DROP INDEX IF EXISTS public.policy_units_unit_vec_hnsw;
--   DROP INDEX IF EXISTS public.policy_units_source_meta_gin;
--   DROP INDEX IF EXISTS public.policy_units_heading_path_gin;
--   DROP INDEX IF EXISTS public.policy_units_page_start_idx;
--   DROP INDEX IF EXISTS public.policy_units_policy_number_idx;
--   DROP INDEX IF EXISTS public.policy_units_unit_type_idx;
--   DROP INDEX IF EXISTS public.policy_units_doc_id_idx;
--   DROP TABLE IF EXISTS public.policy_units;

BEGIN;

CREATE TABLE IF NOT EXISTS public.policy_units (
  id bigserial PRIMARY KEY,
  doc_id uuid NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
  unit_key text NOT NULL,
  unit_type text NOT NULL,
  section_title text,
  heading_path_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  policy_number text,
  policy_title text,
  policy_text text,
  supporting_text text,
  keywords_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  topics_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  page_start integer,
  page_end integer,
  prev_unit_id bigint REFERENCES public.policy_units(id) ON DELETE SET NULL,
  next_unit_id bigint REFERENCES public.policy_units(id) ON DELETE SET NULL,
  source_meta_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  unit_vec vector(1536),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT policy_units_doc_key_uq UNIQUE (doc_id, unit_key)
);

CREATE INDEX IF NOT EXISTS policy_units_doc_id_idx
  ON public.policy_units (doc_id);

CREATE INDEX IF NOT EXISTS policy_units_unit_type_idx
  ON public.policy_units (unit_type);

CREATE INDEX IF NOT EXISTS policy_units_policy_number_idx
  ON public.policy_units (policy_number);

CREATE INDEX IF NOT EXISTS policy_units_page_start_idx
  ON public.policy_units (doc_id, page_start, page_end);

CREATE INDEX IF NOT EXISTS policy_units_heading_path_gin
  ON public.policy_units
  USING gin (heading_path_json);

CREATE INDEX IF NOT EXISTS policy_units_source_meta_gin
  ON public.policy_units
  USING gin (source_meta_json);

CREATE INDEX IF NOT EXISTS policy_units_unit_vec_hnsw
  ON public.policy_units
  USING hnsw (unit_vec vector_cosine_ops);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.policy_units TO webapp;
GRANT USAGE, SELECT ON SEQUENCE public.policy_units_id_seq TO webapp;

COMMIT;
