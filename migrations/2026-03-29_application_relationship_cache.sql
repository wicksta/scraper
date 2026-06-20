-- Relationship analysis cache
-- Rollback:
--   DROP TABLE IF EXISTS public.application_relationship_family_members;
--   DROP TABLE IF EXISTS public.application_relationship_families;

CREATE TABLE IF NOT EXISTS public.application_relationship_families (
  id bigserial PRIMARY KEY,
  ons_code text NOT NULL,
  root_reference text,
  family_hash text NOT NULL,
  graph_json jsonb NOT NULL,
  relationship_json jsonb NOT NULL,
  model text,
  graph_version text NOT NULL,
  prompt_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT application_relationship_families_identity_uq
    UNIQUE (ons_code, family_hash, graph_version, prompt_version)
);

CREATE INDEX IF NOT EXISTS application_relationship_families_ons_root_idx
  ON public.application_relationship_families (ons_code, root_reference);

CREATE INDEX IF NOT EXISTS application_relationship_families_graph_gin
  ON public.application_relationship_families
  USING gin (graph_json);

CREATE INDEX IF NOT EXISTS application_relationship_families_relationship_gin
  ON public.application_relationship_families
  USING gin (relationship_json);

CREATE TABLE IF NOT EXISTS public.application_relationship_family_members (
  family_id bigint NOT NULL REFERENCES public.application_relationship_families(id) ON DELETE CASCADE,
  ons_code text NOT NULL,
  reference text NOT NULL,
  role_hint text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (family_id, reference),
  CONSTRAINT application_relationship_family_members_ref_uq
    UNIQUE (ons_code, reference)
);

CREATE INDEX IF NOT EXISTS application_relationship_family_members_ref_idx
  ON public.application_relationship_family_members (ons_code, reference);
