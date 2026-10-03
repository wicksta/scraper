CREATE TABLE IF NOT EXISTS public.lvmf_legacy_snapshots (
  id bigserial PRIMARY KEY,
  snapshot_label text NOT NULL UNIQUE,
  source_sha256 text NOT NULL,
  rows_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.lvmf_legacy_comparison_matches (
  id bigserial PRIMARY KEY,
  dataset_id bigint NOT NULL REFERENCES public.lvmf_2026_datasets(id) ON DELETE CASCADE,
  snapshot_id bigint NOT NULL REFERENCES public.lvmf_legacy_snapshots(id) ON DELETE CASCADE,
  new_view_code text NOT NULL,
  legacy_view_ref text NOT NULL,
  confidence text NOT NULL DEFAULT 'matched',
  UNIQUE(dataset_id,snapshot_id,new_view_code)
);
CREATE TABLE IF NOT EXISTS public.lvmf_legacy_comparison_areas (
  id bigserial PRIMARY KEY,
  match_id bigint NOT NULL REFERENCES public.lvmf_legacy_comparison_matches(id) ON DELETE CASCADE,
  area_code text NOT NULL,
  geom geometry(Polygon,27700) NOT NULL,
  UNIQUE(match_id,area_code)
);
CREATE INDEX IF NOT EXISTS lvmf_legacy_comparison_areas_geom_gix ON public.lvmf_legacy_comparison_areas USING gist(geom);
CREATE TABLE IF NOT EXISTS public.lvmf_legacy_comparison_results (
  id bigserial PRIMARY KEY,
  match_id bigint NOT NULL REFERENCES public.lvmf_legacy_comparison_matches(id) ON DELETE CASCADE,
  area_code text NOT NULL,
  legacy_area_m2 numeric,
  current_area_m2 numeric,
  intersection_m2 numeric,
  union_m2 numeric,
  jaccard numeric,
  symmetric_difference_m2 numeric,
  hausdorff_m numeric,
  height_profile_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(match_id,area_code)
);
-- Rollback: DROP TABLE public.lvmf_legacy_snapshots CASCADE;
