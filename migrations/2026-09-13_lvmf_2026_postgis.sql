-- 2026 LVMF consultation-draft dataset.  This is intentionally separate from
-- the legacy MySQL `lvmf` table and the existing nGISt viewer.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS public.lvmf_2026_datasets (
  id bigserial PRIMARY KEY,
  version_label text NOT NULL UNIQUE,
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_views (
  id bigserial PRIMARY KEY,
  dataset_id bigint NOT NULL REFERENCES public.lvmf_2026_datasets(id) ON DELETE CASCADE,
  view_code text NOT NULL,
  view_name text NOT NULL,
  source_page integer,
  geometry_template text NOT NULL DEFAULT 'protected_vista',
  UNIQUE (dataset_id, view_code)
);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_control_points (
  id bigserial PRIMARY KEY,
  view_id bigint NOT NULL REFERENCES public.lvmf_2026_views(id) ON DELETE CASCADE,
  point_code text NOT NULL,
  height_m_aod numeric(8,2) NOT NULL,
  geom geometry(PointZ,27700) NOT NULL,
  source_page integer,
  UNIQUE (view_id, point_code)
);
CREATE INDEX IF NOT EXISTS lvmf_2026_control_points_geom_gix ON public.lvmf_2026_control_points USING gist (geom);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_areas (
  id bigserial PRIMARY KEY,
  view_id bigint NOT NULL REFERENCES public.lvmf_2026_views(id) ON DELETE CASCADE,
  area_code text NOT NULL,
  display_name text NOT NULL,
  geom geometry(Polygon,27700) NOT NULL,
  UNIQUE (view_id, area_code)
);
CREATE INDEX IF NOT EXISTS lvmf_2026_areas_geom_gix ON public.lvmf_2026_areas USING gist (geom);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_area_vertices (
  area_id bigint NOT NULL REFERENCES public.lvmf_2026_areas(id) ON DELETE CASCADE,
  vertex_order smallint NOT NULL,
  control_point_id bigint NOT NULL REFERENCES public.lvmf_2026_control_points(id),
  PRIMARY KEY (area_id, vertex_order)
);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_control_lines (
  id bigserial PRIMARY KEY,
  view_id bigint NOT NULL REFERENCES public.lvmf_2026_views(id) ON DELETE CASCADE,
  line_code text NOT NULL,
  display_name text NOT NULL,
  geom geometry(LineString,27700) NOT NULL,
  UNIQUE (view_id, line_code)
);
CREATE INDEX IF NOT EXISTS lvmf_2026_control_lines_geom_gix ON public.lvmf_2026_control_lines USING gist (geom);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_assessment_paths (
  id bigserial PRIMARY KEY,
  dataset_id bigint NOT NULL REFERENCES public.lvmf_2026_datasets(id) ON DELETE CASCADE,
  view_code text NOT NULL,
  view_name text NOT NULL,
  view_type text NOT NULL,
  source_page integer,
  UNIQUE (dataset_id, view_code)
);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_assessment_path_points (
  id bigserial PRIMARY KEY,
  assessment_path_id bigint NOT NULL REFERENCES public.lvmf_2026_assessment_paths(id) ON DELETE CASCADE,
  point_role text NOT NULL,
  point_order smallint NOT NULL,
  height_m_aod numeric(8,2) NOT NULL,
  geom geometry(PointZ,27700) NOT NULL,
  UNIQUE (assessment_path_id, point_role)
);
CREATE INDEX IF NOT EXISTS lvmf_2026_assessment_path_points_geom_gix ON public.lvmf_2026_assessment_path_points USING gist (geom);

CREATE TABLE IF NOT EXISTS public.lvmf_2026_threshold_rules (
  id bigserial PRIMARY KEY,
  view_id bigint NOT NULL REFERENCES public.lvmf_2026_views(id) ON DELETE CASCADE,
  area_code text NOT NULL,
  rule_code text NOT NULL,
  endpoint_a_code text NOT NULL DEFAULT 'a',
  endpoint_b_code text NOT NULL DEFAULT 'd',
  curvature_coefficient numeric(10,6),
  source_reference text NOT NULL,
  UNIQUE (view_id, area_code)
);

CREATE OR REPLACE FUNCTION public.lvmf_2026_set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END $$;
DROP TRIGGER IF EXISTS lvmf_2026_datasets_updated_at ON public.lvmf_2026_datasets;
CREATE TRIGGER lvmf_2026_datasets_updated_at BEFORE UPDATE ON public.lvmf_2026_datasets
FOR EACH ROW EXECUTE FUNCTION public.lvmf_2026_set_updated_at();

-- Rollback: DROP TABLE public.lvmf_2026_datasets CASCADE; DROP FUNCTION public.lvmf_2026_set_updated_at();
