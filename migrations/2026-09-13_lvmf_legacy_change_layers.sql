-- Materialised spatial and control-surface changes between the immutable
-- legacy MySQL snapshot and the Appendix E dataset.  This is deliberately
-- separate from both source datasets and can be regenerated at any time.
CREATE TABLE IF NOT EXISTS public.lvmf_legacy_comparison_change_sets (
  id bigserial PRIMARY KEY,
  dataset_id bigint NOT NULL REFERENCES public.lvmf_2026_datasets(id) ON DELETE CASCADE,
  snapshot_id bigint NOT NULL REFERENCES public.lvmf_legacy_snapshots(id) ON DELETE CASCADE,
  methodology text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (dataset_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS public.lvmf_legacy_comparison_changes (
  id bigserial PRIMARY KEY,
  change_set_id bigint NOT NULL REFERENCES public.lvmf_legacy_comparison_change_sets(id) ON DELETE CASCADE,
  match_id bigint REFERENCES public.lvmf_legacy_comparison_matches(id) ON DELETE SET NULL,
  new_view_code text,
  legacy_view_ref text,
  change_class text NOT NULL CHECK (change_class IN ('newly_protected', 'released', 'changed')),
  legacy_area_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  current_area_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  explanation text NOT NULL,
  area_m2 numeric NOT NULL,
  geom geometry(MultiPolygon,27700) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS lvmf_legacy_comparison_changes_geom_gix
  ON public.lvmf_legacy_comparison_changes USING gist (geom);
CREATE INDEX IF NOT EXISTS lvmf_legacy_comparison_changes_set_class_idx
  ON public.lvmf_legacy_comparison_changes (change_set_id, change_class);

CREATE TABLE IF NOT EXISTS public.lvmf_legacy_comparison_height_changes (
  id bigserial PRIMARY KEY,
  change_set_id bigint NOT NULL REFERENCES public.lvmf_legacy_comparison_change_sets(id) ON DELETE CASCADE,
  match_id bigint NOT NULL REFERENCES public.lvmf_legacy_comparison_matches(id) ON DELETE CASCADE,
  station_fraction numeric(5,4) NOT NULL,
  legacy_height_m_aod numeric,
  current_height_m_aod numeric,
  delta_m numeric,
  methodology text NOT NULL,
  UNIQUE (change_set_id, match_id, station_fraction)
);

-- Rollback: DROP TABLE public.lvmf_legacy_comparison_change_sets CASCADE;
