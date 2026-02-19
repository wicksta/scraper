-- Add native PostGIS geometry storage for Westminster WFS geometry.
-- Canonical spatial column (SRID 27700) for map/spatial queries.
--
-- Rollback notes:
--   DROP INDEX IF EXISTS public.applications_wfs_geom_gix;
--   ALTER TABLE public.applications DROP COLUMN IF EXISTS wfs_geom;

BEGIN;

ALTER TABLE public.applications
  ADD COLUMN IF NOT EXISTS wfs_geom geometry(Geometry, 27700);

CREATE INDEX IF NOT EXISTS applications_wfs_geom_gix
  ON public.applications
  USING GIST (wfs_geom);

COMMIT;
