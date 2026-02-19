-- Add WFS geometry payload storage to applications.
-- Intended payload shape:
-- {
--   "wkt": "POLYGON((...))" | "POINT(...)",
--   "bbox": { "minE": ..., "minN": ..., "maxE": ..., "maxN": ... },
--   ...optional extra fields
-- }
--
-- Rollback notes:
--   ALTER TABLE public.applications DROP CONSTRAINT IF EXISTS applications_wfs_geometry_is_object;
--   ALTER TABLE public.applications DROP COLUMN IF EXISTS wfs_geometry;

BEGIN;

ALTER TABLE public.applications
  ADD COLUMN IF NOT EXISTS wfs_geometry jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'applications_wfs_geometry_is_object'
      AND conrelid = 'public.applications'::regclass
  ) THEN
    ALTER TABLE public.applications
      ADD CONSTRAINT applications_wfs_geometry_is_object
      CHECK (wfs_geometry IS NULL OR jsonb_typeof(wfs_geometry) = 'object');
  END IF;
END $$;

COMMIT;
