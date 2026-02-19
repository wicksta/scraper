-- Document SRID/projection expectations for applications.wfs_geom.

BEGIN;

COMMENT ON COLUMN public.applications.wfs_geom IS
  'Westminster WFS geometry stored in EPSG:27700 (British National Grid). Reproject with ST_Transform(..., 4326 or 3857) for web maps.';

COMMIT;
