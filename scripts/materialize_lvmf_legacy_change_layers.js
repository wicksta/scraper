#!/usr/bin/env node
import "../bootstrap.js";
import pg from "pg";

const version = "2026-consultation-draft";
const snapshot = "legacy-mysql-viewer-2026-09-13";
const methodology = "Literal legacy viewer versus Appendix E: red/green are union set differences; yellow is shared land without a like-for-like area category.";
const client = new pg.Client(process.env.DATABASE_URL || { host: process.env.PGHOST, port: Number(process.env.PGPORT || 5432), database: process.env.PGDATABASE, user: process.env.PGUSER, password: process.env.PGPASSWORD });
const clean = (expression) => `ST_Multi(ST_CollectionExtract(ST_MakeValid(${expression}),3))`;
const insertMatched = (changeClass, geometry, explanation) => `
  WITH matched AS (
    SELECT m.id,m.new_view_code,m.legacy_view_ref,
      (SELECT ST_UnaryUnion(ST_Collect(la.geom)) FROM lvmf_legacy_comparison_areas la WHERE la.match_id=m.id) legacy_geom,
      (SELECT ST_UnaryUnion(ST_Collect(ca.geom)) FROM lvmf_2026_areas ca JOIN lvmf_2026_views v ON v.id=ca.view_id WHERE v.dataset_id=$2 AND v.view_code=m.new_view_code) current_geom,
      (SELECT jsonb_agg(DISTINCT la.area_code ORDER BY la.area_code) FROM lvmf_legacy_comparison_areas la WHERE la.match_id=m.id) legacy_codes,
      (SELECT jsonb_agg(DISTINCT ca.area_code ORDER BY ca.area_code) FROM lvmf_2026_areas ca JOIN lvmf_2026_views v ON v.id=ca.view_id WHERE v.dataset_id=$2 AND v.view_code=m.new_view_code) current_codes,
      (SELECT ST_UnaryUnion(ST_Collect(ST_Intersection(la.geom,ca.geom)))
       FROM lvmf_legacy_comparison_areas la
       JOIN lvmf_2026_views v ON v.dataset_id=$2 AND v.view_code=m.new_view_code
       JOIN lvmf_2026_areas ca ON ca.view_id=v.id AND ca.area_code=la.area_code
       WHERE la.match_id=m.id) same_category_geom
    FROM lvmf_legacy_comparison_matches m
    WHERE m.dataset_id=$2 AND m.snapshot_id=$3 AND m.confidence='matched'
  ), raw AS (
    SELECT *, ${clean(geometry)} geom FROM matched
  )
  INSERT INTO lvmf_legacy_comparison_changes(change_set_id,match_id,new_view_code,legacy_view_ref,change_class,legacy_area_codes,current_area_codes,explanation,area_m2,geom)
  SELECT $1,id,new_view_code,legacy_view_ref,'${changeClass}',COALESCE(legacy_codes,'[]'::jsonb),COALESCE(current_codes,'[]'::jsonb),'${explanation}',ST_Area(geom),geom
  FROM raw WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)`;

try {
  await client.connect(); await client.query("BEGIN");
  const ids = await client.query(`SELECT d.id dataset_id,s.id snapshot_id FROM lvmf_2026_datasets d CROSS JOIN lvmf_legacy_snapshots s WHERE d.version_label=$1 AND s.snapshot_label=$2`, [version, snapshot]);
  if (ids.rowCount !== 1) throw new Error("Expected one Appendix E dataset and one legacy snapshot.");
  const { dataset_id: datasetId, snapshot_id: snapshotId } = ids.rows[0];
  const set = await client.query(`INSERT INTO lvmf_legacy_comparison_change_sets(dataset_id,snapshot_id,methodology) VALUES($1,$2,$3) ON CONFLICT(dataset_id,snapshot_id) DO UPDATE SET methodology=EXCLUDED.methodology,created_at=now() RETURNING id`, [datasetId, snapshotId, methodology]);
  const changeSetId = set.rows[0].id;
  await client.query("DELETE FROM lvmf_legacy_comparison_changes WHERE change_set_id=$1", [changeSetId]);
  await client.query("DELETE FROM lvmf_legacy_comparison_height_changes WHERE change_set_id=$1", [changeSetId]);

  await client.query(insertMatched("newly_protected", "ST_Difference(current_geom,legacy_geom)", "Appendix E constraint footprint outside every legacy constraint area."), [changeSetId, datasetId, snapshotId]);
  await client.query(insertMatched("released", "ST_Difference(legacy_geom,current_geom)", "Legacy constraint footprint outside every Appendix E constraint area."), [changeSetId, datasetId, snapshotId]);
  await client.query(insertMatched("changed", "ST_Difference(ST_Intersection(legacy_geom,current_geom),COALESCE(same_category_geom,ST_GeomFromText('POLYGON EMPTY',27700)))", "Shared footprint where the legacy and Appendix E area categories do not match."), [changeSetId, datasetId, snapshotId]);

  await client.query(`WITH raw AS (
    SELECT m.id,m.new_view_code,m.legacy_view_ref,${clean("ST_UnaryUnion(ST_Collect(a.geom))")} geom,
      (SELECT jsonb_agg(DISTINCT area_code ORDER BY area_code) FROM lvmf_legacy_comparison_areas WHERE match_id=m.id) legacy_codes
    FROM lvmf_legacy_comparison_matches m JOIN lvmf_legacy_comparison_areas a ON a.match_id=m.id
    WHERE m.dataset_id=$2 AND m.snapshot_id=$3 AND m.confidence='legacy_only' GROUP BY m.id,m.new_view_code,m.legacy_view_ref
  ) INSERT INTO lvmf_legacy_comparison_changes(change_set_id,match_id,new_view_code,legacy_view_ref,change_class,legacy_area_codes,current_area_codes,explanation,area_m2,geom)
  SELECT $1,id,new_view_code,legacy_view_ref,'released',legacy_codes,'[]'::jsonb,'Legacy-only view: its former constraint footprint is released in Appendix E.',ST_Area(geom),geom FROM raw WHERE NOT ST_IsEmpty(geom)`, [changeSetId, datasetId, snapshotId]);

  await client.query(`WITH raw AS (
    SELECT v.view_code,${clean("ST_UnaryUnion(ST_Collect(a.geom))")} geom,jsonb_agg(DISTINCT a.area_code ORDER BY a.area_code) current_codes
    FROM lvmf_2026_views v JOIN lvmf_2026_areas a ON a.view_id=v.id
    WHERE v.dataset_id=$2 AND NOT EXISTS (SELECT 1 FROM lvmf_legacy_comparison_matches m WHERE m.dataset_id=$2 AND m.snapshot_id=$3 AND m.new_view_code=v.view_code)
    GROUP BY v.view_code
  ) INSERT INTO lvmf_legacy_comparison_changes(change_set_id,new_view_code,change_class,legacy_area_codes,current_area_codes,explanation,area_m2,geom)
  SELECT $1,view_code,'newly_protected','[]'::jsonb,current_codes,'Appendix E-only view: its complete constraint footprint is newly protected.',ST_Area(geom),geom FROM raw WHERE NOT ST_IsEmpty(geom)`, [changeSetId, datasetId, snapshotId]);

  await client.query(`INSERT INTO lvmf_legacy_comparison_height_changes(change_set_id,match_id,station_fraction,legacy_height_m_aod,current_height_m_aod,delta_m,methodology)
    SELECT $1,r.match_id,(p->>'station')::numeric,(p->>'legacy_m_aod')::numeric,(p->>'current_m_aod')::numeric,((p->>'current_m_aod')::numeric-(p->>'legacy_m_aod')::numeric),'Legacy A→B and Appendix E A→D curvature/refraction profiles, coefficient 0.0673.'
    FROM lvmf_legacy_comparison_results r CROSS JOIN LATERAL jsonb_array_elements(r.height_profile_json) p
    JOIN lvmf_legacy_comparison_matches m ON m.id=r.match_id
    WHERE m.dataset_id=$2 AND m.snapshot_id=$3 AND m.confidence='matched' AND r.area_code='vc'`, [changeSetId, datasetId, snapshotId]);
  const counts = await client.query(`SELECT change_class,count(*)::int features,round(sum(area_m2))::bigint area_m2 FROM lvmf_legacy_comparison_changes WHERE change_set_id=$1 GROUP BY change_class ORDER BY change_class`, [changeSetId]);
  await client.query("COMMIT");
  console.log(JSON.stringify({ success: true, change_set_id: changeSetId, classes: counts.rows }, null, 2));
} catch (error) { await client.query("ROLLBACK").catch(() => {}); throw error; } finally { await client.end(); }
