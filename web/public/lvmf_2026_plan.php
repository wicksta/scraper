<?php
declare(strict_types=1);
require_once __DIR__ . '/lvmf_2026_lib.php';
lvmf_2026_auth();
try {
    $pg = lvmf_2026_pg();
    $dataset = $pg->query("SELECT id,version_label,source_metadata FROM lvmf_2026_datasets WHERE version_label='2026-consultation-draft'")->fetch();
    if (!$dataset) lvmf_2026_json_error(503, 'LVMF 2026 dataset has not been loaded.');
    $features = [];
    $queries = [
      ['areas', "SELECT v.view_code,v.view_name,a.area_code,a.display_name,ST_AsGeoJSON(ST_Transform(a.geom,4326)) geometry FROM lvmf_2026_areas a JOIN lvmf_2026_views v ON v.id=a.view_id WHERE v.dataset_id=:id", 'area'],
      ['control_points', "SELECT v.view_code,v.view_name,p.point_code,p.height_m_aod,(v.view_code='5B' AND p.point_code IN ('l','m','n','o','p','q','r','s','t')) is_intermediate_control,ST_AsGeoJSON(ST_Transform(p.geom,4326)) geometry FROM lvmf_2026_control_points p JOIN lvmf_2026_views v ON v.id=p.view_id WHERE v.dataset_id=:id", 'control_point'],
      ['assessment_points', "SELECT a.view_code,a.view_name,a.view_type,p.point_role,p.height_m_aod,ST_X(p.geom) easting_m,ST_Y(p.geom) northing_m,ST_AsGeoJSON(ST_Transform(p.geom,4326)) geometry FROM lvmf_2026_assessment_path_points p JOIN lvmf_2026_assessment_paths a ON a.id=p.assessment_path_id WHERE a.dataset_id=:id", 'assessment_point'],
      ['assessment_paths', "SELECT a.view_code,a.view_name,a.view_type,ST_AsGeoJSON(ST_Transform(ST_MakeLine(ST_Force2D(p.geom) ORDER BY p.point_order),4326)) geometry FROM lvmf_2026_assessment_path_points p JOIN lvmf_2026_assessment_paths a ON a.id=p.assessment_path_id WHERE a.dataset_id=:id GROUP BY a.id,a.view_code,a.view_name,a.view_type HAVING count(*) FILTER (WHERE p.point_role='start')=1 AND count(*) FILTER (WHERE p.point_role='end')=1", 'assessment_path'],
      ['control_lines', "SELECT v.view_code,l.line_code,l.display_name,ST_AsGeoJSON(ST_Transform(l.geom,4326)) geometry FROM lvmf_2026_control_lines l JOIN lvmf_2026_views v ON v.id=l.view_id WHERE v.dataset_id=:id", 'control_line'],
    ];
    foreach ($queries as [$key, $sql, $kind]) { $st=$pg->prepare($sql); $st->execute([':id'=>$dataset['id']]); $features[$key]=array_map(static fn($row) => ['type'=>'Feature','geometry'=>json_decode($row['geometry'],true),'properties'=>['kind'=>$kind]+array_diff_key($row,['geometry'=>true])],$st->fetchAll()); }
    docling_json_out(200, ['success'=>true,'dataset'=>['version'=>$dataset['version_label'],'source_metadata'=>json_decode($dataset['source_metadata'],true)],'layers'=>array_map(static fn($items)=>['type'=>'FeatureCollection','features'=>$items],$features)]);
} catch (Throwable $e) { error_log('[lvmf-2026-plan] '.$e->getMessage()); lvmf_2026_json_error(500, 'Unable to load the LVMF plan.'); }
