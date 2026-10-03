<?php
declare(strict_types=1);

require_once __DIR__ . '/lvmf_2026_lib.php';
lvmf_2026_auth();

try {
    $pg = lvmf_2026_pg();
    $changeSet = $pg->query("SELECT s.id,s.methodology,s.created_at FROM lvmf_legacy_comparison_change_sets s JOIN lvmf_2026_datasets d ON d.id=s.dataset_id JOIN lvmf_legacy_snapshots l ON l.id=s.snapshot_id WHERE d.version_label='2026-consultation-draft' AND l.snapshot_label='legacy-mysql-viewer-2026-09-13' ORDER BY s.created_at DESC LIMIT 1")->fetch();
    if (!$changeSet) lvmf_2026_json_error(404, 'The materialised LVMF change layer has not yet been generated.');
    $stmt = $pg->prepare("SELECT change_class,new_view_code,legacy_view_ref,legacy_area_codes,current_area_codes,explanation,area_m2,ST_AsGeoJSON(ST_Transform(geom,4326)) geometry FROM lvmf_legacy_comparison_changes WHERE change_set_id=? ORDER BY change_class,new_view_code NULLS LAST,legacy_view_ref NULLS LAST");
    $stmt->execute([$changeSet['id']]);
    $features = [];
    foreach ($stmt as $row) {
        $row['legacy_area_codes'] = json_decode($row['legacy_area_codes'], true) ?? [];
        $row['current_area_codes'] = json_decode($row['current_area_codes'], true) ?? [];
        $features[] = ['type'=>'Feature','geometry'=>json_decode($row['geometry'], true),'properties'=>array_diff_key($row, ['geometry'=>true])];
    }
    $heights = $pg->prepare("SELECT m.new_view_code,m.legacy_view_ref,h.station_fraction,h.legacy_height_m_aod,h.current_height_m_aod,h.delta_m,h.methodology FROM lvmf_legacy_comparison_height_changes h JOIN lvmf_legacy_comparison_matches m ON m.id=h.match_id WHERE h.change_set_id=? ORDER BY m.new_view_code,h.station_fraction");
    $heights->execute([$changeSet['id']]);
    docling_json_out(200, ['success'=>true,'change_set'=>['created_at'=>$changeSet['created_at'],'methodology'=>$changeSet['methodology']],'changes'=>['type'=>'FeatureCollection','features'=>$features],'height_changes'=>$heights->fetchAll()]);
} catch (Throwable $e) {
    error_log('[lvmf-comparison-changes] ' . $e->getMessage());
    lvmf_2026_json_error(500, 'Unable to load materialised comparison changes.');
}
