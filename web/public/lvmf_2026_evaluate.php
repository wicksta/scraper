<?php
declare(strict_types=1);
require_once __DIR__ . '/lvmf_2026_lib.php';
lvmf_2026_auth();
try {
    $lon=lvmf_2026_number($_GET['lon'] ?? null,'longitude',-8,3); $lat=lvmf_2026_number($_GET['lat'] ?? null,'latitude',49,62);
    $pg=lvmf_2026_pg();
    $sql="WITH p AS (SELECT ST_Transform(ST_SetSRID(ST_MakePoint(:lon,:lat),4326),27700) geom) SELECT a.id area_id,a.area_code,a.display_name,v.view_code,v.view_name,r.rule_code,r.endpoint_a_code,r.endpoint_b_code,r.curvature_coefficient,ST_X(p.geom) x,ST_Y(p.geom) y FROM p JOIN lvmf_2026_areas a ON ST_Covers(a.geom,p.geom) JOIN lvmf_2026_views v ON v.id=a.view_id JOIN lvmf_2026_threshold_rules r ON r.view_id=v.id AND r.area_code=a.area_code";
    $st=$pg->prepare($sql);$st->execute([':lon'=>$lon,':lat'=>$lat]);$hits=$st->fetchAll(); $results=[];
    foreach ($hits as $hit) {
      $points=$pg->prepare("SELECT p.point_code,ST_X(p.geom) x,ST_Y(p.geom) y,ST_Z(p.geom) z FROM lvmf_2026_area_vertices av JOIN lvmf_2026_control_points p ON p.id=av.control_point_id WHERE av.area_id=:id ORDER BY av.vertex_order");$points->execute([':id'=>$hit['area_id']]);$vertices=$points->fetchAll();
      $a=null;$b=null; foreach ($vertices as $vertex) { if ($vertex['point_code']===$hit['endpoint_a_code']) $a=$vertex; if ($vertex['point_code']===$hit['endpoint_b_code']) $b=$vertex; }
      // a/d are not necessarily vertices of a BAA, so resolve from the view directly.
      if (!$a || !$b) { $endpoints=$pg->prepare("SELECT point_code,ST_X(geom) x,ST_Y(geom) y,ST_Z(geom) z FROM lvmf_2026_control_points p JOIN lvmf_2026_views v ON v.id=p.view_id WHERE v.view_code=:view AND p.point_code IN (:a,:b) AND v.id=(SELECT id FROM lvmf_2026_views WHERE view_code=:view LIMIT 1)"); $endpoints->execute([':view'=>$hit['view_code'],':a'=>$hit['endpoint_a_code'],':b'=>$hit['endpoint_b_code']]); foreach ($endpoints as $endpoint) { if ($endpoint['point_code']===$hit['endpoint_a_code']) $a=$endpoint; else $b=$endpoint; } }
      $threshold=null;$trace=['rule'=>$hit['rule_code'],'source'=>'Appendix E'];
      if ($hit['rule_code']==='source_control_surface') { $threshold=lvmf_2026_barycentric_height($vertices,(float)$hit['x'],(float)$hit['y']); $trace['method']='Appendix E source-control-surface interpolation'; }
      elseif ($a && $b) { $dx=$b['x']-$a['x'];$dy=$b['y']-$a['y'];$l2=hypot($dx,$dy);$l1=max(0,min($l2,(($hit['x']-$a['x'])*$dx+($hit['y']-$a['y'])*$dy)/$l2));$l3=$l2-$l1; if ($hit['rule_code']==='curvature_refraction') {$az=$a['z']-((float)$hit['curvature_coefficient']*($l1/1000)**2);$bz=$b['z']-((float)$hit['curvature_coefficient']*($l3/1000)**2);$trace['curvature_coefficient']=(float)$hit['curvature_coefficient'];} else {$az=$a['z'];$bz=$b['z'];} $threshold=$az+($l1/$l2)*($bz-$az);$trace+=['a'=>$a,'d'=>$b,'l1_m'=>$l1,'l2_m'=>$l2,'l3_m'=>$l3]; }
      $results[]=['view_code'=>$hit['view_code'],'view_name'=>$hit['view_name'],'area_code'=>$hit['area_code'],'area_name'=>$hit['display_name'],'threshold_m_aod'=>$threshold===null?null:round($threshold,3),'trace'=>$trace];
    }
    docling_json_out(200,['success'=>true,'input'=>['lon'=>$lon,'lat'=>$lat],'matches'=>$results]);
} catch (Throwable $e) { error_log('[lvmf-2026-evaluate] '.$e->getMessage()); lvmf_2026_json_error(500,'Unable to evaluate the LVMF threshold.'); }
