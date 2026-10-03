#!/usr/bin/env node
import "../bootstrap.js";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import mysql from "mysql2/promise";

const sourcePath = "/mnt/ngist/public_html/lvmf/LVMF_2016_Appendix_D_geometry.csv";
function parseLine(line) { const output=[]; let value="", quoted=false; for(let index=0;index<line.length;index+=1){const char=line[index];if(char==='"'){if(quoted&&line[index+1]==='"'){value+=char;index+=1;}else quoted=!quoted;}else if(char===','&&!quoted){output.push(value);value='';}else value+=char;}output.push(value);return output; }
const raw = readFileSync(sourcePath); const lines=raw.toString("utf8").replace(/^\uFEFF/,"").trim().split(/\r?\n/); const headers=parseLine(lines.shift()); const rows=lines.map((line)=>Object.fromEntries(parseLine(line).map((value,index)=>[headers[index],value])));
const number = (value) => value === "" ? null : Number(value);
const connection = await mysql.createConnection({ host:process.env.MYSQL_HOST,port:Number(process.env.MYSQL_PORT||3306),database:process.env.MYSQL_DATABASE||process.env.MYSQL_DB,user:process.env.MYSQL_USER,password:process.env.MYSQL_PASSWORD||process.env.MYSQL_PASS });
try {
  await connection.beginTransaction(); const sha=createHash("sha256").update(raw).digest("hex"); const sourceName="LVMF 2016 Appendix D geometry";
  await connection.execute("INSERT INTO lvmf_2016_appendix_d_datasets(source_name,source_sha256,source_path) VALUES(?,?,?) ON DUPLICATE KEY UPDATE source_sha256=VALUES(source_sha256),source_path=VALUES(source_path),imported_at=CURRENT_TIMESTAMP",[sourceName,sha,sourcePath]);
  const [[dataset]]=await connection.execute("SELECT id FROM lvmf_2016_appendix_d_datasets WHERE source_name=?",[sourceName]); await connection.execute("DELETE FROM lvmf_2016_appendix_d_areas WHERE dataset_id=?",[dataset.id]);
  for(const [index,row] of rows.entries()){
    const [area]=await connection.execute("INSERT INTO lvmf_2016_appendix_d_areas(dataset_id,source_row_number,view_ref,assessment_from,assessment_to,area_code,area_name,geometry_class,coordinate_system,vertex_count,geometry_wkt_3d,length_m,length_reference,width_at_monument_m,width_reference,source_pdf_page,source_printed_page) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[dataset.id,index+1,row.assessment_point,row.assessment_from,row.assessment_to,row.area_code,row.area_name,row.geometry_class,row.coordinate_system,number(row.vertex_count),row.geometry_wkt_3d,number(row.length_m),row.length_reference||null,number(row.width_at_monument_m),row.width_reference||null,number(row.source_pdf_page),number(row.source_printed_page)]);
    for(let order=1;order<=4;order+=1){const label=row[`point_${order}_label`];if(!label)continue;await connection.execute("INSERT INTO lvmf_2016_appendix_d_area_vertices(area_id,vertex_order,point_label,easting,northing,height_m_aod) VALUES(?,?,?,?,?,?)",[area.insertId,order,label,number(row[`point_${order}_easting_m`]),number(row[`point_${order}_northing_m`]),number(row[`point_${order}_height_m_aod`])]);}
  }
  await connection.commit(); console.log(JSON.stringify({success:true,areas:rows.length,vertices:rows.reduce((sum,row)=>sum+Number(row.vertex_count||0),0),source_sha256:sha},null,2));
} catch(error) { await connection.rollback(); throw error; } finally { await connection.end(); }
