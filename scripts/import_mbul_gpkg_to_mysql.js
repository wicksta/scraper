import "../bootstrap.js";

import crypto from "node:crypto";
import fs from "node:fs";
import https from "node:https";
import path from "node:path";
import { spawnSync } from "node:child_process";
import mysql from "mysql2/promise";

const DATA_DIR = path.resolve("ngist/private/data/mbul_gpkg");

const DATASETS = [
  { slug: "london-context", title: "London Context", filename: "london context.gpkg", url: "https://data.london.gov.uk/download/2j8nl/zwq/london%20context.gpkg" },
  { slug: "town-centres-character", title: "Character of London's town centres", filename: "character of london's town centres.gpkg", url: "https://data.london.gov.uk/download/2j8nl/kx1/character%20of%20london's%20town%20centres.gpkg" },
  { slug: "london-sam", title: "London SAM", filename: "London SAM.gpkg", url: "https://data.london.gov.uk/download/2j8nl/8r0/London%20SAM.gpkg" },
];

function requireEnv(name, fallback = null) {
  const value = process.env[name] ?? fallback;
  if (value === null || value === undefined || value === "") throw new Error(`Missing required environment variable ${name}`);
  return value;
}

function hasCommand(name) {
  return spawnSync("which", [name], { stdio: "ignore" }).status === 0;
}

function sha256File(filePath) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("hex");
}

function sha1Json(value) {
  return crypto.createHash("sha1").update(JSON.stringify(value)).digest("hex");
}

function download(url, dest) {
  return new Promise((resolve, reject) => {
    const tmp = `${dest}.download`;
    const file = fs.createWriteStream(tmp);
    const request = https.get(url, (response) => {
      if ([301, 302, 303, 307, 308].includes(response.statusCode ?? 0) && response.headers.location) {
        file.close();
        fs.rmSync(tmp, { force: true });
        download(new URL(response.headers.location, url).toString(), dest).then(resolve, reject);
        return;
      }
      if (response.statusCode !== 200) {
        file.close();
        fs.rmSync(tmp, { force: true });
        reject(new Error(`Download failed ${response.statusCode}: ${url}`));
        return;
      }
      response.pipe(file);
      file.on("finish", () => {
        file.close();
        fs.renameSync(tmp, dest);
        resolve();
      });
    });
    request.on("error", (err) => {
      file.close();
      fs.rmSync(tmp, { force: true });
      reject(err);
    });
  });
}

function runCapture(command, args, options = {}) {
  const res = spawnSync(command, args, { encoding: "utf8", maxBuffer: options.maxBuffer ?? 1024 * 1024 * 256 });
  if (res.status !== 0) throw new Error(`${command} failed: ${res.stderr || res.stdout || `exit ${res.status}`}`);
  return res.stdout;
}

async function createMysqlConnection() {
  return mysql.createConnection({
    host: requireEnv("MYSQL_HOST"),
    port: Number(process.env.MYSQL_PORT || 3306),
    user: requireEnv("MYSQL_USER"),
    password: requireEnv("MYSQL_PASSWORD", process.env.MYSQL_PASS ?? null),
    database: requireEnv("MYSQL_DATABASE", process.env.MYSQL_DB ?? null),
    multipleStatements: true,
  });
}

async function ensureSchema(conn) {
  await conn.query(`
CREATE TABLE IF NOT EXISTS mbul_datasets (
  id INT NOT NULL AUTO_INCREMENT,
  slug VARCHAR(80) NOT NULL,
  title VARCHAR(255) NOT NULL,
  source_url TEXT NOT NULL,
  local_path TEXT NOT NULL,
  sha256 CHAR(64) DEFAULT NULL,
  bytes BIGINT DEFAULT NULL,
  imported_at TIMESTAMP NULL DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY mbul_datasets_slug_uq (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS mbul_layers (
  id INT NOT NULL AUTO_INCREMENT,
  dataset_id INT NOT NULL,
  layer_name VARCHAR(190) NOT NULL,
  geometry_type VARCHAR(80) DEFAULT NULL,
  feature_count INT DEFAULT NULL,
  attribute_schema JSON DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY mbul_layers_dataset_layer_uq (dataset_id, layer_name),
  KEY mbul_layers_dataset_idx (dataset_id),
  CONSTRAINT mbul_layers_dataset_fk FOREIGN KEY (dataset_id) REFERENCES mbul_datasets(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS mbul_features (
  id BIGINT NOT NULL AUTO_INCREMENT,
  layer_id INT NOT NULL,
  source_fid VARCHAR(190) NOT NULL,
  display_name VARCHAR(255) DEFAULT NULL,
  geom GEOMETRY NOT NULL SRID 4326,
  properties JSON DEFAULT NULL,
  min_lon DOUBLE DEFAULT NULL,
  min_lat DOUBLE DEFAULT NULL,
  max_lon DOUBLE DEFAULT NULL,
  max_lat DOUBLE DEFAULT NULL,
  imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY mbul_features_layer_fid_uq (layer_id, source_fid),
  SPATIAL KEY mbul_features_geom_gix (geom),
  KEY mbul_features_layer_idx (layer_id),
  KEY mbul_features_bbox_idx (min_lon, min_lat, max_lon, max_lat),
  CONSTRAINT mbul_features_layer_fk FOREIGN KEY (layer_id) REFERENCES mbul_layers(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
`);
}

async function upsertDataset(conn, dataset, localPath, fileHash, bytes) {
  await conn.query(`
    INSERT INTO mbul_datasets (slug, title, source_url, local_path, sha256, bytes, imported_at)
    VALUES (?, ?, ?, ?, ?, ?, NOW())
    ON DUPLICATE KEY UPDATE title = VALUES(title), source_url = VALUES(source_url), local_path = VALUES(local_path), sha256 = VALUES(sha256), bytes = VALUES(bytes), imported_at = NOW()
  `, [dataset.slug, dataset.title, dataset.url, localPath, fileHash, bytes]);
  const [[row]] = await conn.query("SELECT id FROM mbul_datasets WHERE slug = ?", [dataset.slug]);
  return row.id;
}

async function upsertLayer(conn, datasetId, layerName, geometryType, featureCount, attributeSchema) {
  await conn.query(`
    INSERT INTO mbul_layers (dataset_id, layer_name, geometry_type, feature_count, attribute_schema)
    VALUES (?, ?, ?, ?, CAST(? AS JSON))
    ON DUPLICATE KEY UPDATE geometry_type = VALUES(geometry_type), feature_count = VALUES(feature_count), attribute_schema = VALUES(attribute_schema)
  `, [datasetId, layerName, geometryType, featureCount, JSON.stringify(attributeSchema ?? {})]);
  const [[row]] = await conn.query("SELECT id FROM mbul_layers WHERE dataset_id = ? AND layer_name = ?", [datasetId, layerName]);
  return row.id;
}

function getDisplayName(properties) {
  if (!properties || typeof properties !== "object") return null;
  for (const key of ["name", "Name", "NAME", "title", "Title", "label", "Label"]) {
    const value = properties[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") return String(value).trim().slice(0, 255);
  }
  return null;
}

function walkCoords(coords, acc) {
  if (!Array.isArray(coords)) return;
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    const lon = Number(coords[0]);
    const lat = Number(coords[1]);
    if (Number.isFinite(lon) && Number.isFinite(lat)) {
      acc.minLon = Math.min(acc.minLon, lon);
      acc.minLat = Math.min(acc.minLat, lat);
      acc.maxLon = Math.max(acc.maxLon, lon);
      acc.maxLat = Math.max(acc.maxLat, lat);
    }
    return;
  }
  for (const item of coords) walkCoords(item, acc);
}

function geometryBbox(geometry) {
  const acc = { minLon: Infinity, minLat: Infinity, maxLon: -Infinity, maxLat: -Infinity };
  walkCoords(geometry?.coordinates, acc);
  if (!Number.isFinite(acc.minLon)) return [null, null, null, null];
  return [acc.minLon, acc.minLat, acc.maxLon, acc.maxLat];
}

function force2DCoordinates(coords) {
  if (!Array.isArray(coords)) return coords;
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    return [coords[0], coords[1]];
  }
  return coords.map(force2DCoordinates);
}

function force2DGeometry(geometry) {
  if (!geometry || typeof geometry !== "object") return geometry;
  if (geometry.type === "GeometryCollection") {
    return {
      ...geometry,
      geometries: Array.isArray(geometry.geometries)
        ? geometry.geometries.map(force2DGeometry)
        : [],
    };
  }
  return {
    ...geometry,
    coordinates: force2DCoordinates(geometry.coordinates),
  };
}

function listLayers(gpkgPath) {
  const info = JSON.parse(runCapture("ogrinfo", ["-json", gpkgPath]));
  return (info.layers ?? []).map((layer) => ({
    name: layer.name,
    geometryType: layer.geometryFields?.[0]?.type ?? layer.geometryType ?? null,
    featureCount: layer.featureCount ?? null,
    fields: layer.fields ?? [],
  }));
}

function exportLayerGeoJson(gpkgPath, layerName) {
  return JSON.parse(runCapture("ogr2ogr", ["-f", "GeoJSON", "/vsistdout/", gpkgPath, layerName, "-t_srs", "EPSG:4326"]));
}

async function importLayer(conn, layerId, geojson, label) {
  const features = Array.isArray(geojson.features) ? geojson.features : [];
  await conn.query("DELETE FROM mbul_features WHERE layer_id = ?", [layerId]);
  const sql = `
    INSERT INTO mbul_features (layer_id, source_fid, display_name, geom, properties, min_lon, min_lat, max_lon, max_lat, imported_at)
    VALUES (?, ?, ?, ST_SRID(ST_GeomFromGeoJSON(?), 4326), CAST(? AS JSON), ?, ?, ?, ?, NOW())
    ON DUPLICATE KEY UPDATE display_name = VALUES(display_name), geom = VALUES(geom), properties = VALUES(properties), min_lon = VALUES(min_lon), min_lat = VALUES(min_lat), max_lon = VALUES(max_lon), max_lat = VALUES(max_lat), imported_at = NOW()
  `;
  let imported = 0;
  const total = features.length;
  for (const feature of features) {
    if (!feature?.geometry) continue;
    const properties = feature.properties ?? {};
    const geometry2d = force2DGeometry(feature.geometry);
    const sourceFid = String(feature.id ?? properties.fid ?? properties.ogc_fid ?? sha1Json({ ...feature, geometry: geometry2d })).slice(0, 190);
    const [minLon, minLat, maxLon, maxLat] = geometryBbox(geometry2d);
    await conn.query(sql, [layerId, sourceFid, getDisplayName(properties), JSON.stringify(geometry2d), JSON.stringify(properties), minLon, minLat, maxLon, maxLat]);
    imported += 1;
    if (imported % 1000 === 0) {
      console.log(`${label}: imported ${imported.toLocaleString()} / ${total.toLocaleString()} features`);
    }
  }
  if (imported > 0 && imported % 1000 !== 0) {
    console.log(`${label}: imported ${imported.toLocaleString()} / ${total.toLocaleString()} features`);
  }
  return imported;
}

function getArgValue(name) {
  const prefix = `--${name}=`;
  const arg = process.argv.slice(2).find((value) => value.startsWith(prefix));
  return arg ? arg.slice(prefix.length).trim() : null;
}

async function getLayerFeatureCount(conn, layerId) {
  const [[row]] = await conn.query("SELECT COUNT(*) AS count FROM mbul_features WHERE layer_id = ?", [layerId]);
  return Number(row?.count ?? 0);
}

async function main() {
  const args = new Set(process.argv.slice(2));
  const downloadOnly = args.has("--download-only");
  const skipDownload = args.has("--skip-download");
  const migrateOnly = args.has("--migrate-only");
  const skipComplete = args.has("--skip-complete");
  const datasetFilter = getArgValue("dataset");
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const conn = await createMysqlConnection();
  try {
    await ensureSchema(conn);
    if (migrateOnly) { console.log("Schema ready."); return; }
    const selectedDatasets = datasetFilter
      ? DATASETS.filter((dataset) => dataset.slug === datasetFilter || dataset.title.toLowerCase() === datasetFilter.toLowerCase())
      : DATASETS;
    if (datasetFilter && selectedDatasets.length === 0) {
      throw new Error(`Unknown --dataset=${datasetFilter}. Valid slugs: ${DATASETS.map((dataset) => dataset.slug).join(", ")}`);
    }

    for (const dataset of selectedDatasets) {
      const localPath = path.join(DATA_DIR, dataset.filename);
      if (!skipDownload && !fs.existsSync(localPath)) {
        console.log(`Downloading ${dataset.title}...`);
        await download(dataset.url, localPath);
      }
      if (!fs.existsSync(localPath)) throw new Error(`Missing ${localPath}. Run without --skip-download or place the file there.`);
      const bytes = fs.statSync(localPath).size;
      const fileHash = sha256File(localPath);
      const datasetId = await upsertDataset(conn, dataset, localPath, fileHash, bytes);
      console.log(`${dataset.title}: ${bytes} bytes, sha256 ${fileHash}`);
      if (downloadOnly) continue;
      if (!hasCommand("ogrinfo") || !hasCommand("ogr2ogr")) throw new Error("GDAL tools are required but missing. Install gdal-bin so ogrinfo and ogr2ogr are available.");
      const layers = listLayers(localPath);
      console.log(`${dataset.title}: ${layers.length} layers`);
      for (const layer of layers) {
        const label = `${dataset.slug}/${layer.name}`;
        console.log(`Importing ${label}...`);
        const layerId = await upsertLayer(conn, datasetId, layer.name, layer.geometryType, layer.featureCount, layer.fields);
        const existingFeatureCount = await getLayerFeatureCount(conn, layerId);
        const expectedFeatureCount = Number(layer.featureCount ?? 0);
        if (skipComplete && expectedFeatureCount > 0 && existingFeatureCount === expectedFeatureCount) {
          console.log(`Skipping ${label}: already complete (${existingFeatureCount.toLocaleString()} features).`);
          continue;
        }
        if (skipComplete && existingFeatureCount > 0) {
          console.log(`${label}: existing feature count ${existingFeatureCount.toLocaleString()} does not match expected ${expectedFeatureCount.toLocaleString()}; reimporting.`);
        }
        const imported = await importLayer(conn, layerId, exportLayerGeoJson(localPath, layer.name), label);
        await conn.query("UPDATE mbul_layers SET feature_count = ? WHERE id = ?", [imported, layerId]);
        console.log(`Imported ${imported.toLocaleString()} features for ${label}.`);
      }
    }
  } finally {
    await conn.end();
  }
}

main().catch((err) => { console.error(err.message); process.exit(1); });
