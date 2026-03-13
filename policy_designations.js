const DEFAULT_ARCGIS_SERVICE_ROOT = "https://services.arcgis.com/drifeOPKLpgnJ8Qa/arcgis/rest/services";
const DEFAULT_API_BASE_URL = `${DEFAULT_ARCGIS_SERVICE_ROOT}/planning_local_plan_data_33/FeatureServer`;
const DEFAULT_PLANNING_DATA_API_URL = "https://www.planning.data.gov.uk/entity.json";
const DEFAULT_PLANNING_DATA_DATASET_CATALOGUE_URL = "https://www.planning.data.gov.uk/dataset.json";
const DEFAULT_PLANNING_DATA_EXCLUDED_DATASETS = ["listed-building", "listed-building-outline"];
const DEFAULT_PLANNING_DATA_EXCLUDED_PREFIXES = ["statistical-geography"];
const DEFAULT_NEARBY_HERITAGE_DATASETS = ["listed-building", "listed-building-outline", "scheduled-monument"];

let planningDataDatasetCache = null;

const DEFAULT_OUT_FIELDS = [
  "objectid",
  "layerreference",
  "sitereference",
  "sitename",
  "address",
  "uprn",
  "borough",
  "planningauthority",
  "firstaddeddate",
  "lastupdateddate",
  "removeddate",
  "status",
  "hectares",
  "easting",
  "northing",
  "designation",
  "boroughdesignation",
  "classification",
  "notes",
  "source",
  "extrainfo1",
  "extrainfo2",
  "extrainfo3",
  "missing",
].join(",");

const layerCache = new Map();

function normalizeApiBaseUrl(input) {
  return String(input || DEFAULT_API_BASE_URL).replace(/\/+$/, "");
}

export function buildArcgisFeatureServerUrl(datastoreId) {
  const n = Number(datastoreId);
  if (!Number.isInteger(n) || n < 1) {
    throw new Error("Invalid datastore_id");
  }
  return `${DEFAULT_ARCGIS_SERVICE_ROOT}/planning_local_plan_data_${String(n).padStart(2, "0")}/FeatureServer`;
}

async function fetchJson(url, { timeoutMs = 10000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} from ArcGIS service`);
    }
    const data = await res.json();
    if (data?.error) {
      throw new Error(data.error.message || "ArcGIS API error");
    }
    return data;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchPlanningDataEntities(url, { timeoutMs = 10000 } = {}) {
  const allEntities = [];
  let nextUrl = url.toString();
  let totalCount = null;
  let guard = 0;

  while (nextUrl && guard < 100) {
    guard += 1;
    const data = await fetchJson(nextUrl, { timeoutMs });
    const pageEntities = Array.isArray(data?.entities) ? data.entities : [];
    allEntities.push(...pageEntities);
    if (Number.isFinite(Number(data?.count))) {
      totalCount = Number(data.count);
    }

    const nextLink = data?.links?.next ? String(data.links.next).trim() : "";
    if (!nextLink) break;
    nextUrl = nextLink;
  }

  return {
    count: totalCount != null ? totalCount : allEntities.length,
    entities: allEntities,
  };
}

export async function listPlanningDataGeographyDatasets({
  catalogueUrl = DEFAULT_PLANNING_DATA_DATASET_CATALOGUE_URL,
  timeoutMs = 10000,
  noCache = false,
  excludeDatasets = DEFAULT_PLANNING_DATA_EXCLUDED_DATASETS,
  excludePrefixes = DEFAULT_PLANNING_DATA_EXCLUDED_PREFIXES,
} = {}) {
  const cacheKey =
    `${catalogueUrl}::` +
    Array.from(excludeDatasets || [])
      .map((x) => String(x))
      .sort()
      .join(",") +
    "::" +
    Array.from(excludePrefixes || [])
      .map((x) => String(x))
      .sort()
      .join(",");

  if (!noCache && planningDataDatasetCache?.key === cacheKey) {
    return planningDataDatasetCache.datasets.slice();
  }

  const data = await fetchJson(catalogueUrl, { timeoutMs });
  const excluded = new Set(Array.from(excludeDatasets || []).map((x) => String(x).trim()).filter(Boolean));
  const excludedPrefixes = new Set(Array.from(excludePrefixes || []).map((x) => String(x).trim()).filter(Boolean));
  const rawDatasets = Array.isArray(data?.datasets)
    ? data.datasets
    : data?.datasets && typeof data.datasets === "object"
      ? Object.values(data.datasets).flatMap((value) => (Array.isArray(value?.dataset) ? value.dataset : []))
      : [];
  const datasets = [];

  for (const item of rawDatasets) {
    if (item?.typology !== "geography") continue;
    const slug = String(item.dataset || "").trim();
    if (!slug) continue;
    if (excluded.has(slug)) continue;
    const prefix = String(item.prefix || "").trim();
    if (prefix && excludedPrefixes.has(prefix)) continue;
    const count = Number(item["entity-count"] || 0);
    if (!Number.isFinite(count) || count <= 0) continue;
    datasets.push(slug);
  }

  const unique = Array.from(new Set(datasets)).sort();
  if (!noCache) planningDataDatasetCache = { key: cacheKey, datasets: unique };
  return unique.slice();
}

export async function listPolicyDesignationLayers({
  apiBaseUrl = DEFAULT_API_BASE_URL,
  timeoutMs = 10000,
  noCache = false,
} = {}) {
  const cacheKey = normalizeApiBaseUrl(apiBaseUrl);
  if (!noCache && layerCache.has(cacheKey)) {
    return layerCache.get(cacheKey).map((layer) => ({ ...layer }));
  }

  const url = new URL(`${cacheKey}/layers`);
  url.searchParams.set("f", "json");
  const data = await fetchJson(url.toString(), { timeoutMs });
  const layers = Array.isArray(data?.layers)
    ? data.layers.map((layer) => ({
        id: Number(layer.id),
        name: String(layer.name || ""),
        geometryType: layer.geometryType || null,
      }))
    : [];

  if (!noCache) {
    layerCache.set(cacheKey, layers.map((layer) => ({ ...layer })));
  }

  return layers;
}

function buildPointInput({ lon, lat, easting, northing }) {
  const lonNum = lon == null || lon === "" ? null : Number(lon);
  const latNum = lat == null || lat === "" ? null : Number(lat);
  const eastingNum = easting == null || easting === "" ? null : Number(easting);
  const northingNum = northing == null || northing === "" ? null : Number(northing);
  const hasLonLat = Number.isFinite(lonNum) && Number.isFinite(latNum);
  const hasGrid = Number.isFinite(eastingNum) && Number.isFinite(northingNum);

  if (!hasLonLat && !hasGrid) {
    throw new Error("Provide either lon/lat or easting/northing");
  }

  if (hasGrid) {
    return {
      geometry: `${eastingNum},${northingNum}`,
      inSR: 27700,
      point: { easting: eastingNum, northing: northingNum },
      pointSource: "easting_northing",
      sourceCrs: "EPSG:27700",
    };
  }

  return {
    geometry: `${lonNum},${latNum}`,
    inSR: 4326,
    point: { lon: lonNum, lat: latNum },
    pointSource: "lon_lat",
    sourceCrs: "EPSG:4326",
  };
}

function compactAttributes(attributes) {
  return {
    objectid: attributes.objectid ?? null,
    layerreference: attributes.layerreference ?? null,
    sitereference: attributes.sitereference ?? null,
    sitename: attributes.sitename ?? null,
    address: attributes.address ?? null,
    uprn: attributes.uprn ?? null,
    borough: attributes.borough ?? null,
    planningauthority: attributes.planningauthority ?? null,
    firstaddeddate: attributes.firstaddeddate ?? null,
    lastupdateddate: attributes.lastupdateddate ?? null,
    removeddate: attributes.removeddate ?? null,
    status: attributes.status ?? null,
    hectares: attributes.hectares ?? null,
    easting: attributes.easting ?? null,
    northing: attributes.northing ?? null,
    designation: attributes.designation ?? null,
    boroughdesignation: attributes.boroughdesignation ?? null,
    classification: attributes.classification ?? null,
    notes: attributes.notes ?? null,
    source: attributes.source ?? null,
    extrainfo1: attributes.extrainfo1 ?? null,
    extrainfo2: attributes.extrainfo2 ?? null,
    extrainfo3: attributes.extrainfo3 ?? null,
    missing: attributes.missing ?? null,
  };
}

function compactFeature(feature, includeGeometry) {
  const out = compactAttributes(feature?.attributes || {});
  if (includeGeometry) {
    out.geometry = feature?.geometry ?? null;
  }
  return out;
}

function dedupeMatches(matches) {
  const seen = new Set();
  const out = [];
  for (const match of matches) {
    const key = JSON.stringify(match);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(match);
  }
  return out;
}

async function querySingleLayer(layer, pointInput, { apiBaseUrl, timeoutMs, outFields, resultRecordCount, includeGeometry }) {
  const url = new URL(`${normalizeApiBaseUrl(apiBaseUrl)}/${layer.id}/query`);
  url.searchParams.set("where", "1=1");
  url.searchParams.set("geometry", pointInput.geometry);
  url.searchParams.set("geometryType", "esriGeometryPoint");
  url.searchParams.set("inSR", String(pointInput.inSR));
  url.searchParams.set("spatialRel", "esriSpatialRelIntersects");
  url.searchParams.set("outFields", outFields);
  url.searchParams.set("returnGeometry", includeGeometry ? "true" : "false");
  url.searchParams.set("resultRecordCount", String(resultRecordCount));
  url.searchParams.set("f", "json");

  const data = await fetchJson(url.toString(), { timeoutMs });
  const features = Array.isArray(data?.features) ? data.features : [];
  const matches = dedupeMatches(features.map((feature) => compactFeature(feature, includeGeometry)));

  return {
    layerId: layer.id,
    layerName: layer.name,
    count: matches.length,
    matches,
  };
}

export async function lookupPolicyDesignationsByPoint({
  lon,
  lat,
  easting,
  northing,
  apiBaseUrl = DEFAULT_API_BASE_URL,
  timeoutMs = 10000,
  layerIds = null,
  outFields = DEFAULT_OUT_FIELDS,
  resultRecordCount = 10,
  includeGeometry = false,
} = {}) {
  const pointInput = buildPointInput({ lon, lat, easting, northing });
  const allLayers = await listPolicyDesignationLayers({ apiBaseUrl, timeoutMs });
  const selectedLayers = Array.isArray(layerIds) && layerIds.length
    ? allLayers.filter((layer) => layerIds.includes(layer.id))
    : allLayers;

  const results = await Promise.all(
    selectedLayers.map((layer) =>
      querySingleLayer(layer, pointInput, { apiBaseUrl, timeoutMs, outFields, resultRecordCount, includeGeometry }),
    ),
  );

  const hits = results.filter((result) => result.count > 0);

  return {
    apiBaseUrl: normalizeApiBaseUrl(apiBaseUrl),
    point: pointInput.point,
    pointSource: pointInput.pointSource,
    sourceCrs: pointInput.sourceCrs,
    queriedLayerCount: selectedLayers.length,
    matchedLayerCount: hits.length,
    layers: hits,
  };
}

function compactPlanningDataEntity(entity, includeGeometry = false) {
  const out = {
    entity: entity.entity ?? null,
    dataset: entity.dataset ?? null,
    name: entity.name ?? null,
    reference: entity.reference ?? null,
    listedBuilding: entity["listed-building"] ?? null,
    listedBuildingName: entity["listed-building-name"] ?? null,
    listedBuildingGrade: entity["listed-building-grade"] ?? null,
    prefix: entity.prefix ?? null,
    typology: entity.typology ?? null,
    point: entity.point ?? null,
    startDate: entity["start-date"] ?? null,
    endDate: entity["end-date"] ?? null,
    entryDate: entity["entry-date"] ?? null,
    designationDate: entity["designation-date"] ?? null,
    documentationUrl: entity["documentation-url"] ?? entity["document-url"] ?? null,
    notes: entity.notes ?? null,
    organisationEntity: entity["organisation-entity"] ?? null,
  };
  if (includeGeometry) {
    out.geometry = entity?.geometry ?? null;
  }
  return out;
}

function parseWktPoint(wkt) {
  const m = String(wkt || "").trim().match(/^POINT\s*\(\s*([-0-9.]+)\s+([-0-9.]+)\s*\)$/i);
  if (!m) return null;
  const lon = Number(m[1]);
  const lat = Number(m[2]);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return { lon, lat };
}

function haversineDistanceMeters(aLon, aLat, bLon, bLat) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const r = 6371008.8;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);
  const lat1 = toRad(aLat);
  const lat2 = toRad(bLat);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;
  return 2 * r * Math.asin(Math.min(1, Math.sqrt(h)));
}

function buildCirclePolygonWkt(lon, lat, radiusMeters, sides = 32) {
  const latRad = (lat * Math.PI) / 180;
  const metersPerDegLat = 111320;
  const metersPerDegLon = 111320 * Math.cos(latRad);
  const points = [];

  for (let i = 0; i < sides; i++) {
    const angle = (2 * Math.PI * i) / sides;
    const dx = Math.cos(angle) * radiusMeters;
    const dy = Math.sin(angle) * radiusMeters;
    const pLon = lon + dx / metersPerDegLon;
    const pLat = lat + dy / metersPerDegLat;
    points.push(`${pLon} ${pLat}`);
  }
  points.push(points[0]);
  return `POLYGON ((${points.join(", ")}))`;
}

function compactNearbyHeritageEntity(entity, centerLon, centerLat) {
  const out = compactPlanningDataEntity(entity);
  const point = parseWktPoint(entity?.point);
  const distanceMeters = point ? haversineDistanceMeters(centerLon, centerLat, point.lon, point.lat) : null;
  return {
    ...out,
    distanceMeters: distanceMeters != null ? Math.round(distanceMeters) : null,
  };
}

export async function lookupPlanningDataDesignations({
  lon,
  lat,
  apiUrl = DEFAULT_PLANNING_DATA_API_URL,
  datasets = null,
  limit = 50,
  timeoutMs = 10000,
  catalogueUrl = DEFAULT_PLANNING_DATA_DATASET_CATALOGUE_URL,
  excludeDatasets = DEFAULT_PLANNING_DATA_EXCLUDED_DATASETS,
  excludePrefixes = DEFAULT_PLANNING_DATA_EXCLUDED_PREFIXES,
  includeGeometry = false,
} = {}) {
  const lonNum = lon == null || lon === "" ? null : Number(lon);
  const latNum = lat == null || lat === "" ? null : Number(lat);
  const hasLonLat = Number.isFinite(lonNum) && Number.isFinite(latNum);
  if (!hasLonLat) {
    return {
      apiUrl,
      queried: false,
      reason: "planning.data.gov.uk lookup requires lon/lat in EPSG:4326",
      point: null,
      datasets: Array.from(datasets || []),
      count: 0,
      entities: [],
    };
  }

  const resolvedDatasets =
    Array.isArray(datasets) && datasets.length
      ? Array.from(new Set(datasets.map((x) => String(x).trim()).filter(Boolean))).sort()
      : await listPlanningDataGeographyDatasets({ catalogueUrl, timeoutMs, excludeDatasets, excludePrefixes });

  const url = new URL(apiUrl);
  for (const dataset of resolvedDatasets) {
    if (String(dataset || "").trim()) url.searchParams.append("dataset", String(dataset).trim());
  }
  url.searchParams.set("longitude", String(lonNum));
  url.searchParams.set("latitude", String(latNum));
  url.searchParams.set("limit", String(Math.max(1, Number(limit || 50))));

  const data = await fetchPlanningDataEntities(url, { timeoutMs });
  const entities = Array.isArray(data?.entities)
    ? data.entities.map((entity) => compactPlanningDataEntity(entity, includeGeometry))
    : [];

  return {
    apiUrl,
    queried: true,
    point: { lon: lonNum, lat: latNum },
    datasets: resolvedDatasets,
    count: Number.isFinite(Number(data?.count)) ? Number(data.count) : entities.length,
    entities,
  };
}

export async function lookupNearbyPlanningDataHeritage({
  lon,
  lat,
  radiusMeters = 500,
  datasets = DEFAULT_NEARBY_HERITAGE_DATASETS,
  apiUrl = DEFAULT_PLANNING_DATA_API_URL,
  limit = 100,
  timeoutMs = 10000,
  includeGeometry = false,
} = {}) {
  const lonNum = lon == null || lon === "" ? null : Number(lon);
  const latNum = lat == null || lat === "" ? null : Number(lat);
  const hasLonLat = Number.isFinite(lonNum) && Number.isFinite(latNum);
  if (!hasLonLat) {
    return {
      apiUrl,
      queried: false,
      reason: "Nearby heritage lookup requires lon/lat in EPSG:4326",
      point: null,
      radiusMeters: Math.max(1, Number(radiusMeters || 500)),
      datasets: Array.from(datasets || []),
      count: 0,
      entities: [],
    };
  }

  const resolvedDatasets = Array.from(new Set(Array.from(datasets || []).map((x) => String(x).trim()).filter(Boolean)));
  const polygonWkt = buildCirclePolygonWkt(lonNum, latNum, Math.max(1, Number(radiusMeters || 500)));
  const url = new URL(apiUrl);
  for (const dataset of resolvedDatasets) url.searchParams.append("dataset", dataset);
  url.searchParams.append("geometry", polygonWkt);
  url.searchParams.set("geometry_relation", "intersects");
  url.searchParams.set("limit", String(Math.max(1, Number(limit || 100))));

  const data = await fetchPlanningDataEntities(url, { timeoutMs });
  const rawEntities = Array.isArray(data?.entities) ? data.entities : [];
  const listedBuildingByReference = new Map(
    rawEntities
      .filter((entity) => entity?.dataset === "listed-building")
      .map((entity) => [String(entity?.reference ?? "").trim(), entity]),
  );
  const entities = Array.isArray(data?.entities)
    ? rawEntities
        .map((entity) => {
          if (entity?.dataset !== "listed-building-outline") return entity;
          const key = String(entity?.["listed-building"] ?? entity?.reference ?? "").trim();
          const listedBuilding = listedBuildingByReference.get(key);
          if (!listedBuilding) return entity;
          return {
            ...entity,
            "listed-building-name": listedBuilding?.name ?? entity?.["listed-building-name"] ?? null,
            "listed-building-grade": listedBuilding?.["listed-building-grade"] ?? entity?.["listed-building-grade"] ?? null,
            "documentation-url": listedBuilding?.["documentation-url"] ?? entity?.["documentation-url"] ?? null,
          };
        })
        .map((entity) => {
          const out = compactNearbyHeritageEntity(entity, lonNum, latNum);
          if (includeGeometry) out.geometry = entity?.geometry ?? null;
          return out;
        })
        .sort((a, b) => {
          const aDist = a.distanceMeters == null ? Number.POSITIVE_INFINITY : a.distanceMeters;
          const bDist = b.distanceMeters == null ? Number.POSITIVE_INFINITY : b.distanceMeters;
          return aDist - bDist;
        })
    : [];

  return {
    apiUrl,
    queried: true,
    point: { lon: lonNum, lat: latNum },
    radiusMeters: Math.max(1, Number(radiusMeters || 500)),
    datasets: resolvedDatasets,
    count: Number.isFinite(Number(data?.count)) ? Number(data.count) : entities.length,
    entities,
  };
}

export {
  DEFAULT_API_BASE_URL,
  DEFAULT_ARCGIS_SERVICE_ROOT,
  DEFAULT_PLANNING_DATA_API_URL,
  DEFAULT_PLANNING_DATA_DATASET_CATALOGUE_URL,
  DEFAULT_PLANNING_DATA_EXCLUDED_DATASETS,
  DEFAULT_PLANNING_DATA_EXCLUDED_PREFIXES,
  DEFAULT_NEARBY_HERITAGE_DATASETS,
};
