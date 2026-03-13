#!/usr/bin/env node
import "../bootstrap.js";

import pg from "pg";
import mysql from "mysql2/promise";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { createRequire } from "node:module";

import {
  DEFAULT_NEARBY_HERITAGE_DATASETS,
  DEFAULT_PLANNING_DATA_EXCLUDED_DATASETS,
  DEFAULT_PLANNING_DATA_EXCLUDED_PREFIXES,
  buildArcgisFeatureServerUrl,
  lookupNearbyPlanningDataHeritage,
  lookupPlanningDataDesignations,
  lookupPolicyDesignationsByPoint,
} from "../policy_designations.js";

const require = createRequire(import.meta.url);
const { resolvePostcodeViaOnspd } = require("../library.cjs");

const { Client } = pg;

const argv = yargs(hideBin(process.argv))
  .scriptName("policy-designations-lookup")
  .option("application-id", {
    type: "number",
    default: null,
    describe: "Lookup using public.applications.id.",
  })
  .option("reference", {
    type: "string",
    default: "",
    describe: "Lookup using public.applications.reference.",
  })
  .option("ons-code", {
    type: "string",
    default: "",
    describe: "Optional filter used with --reference.",
  })
  .option("postcode", {
    type: "string",
    default: "",
    describe: "UK postcode to resolve via ONSPD before designation lookup.",
  })
  .option("address", {
    type: "string",
    default: "",
    describe: "Address string containing a UK postcode to resolve via ONSPD.",
  })
  .option("lon", {
    type: "number",
    default: null,
    describe: "Longitude in EPSG:4326.",
  })
  .option("lat", {
    type: "number",
    default: null,
    describe: "Latitude in EPSG:4326.",
  })
  .option("easting", {
    type: "number",
    default: null,
    describe: "Easting in EPSG:27700.",
  })
  .option("northing", {
    type: "number",
    default: null,
    describe: "Northing in EPSG:27700.",
  })
  .option("limit-per-layer", {
    type: "number",
    default: 10,
    describe: "Max features returned per matched layer.",
  })
  .option("with-geometry", {
    type: "boolean",
    default: false,
    describe: "Include raw geometry for local ArcGIS matches and national planning.data.gov.uk entities.",
  })
  .option("timeout-ms", {
    type: "number",
    default: 10000,
    describe: "HTTP timeout per ArcGIS request.",
  })
  .option("json", {
    type: "boolean",
    default: true,
    describe: "Output JSON.",
  })
  .option("output-mode", {
    type: "string",
    default: "raw",
    choices: ["raw", "summary", "llm"],
    describe: "Output shape: raw debug JSON, compact summary JSON, or LLM-oriented context JSON.",
  })
  .option("llm-listed-buildings-limit", {
    type: "number",
    default: 10,
    describe: "Max nearby listed buildings to include by name in llm mode.",
  })
  .option("llm-scheduled-monuments-limit", {
    type: "number",
    default: 10,
    describe: "Max nearby scheduled monuments to include by name in llm mode.",
  })
  .option("include-planning-data", {
    type: "boolean",
    default: true,
    describe: "Also query planning.data.gov.uk national designation datasets when lon/lat is available.",
  })
  .option("planning-data-dataset", {
    type: "array",
    default: [],
    describe: "planning.data.gov.uk dataset slug(s) to query. If omitted, all geography datasets are queried except excluded ones.",
  })
  .option("planning-data-exclude-dataset", {
    type: "array",
    default: DEFAULT_PLANNING_DATA_EXCLUDED_DATASETS,
    describe: "planning.data.gov.uk dataset slug(s) to exclude from the default live dataset list.",
  })
  .option("planning-data-exclude-prefix", {
    type: "array",
    default: DEFAULT_PLANNING_DATA_EXCLUDED_PREFIXES,
    describe: "planning.data.gov.uk entity prefix(es) to exclude from the default live dataset list.",
  })
  .option("planning-data-limit", {
    type: "number",
    default: 50,
    describe: "Max planning.data.gov.uk entities to return.",
  })
  .option("include-nearby-heritage", {
    type: "boolean",
    default: true,
    describe: "Also query nearby listed buildings and scheduled monuments from planning.data.gov.uk.",
  })
  .option("nearby-heritage-dataset", {
    type: "array",
    default: DEFAULT_NEARBY_HERITAGE_DATASETS,
    describe: "planning.data.gov.uk dataset slug(s) to query in the nearby heritage search.",
  })
  .option("nearby-heritage-radius-m", {
    type: "number",
    default: 500,
    describe: "Radius in metres for nearby listed building / scheduled monument search.",
  })
  .option("nearby-heritage-limit", {
    type: "number",
    default: 100,
    describe: "Max nearby heritage entities to return.",
  })
  .check((args) => {
    const hasAppLookup = Number.isFinite(args["application-id"]) || String(args.reference || "").trim();
    const hasPostcode = String(args.postcode || "").trim() || String(args.address || "").trim();
    const hasLonLat = Number.isFinite(args.lon) && Number.isFinite(args.lat);
    const hasGrid = Number.isFinite(args.easting) && Number.isFinite(args.northing);
    if (!hasAppLookup && !hasPostcode && !hasLonLat && !hasGrid) {
      throw new Error("Provide --application-id, --reference, --postcode/--address, or coordinates");
    }
    return true;
  })
  .strict()
  .help()
  .argv;

function getPgClientConfig() {
  return process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: process.env.PGHOST,
        port: process.env.PGPORT ? Number(process.env.PGPORT) : undefined,
        database: process.env.PGDATABASE,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
      };
}

function getMysqlConfig() {
  return {
    host: process.env.MYSQL_HOST,
    port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
    database: process.env.MYSQL_DATABASE,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
  };
}

function nonEmpty(value) {
  return value != null && String(value).trim() !== "";
}

function titleCaseLabel(slug) {
  return String(slug || "")
    .split(/[_-]+/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function uniqStrings(items) {
  return Array.from(new Set(items.filter((x) => nonEmpty(x)).map((x) => String(x).trim())));
}

function compactLocalDesignation(match) {
  const label = match.sitename || match.designation || match.boroughdesignation || match.layerreference || null;
  return {
    name: label,
    designation: match.designation || match.boroughdesignation || null,
    reference: match.layerreference || null,
    source: "london_datastore_arcgis",
  };
}

function compactPlanningDataEntity(entity) {
  return {
    dataset: entity.dataset || null,
    label: entity.name || titleCaseLabel(entity.dataset || ""),
    reference: entity.reference || null,
    source: "planning_data_gov_uk",
    url: entity.documentationUrl || null,
  };
}

function compactNearbyHeritageEntity(entity) {
  return {
    dataset: entity.dataset || null,
    name: entity.name || titleCaseLabel(entity.dataset || ""),
    reference: entity.reference || null,
    listed_building_grade: entity.listedBuildingGrade || null,
    distance_m: entity.distanceMeters ?? null,
    url: entity.documentationUrl || null,
  };
}

function buildSummaryPayload(raw, options) {
  const localDesignations = raw.layers.flatMap((layer) =>
    layer.matches.map((match) => ({
      layer: layer.layerName,
      ...compactLocalDesignation(match),
    })),
  );

  const nationalDesignations = (raw.planningData?.entities || []).map(compactPlanningDataEntity);
  const nearbyListed = (raw.nearbyHeritage?.entities || [])
    .filter((entity) => entity.dataset === "listed-building")
    .map(compactNearbyHeritageEntity);
  const nearbyScheduled = (raw.nearbyHeritage?.entities || [])
    .filter((entity) => entity.dataset === "scheduled-monument")
    .map(compactNearbyHeritageEntity);

  return {
    ok: raw.ok,
    mode: "summary",
    site: {
      input: raw.input,
      resolvedFrom: raw.resolvedFrom,
      lpa: raw.resolvedLpa,
      point: raw.point,
      sourceCrs: raw.sourceCrs,
    },
    londonDatastore: {
      queried: raw.queried ?? true,
      reason: raw.reason || null,
      apiBaseUrl: raw.apiBaseUrl,
      localDesignationCount: localDesignations.length,
      localDesignations,
    },
    planningData: {
      queried: raw.planningData?.queried ?? false,
      count: raw.planningData?.count ?? 0,
      nationalDesignations,
    },
    nearbyHeritage: {
      queried: raw.nearbyHeritage?.queried ?? false,
      radiusMeters: raw.nearbyHeritage?.radiusMeters ?? null,
      listedBuildingCount: nearbyListed.length,
      scheduledMonumentCount: nearbyScheduled.length,
      closestListedBuildings: nearbyListed.slice(0, Math.max(1, Number(options.listedLimit || 10))),
      scheduledMonuments: nearbyScheduled.slice(0, Math.max(1, Number(options.scheduledLimit || 10))),
    },
  };
}

function buildLlmPayload(raw, options) {
  const localDesignationLines = uniqStrings(
    raw.layers.flatMap((layer) =>
      layer.matches.map((match) => {
        const label = match.sitename || match.designation || match.boroughdesignation || layer.layerName;
        const prefix = layer.layerName === label ? label : `${layer.layerName}: ${label}`;
        return prefix;
      }),
    ),
  );

  const nationalDesignationLines = uniqStrings(
    (raw.planningData?.entities || []).map((entity) => {
      const label = entity.name || titleCaseLabel(entity.dataset || "");
      return `${titleCaseLabel(entity.dataset || "")}: ${label}`;
    }),
  );

  const scheduled = (raw.nearbyHeritage?.entities || [])
    .filter((entity) => entity.dataset === "scheduled-monument")
    .sort((a, b) => (a.distanceMeters ?? 1e9) - (b.distanceMeters ?? 1e9));
  const listed = (raw.nearbyHeritage?.entities || [])
    .filter((entity) => entity.dataset === "listed-building")
    .sort((a, b) => (a.distanceMeters ?? 1e9) - (b.distanceMeters ?? 1e9));

  const scheduledLines = scheduled
    .slice(0, Math.max(1, Number(options.scheduledLimit || 10)))
    .map((entity) => `${entity.name || "Scheduled monument"} (${entity.distanceMeters ?? "?"}m)`);
  const listedLines = listed
    .slice(0, Math.max(1, Number(options.listedLimit || 10)))
    .map((entity) => {
      const grade = entity.listedBuildingGrade ? `, Grade ${entity.listedBuildingGrade}` : "";
      return `${entity.name || "Listed building"} (${entity.distanceMeters ?? "?"}m${grade})`;
    });

  const redFlags = [];
  if (localDesignationLines.some((x) => /world heritage/i.test(x))) redFlags.push("Site lies within or intersects a World Heritage Site.");
  if (localDesignationLines.some((x) => /conservation area/i.test(x))) redFlags.push("Site lies within or intersects a Conservation Area.");
  if (localDesignationLines.some((x) => /thames policy area/i.test(x))) redFlags.push("Site lies within or intersects the Thames Policy Area.");
  if (localDesignationLines.some((x) => /monument saturation zone/i.test(x))) redFlags.push("Site lies within or intersects a Monument Saturation Zone.");
  if (scheduled.length > 0) redFlags.push("Scheduled monuments are nearby and may create heritage setting constraints.");
  if (listed.length > 0) redFlags.push(`Listed buildings nearby within ${raw.nearbyHeritage?.radiusMeters ?? 500}m: ${listed.length}.`);

  const prose = [];
  if (raw.resolvedLpa?.lpaName) prose.push(`Site falls within ${raw.resolvedLpa.lpaName}.`);
  if (localDesignationLines.length) prose.push(`Local London Datastore designations identified: ${localDesignationLines.slice(0, 6).join("; ")}.`);
  if (nationalDesignationLines.length) prose.push(`National planning.data.gov.uk context identified: ${nationalDesignationLines.slice(0, 6).join("; ")}.`);
  if (scheduledLines.length) prose.push(`Nearby scheduled monuments: ${scheduledLines.join("; ")}.`);
  if (listed.length) prose.push(`There are ${listed.length} listed buildings within ${raw.nearbyHeritage?.radiusMeters ?? 500}m; closest examples: ${listedLines.slice(0, 5).join("; ")}.`);

  return {
    ok: raw.ok,
    mode: "llm",
    site: {
      input: raw.input,
      lpa: raw.resolvedLpa?.lpaName || null,
      full_lpa_name: raw.resolvedLpa?.fullName || null,
      ons_code: raw.resolvedLpa?.onsCode || null,
      coordinates: raw.point,
    },
    site_context: prose.join(" "),
    local_designations: localDesignationLines,
    national_designations: nationalDesignationLines,
    nearby_heritage_assets: {
      radius_m: raw.nearbyHeritage?.radiusMeters ?? null,
      scheduled_monuments_within_radius: scheduled.length,
      scheduled_monuments: scheduledLines,
      listed_buildings_within_radius: listed.length,
      closest_listed_buildings: listedLines,
    },
    red_flags: uniqStrings(redFlags),
    planning_relevance_notes: uniqStrings([
      localDesignationLines.length ? "Treat local designations as primary local policy constraints for the site." : null,
      nationalDesignationLines.length ? "Use planning.data.gov.uk results as wider designation/context evidence around the site." : null,
      scheduled.length ? "Scheduled monuments nearby may affect archaeological, heritage, and setting assessments." : null,
      listed.length ? "Nearby listed buildings indicate likely heritage setting sensitivity even where the site itself is not listed." : null,
    ]),
  };
}

async function findLpaByQuery(sql, params) {
  const conn = await mysql.createConnection(getMysqlConfig());
  try {
    const [rows] = await conn.execute(sql, params);
    return rows[0] || null;
  } finally {
    await conn.end();
  }
}

async function resolveLpaByOnsCode(onsCode) {
  const value = String(onsCode || "").trim();
  if (!value) return null;
  return await findLpaByQuery(
    `
      SELECT ons_code, lpa_name, full_name, short_ref, datastore_id, local_planning_authority
      FROM lpa_codes
      WHERE ons_code = ?
      LIMIT 1
    `,
    [value],
  );
}

async function resolveLpaByLocalPlanningAuthorityCode(code) {
  const value = String(code || "").trim();
  if (!value) return null;
  return await findLpaByQuery(
    `
      SELECT ons_code, lpa_name, full_name, short_ref, datastore_id, local_planning_authority
      FROM lpa_codes
      WHERE local_planning_authority = ?
      LIMIT 1
    `,
    [value],
  );
}

async function resolveLpaByLonLat(lon, lat, timeoutMs) {
  const lookup = await lookupPlanningDataDesignations({
    lon,
    lat,
    datasets: ["local-planning-authority"],
    excludeDatasets: [],
    excludePrefixes: [],
    limit: 5,
    timeoutMs,
  });

  const entity = Array.isArray(lookup.entities) ? lookup.entities[0] : null;
  if (!entity?.reference) return null;

  const lpa = await resolveLpaByLocalPlanningAuthorityCode(entity.reference);
  if (!lpa) return null;

  return {
    ...lpa,
    planningDataEntity: entity,
  };
}

async function resolveApplicationPoint() {
  const byId = Number.isFinite(argv["application-id"]);
  const reference = String(argv.reference || "").trim();
  if (!byId && !reference) return null;

  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const params = [];
    const where = [];
    if (byId) {
      params.push(Number(argv["application-id"]));
      where.push(`id = $${params.length}`);
    } else {
      params.push(reference);
      where.push(`reference = $${params.length}`);
      const onsCode = String(argv["ons-code"] || "").trim();
      if (onsCode) {
        params.push(onsCode);
        where.push(`ons_code = $${params.length}`);
      }
    }

    const { rows } = await client.query(
      `
        SELECT
          id,
          ons_code,
          reference,
          lat,
          lon,
          CASE
            WHEN wfs_geom IS NULL THEN NULL
            ELSE ST_X(ST_PointOnSurface(wfs_geom))
          END AS wfs_easting,
          CASE
            WHEN wfs_geom IS NULL THEN NULL
            ELSE ST_Y(ST_PointOnSurface(wfs_geom))
          END AS wfs_northing,
          CASE
            WHEN wfs_geom IS NULL THEN NULL
            ELSE ST_X(ST_Transform(ST_PointOnSurface(wfs_geom), 4326))
          END AS wfs_lon,
          CASE
            WHEN wfs_geom IS NULL THEN NULL
            ELSE ST_Y(ST_Transform(ST_PointOnSurface(wfs_geom), 4326))
          END AS wfs_lat
        FROM public.applications
        WHERE ${where.join(" AND ")}
        ORDER BY id DESC
        LIMIT 1
      `,
      params,
    );

    if (!rows.length) {
      throw new Error("Application not found");
    }

    const row = rows[0];
    const wfsEasting = row.wfs_easting != null ? Number(row.wfs_easting) : null;
    const wfsNorthing = row.wfs_northing != null ? Number(row.wfs_northing) : null;
    const lon = row.lon != null ? Number(row.lon) : row.wfs_lon != null ? Number(row.wfs_lon) : null;
    const lat = row.lat != null ? Number(row.lat) : row.wfs_lat != null ? Number(row.wfs_lat) : null;

    return {
      application: {
        id: row.id,
        ons_code: row.ons_code,
        reference: row.reference,
      },
      easting: Number.isFinite(wfsEasting) ? wfsEasting : null,
      northing: Number.isFinite(wfsNorthing) ? wfsNorthing : null,
      lon: Number.isFinite(lon) ? lon : null,
      lat: Number.isFinite(lat) ? lat : null,
    };
  } finally {
    await client.end();
  }
}

async function resolvePostcodePoint() {
  const input = String(argv.postcode || "").trim() || String(argv.address || "").trim();
  if (!input) return null;

  const resolved = await resolvePostcodeViaOnspd(input, {
    noCache: true,
    timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
  });

  if (!resolved?.success) {
    throw new Error(resolved?.error || "Postcode lookup failed");
  }

  return {
    postcode_lookup: {
      input,
      postcode: resolved.postcode || null,
      lad25cd: resolved.lad25cd || null,
      terminated: Boolean(resolved.terminated),
    },
    lon: Number.isFinite(Number(resolved.long)) ? Number(resolved.long) : null,
    lat: Number.isFinite(Number(resolved.lat)) ? Number(resolved.lat) : null,
    easting: null,
    northing: null,
  };
}

async function main() {
  const appPoint = await resolveApplicationPoint();
  const postcodePoint = appPoint ? null : await resolvePostcodePoint();
  let resolvedFrom = "coordinates";
  const input = appPoint || postcodePoint || {
    lon: Number.isFinite(argv.lon) ? Number(argv.lon) : null,
    lat: Number.isFinite(argv.lat) ? Number(argv.lat) : null,
    easting: Number.isFinite(argv.easting) ? Number(argv.easting) : null,
    northing: Number.isFinite(argv.northing) ? Number(argv.northing) : null,
  };
  if (appPoint) resolvedFrom = "application";
  else if (postcodePoint) resolvedFrom = "postcode";

  const explicitOnsCode = String(argv["ons-code"] || "").trim();
  let resolvedLpa = null;
  if (appPoint?.application?.ons_code) {
    resolvedLpa = await resolveLpaByOnsCode(appPoint.application.ons_code);
  } else if (postcodePoint?.postcode_lookup?.lad25cd) {
    resolvedLpa = await resolveLpaByOnsCode(postcodePoint.postcode_lookup.lad25cd);
  } else if (explicitOnsCode) {
    resolvedLpa = await resolveLpaByOnsCode(explicitOnsCode);
  } else if (Number.isFinite(input.lon) && Number.isFinite(input.lat)) {
    resolvedLpa = await resolveLpaByLonLat(
      Number(input.lon),
      Number(input.lat),
      Math.max(1000, Number(argv["timeout-ms"] || 10000)),
    );
  }

  const resultPromise = resolvedLpa?.datastore_id
    ? lookupPolicyDesignationsByPoint({
        lon: input.lon,
        lat: input.lat,
        easting: input.easting,
        northing: input.northing,
        apiBaseUrl: buildArcgisFeatureServerUrl(resolvedLpa.datastore_id),
        timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
        resultRecordCount: Math.max(1, Number(argv["limit-per-layer"] || 10)),
        includeGeometry: Boolean(argv["with-geometry"]),
      })
    : Promise.resolve({
        apiBaseUrl: null,
        point: Number.isFinite(input.lon) && Number.isFinite(input.lat)
          ? { lon: Number(input.lon), lat: Number(input.lat) }
          : { easting: input.easting ?? null, northing: input.northing ?? null },
        pointSource: Number.isFinite(input.lon) && Number.isFinite(input.lat) ? "lon_lat" : "easting_northing",
        sourceCrs: Number.isFinite(input.lon) && Number.isFinite(input.lat) ? "EPSG:4326" : "EPSG:27700",
        queried: false,
        reason: resolvedLpa
          ? "Resolved LPA has no London Datastore ArcGIS mapping; borough lookup skipped."
          : "Could not resolve an LPA for the London Datastore ArcGIS lookup; borough lookup skipped.",
        queriedLayerCount: 0,
        matchedLayerCount: 0,
        layers: [],
      });

  const planningDataPromise = argv["include-planning-data"]
    ? lookupPlanningDataDesignations({
        lon: input.lon,
        lat: input.lat,
        datasets: Array.from(argv["planning-data-dataset"] || []).map((x) => String(x)).filter(Boolean),
        excludeDatasets: Array.from(argv["planning-data-exclude-dataset"] || []).map((x) => String(x)).filter(Boolean),
        excludePrefixes: Array.from(argv["planning-data-exclude-prefix"] || []).map((x) => String(x)).filter(Boolean),
        limit: Math.max(1, Number(argv["planning-data-limit"] || 50)),
        timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
        includeGeometry: Boolean(argv["with-geometry"]),
      })
    : Promise.resolve({
        apiUrl: null,
        queried: false,
        reason: "Disabled by --no-include-planning-data",
        point: null,
        datasets: [],
        count: 0,
        entities: [],
      });

  const nearbyHeritagePromise = argv["include-nearby-heritage"]
    ? lookupNearbyPlanningDataHeritage({
        lon: input.lon,
        lat: input.lat,
        datasets: Array.from(argv["nearby-heritage-dataset"] || []).map((x) => String(x)).filter(Boolean),
        radiusMeters: Math.max(1, Number(argv["nearby-heritage-radius-m"] || 500)),
        limit: Math.max(1, Number(argv["nearby-heritage-limit"] || 100)),
        timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
        includeGeometry: Boolean(argv["with-geometry"]),
      })
    : Promise.resolve({
        apiUrl: null,
        queried: false,
        reason: "Disabled by --no-include-nearby-heritage",
        point: null,
        radiusMeters: Math.max(1, Number(argv["nearby-heritage-radius-m"] || 500)),
        datasets: [],
        count: 0,
        entities: [],
      });

  const [result, planningData, nearbyHeritage] = await Promise.all([
    resultPromise,
    planningDataPromise,
    nearbyHeritagePromise,
  ]);

  const payload = {
    ok: true,
    resolvedFrom,
    resolvedLpa: {
      onsCode: resolvedLpa.ons_code || null,
      lpaName: resolvedLpa.lpa_name || null,
      fullName: resolvedLpa.full_name || null,
      shortRef: resolvedLpa.short_ref || null,
      localPlanningAuthority: resolvedLpa.local_planning_authority || null,
      datastoreId: resolvedLpa?.datastore_id != null ? Number(resolvedLpa.datastore_id) : null,
      resolutionSource: appPoint?.application?.ons_code
        ? "application_ons_code"
        : postcodePoint?.postcode_lookup?.lad25cd
          ? "postcode_lad25cd"
          : explicitOnsCode
            ? "explicit_ons_code"
            : resolvedLpa
              ? "planning_data_local_planning_authority"
              : null,
    },
    input: appPoint
      ? { ...appPoint.application, lon: input.lon, lat: input.lat, easting: input.easting, northing: input.northing }
      : postcodePoint
        ? { ...postcodePoint.postcode_lookup, lon: input.lon, lat: input.lat, easting: null, northing: null }
        : input,
    ...result,
    planningData,
    nearbyHeritage,
  };

  const outputMode = String(argv["output-mode"] || "raw");
  const finalPayload =
    outputMode === "summary"
      ? buildSummaryPayload(payload, {
          listedLimit: Number(argv["llm-listed-buildings-limit"] || 10),
          scheduledLimit: Number(argv["llm-scheduled-monuments-limit"] || 10),
        })
      : outputMode === "llm"
        ? buildLlmPayload(payload, {
            listedLimit: Number(argv["llm-listed-buildings-limit"] || 10),
            scheduledLimit: Number(argv["llm-scheduled-monuments-limit"] || 10),
          })
        : payload;

  if (argv.json) {
    console.log(JSON.stringify(finalPayload, null, 2));
    return;
  }

  console.log(`Matched ${payload.matchedLayerCount} layer(s)`);
  for (const layer of payload.layers) {
    console.log(`- [${layer.layerId}] ${layer.layerName}: ${layer.count}`);
    for (const match of layer.matches) {
      const label = match.sitename || match.designation || match.layerreference || `objectid=${match.objectid}`;
      console.log(`  ${label}`);
    }
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
