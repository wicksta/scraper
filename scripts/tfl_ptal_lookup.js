#!/usr/bin/env node
import "../bootstrap.js";

import http from "node:http";
import https from "node:https";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { extractUkPostcode } = require("../library.cjs");

const PTAL_LAYER_URL =
  "https://services1.arcgis.com/YswvgzOodUvqkoCN/arcgis/rest/services/PTAL_2023_DEV5_view1/FeatureServer/0";
const PTAL_SERVICES_LAYER_URL =
  "https://services1.arcgis.com/YswvgzOodUvqkoCN/arcgis/rest/services/Transprt_Services_2023_Calculation_Table_view/FeatureServer/0";
const ONSPD_QUERY_URL =
  "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ONSPD_AUG_2025_UK/FeatureServer/0/query";

const argv = yargs(hideBin(process.argv))
  .scriptName("tfl-ptal-lookup")
  .option("postcode", {
    type: "string",
    default: "",
    describe: "UK postcode to resolve via ONSPD before PTAL lookup.",
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
  .option("grid-id", {
    type: "number",
    default: null,
    describe: "Skip the point lookup and query the supporting services rows by GridID.",
  })
  .option("include-services", {
    type: "boolean",
    default: false,
    describe: "Also query the supporting transport-service rows for the matched GridID.",
  })
  .option("services-limit", {
    type: "number",
    default: 200,
    describe: "Max supporting transport-service rows to return.",
  })
  .option("with-geometry", {
    type: "boolean",
    default: false,
    describe: "Include PTAL cell geometry and supporting row geometry.",
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
  .check((args) => {
    const hasPostcode = String(args.postcode || "").trim() || String(args.address || "").trim();
    const hasLonLat = Number.isFinite(args.lon) && Number.isFinite(args.lat);
    const hasGrid = Number.isFinite(args.easting) && Number.isFinite(args.northing);
    const hasGridId = Number.isFinite(args["grid-id"]);
    if (!hasPostcode && !hasLonLat && !hasGrid && !hasGridId) {
      throw new Error("Provide --grid-id, --postcode/--address, lon/lat, or easting/northing");
    }
    return true;
  })
  .strict()
  .help()
  .argv;

async function fetchJson(url, { timeoutMs = 10000 } = {}) {
  const client = url.startsWith("https:") ? https : http;
  return await new Promise((resolve, reject) => {
    const req = client.get(
      url,
      {
        headers: { Accept: "application/json" },
      },
      (res) => {
        const chunks = [];
        res.setEncoding("utf8");
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          const body = chunks.join("");
          if ((res.statusCode || 500) >= 400) {
            reject(new Error(`HTTP ${res.statusCode}`));
            return;
          }
          try {
            const data = JSON.parse(body);
            if (data?.error) {
              reject(new Error(data.error.message || "ArcGIS API error"));
              return;
            }
            resolve(data);
          } catch (err) {
            reject(err);
          }
        });
      },
    );
    req.setTimeout(timeoutMs, () => req.destroy(new Error("Request timed out")));
    req.on("error", reject);
  });
}

function compactAttributes(attributes) {
  return {
    gridId: attributes.GridID ?? null,
    ptal: attributes.PTAL ?? null,
    ai: attributes.AI ?? null,
    bus: attributes.BUS ?? null,
    lul: attributes.LUL ?? null,
    rail: attributes.RAIL ?? null,
    tram: attributes.TRAM ?? null,
    easting: attributes.Centroids_ ?? null,
    northing: attributes.Centroid_1 ?? null,
  };
}

function compactServiceAttributes(attributes) {
  return {
    gridId: attributes.GridID ?? null,
    mode: attributes.Mode ?? null,
    stopName: attributes.Stop_Name ?? null,
    routeName: attributes.Route_Name ?? null,
    distance: attributes.Distance ?? null,
    vph: attributes.vph ?? null,
    walkTime: attributes.Walk_Time ?? null,
    swt: attributes.SWT ?? null,
    tat: attributes.TAT ?? null,
    edf: attributes.EDF ?? null,
    weight: attributes.Weight ?? null,
    serviceAi: attributes.Service_AI ?? null,
  };
}

async function resolveInputPoint() {
  const postcodeInput = String(argv.postcode || "").trim() || String(argv.address || "").trim();
  if (postcodeInput) {
    const postcode = extractUkPostcode(postcodeInput);
    if (!postcode) {
      throw new Error("No valid UK postcode found");
    }
    const url = new URL(ONSPD_QUERY_URL);
    url.searchParams.set("where", `pcds='${postcode.replace(/'/g, "''")}' AND doterm IS NULL`);
    url.searchParams.set("outFields", "pcds,lad25cd,lat,long,doterm");
    url.searchParams.set("f", "json");
    url.searchParams.set("returnIdsOnly", "false");
    url.searchParams.set("returnCountOnly", "false");
    let data;
    try {
      data = await fetchJson(url.toString(), {
        timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
      });
    } catch {
      const fallbackUrl = new URL(ONSPD_QUERY_URL);
      fallbackUrl.searchParams.set("where", `pcds='${postcode.replace(/'/g, "''")}'`);
      fallbackUrl.searchParams.set("outFields", "pcds,lad25cd,lat,long,doterm");
      fallbackUrl.searchParams.set("f", "json");
      fallbackUrl.searchParams.set("returnIdsOnly", "false");
      fallbackUrl.searchParams.set("returnCountOnly", "false");
      data = await fetchJson(fallbackUrl.toString(), {
        timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
      });
    }
    const attributes = Array.isArray(data?.features) ? data.features[0]?.attributes : null;
    if (!attributes) {
      throw new Error("Postcode not found in ONSPD");
    }
    const lon = Number(attributes.long);
    const lat = Number(attributes.lat);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
      throw new Error("Incomplete data from ONSPD for postcode");
    }
    return {
      source: "postcode",
      postcodeLookup: {
        postcode: String(attributes.pcds || postcode).toUpperCase(),
        lad25cd: attributes.lad25cd ?? null,
        terminated: attributes.doterm != null,
      },
      lon,
      lat,
      easting: null,
      northing: null,
      inSR: 4326,
      geometry: `${lon},${lat}`,
    };
  }

  if (Number.isFinite(argv.lon) && Number.isFinite(argv.lat)) {
    return {
      source: "lon_lat",
      postcodeLookup: null,
      lon: Number(argv.lon),
      lat: Number(argv.lat),
      easting: null,
      northing: null,
      inSR: 4326,
      geometry: `${Number(argv.lon)},${Number(argv.lat)}`,
    };
  }

  if (Number.isFinite(argv.easting) && Number.isFinite(argv.northing)) {
    return {
      source: "grid",
      postcodeLookup: null,
      lon: null,
      lat: null,
      easting: Number(argv.easting),
      northing: Number(argv.northing),
      inSR: 27700,
      geometry: `${Number(argv.easting)},${Number(argv.northing)}`,
    };
  }

  return null;
}

async function lookupPtalByPoint(point) {
  const url = new URL(`${PTAL_LAYER_URL}/query`);
  url.searchParams.set("where", "1=1");
  url.searchParams.set("geometry", point.geometry);
  url.searchParams.set("geometryType", "esriGeometryPoint");
  url.searchParams.set("inSR", String(point.inSR));
  url.searchParams.set("spatialRel", "esriSpatialRelIntersects");
  url.searchParams.set("outFields", "PTAL");
  url.searchParams.set("returnGeometry", argv["with-geometry"] ? "true" : "false");
  url.searchParams.set("f", "json");

  const data = await fetchJson(url.toString(), {
    timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
  });
  const feature = Array.isArray(data?.features) ? data.features[0] : null;
  if (!feature?.attributes) {
    return {
      queried: true,
      found: false,
      reason: "No PTAL cell matched the supplied point.",
      cell: null,
    };
  }

  const cell = {
    ptal: feature.attributes.PTAL ?? null,
  };
  if (argv["with-geometry"]) {
    cell.geometry = feature.geometry ?? null;
  }

  return {
    queried: true,
    found: true,
    reason: null,
    cell,
  };
}

async function lookupServicesByGridId(gridId) {
  const url = new URL(`${PTAL_SERVICES_LAYER_URL}/query`);
  url.searchParams.set("where", `GridID=${Number(gridId)}`);
  url.searchParams.set(
    "outFields",
    "GridID,Mode,Stop_Name,Route_Name,Distance,vph,Walk_Time,SWT,TAT,EDF,Weight,Service_AI",
  );
  url.searchParams.set("returnGeometry", argv["with-geometry"] ? "true" : "false");
  url.searchParams.set("resultRecordCount", String(Math.max(1, Number(argv["services-limit"] || 200))));
  url.searchParams.set("orderByFields", "Mode ASC, Stop_Name ASC, Route_Name ASC");
  url.searchParams.set("f", "json");

  const data = await fetchJson(url.toString(), {
    timeoutMs: Math.max(1000, Number(argv["timeout-ms"] || 10000)),
  });
  const rows = Array.isArray(data?.features)
    ? data.features.map((feature) => {
        const row = compactServiceAttributes(feature.attributes || {});
        if (argv["with-geometry"]) {
          row.geometry = feature.geometry ?? null;
        }
        return row;
      })
    : [];

  return {
    queried: true,
    gridId: Number(gridId),
    count: rows.length,
    rows,
  };
}

async function main() {
  const explicitGridId = Number.isFinite(argv["grid-id"]) ? Number(argv["grid-id"]) : null;
  const inputPoint = explicitGridId == null ? await resolveInputPoint() : null;
  const ptal = explicitGridId == null ? await lookupPtalByPoint(inputPoint) : null;
  const services = argv["include-services"] && explicitGridId != null
    ? await lookupServicesByGridId(explicitGridId)
    : argv["include-services"] && explicitGridId == null
      ? {
          queried: false,
          gridId: null,
          count: 0,
          rows: [],
          reason: "Supporting services require --grid-id in minimal mode.",
        }
      : {
          queried: false,
          gridId: null,
          count: 0,
          rows: [],
          reason: "Skipped by default. Pass --include-services to fetch supporting rows.",
        };

  const payload = {
    ok: true,
    source: explicitGridId != null ? "grid_id" : inputPoint?.source || null,
    input: explicitGridId != null
      ? { gridId: explicitGridId }
      : inputPoint
        ? {
            postcodeLookup: inputPoint.postcodeLookup,
            lon: inputPoint.lon,
            lat: inputPoint.lat,
            easting: inputPoint.easting,
            northing: inputPoint.northing,
          }
        : null,
    ptal: ptal || {
      queried: false,
      found: false,
      reason: "Skipped because --grid-id was provided.",
      cell: null,
    },
  };

  if (argv["include-services"]) {
    payload.services = services;
  }

  if (argv.json) {
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  if (!payload.ptal?.found && payload.source !== "grid_id") {
    console.log(payload.ptal?.reason || "No PTAL match found");
    return;
  }

  const cell = payload.ptal?.cell;
  if (cell) {
    console.log(`PTAL ${cell.ptal}`);
  } else {
    console.log(`GridID ${explicitGridId}`);
  }
  if (payload.services) {
    console.log(`Supporting services: ${payload.services.count}`);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
