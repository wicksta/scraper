#!/usr/bin/env node
/**
 * Westminster Idox PAM MapServer WFS geometry fetcher
 *
 * - Accepts KEYVAL directly, or UID (reference like 25/12435/FULL)
 * - If UID, resolves keyval from public.applications
 * - Fetch polygon by KEYVAL from Planning_Application_Polygons
 * - Fallback to point from Planning_Application_Points
 * - Returns WKT in EPSG:27700 + bbox + rough centroid
 *
 * Usage:
 *   node wcc_wfs_geometry.cjs S8UHN3RPI6S00
 *   node wcc_wfs_geometry.cjs 25/12435/FULL
 */

const pg = require("pg");
const { Client } = pg;

const WFS_URL = "https://idoxpa.westminster.gov.uk/PAM/LIVE/MapServer?map=pa";

const DEFAULT_HEADERS = {
  "Content-Type": "text/xml",
  "Accept": "*/*",
  "Origin": "https://idoxpa.westminster.gov.uk",
  "Referer": "https://idoxpa.westminster.gov.uk/PAM/LIVE/pamap/",
  "User-Agent":
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Safari/605.1.15",
};

function buildGetFeatureXml({ keyVal, typeName }) {
  // typeName: Planning_Application_Polygons | Planning_Application_Points
  return `<?xml version="1.0" encoding="UTF-8"?>
<wfs:GetFeature service="WFS" version="2.0.0"
  xmlns:wfs="http://www.opengis.net/wfs/2.0"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:gml="http://www.opengis.net/gml/3.2"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <wfs:Query typeName="${typeName}">
    <ogc:Filter>
      <ogc:PropertyIsEqualTo>
        <ogc:PropertyName>KEYVAL</ogc:PropertyName>
        <ogc:Literal>${escapeXml(keyVal)}</ogc:Literal>
      </ogc:PropertyIsEqualTo>
    </ogc:Filter>
  </wfs:Query>
</wfs:GetFeature>`;
}

function escapeXml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

async function postXml(xml) {
  const res = await fetch(WFS_URL, {
    method: "POST",
    headers: DEFAULT_HEADERS,
    body: xml,
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`WFS HTTP ${res.status}: ${text.slice(0, 300)}`);
  }
  // Westminster sometimes replies with HTML 403 even with 200 in other contexts;
  // defend anyway.
  if (text.toLowerCase().includes("<html") && text.includes("403")) {
    throw new Error(`WFS blocked (HTML 403 returned)`);
  }
  return text;
}

function parseNumberMatched(xml) {
  const m = xml.match(/numberMatched="([^"]+)"/);
  if (!m) return null;
  if (m[1] === "unknown") return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

function parseBbox(xml) {
  // first envelope in doc is fine
  const lower = xml.match(/<gml:lowerCorner>\s*([^<]+)\s*<\/gml:lowerCorner>/);
  const upper = xml.match(/<gml:upperCorner>\s*([^<]+)\s*<\/gml:upperCorner>/);
  if (!lower || !upper) return null;

  const [minE, minN] = lower[1].trim().split(/\s+/).map(Number);
  const [maxE, maxN] = upper[1].trim().split(/\s+/).map(Number);
  if (![minE, minN, maxE, maxN].every(Number.isFinite)) return null;

  return { minE, minN, maxE, maxN };
}

function parsePolygonPosListToWkt(xml) {
  const m = xml.match(/<gml:posList[^>]*>([\s\S]*?)<\/gml:posList>/);
  if (!m) return null;
  const nums = m[1].trim().split(/\s+/).map(Number);
  if (nums.length < 8 || nums.length % 2 !== 0 || nums.some((x) => !Number.isFinite(x))) {
    throw new Error("Bad gml:posList");
  }

  const coords = [];
  for (let i = 0; i < nums.length; i += 2) {
    coords.push([nums[i], nums[i + 1]]);
  }

  const wkt = `POLYGON((${coords.map(([x, y]) => `${x} ${y}`).join(",")}))`;
  const centroid = roughCentroid(coords);
  return { wkt, coords, centroid };
}

function parsePointToWkt(xml) {
  // Common GML point patterns
  // <gml:Point ...><gml:pos>526... 181...</gml:pos></gml:Point>
  const pos = xml.match(/<gml:pos[^>]*>\s*([0-9.\-]+)\s+([0-9.\-]+)\s*<\/gml:pos>/);
  if (!pos) return null;
  const x = Number(pos[1]);
  const y = Number(pos[2]);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  return { wkt: `POINT(${x} ${y})`, centroid: { e: x, n: y } };
}

function roughCentroid(coords) {
  // Simple average of vertices (not true polygon centroid, but good for pin)
  let sx = 0,
    sy = 0,
    c = 0;
  for (const [x, y] of coords) {
    sx += x;
    sy += y;
    c++;
  }
  return c ? { e: sx / c, n: sy / c } : null;
}

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

function looksLikeUid(value) {
  return String(value || "").includes("/");
}

async function lookupKeyValByUid(uid) {
  const client = new Client(getPgClientConfig());
  await client.connect();
  try {
    const { rows } = await client.query(
      `
        SELECT NULLIF(BTRIM(keyval), '') AS keyval
        FROM public.applications
        WHERE UPPER(reference) = UPPER($1)
        ORDER BY scraped_at DESC NULLS LAST, id DESC
        LIMIT 1
      `,
      [uid],
    );
    return rows[0]?.keyval || null;
  } finally {
    await client.end();
  }
}

async function fetchGeometryByKeyVal(keyVal) {
  // 1) polygon
  const polyXmlReq = buildGetFeatureXml({ keyVal, typeName: "Planning_Application_Polygons" });
  const polyXml = await postXml(polyXmlReq);

  const matchedPoly = parseNumberMatched(polyXml);
  if (matchedPoly === null || matchedPoly > 0 || polyXml.includes("Planning_Application_Polygons")) {
    const poly = parsePolygonPosListToWkt(polyXml);
    if (poly?.wkt) {
      return {
        keyVal,
        srid: 27700,
        type: "polygon",
        wkt: poly.wkt,
        bbox: parseBbox(polyXml),
        centroid: poly.centroid,
        raw: null, // set to polyXml if you want to store it
      };
    }
  }

  // 2) fallback point
  const ptXmlReq = buildGetFeatureXml({ keyVal, typeName: "Planning_Application_Points" });
  const ptXml = await postXml(ptXmlReq);
  const pt = parsePointToWkt(ptXml);

  if (pt?.wkt) {
    return {
      keyVal,
      srid: 27700,
      type: "point",
      wkt: pt.wkt,
      bbox: parseBbox(ptXml),
      centroid: pt.centroid,
      raw: null,
    };
  }

  return { keyVal, srid: 27700, type: "none", wkt: null, bbox: parseBbox(polyXml) ?? parseBbox(ptXml), centroid: null, raw: null };
}

// CLI
async function main() {
  // Ensure PG env vars are available regardless of current working directory.
  const { loadDotEnv } = await import("./bootstrap.js");
  loadDotEnv(require("node:path").resolve(__dirname, ".env"));

  const rawInput = String(process.argv[2] || "").trim();
  if (!rawInput) {
    console.error("Usage: node wcc_wfs_geometry.cjs <KEYVAL|UID>");
    process.exit(2);
  }

  // Keep existing KEYVAL flow unchanged, but allow UID lookup by reference.
  let keyVal = rawInput;
  if (looksLikeUid(rawInput)) {
    keyVal = await lookupKeyValByUid(rawInput);
    if (!keyVal) {
      process.stdout.write("No keyval\n");
      return;
    }
  }

  const out = await fetchGeometryByKeyVal(keyVal);
  process.stdout.write(JSON.stringify(out, null, 2) + "\n");
}

if (require.main === module) {
  main().catch((e) => {
    console.error(e?.stack || String(e));
    process.exit(1);
  });
}

module.exports = { fetchGeometryByKeyVal, lookupKeyValByUid };
