// Shared helpers used by scrapers/mappers.

function extractUkPostcode(address) {
  if (!address) return null;

  const s = String(address).toUpperCase();

  // Covers standard UK postcodes plus GIR 0AA.
  // Notes:
  // - We accept optional internal space.
  // - We return normalized "OUTCODE INCODE".
  const re = /\b(GIR\s?0AA|(?:[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}))\b/;

  const m = s.match(re);
  if (!m) return null;

  const compact = m[1].replace(/\s+/g, '');
  if (compact.length < 5) return null;

  return `${compact.slice(0, -3)} ${compact.slice(-3)}`;
}

module.exports = {
  extractUkPostcode,
  resolvePostcodeViaOnspd,
};

const __onspdCache = new Map(); // key: normalized postcode (e.g. "SW1A 1AA")

async function resolvePostcodeViaOnspd(input, opts = {}) {
  const trimmed = String(input ?? '').trim();
  if (!trimmed) {
    return { success: false, input: trimmed, error: 'Empty input' };
  }

  // Accept either a postcode or a full address containing one.
  const postcode = extractUkPostcode(trimmed);
  if (!postcode) {
    return { success: false, input: trimmed, error: 'No valid UK postcode found' };
  }

  const cacheKey = postcode;
  if (!opts.noCache && __onspdCache.has(cacheKey)) {
    // Return a shallow clone so callers can mutate safely.
    return { ...__onspdCache.get(cacheKey) };
  }

  const timeoutMs = Number(opts.timeoutMs ?? 6000);
  const api =
    opts.apiUrl ||
    'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ONSPD_AUG_2025_UK/FeatureServer/0/query';

  const fetchJson = async (where) => {
    const u = new URL(api);
    u.searchParams.set('where', where);
    u.searchParams.set('outFields', 'pcds,lad25cd,lat,long,doterm');
    u.searchParams.set('f', 'json');
    u.searchParams.set('returnIdsOnly', 'false');
    u.searchParams.set('returnCountOnly', 'false');

    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(u.toString(), {
        headers: { Accept: 'application/json' },
        signal: controller.signal,
      });
      if (!res.ok) return null;
      return await res.json();
    } catch {
      return null;
    } finally {
      clearTimeout(t);
    }
  };

  // doterm IS NULL => not terminated (live postcode)
  const whereLive = `pcds='${postcode.replace(/'/g, "''")}' AND doterm IS NULL`;
  let data = await fetchJson(whereLive);
  if (data && data.error) {
    const out = { success: false, input: trimmed, error: `ONSPD API error: ${data.message || 'unknown'}`, postcode };
    if (!opts.noCache) __onspdCache.set(cacheKey, out);
    return { ...out };
  }

  if (!data || !data.features || !data.features[0] || !data.features[0].attributes) {
    // Fallback: allow terminated postcodes too.
    const whereAny = `pcds='${postcode.replace(/'/g, "''")}'`;
    data = await fetchJson(whereAny);
    if (!data) {
      const out = { success: false, input: trimmed, error: 'Failed to contact ONSPD API', postcode };
      if (!opts.noCache) __onspdCache.set(cacheKey, out);
      return { ...out };
    }
    if (data && data.error) {
      const out = { success: false, input: trimmed, error: `ONSPD API error: ${data.message || 'unknown'}`, postcode };
      if (!opts.noCache) __onspdCache.set(cacheKey, out);
      return { ...out };
    }
    if (!data.features || !data.features[0] || !data.features[0].attributes) {
      const out = { success: false, input: trimmed, error: 'Postcode not found in ONSPD', postcode };
      if (!opts.noCache) __onspdCache.set(cacheKey, out);
      return { ...out };
    }
  }

  const attr = data.features[0].attributes || {};
  const lad25cd = attr.lad25cd ?? null;
  const lat = attr.lat != null ? Number(attr.lat) : null;
  const long = attr.long != null ? Number(attr.long) : null;
  const doterm = attr.doterm != null ? String(attr.doterm) : null;
  const postcodeOut = String(attr.pcds || postcode).toUpperCase();

  if (!lad25cd || !Number.isFinite(lat) || !Number.isFinite(long)) {
    const out = {
      success: false,
      input: trimmed,
      error: 'Incomplete data from ONSPD for postcode',
      postcode: postcodeOut,
      lad25cd,
      lat,
      long,
      doterm,
    };
    if (!opts.noCache) __onspdCache.set(cacheKey, out);
    return { ...out };
  }

  const out = {
    success: true,
    input: trimmed,
    postcode: postcodeOut,
    lad25cd,
    lat,
    long,
    doterm,
    terminated: doterm != null,
  };
  if (!opts.noCache) __onspdCache.set(cacheKey, out);
  return { ...out };
}
