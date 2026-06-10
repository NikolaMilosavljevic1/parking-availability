const OVERPASS_ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
];

const OVERPASS_TIMEOUT_MS = 14_000;
const OVERPASS_RADIUS_M = 5_000;
const OVERPASS_RESULT_LIMIT = 20;

export interface OverpassPlace {
  lat: number;
  lng: number;
  name: string;
  brand?: string;
  shop?: string;
  street?: string;
}

function escapeOverpassRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** First token is used for POI name matching (e.g. "maxi paviljoni" → "Maxi"). */
function brandToken(query: string): string {
  return query.trim().split(/\s+/)[0] ?? '';
}

function matchesBrandQuery(
  query: string,
  name?: string,
  brand?: string,
  shop?: string,
): boolean {
  const base = query.trim().split(',')[0].trim().toLowerCase();
  const tokens = base.split(/\s+/).filter((t) => t.length > 0);
  if (tokens.length === 0) return false;

  const nameL = (name ?? '').toLowerCase();
  const brandL = (brand ?? '').toLowerCase();

  if (tokens.length === 1 && tokens[0] === 'maxi') {
    if (shop === 'cleaning') return false;
    if (/maxigo/i.test(name ?? '')) return false;
    return nameL.startsWith('maxi') || brandL === 'maxi';
  }

  return tokens.every((token) => {
    if (brandL === token) return true;
    if (nameL.includes(token)) return true;
    const re = new RegExp(`(^|[^a-z0-9])${escapeOverpassRegex(token)}`, 'i');
    return re.test(name ?? '');
  });
}

function parseElement(
  el: {
    lat?: number;
    lon?: number;
    center?: { lat: number; lon: number };
    tags?: Record<string, string>;
  },
  fallbackName: string,
): OverpassPlace | null {
  const lat = el.lat ?? el.center?.lat;
  const lon = el.lon ?? el.center?.lon;
  if (lat == null || lon == null || !Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }

  const tags = el.tags ?? {};
  return {
    lat,
    lng: lon,
    name: tags.name ?? fallbackName,
    brand: tags.brand,
    shop: tags.shop,
    street: tags['addr:street'],
  };
}

async function fetchOverpass(
  queryQl: string,
  fallbackName: string,
): Promise<OverpassPlace[]> {
  const body = `data=${encodeURIComponent(queryQl)}`;

  for (const endpoint of OVERPASS_ENDPOINTS) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), OVERPASS_TIMEOUT_MS);

    try {
      const resp = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': 'BelgradeParkingApp/1.0 (parking-availability)',
        },
        body,
        signal: controller.signal,
      });

      if (!resp.ok) continue;

      const data: {
        elements?: Array<{
          lat?: number;
          lon?: number;
          center?: { lat: number; lon: number };
          tags?: Record<string, string>;
        }>;
      } = await resp.json();

      const places: OverpassPlace[] = [];

      for (const el of data.elements ?? []) {
        const place = parseElement(el, fallbackName);
        if (!place) continue;
        places.push(place);
      }

      return places;
    } catch {
      // try next mirror
    } finally {
      clearTimeout(timer);
    }
  }

  return [];
}

/**
 * Find named shops/POIs near the user via Overpass. Best for short chain queries
 * (e.g. "Maxi") where Nominatim text search misses nearby branches.
 */
export async function searchOverpassNearby(
  query: string,
  userCoords: { lat: number; lng: number },
): Promise<OverpassPlace[]> {
  const token = brandToken(query);
  if (!token || token.length < 2) return [];

  const pattern = escapeOverpassRegex(token);
  const { lat, lng } = userCoords;

  const overpassQl = `
[out:json][timeout:15];
(
  node["shop"]["name"~"^${pattern}",i](around:${OVERPASS_RADIUS_M},${lat},${lng});
  way["shop"]["name"~"^${pattern}",i](around:${OVERPASS_RADIUS_M},${lat},${lng});
  node["brand"~"^${pattern}",i](around:${OVERPASS_RADIUS_M},${lat},${lng});
  way["brand"~"^${pattern}",i](around:${OVERPASS_RADIUS_M},${lat},${lng});
);
out center ${OVERPASS_RESULT_LIMIT};
`.trim();

  const raw = await fetchOverpass(overpassQl, token);

  return raw.filter((place) =>
    matchesBrandQuery(query, place.name, place.brand, place.shop),
  );
}

export function isShortBrandQuery(query: string): boolean {
  const base = query.trim().split(',')[0].trim();
  if (!base || base.length > 48) return false;

  const tokens = base.split(/\s+/).filter(Boolean);
  if (tokens.length === 0 || tokens.length > 4) return false;

  const last = tokens[tokens.length - 1] ?? '';
  if (/^\d+[a-z]?$/i.test(last) && tokens.length >= 2) return false;

  return true;
}
