import * as ExpoLocation from 'expo-location';

import { haversineKm } from './geo';
import {
  isShortBrandQuery,
  searchOverpassNearby,
} from './overpass';

export interface DestinationOption {
  lat: number;
  lng: number;
  label: string;
  distanceKm: number | null;
}

const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search';
const NOMINATIM_REVERSE_URL = 'https://nominatim.openstreetmap.org/reverse';
const MAX_OPTIONS = 5;
const NOMINATIM_FETCH_LIMIT = 20;
const LOCAL_RADIUS_KM = 6;

const NOMINATIM_HEADERS = {
  'User-Agent': 'BelgradeParkingApp/1.0 (parking-availability)',
  Accept: 'application/json',
};

interface NominatimAddress {
  road?: string;
  house_number?: string;
  neighbourhood?: string;
  suburb?: string;
  quarter?: string;
  city_district?: string;
  city?: string;
  town?: string;
}

function normalizeQuery(query: string): string {
  const trimmed = query.trim();
  if (!trimmed) return trimmed;
  if (
    trimmed.toLowerCase().includes('belgrade') ||
    trimmed.toLowerCase().includes('beograd')
  ) {
    return trimmed;
  }
  return `${trimmed}, Belgrade, Serbia`;
}

/** viewbox for Nominatim: minLon, maxLat, maxLon, minLat */
function viewboxAround(
  lat: number,
  lng: number,
  radiusKm: number,
): string {
  const dLat = radiusKm / 111.0;
  const dLng = radiusKm / (111.0 * Math.cos((lat * Math.PI) / 180));
  const minLat = lat - dLat;
  const maxLat = lat + dLat;
  const minLng = lng - dLng;
  const maxLng = lng + dLng;
  return `${minLng},${maxLat},${maxLng},${minLat}`;
}

async function reverseNominatim(
  lat: number,
  lng: number,
): Promise<NominatimAddress | null> {
  const params = new URLSearchParams({
    lat: String(lat),
    lon: String(lng),
    format: 'json',
    addressdetails: '1',
    zoom: '17',
  });

  const resp = await fetch(`${NOMINATIM_REVERSE_URL}?${params}`, {
    headers: NOMINATIM_HEADERS,
  });

  if (!resp.ok) return null;

  const data: { address?: NominatimAddress } = await resp.json();
  return data.address ?? null;
}

function neighbourhoodFromAddress(addr: NominatimAddress): string | null {
  const hint = [
    addr.neighbourhood,
    addr.suburb,
    addr.quarter,
    addr.city_district,
  ]
    .filter(Boolean)
    .find((part) => part && part.length > 2);
  return hint ?? null;
}

async function getExpoAreaHint(
  userCoords: { lat: number; lng: number },
): Promise<string | null> {
  const addrs = await ExpoLocation.reverseGeocodeAsync({
    latitude: userCoords.lat,
    longitude: userCoords.lng,
  });
  const a = addrs[0];
  if (!a) return null;

  const hint = [a.district, a.subregion, a.name, a.city]
    .filter(Boolean)
    .find((part) => part && part.length > 2);
  return hint ?? null;
}

async function getUserAreaHint(
  userCoords: { lat: number; lng: number },
): Promise<string | null> {
  const [nominatimHint, expoHint] = await Promise.all([
    reverseNominatim(userCoords.lat, userCoords.lng)
      .then((addr) => (addr ? neighbourhoodFromAddress(addr) : null))
      .catch(() => null),
    getExpoAreaHint(userCoords).catch(() => null),
  ]);

  return nominatimHint ?? expoHint;
}

function formatStreetFromNominatim(addr: NominatimAddress): string | null {
  const street = [addr.road, addr.house_number].filter(Boolean).join(' ');
  return street || null;
}

function formatAreaFromNominatim(addr: NominatimAddress): string | null {
  const area = [
    addr.neighbourhood ?? addr.suburb ?? addr.quarter,
    addr.city_district,
    addr.city ?? addr.town,
  ]
    .filter(Boolean)
    .join(', ');
  return area || null;
}

async function buildLabelFromCoords(
  query: string,
  lat: number,
  lng: number,
  streetHint?: string,
): Promise<string> {
  const title = query.trim();

  if (streetHint) {
    return `${title} — ${streetHint}`;
  }

  try {
    const nominatimAddr = await reverseNominatim(lat, lng);
    if (nominatimAddr) {
      const street = formatStreetFromNominatim(nominatimAddr);
      const area = formatAreaFromNominatim(nominatimAddr);
      const location = street ?? area;
      if (location) return `${title} — ${location}`;
    }
  } catch {
    // fall through to Expo
  }

  try {
    const addrs = await ExpoLocation.reverseGeocodeAsync({
      latitude: lat,
      longitude: lng,
    });
    const a = addrs[0];
    if (a) {
      const street = [
        a.streetNumber && a.street ? `${a.street} ${a.streetNumber}` : a.street,
        a.district,
        a.city,
      ]
        .filter(Boolean)
        .join(', ');
      return street ? `${title} — ${street}` : title;
    }
  } catch {
    // fall through
  }

  return title;
}

function buildLabelFromNominatim(
  query: string,
  displayName: string,
  name?: string,
): string {
  const title = name || query.trim();
  const parts = displayName.split(',').map((p) => p.trim());
  const area = parts.slice(0, 3).join(', ');
  if (area.toLowerCase().startsWith(title.toLowerCase())) {
    return area;
  }
  return `${title} — ${area}`;
}

function dedupeKey(lat: number, lng: number): string {
  return `${lat.toFixed(4)},${lng.toFixed(4)}`;
}

function matchesQuery(query: string, label: string, nominatimName?: string): boolean {
  const tokens = query
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter((t) => t.length > 1);
  if (tokens.length === 0) return true;

  const haystack = `${label} ${nominatimName ?? ''}`.toLowerCase();
  return tokens.every((token) => haystack.includes(token));
}

function withDistance(
  options: Omit<DestinationOption, 'distanceKm'>[],
  userCoords: { lat: number; lng: number } | null,
): DestinationOption[] {
  const enriched = options.map((opt) => ({
    ...opt,
    distanceKm: userCoords
      ? haversineKm(userCoords.lat, userCoords.lng, opt.lat, opt.lng)
      : null,
  }));

  enriched.sort((a, b) => {
    if (a.distanceKm == null && b.distanceKm == null) return 0;
    if (a.distanceKm == null) return 1;
    if (b.distanceKm == null) return -1;
    return a.distanceKm - b.distanceKm;
  });

  return enriched;
}

async function searchExpo(normalized: string): Promise<DestinationOption[]> {
  const results = await ExpoLocation.geocodeAsync(normalized);
  const query = normalized.split(',')[0].trim();

  const options: DestinationOption[] = [];
  for (const r of results.slice(0, NOMINATIM_FETCH_LIMIT)) {
    const label = await buildLabelFromCoords(query, r.latitude, r.longitude);
    options.push({ lat: r.latitude, lng: r.longitude, label });
  }
  return options;
}

interface NominatimSearchOpts {
  viewbox?: string;
  bounded?: boolean;
  limit?: number;
}

async function searchNominatim(
  normalized: string,
  opts: NominatimSearchOpts = {},
): Promise<DestinationOption[]> {
  const params = new URLSearchParams({
    q: normalized,
    format: 'json',
    limit: String(opts.limit ?? NOMINATIM_FETCH_LIMIT),
    countrycodes: 'rs',
    addressdetails: '1',
  });

  if (opts.viewbox) {
    params.set('viewbox', opts.viewbox);
    if (opts.bounded) {
      params.set('bounded', '1');
    }
  }

  const resp = await fetch(`${NOMINATIM_URL}?${params}`, {
    headers: NOMINATIM_HEADERS,
  });

  if (!resp.ok) return [];

  const data: Array<{
    lat: string;
    lon: string;
    display_name: string;
    name?: string;
  }> = await resp.json();

  const query = normalized.split(',')[0].trim();

  return data
    .filter((item) =>
      matchesQuery(query.split(',')[0], item.display_name, item.name),
    )
    .map((item) => ({
      lat: parseFloat(item.lat),
      lng: parseFloat(item.lon),
      label: buildLabelFromNominatim(query, item.display_name, item.name),
    }));
}

function quickOverpassLabel(name: string, street?: string): string {
  return street ? `${name} — ${street}` : name;
}

async function searchOverpassDestinations(
  query: string,
  userCoords: { lat: number; lng: number },
): Promise<DestinationOption[]> {
  const places = await searchOverpassNearby(query, userCoords);
  if (!places.length) return [];

  const sorted = [...places].sort(
    (a, b) =>
      haversineKm(userCoords.lat, userCoords.lng, a.lat, a.lng) -
      haversineKm(userCoords.lat, userCoords.lng, b.lat, b.lng),
  );

  return sorted.slice(0, MAX_OPTIONS + 3).map((place) => ({
    lat: place.lat,
    lng: place.lng,
    label: quickOverpassLabel(place.name, place.street),
  }));
}

async function enrichResultLabels(
  options: DestinationOption[],
): Promise<DestinationOption[]> {
  return Promise.all(
    options.map(async (opt) => {
      const title = opt.label.split(' — ')[0]?.trim() || opt.label;
      if (opt.label.includes(' — ') && opt.label.split(' — ')[1]?.includes(',')) {
        return opt;
      }
      const label = await buildLabelFromCoords(title, opt.lat, opt.lng);
      return { ...opt, label };
    }),
  );
}

function mergeOptions(...groups: DestinationOption[][]): DestinationOption[] {
  const seen = new Set<string>();
  const merged: DestinationOption[] = [];

  for (const group of groups) {
    for (const opt of group) {
      const key = dedupeKey(opt.lat, opt.lng);
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push(opt);
    }
  }

  return merged;
}

/**
 * Search for destination candidates with strong bias toward the user's GPS.
 * For chains (e.g. "Maxi"), uses Overpass POI search plus Nominatim with a
 * neighbourhood hint so the nearest branch is not missed.
 */
export async function searchDestinations(
  query: string,
  userCoords: { lat: number; lng: number } | null,
): Promise<DestinationOption[]> {
  const trimmed = query.trim();
  if (!trimmed) return [];

  const normalized = normalizeQuery(trimmed);
  const baseTerm = trimmed.split(',')[0].trim();

  const nominatimSearches: Promise<DestinationOption[]>[] = [];

  if (userCoords) {
    const viewbox = viewboxAround(
      userCoords.lat,
      userCoords.lng,
      LOCAL_RADIUS_KM,
    );

    nominatimSearches.push(
      searchNominatim(normalized, { viewbox, bounded: false }).catch(
        () => [] as DestinationOption[],
      ),
      searchNominatim(normalized, { viewbox, bounded: true }).catch(
        () => [] as DestinationOption[],
      ),
    );

    const areaHint = await getUserAreaHint(userCoords);
    if (areaHint) {
      const localQuery = `${baseTerm}, ${areaHint}, Belgrade, Serbia`;
      nominatimSearches.push(
        searchNominatim(localQuery, { viewbox, bounded: false }).catch(
          () => [] as DestinationOption[],
        ),
        searchNominatim(localQuery, { viewbox, bounded: true }).catch(
          () => [] as DestinationOption[],
        ),
      );
    }

  }

  nominatimSearches.push(
    searchNominatim(normalized).catch(() => [] as DestinationOption[]),
  );

  const overpassPromise =
    userCoords && isShortBrandQuery(trimmed)
      ? searchOverpassDestinations(trimmed, userCoords).catch(
          () => [] as DestinationOption[],
        )
      : Promise.resolve([] as DestinationOption[]);

  const [expoResults, overpassResults, ...nominatimGroups] = await Promise.all([
    searchExpo(normalized).catch(() => [] as DestinationOption[]),
    overpassPromise,
    ...nominatimSearches,
  ]);

  const allNominatim = mergeOptions(...nominatimGroups);

  let options = mergeOptions(overpassResults, allNominatim, expoResults);

  if (options.length === 0) {
    options = mergeOptions(expoResults, allNominatim);
  }

  const ranked = withDistance(options, userCoords).slice(0, MAX_OPTIONS);
  return enrichResultLabels(ranked);
}
