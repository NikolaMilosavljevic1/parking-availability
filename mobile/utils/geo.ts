import { AnchorPoint, Location, SortMode } from '../types';

const MAX_RADIUS_NEAR_ME_KM = 2.0;
const MAX_RADIUS_DESTINATION_KM = 1.5;
const MAX_RECOMMENDED_OCCUPANCY = 85;

export function haversineKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export function formatDistance(km: number): string {
  if (km < 1) return `${Math.round(km * 1000)} m`;
  return `${km.toFixed(1)} km`;
}

export function formatDistanceLabel(km: number, sortMode: SortMode): string {
  const base = formatDistance(km);
  return sortMode === 'destination' ? `${base} to destination` : base;
}

/** Minimum free spots required — scales down for small garages. */
export function minRequiredFreeSpots(totalSpots: number | null): number {
  if (totalSpots == null) return 3;
  if (totalSpots <= 30) return 3;
  if (totalSpots <= 120) return 5;
  return 10;
}

export function maxRadiusKm(sortMode: SortMode): number {
  return sortMode === 'destination'
    ? MAX_RADIUS_DESTINATION_KM
    : MAX_RADIUS_NEAR_ME_KM;
}

export function attachDistances(
  locs: Location[],
  anchor: AnchorPoint,
): Location[] {
  return locs.map((loc) => ({
    ...loc,
    distanceKm:
      loc.latitude != null && loc.longitude != null
        ? haversineKm(anchor.lat, anchor.lng, loc.latitude, loc.longitude)
        : undefined,
  }));
}

export function sortByDistanceThenAvailability(locs: Location[]): Location[] {
  if (!Array.isArray(locs)) return [];
  return locs.slice().sort((a, b) => {
    const distA = a.distanceKm ?? Infinity;
    const distB = b.distanceKm ?? Infinity;
    if (distA !== distB) return distA - distB;

    const freeA = a.free_spots ?? -1;
    const freeB = b.free_spots ?? -1;
    return freeB - freeA;
  });
}

function isEligibleForRecommendation(
  loc: Location,
  sortMode: SortMode,
): boolean {
  if (loc.latitude == null || loc.longitude == null) return false;
  if (loc.distanceKm == null) return false;
  if (loc.distanceKm > maxRadiusKm(sortMode)) return false;

  const free = loc.free_spots;
  if (free == null || free < minRequiredFreeSpots(loc.total_spots)) return false;

  if (loc.occupancy_pct != null && loc.occupancy_pct > MAX_RECOMMENDED_OCCUPANCY) {
    return false;
  }

  return true;
}

/**
 * Pick the closest location that has "good enough" availability.
 * Closeness wins over extra free spots (e.g. 10 free at 200 m beats 15 free at 1 km).
 */
export function pickRecommended(
  locs: Location[],
  _anchor: AnchorPoint,
  sortMode: SortMode = 'near_me',
): Location | null {
  let best: Location | null = null;

  for (const loc of locs) {
    if (!isEligibleForRecommendation(loc, sortMode)) continue;

    if (best == null) {
      best = loc;
      continue;
    }

    const dist = loc.distanceKm!;
    const bestDist = best.distanceKm!;

    if (dist < bestDist) {
      best = loc;
    } else if (dist === bestDist) {
      if ((loc.free_spots ?? 0) > (best.free_spots ?? 0)) {
        best = loc;
      }
    }
  }

  return best;
}

export function enrichAndSortLocations(
  locs: Location[],
  anchor: AnchorPoint | null,
  sortMode: SortMode = 'near_me',
): { locations: Location[]; recommended: Location | null } {
  if (!anchor || !Array.isArray(locs)) {
    return { locations: locs ?? [], recommended: null };
  }

  const withDist = attachDistances(locs, anchor);
  const recommended = pickRecommended(withDist, anchor, sortMode);
  const sorted = sortByDistanceThenAvailability(withDist);
  const locations = sorted.map((loc) => ({
    ...loc,
    isRecommended: recommended?.id === loc.id,
  }));

  return { locations, recommended };
}
