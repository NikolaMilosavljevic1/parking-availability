import { AnchorPoint, Location, SortMode } from '../types';

const DISTANCE_FLOOR_KM = 0.3;
const MAX_RECOMMENDED_OCCUPANCY = 90;

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

function isEligibleForRecommendation(loc: Location): boolean {
  if (loc.latitude == null || loc.longitude == null) return false;
  if (loc.distanceKm == null) return false;
  if (loc.free_spots == null || loc.free_spots <= 0) return false;
  if (loc.occupancy_pct != null && loc.occupancy_pct > MAX_RECOMMENDED_OCCUPANCY) {
    return false;
  }
  return true;
}

function recommendationScore(loc: Location): number {
  return (loc.free_spots ?? 0) / (loc.distanceKm! + DISTANCE_FLOOR_KM);
}

export function pickRecommended(
  locs: Location[],
  _anchor: AnchorPoint,
): Location | null {
  let best: Location | null = null;
  let bestScore = -1;

  for (const loc of locs) {
    if (!isEligibleForRecommendation(loc)) continue;
    const score = recommendationScore(loc);
    if (score > bestScore) {
      bestScore = score;
      best = loc;
    }
  }

  return best;
}

export function enrichAndSortLocations(
  locs: Location[],
  anchor: AnchorPoint | null,
): { locations: Location[]; recommended: Location | null } {
  if (!anchor || !Array.isArray(locs)) {
    return { locations: locs ?? [], recommended: null };
  }

  const withDist = attachDistances(locs, anchor);
  const recommended = pickRecommended(withDist, anchor);
  const sorted = sortByDistanceThenAvailability(withDist);
  const locations = sorted.map((loc) => ({
    ...loc,
    isRecommended: recommended?.id === loc.id,
  }));

  return { locations, recommended };
}
