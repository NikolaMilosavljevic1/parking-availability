/** Anchor for distance sorting — user GPS or a geocoded destination. */
export interface AnchorPoint {
  lat: number;
  lng: number;
  label: string;
}

export type SortMode = 'near_me' | 'destination';

/** A parking location with its current live availability merged in. */
export interface Location {
  id: string;
  name: string;
  address: string | null;
  location_type: 'garage' | 'parking_lot';
  total_spots: number | null;
  latitude: number | null;
  longitude: number | null;
  neighborhood: string | null;

  // JKP cenovnik (static, from DB)
  price_first_hour_rsd: number | null;
  price_extra_hour_rsd: number | null;
  price_daily_rsd: number | null;
  hours_note: string | null;
  pricing_note: string | null;

  // Live data (from Redis via API — null means scraper hasn't run yet)
  free_spots: number | null;
  occupancy_pct: number | null;
  scraped_at: string | null;
  live: boolean;

  // Pre-computed distances to major venues (km)
  dist_to_arena_km: number | null;
  dist_to_hram_km: number | null;
  dist_to_marakana_km: number | null;
  dist_to_partizan_km: number | null;
  dist_to_narodno_pozoriste_km: number | null;
  dist_to_sava_centar_km: number | null;

  // Rule-based demand hint (detail endpoint only)
  elevated_demand?: boolean;
  demand_event_type?: string | null;
  demand_venue_name?: string | null;
  demand_event_name?: string | null;

  // Computed client-side relative to current anchor
  distanceKm?: number;
  isRecommended?: boolean;
}

/** One row from the history endpoint. */
export interface Snapshot {
  scraped_at: string;
  free_spots: number | null;
  total_spots: number | null;
  occupancy_pct: number | null;
  temperature_c: number | null;
  is_raining: boolean | null;
}

/** The initial WebSocket snapshot message. */
export interface WsSnapshot {
  type: 'snapshot';
  payload: WsLocationUpdate[];
}

/** A single-location live update published by the scraper. */
export interface WsLocationUpdate {
  location_id: string;
  name: string;
  location_type: string;
  free_spots: number | null;
  total_spots: number | null;
  occupancy_pct: number | null;
  latitude: number | null;
  longitude: number | null;
  neighborhood: string | null;
  scraped_at: string;
}
