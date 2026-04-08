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

  // Computed client-side from device GPS
  distanceKm?: number;
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

/** A city event from the /events endpoint. */
export interface CityEvent {
  id: number;
  event_name: string;
  event_type: string | null;
  venue_name: string | null;
  venue_lat: number | null;
  venue_lng: number | null;
  event_date: string;
  event_time: string | null;
  expected_attendance: number | null;
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
