"""
db.py — PostgreSQL connection and write logic for the scraper.

Uses asyncpg directly (no ORM) for maximum insert throughput.

Public API
----------
    pool = await create_pool()
    await upsert_location(pool, reading)   # ensure row exists in parking_locations
    await insert_snapshot(pool, reading, weather, event_ctx, scraped_at)
    await get_all_locations(pool)          # → dict[id, row] for cache warm-up
"""

import logging
import math
import os
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from weather import WeatherSnapshot
from parking_scraper import ParkingReading

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Venue coordinates — used to compute dist_to_* columns
# ---------------------------------------------------------------------------
VENUES = {
    "arena":               (44.8065, 20.4084),
    "hram":                (44.7990, 20.4681),
    "marakana":            (44.7836, 20.4722),
    "partizan":            (44.7863, 20.4480),
    "narodno_pozoriste":   (44.8181, 20.4575),
    "sava_centar":         (44.8034, 20.4247),
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    R = 6371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _compute_venue_distances(lat: float, lon: float) -> dict:
    return {
        f"dist_to_{key}_km": round(_haversine_km(lat, lon, vlat, vlon), 3)
        for key, (vlat, vlon) in VENUES.items()
    }


# ---------------------------------------------------------------------------
# Serbian public holidays — (month, day) tuples
# Orthodox Easter (Vaskrs) is added dynamically per year.
# ---------------------------------------------------------------------------
_FIXED_HOLIDAYS: set[tuple[int, int]] = {
    (1, 1), (1, 2),   # Nova Godina
    (1, 7),           # Božić
    (2, 15), (2, 16), # Dan državnosti
    (5, 1), (5, 2),   # Praznik rada
    (11, 11),         # Dan primirja
}


def _orthodox_easter(year: int) -> tuple[int, int]:
    """
    Return (month, day) of Orthodox Easter for the given year
    using the Julian calendar algorithm (Meeus algorithm).
    """
    a = year % 4
    b = year % 7
    c = year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    f = d + e + 114
    month = f // 31
    day = (f % 31) + 1
    # Convert Julian → Gregorian (add 13 days for 21st century)
    import datetime as dt
    julian = dt.date(year, month, day)
    gregorian = julian + dt.timedelta(days=13)
    return gregorian.month, gregorian.day


def is_public_holiday(dt: datetime) -> bool:
    key = (dt.month, dt.day)
    if key in _FIXED_HOLIDAYS:
        return True
    # Check Orthodox Easter (Vaskrs) and Great Friday (Veliki Petak)
    em, ed = _orthodox_easter(dt.year)
    easter = datetime(dt.year, em, ed, tzinfo=dt.tzinfo)
    good_friday = easter - __import__("datetime").timedelta(days=2)
    if (dt.month, dt.day) in {(em, ed), (good_friday.month, good_friday.day)}:
        return True
    return False


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------
async def create_pool() -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    raw_url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(raw_url, min_size=2, max_size=5)
    logger.info("PostgreSQL connection pool created")
    return pool


# ---------------------------------------------------------------------------
# Location upsert
# Inserts the location row if it doesn't exist yet, and updates coordinates
# + venue distances if we now have lat/lng from the scraper.
# ---------------------------------------------------------------------------
_UPSERT_LOCATION_SQL = """
INSERT INTO parking_locations
    (id, name, location_type, latitude, longitude,
     dist_to_arena_km, dist_to_hram_km, dist_to_marakana_km,
     dist_to_partizan_km, dist_to_narodno_pozoriste_km, dist_to_sava_centar_km)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (id) DO UPDATE SET
    latitude                     = COALESCE(EXCLUDED.latitude, parking_locations.latitude),
    longitude                    = COALESCE(EXCLUDED.longitude, parking_locations.longitude),
    dist_to_arena_km             = COALESCE(EXCLUDED.dist_to_arena_km, parking_locations.dist_to_arena_km),
    dist_to_hram_km              = COALESCE(EXCLUDED.dist_to_hram_km, parking_locations.dist_to_hram_km),
    dist_to_marakana_km          = COALESCE(EXCLUDED.dist_to_marakana_km, parking_locations.dist_to_marakana_km),
    dist_to_partizan_km          = COALESCE(EXCLUDED.dist_to_partizan_km, parking_locations.dist_to_partizan_km),
    dist_to_narodno_pozoriste_km = COALESCE(EXCLUDED.dist_to_narodno_pozoriste_km, parking_locations.dist_to_narodno_pozoriste_km),
    dist_to_sava_centar_km       = COALESCE(EXCLUDED.dist_to_sava_centar_km, parking_locations.dist_to_sava_centar_km);
"""


async def upsert_location(pool: asyncpg.Pool, reading: ParkingReading) -> None:
    """Ensure the parking_locations row exists and has up-to-date coordinates."""
    dists = {}
    if reading.latitude and reading.longitude:
        dists = _compute_venue_distances(reading.latitude, reading.longitude)

    async with pool.acquire() as conn:
        await conn.execute(
            _UPSERT_LOCATION_SQL,
            reading.location_id,
            reading.name,
            reading.location_type,
            reading.latitude,
            reading.longitude,
            dists.get("dist_to_arena_km"),
            dists.get("dist_to_hram_km"),
            dists.get("dist_to_marakana_km"),
            dists.get("dist_to_partizan_km"),
            dists.get("dist_to_narodno_pozoriste_km"),
            dists.get("dist_to_sava_centar_km"),
        )


# ---------------------------------------------------------------------------
# Snapshot insert
# ---------------------------------------------------------------------------
_INSERT_SNAPSHOT_SQL = """
INSERT INTO parking_snapshots (
    location_id, free_spots, total_spots, occupancy_pct, scraped_at,
    hour_of_day, day_of_week, month, is_weekend, is_public_holiday,
    temperature_c, precipitation_mm, weather_code, is_raining,
    hours_to_next_event, nearest_event_venue, nearest_event_type,
    nearest_event_attendance_est, nearest_event_distance_km
) VALUES (
    $1,  $2,  $3,  $4,  $5,
    $6,  $7,  $8,  $9,  $10,
    $11, $12, $13, $14,
    $15, $16, $17, $18, $19
);
"""


async def insert_snapshot(
    pool: asyncpg.Pool,
    reading: ParkingReading,
    total_spots: Optional[int],
    weather: Optional[WeatherSnapshot],
    event_ctx: Optional[dict],
    scraped_at: datetime,
) -> None:
    """
    Insert one parking snapshot row.

    Args:
        reading:     Live data from the scraper.
        total_spots: From DB seed (parking_locations.total_spots).
        weather:     Current weather, or None if fetch failed.
        event_ctx:   Dict with nearest event fields, or None.
        scraped_at:  UTC timestamp for this scrape cycle.
    """
    occupancy = None
    if total_spots and total_spots > 0:
        occupancy = round((total_spots - reading.free_spots) / total_spots * 100, 2)
        # Clamp — free_spots can momentarily exceed total if cars leave between
        # the live feed update and our scrape
        occupancy = max(0.0, min(100.0, occupancy))

    local_dt = scraped_at.astimezone()
    holiday = is_public_holiday(scraped_at)

    async with pool.acquire() as conn:
        await conn.execute(
            _INSERT_SNAPSHOT_SQL,
            reading.location_id,
            reading.free_spots,
            total_spots,
            occupancy,
            scraped_at,
            scraped_at.hour,
            scraped_at.weekday(),          # 0 = Monday
            scraped_at.month,
            scraped_at.weekday() >= 5,     # is_weekend
            holiday,
            weather.temperature_c        if weather else None,
            weather.precipitation_mm     if weather else None,
            weather.weather_code         if weather else None,
            weather.is_raining           if weather else None,
            event_ctx.get("hours_to_next_event")          if event_ctx else None,
            event_ctx.get("nearest_event_venue")          if event_ctx else None,
            event_ctx.get("nearest_event_type")           if event_ctx else None,
            event_ctx.get("nearest_event_attendance_est") if event_ctx else None,
            event_ctx.get("nearest_event_distance_km")    if event_ctx else None,
        )


# ---------------------------------------------------------------------------
# Load all locations (for cache warm-up and total_spots lookup)
# ---------------------------------------------------------------------------
async def get_all_locations(pool: asyncpg.Pool) -> dict[str, asyncpg.Record]:
    """Return a dict of location_id → DB row for all parking_locations."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM parking_locations ORDER BY id")
    return {row["id"]: row for row in rows}


# ---------------------------------------------------------------------------
# Nearest upcoming event context for a location
# ---------------------------------------------------------------------------
_NEAREST_EVENT_SQL = """
SELECT
    e.event_name,
    e.event_type,
    e.venue_name,
    e.venue_lat,
    e.venue_lng,
    e.expected_attendance,
    -- Cast date+time to TIMESTAMPTZ so both sides of the subtraction are the same type
    EXTRACT(EPOCH FROM ((e.event_date + COALESCE(e.event_time, '19:00'::time))::timestamptz - $1)) / 3600.0
        AS hours_to_event
FROM city_events e
WHERE e.event_date >= CURRENT_DATE
  AND e.event_date <= CURRENT_DATE + INTERVAL '1 day'
ORDER BY e.event_date, e.event_time NULLS LAST
LIMIT 20;
"""


async def get_event_context(
    pool: asyncpg.Pool,
    lat: Optional[float],
    lon: Optional[float],
    now: datetime,
) -> Optional[dict]:
    """
    Find the nearest upcoming event (within 24h) and compute its distance
    to this parking location. Returns None if no events or no coordinates.
    """
    if lat is None or lon is None:
        return None

    async with pool.acquire() as conn:
        rows = await conn.fetch(_NEAREST_EVENT_SQL, now)

    if not rows:
        return None

    # Pick the event that is geographically closest to this location
    best = None
    best_dist = float("inf")
    for row in rows:
        if row["venue_lat"] is None or row["venue_lng"] is None:
            continue
        dist = _haversine_km(lat, lon, row["venue_lat"], row["venue_lng"])
        if dist < best_dist:
            best_dist = dist
            best = row

    if best is None:
        return None

    return {
        "hours_to_next_event":          round(float(best["hours_to_event"]), 2),
        "nearest_event_venue":          best["venue_name"],
        "nearest_event_type":           best["event_type"],
        "nearest_event_attendance_est": best["expected_attendance"],
        "nearest_event_distance_km":    round(best_dist, 3),
    }
