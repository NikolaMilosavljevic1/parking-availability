"""
db.py — PostgreSQL connection and read logic for the API.

Uses asyncpg directly (no ORM) for async performance.
The connection pool is stored on app.state and injected via FastAPI dependency.

Public API
----------
    pool = await create_pool()
    # then use via dependency:
    async def endpoint(pool=Depends(get_pool)): ...
"""

import logging
import os

import asyncpg

logger = logging.getLogger(__name__)


async def create_pool() -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    # Strip SQLAlchemy-style prefix if present — asyncpg uses plain postgresql://
    url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(url, min_size=2, max_size=10)
    logger.info("PostgreSQL connection pool created")
    return pool


# ---------------------------------------------------------------------------
# Location queries
# ---------------------------------------------------------------------------

async def get_locations(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    """All parking locations with their static metadata."""
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT
                id, name, address, location_type, total_spots,
                latitude, longitude, neighborhood,
                dist_to_arena_km, dist_to_hram_km, dist_to_marakana_km,
                dist_to_partizan_km, dist_to_narodno_pozoriste_km, dist_to_sava_centar_km
            FROM parking_locations
            ORDER BY name
        """)


async def get_location(pool: asyncpg.Pool, location_id: str) -> asyncpg.Record | None:
    """Single location by id, or None if not found."""
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT
                id, name, address, location_type, total_spots,
                latitude, longitude, neighborhood,
                dist_to_arena_km, dist_to_hram_km, dist_to_marakana_km,
                dist_to_partizan_km, dist_to_narodno_pozoriste_km, dist_to_sava_centar_km
            FROM parking_locations
            WHERE id = $1
        """, location_id)


# ---------------------------------------------------------------------------
# History queries
# ---------------------------------------------------------------------------

_PERIOD_INTERVALS = {
    "24h": "24 hours",
    "7d":  "7 days",
    "30d": "30 days",
}


async def get_history(
    pool: asyncpg.Pool,
    location_id: str,
    period: str = "24h",
) -> list[asyncpg.Record]:
    """
    Return snapshots for a given location and time window.

    For 24h: every row (scrape is every 60s, so ~1440 rows max).
    For 7d/30d: one row per hour (avg), to keep response size sane.
    """
    interval = _PERIOD_INTERVALS.get(period, "24 hours")

    if period == "24h":
        # One row per completed hour — up to 24 clean data points
        query = """
            SELECT
                date_trunc('hour', scraped_at)          AS scraped_at,
                ROUND(AVG(free_spots))::int             AS free_spots,
                MAX(total_spots)                        AS total_spots,
                ROUND(AVG(occupancy_pct)::numeric, 1)  AS occupancy_pct,
                ROUND(AVG(temperature_c)::numeric, 1)  AS temperature_c,
                BOOL_OR(is_raining)                     AS is_raining,
                EXTRACT(HOUR FROM MIN(scraped_at))::int AS hour_of_day,
                EXTRACT(DOW  FROM MIN(scraped_at))::int AS day_of_week
            FROM parking_snapshots
            WHERE location_id = $1
              AND scraped_at >= NOW() - INTERVAL '{interval}'
            GROUP BY date_trunc('hour', scraped_at)
            ORDER BY scraped_at ASC
        """.replace("{interval}", interval)
    else:
        # Hourly averages for longer periods
        query = """
            SELECT
                date_trunc('hour', scraped_at)          AS scraped_at,
                ROUND(AVG(free_spots))::int             AS free_spots,
                MAX(total_spots)                        AS total_spots,
                ROUND(AVG(occupancy_pct)::numeric, 2)  AS occupancy_pct,
                ROUND(AVG(temperature_c)::numeric, 1)  AS temperature_c,
                BOOL_OR(is_raining)                     AS is_raining,
                EXTRACT(HOUR FROM MIN(scraped_at))::int AS hour_of_day,
                EXTRACT(DOW  FROM MIN(scraped_at))::int AS day_of_week
            FROM parking_snapshots
            WHERE location_id = $1
              AND scraped_at >= NOW() - INTERVAL '{interval}'
            GROUP BY date_trunc('hour', scraped_at)
            ORDER BY scraped_at ASC
        """.replace("{interval}", interval)

    async with pool.acquire() as conn:
        return await conn.fetch(query, location_id)


# ---------------------------------------------------------------------------
# Events queries
# ---------------------------------------------------------------------------

async def get_events(pool: asyncpg.Pool, days_ahead: int = 7) -> list[asyncpg.Record]:
    """Upcoming city events for the next N days."""
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT
                id, event_name, event_type, venue_name,
                venue_lat, venue_lng, event_date, event_time,
                expected_attendance, scraped_at
            FROM city_events
            WHERE event_date >= CURRENT_DATE
              AND event_date <= CURRENT_DATE + ($1 * INTERVAL '1 day')
            ORDER BY event_date, event_time NULLS LAST
        """, days_ahead)


async def insert_event(pool: asyncpg.Pool, event: dict) -> int:
    """Manually insert an event. Returns the new event id."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO city_events
                (event_name, event_type, venue_name, venue_lat, venue_lng,
                 event_date, event_time, expected_attendance)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
        """,
            event["event_name"],
            event.get("event_type"),
            event.get("venue_name"),
            event.get("venue_lat"),
            event.get("venue_lng"),
            event["event_date"],
            event.get("event_time"),
            event.get("expected_attendance"),
        )
        return row["id"]
