"""
main.py — APScheduler orchestration for the Belgrade Parking scraper.

Jobs
----
  parking_job   every 60 seconds  — scrape parking-servis, write PG + Redis
  event_job     every 24 hours    — scrape city events from all venues

Startup sequence
----------------
  1. Create asyncpg pool + Redis client
  2. Warm Redis cache from DB (so the API has data immediately on restart)
  3. Start scheduler — parking_job fires immediately on first run
  4. Keep event loop alive until SIGINT/SIGTERM

Error policy
------------
  Every job catches all exceptions and logs them. The scheduler is never
  allowed to crash — a failed cycle is logged and skipped.
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from parking_scraper import scrape_parking
from weather import get_current_weather
from db import create_pool, upsert_location, insert_snapshot, get_all_locations, get_event_context
from cache import create_client, close_client, write_live
from event_scraper import scrape_all_events, save_events

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("scraper.main")

SCRAPE_INTERVAL_SECONDS = 60
EVENT_SCRAPE_HOUR = 3   # run event scraper at 03:00 each day


# ---------------------------------------------------------------------------
# Cache warm-up
# Reads all existing locations + their latest snapshot from PG and populates
# Redis so the API returns live data immediately on container restart.
# ---------------------------------------------------------------------------
async def warm_cache(pool: asyncpg.Pool, redis: aioredis.Redis) -> None:
    logger.info("Warming Redis cache from database …")
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (location_id)
                s.location_id,
                s.free_spots,
                s.total_spots,
                s.occupancy_pct,
                s.scraped_at,
                l.name,
                l.location_type,
                l.latitude,
                l.longitude,
                l.neighborhood
            FROM parking_snapshots s
            JOIN parking_locations l ON l.id = s.location_id
            ORDER BY location_id, scraped_at DESC
        """)

    if not rows:
        logger.info("No existing snapshots in DB — cache will populate after first scrape")
        return

    pipe = redis.pipeline()
    import json
    from cache import LIVE_KEY_PREFIX, LIVE_TTL_SEC
    for row in rows:
        payload = {
            "location_id":   row["location_id"],
            "name":          row["name"],
            "location_type": row["location_type"],
            "free_spots":    row["free_spots"],
            "total_spots":   row["total_spots"],
            "occupancy_pct": row["occupancy_pct"],
            "latitude":      row["latitude"],
            "longitude":     row["longitude"],
            "neighborhood":  row["neighborhood"],
            "scraped_at":    row["scraped_at"].isoformat(),
        }
        pipe.set(LIVE_KEY_PREFIX + row["location_id"], json.dumps(payload), ex=LIVE_TTL_SEC)
    await pipe.execute()
    logger.info("Cache warmed with %d locations", len(rows))


# ---------------------------------------------------------------------------
# Parking scrape job — runs every 60 seconds
# ---------------------------------------------------------------------------
async def parking_job(pool: asyncpg.Pool, redis: aioredis.Redis) -> None:
    scraped_at = datetime.now(tz=timezone.utc)
    logger.info("── Parking scrape cycle start %s ──", scraped_at.strftime("%H:%M:%S"))

    # 1. Fetch parking data and weather concurrently
    readings, weather = await asyncio.gather(
        scrape_parking(),
        get_current_weather(),
        return_exceptions=True,
    )

    if isinstance(readings, Exception):
        logger.error("scrape_parking() raised: %s", readings)
        readings = []
    if isinstance(weather, Exception):
        logger.error("get_current_weather() raised: %s", weather)
        weather = None

    if not readings:
        logger.warning("No readings this cycle — skipping DB/cache writes")
        return

    # 2. Load location metadata (total_spots, neighborhood, etc.) from DB
    try:
        locations = await get_all_locations(pool)
    except Exception as exc:
        logger.error("Failed to load locations from DB: %s", exc)
        locations = {}

    # 3. Persist each reading
    success = 0
    for reading in readings:
        try:
            # Ensure the location row exists (handles new locations on the site)
            await upsert_location(pool, reading)

            location_row = locations.get(reading.location_id)
            total_spots = location_row["total_spots"] if location_row else None

            # Compute occupancy
            occupancy_pct = None
            if total_spots and total_spots > 0:
                occupancy_pct = round(
                    max(0.0, min(100.0, (total_spots - reading.free_spots) / total_spots * 100)),
                    2,
                )

            # Nearest event context
            event_ctx = await get_event_context(
                pool,
                reading.latitude,
                reading.longitude,
                scraped_at,
            )

            # Write to PostgreSQL
            await insert_snapshot(
                pool, reading, total_spots, weather, event_ctx, scraped_at
            )

            # Write to Redis (SET + PUBLISH)
            await write_live(
                redis, reading, total_spots, occupancy_pct,
                dict(location_row) if location_row else None,
                scraped_at,
            )

            success += 1

        except Exception as exc:
            logger.error("Failed to persist %s: %s", reading.location_id, exc)

    logger.info(
        "── Cycle complete: %d/%d written, weather=%.1f°C raining=%s ──",
        success,
        len(readings),
        weather.temperature_c if weather else float("nan"),
        weather.is_raining if weather else "N/A",
    )


# ---------------------------------------------------------------------------
# Event scrape job — runs once daily at EVENT_SCRAPE_HOUR
# ---------------------------------------------------------------------------
async def event_job(pool: asyncpg.Pool) -> None:
    logger.info("── Daily event scrape start ──")
    try:
        events = await scrape_all_events()
        if events:
            await save_events(pool, events)
            logger.info("── Event scrape complete: %d events saved ──", len(events))
        else:
            logger.warning("Event scrape returned 0 events")
    except Exception as exc:
        logger.error("Event scrape job failed: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    logger.info("Belgrade Parking Scraper starting …")

    pool = await create_pool()
    redis = await create_client()

    # Warm the Redis cache so the API has data immediately
    await warm_cache(pool, redis)

    scheduler = AsyncIOScheduler(timezone="UTC")

    # Parking job — every 60 seconds, fire immediately on startup
    scheduler.add_job(
        parking_job,
        trigger="interval",
        seconds=SCRAPE_INTERVAL_SECONDS,
        args=[pool, redis],
        id="parking_scrape",
        next_run_time=datetime.now(tz=timezone.utc),  # run immediately
        misfire_grace_time=30,
        max_instances=1,   # never overlap two scrape cycles
    )

    # Event job — daily at EVENT_SCRAPE_HOUR:00 UTC
    scheduler.add_job(
        event_job,
        trigger="cron",
        hour=EVENT_SCRAPE_HOUR,
        minute=0,
        args=[pool],
        id="event_scrape",
        misfire_grace_time=600,
        max_instances=1,
    )

    scheduler.start()
    logger.info(
        "Scheduler started — parking every %ds, events daily at %02d:00 UTC",
        SCRAPE_INTERVAL_SECONDS,
        EVENT_SCRAPE_HOUR,
    )

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received")
    finally:
        scheduler.shutdown(wait=False)
        await close_client(redis)
        await pool.close()
        logger.info("Scraper shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
