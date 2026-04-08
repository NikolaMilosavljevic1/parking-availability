"""
cache.py — Redis write logic for the scraper.

The API reads from these keys for fast live responses.

Key schema
----------
    parking:live:{location_id}      JSON blob, TTL 5 min
        {
            "location_id":   str,
            "name":          str,
            "location_type": str,
            "free_spots":    int,
            "total_spots":   int | null,
            "occupancy_pct": float | null,
            "latitude":      float | null,
            "longitude":     float | null,
            "neighborhood":  str | null,
            "scraped_at":    ISO-8601 string
        }

    Pub/sub channel:  parking:updates
        Each scrape cycle publishes one JSON message per location so the
        API's WebSocket relay can push updates to mobile clients instantly.

Public API
----------
    client = await create_client()
    await write_live(client, reading, total_spots, occupancy_pct, location_row, scraped_at)
    await publish_update(client, payload)
    await close_client(client)
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

import redis.asyncio as aioredis

from parking_scraper import ParkingReading

logger = logging.getLogger(__name__)

LIVE_KEY_PREFIX = "parking:live:"
UPDATE_CHANNEL  = "parking:updates"
LIVE_TTL_SEC    = 300   # 5 minutes — stale after this if scraper dies


async def create_client() -> aioredis.Redis:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    client = aioredis.from_url(url, decode_responses=True)
    await client.ping()
    logger.info("Redis client connected to %s", url)
    return client


async def close_client(client: aioredis.Redis) -> None:
    await client.aclose()


async def write_live(
    client: aioredis.Redis,
    reading: ParkingReading,
    total_spots: Optional[int],
    occupancy_pct: Optional[float],
    location_row: Optional[dict],   # full row from parking_locations for extra fields
    scraped_at: datetime,
) -> None:
    """
    Write a live snapshot to Redis.
    The key expires after LIVE_TTL_SEC seconds so stale data is detectable.
    """
    neighborhood = None
    if location_row:
        neighborhood = location_row.get("neighborhood")

    payload = {
        "location_id":   reading.location_id,
        "name":          reading.name,
        "location_type": reading.location_type,
        "free_spots":    reading.free_spots,
        "total_spots":   total_spots,
        "occupancy_pct": occupancy_pct,
        "latitude":      reading.latitude,
        "longitude":     reading.longitude,
        "neighborhood":  neighborhood,
        "scraped_at":    scraped_at.isoformat(),
    }

    key = LIVE_KEY_PREFIX + reading.location_id
    await client.set(key, json.dumps(payload), ex=LIVE_TTL_SEC)
    logger.debug("Redis SET %s (free=%d)", key, reading.free_spots)

    # Publish for WebSocket relay
    await client.publish(UPDATE_CHANNEL, json.dumps(payload))


async def get_live(client: aioredis.Redis, location_id: str) -> Optional[dict]:
    """Read one live entry. Returns None if key is missing or expired."""
    raw = await client.get(LIVE_KEY_PREFIX + location_id)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Corrupt Redis entry for %s", location_id)
        return None


async def get_all_live(client: aioredis.Redis) -> list[dict]:
    """Return all live entries in one pipeline round-trip."""
    keys = await client.keys(LIVE_KEY_PREFIX + "*")
    if not keys:
        return []

    pipe = client.pipeline()
    for key in keys:
        pipe.get(key)
    values = await pipe.execute()

    results = []
    for raw in values:
        if raw:
            try:
                results.append(json.loads(raw))
            except json.JSONDecodeError:
                pass
    return results
