"""
cache.py — Redis read logic for the API.

The scraper writes keys and publishes to these same names.
This module only reads; it never writes.

Key schema (mirrors scraper/cache.py)
--------------------------------------
    parking:live:{location_id}   JSON blob, TTL 5 min
    parking:updates              pub/sub channel (used by WebSocket router)

Public API
----------
    client = await create_client()
    data   = await get_all_live(client)     → list[dict]
    entry  = await get_live(client, id)     → dict | None
    await close_client(client)
"""

import json
import logging
import os
from typing import Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

LIVE_KEY_PREFIX = "parking:live:"
UPDATE_CHANNEL  = "parking:updates"


async def create_client() -> aioredis.Redis:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    client = aioredis.from_url(url, decode_responses=True)
    await client.ping()
    logger.info("Redis client connected to %s", url)
    return client


async def close_client(client: aioredis.Redis) -> None:
    await client.aclose()


async def get_all_live(client: aioredis.Redis) -> list[dict]:
    """
    Return all live parking entries in one pipeline round-trip.
    Missing or corrupt entries are silently skipped.
    """
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


async def get_live(client: aioredis.Redis, location_id: str) -> Optional[dict]:
    """
    Return one live entry by location id, or None if missing / expired.
    A missing key means either the scraper hasn't run yet, or it crashed
    and the TTL elapsed — the API should indicate stale/unknown status.
    """
    raw = await client.get(LIVE_KEY_PREFIX + location_id)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Corrupt Redis entry for %s", location_id)
        return None
