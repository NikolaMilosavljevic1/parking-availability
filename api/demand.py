"""
demand.py — Rule-based elevated-demand evaluation for a parking location.

Uses upcoming city_events within walking distance (2 km) and a time window
around each event start. Returns the single most relevant qualifying event.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import asyncpg

MAX_VENUE_KM = 2.0
MIN_ATTENDANCE = 3000
HOURS_BEFORE_EVENT = 3.0
HOURS_AFTER_EVENT = 2.0
LOOKAHEAD_DAYS = 1

BELGRADE = ZoneInfo("Europe/Belgrade")
DEFAULT_EVENT_TIME = time(19, 0)

_EVENTS_SQL = """
SELECT
    event_name,
    event_type,
    venue_name,
    venue_lat,
    venue_lng,
    event_date,
    event_time,
    expected_attendance
FROM city_events
WHERE event_date >= CURRENT_DATE
  AND event_date <= CURRENT_DATE + ($1 * INTERVAL '1 day')
  AND venue_lat IS NOT NULL
  AND venue_lng IS NOT NULL
"""


@dataclass
class DemandContext:
    elevated: bool
    event_type: Optional[str] = None
    venue_name: Optional[str] = None
    event_name: Optional[str] = None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _event_start(row: asyncpg.Record) -> datetime:
    event_time = row["event_time"] or DEFAULT_EVENT_TIME
    return datetime.combine(row["event_date"], event_time, tzinfo=BELGRADE)


def _qualifies(row: asyncpg.Record, lat: float, lng: float, now: datetime) -> Optional[float]:
    """Return distance_km if event qualifies, else None."""
    distance_km = _haversine_km(lat, lng, row["venue_lat"], row["venue_lng"])
    if distance_km > MAX_VENUE_KM:
        return None

    attendance = row["expected_attendance"] or 0
    if attendance < MIN_ATTENDANCE:
        return None

    event_start = _event_start(row)
    window_start = event_start - timedelta(hours=HOURS_BEFORE_EVENT)
    window_end = event_start + timedelta(hours=HOURS_AFTER_EVENT)
    now_local = now.astimezone(BELGRADE)

    if not (window_start <= now_local <= window_end):
        return None

    return distance_km


async def get_demand_context(
    pool: asyncpg.Pool,
    lat: Optional[float],
    lng: Optional[float],
    now: datetime,
) -> DemandContext:
    if lat is None or lng is None:
        return DemandContext(elevated=False)

    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    async with pool.acquire() as conn:
        rows = await conn.fetch(_EVENTS_SQL, LOOKAHEAD_DAYS)

    best_row = None
    best_dist = float("inf")
    best_attendance = -1

    for row in rows:
        dist = _qualifies(row, lat, lng, now)
        if dist is None:
            continue
        attendance = row["expected_attendance"] or 0
        if dist < best_dist or (dist == best_dist and attendance > best_attendance):
            best_dist = dist
            best_attendance = attendance
            best_row = row

    if best_row is None:
        return DemandContext(elevated=False)

    return DemandContext(
        elevated=True,
        event_type=best_row["event_type"],
        venue_name=best_row["venue_name"],
        event_name=best_row["event_name"],
    )
