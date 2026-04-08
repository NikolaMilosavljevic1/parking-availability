"""
routers/garages.py — REST endpoints for parking locations and events.

Endpoints
---------
    GET  /locations                  All locations + live Redis data
    GET  /locations/{id}             Single location (static + live)
    GET  /locations/{id}/history     Snapshots for 24h / 7d / 30d
    GET  /locations/{id}/predict     ML prediction stub (placeholder until Step 10)
    GET  /events                     Upcoming events (next 7 days)
    POST /events                     Manually add an event (admin fallback)
"""

import logging
from datetime import date, time
from typing import Annotated, Literal, Optional

import asyncpg
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

from db import get_location, get_locations, get_history, get_events, insert_event
from cache import get_all_live, get_live

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Dependency helpers — pull pool and redis off app.state
# ---------------------------------------------------------------------------

def _pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


def _redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def _merge_live(static: asyncpg.Record, live: Optional[dict]) -> dict:
    """Merge a static DB row with the live Redis snapshot into one dict."""
    result = dict(static)
    if live:
        result["free_spots"]    = live.get("free_spots")
        result["occupancy_pct"] = live.get("occupancy_pct")
        result["scraped_at"]    = live.get("scraped_at")
        result["live"]          = True
    else:
        result["free_spots"]    = None
        result["occupancy_pct"] = None
        result["scraped_at"]    = None
        result["live"]          = False   # scraper may be down or hasn't run yet
    return result


# ---------------------------------------------------------------------------
# GET /locations
# ---------------------------------------------------------------------------

@router.get("/locations", summary="All parking locations with live availability")
async def list_locations(
    pool:  asyncpg.Pool   = Depends(_pool),
    redis: aioredis.Redis = Depends(_redis),
):
    """
    Returns all locations sorted by free spots descending (most available first).
    Live data comes from Redis for speed; falls back gracefully if Redis is empty.
    """
    static_rows = await get_locations(pool)
    live_map    = {entry["location_id"]: entry for entry in await get_all_live(redis)}

    results = [
        _merge_live(row, live_map.get(row["id"]))
        for row in static_rows
    ]

    # Sort by free_spots descending; locations with no live data go to the bottom
    results.sort(key=lambda r: r["free_spots"] if r["free_spots"] is not None else -1, reverse=True)
    return results


# ---------------------------------------------------------------------------
# GET /locations/{id}
# ---------------------------------------------------------------------------

@router.get("/locations/{location_id}", summary="Single location with live availability")
async def get_location_detail(
    location_id: str,
    pool:        asyncpg.Pool   = Depends(_pool),
    redis:       aioredis.Redis = Depends(_redis),
):
    static = await get_location(pool, location_id)
    if static is None:
        raise HTTPException(status_code=404, detail=f"Location '{location_id}' not found")

    live = await get_live(redis, location_id)
    return _merge_live(static, live)


# ---------------------------------------------------------------------------
# GET /locations/{id}/history
# ---------------------------------------------------------------------------

_VALID_PERIODS = {"24h", "7d", "30d"}


@router.get(
    "/locations/{location_id}/history",
    summary="Historical occupancy snapshots",
)
async def get_location_history(
    location_id: str,
    period: Annotated[
        Literal["24h", "7d", "30d"],
        Query(description="Time window: 24h (full resolution), 7d or 30d (hourly averages)")
    ] = "24h",
    pool: asyncpg.Pool = Depends(_pool),
):
    # Confirm location exists first
    static = await get_location(pool, location_id)
    if static is None:
        raise HTTPException(status_code=404, detail=f"Location '{location_id}' not found")

    rows = await get_history(pool, location_id, period)

    return {
        "location_id": location_id,
        "name":        static["name"],
        "period":      period,
        "count":       len(rows),
        "snapshots": [
            {
                "scraped_at":    row["scraped_at"].isoformat() if row["scraped_at"] else None,
                "free_spots":    row["free_spots"],
                "total_spots":   row["total_spots"],
                "occupancy_pct": row["occupancy_pct"],
                "temperature_c": row["temperature_c"],
                "is_raining":    row["is_raining"],
            }
            for row in rows
        ],
    }


# ---------------------------------------------------------------------------
# GET /locations/{id}/predict
# ---------------------------------------------------------------------------

@router.get(
    "/locations/{location_id}/predict",
    summary="Predicted occupancy for the next 2 hours (ML — coming in Step 10)",
)
async def predict_occupancy(
    location_id: str,
    pool: asyncpg.Pool = Depends(_pool),
):
    static = await get_location(pool, location_id)
    if static is None:
        raise HTTPException(status_code=404, detail=f"Location '{location_id}' not found")

    # Placeholder until the ML layer (Step 10) is implemented.
    # Returns a 503 so the mobile app can degrade gracefully.
    raise HTTPException(
        status_code=503,
        detail="Prediction model not yet trained. Run ml/train.py first.",
    )


# ---------------------------------------------------------------------------
# GET /events
# ---------------------------------------------------------------------------

@router.get("/events", summary="Upcoming city events (next 7 days)")
async def list_events(
    days: Annotated[int, Query(ge=1, le=30, description="Days ahead to look")] = 7,
    pool: asyncpg.Pool = Depends(_pool),
):
    rows = await get_events(pool, days_ahead=days)
    return [
        {
            "id":                  row["id"],
            "event_name":          row["event_name"],
            "event_type":          row["event_type"],
            "venue_name":          row["venue_name"],
            "venue_lat":           row["venue_lat"],
            "venue_lng":           row["venue_lng"],
            "event_date":          row["event_date"].isoformat(),
            "event_time":          row["event_time"].isoformat() if row["event_time"] else None,
            "expected_attendance": row["expected_attendance"],
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# POST /events — admin fallback for manual event entry
# ---------------------------------------------------------------------------

class EventCreate(BaseModel):
    event_name:          str
    event_type:          Optional[str] = None   # concert, sports, theatre, religious, festival, other
    venue_name:          Optional[str] = None
    venue_lat:           Optional[float] = None
    venue_lng:           Optional[float] = None
    event_date:          date
    event_time:          Optional[time] = None
    expected_attendance: Optional[int] = None

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v):
        allowed = {"concert", "sports", "theatre", "religious", "festival", "other"}
        if v and v not in allowed:
            raise ValueError(f"event_type must be one of {allowed}")
        return v


@router.post("/events", status_code=201, summary="Manually add a city event")
async def create_event(
    body: EventCreate,
    pool: asyncpg.Pool = Depends(_pool),
):
    event_id = await insert_event(pool, {
        "event_name":          body.event_name,
        "event_type":          body.event_type,
        "venue_name":          body.venue_name,
        "venue_lat":           body.venue_lat,
        "venue_lng":           body.venue_lng,
        "event_date":          body.event_date,
        "event_time":          body.event_time,
        "expected_attendance": body.expected_attendance,
    })
    logger.info("Manually added event id=%d: %s on %s", event_id, body.event_name, body.event_date)
    return {"id": event_id, "event_name": body.event_name}
