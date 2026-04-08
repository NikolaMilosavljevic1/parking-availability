"""
routers/websocket.py — WebSocket live feed.

Architecture
------------
On startup (see main.py lifespan), a single background task subscribes to the
Redis pub/sub channel "parking:updates". Every time the scraper publishes a
new snapshot (every ~60 s), that message is received here and broadcast to ALL
currently-connected WebSocket clients in one pass.

This is more efficient than giving each client its own Redis subscriber.

Endpoint
--------
    WS /ws/live

    Client receives JSON messages shaped like:
    {
        "location_id":   "obilicev-venac",
        "name":          "Garaža \"Obilićev venac\"",
        "location_type": "garage",
        "free_spots":    312,
        "total_spots":   804,
        "occupancy_pct": 61.19,
        "latitude":      44.815755,
        "longitude":     20.457341,
        "neighborhood":  "Stari Grad",
        "scraped_at":    "2024-03-15T14:23:01.123456+00:00"
    }

    On connect, the client immediately receives a "snapshot" message containing
    all current live data (from Redis), so it doesn't have to wait 60 s for
    the first update.
"""

import asyncio
import json
import logging
from typing import Optional

import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from cache import UPDATE_CHANNEL, get_all_live

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Connection manager
# Keeps a set of active WebSocket connections.
# ---------------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)
        logger.info("WS client connected  — total: %d", len(self._clients))

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)
        logger.info("WS client disconnected — total: %d", len(self._clients))

    async def broadcast(self, message: str) -> None:
        """Send a raw JSON string to all connected clients. Drop dead connections."""
        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Background listener task
# Called once from main.py lifespan — runs for the lifetime of the app.
# ---------------------------------------------------------------------------

async def redis_listener(redis: aioredis.Redis) -> None:
    """
    Subscribe to the scraper's pub/sub channel and broadcast every message
    to all connected WebSocket clients.

    Reconnects automatically on transient Redis errors.
    """
    while True:
        try:
            pubsub = redis.pubsub()
            await pubsub.subscribe(UPDATE_CHANNEL)
            logger.info("Redis pub/sub subscribed to '%s'", UPDATE_CHANNEL)

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                data = message["data"]
                if manager._clients:
                    await manager.broadcast(data)

        except asyncio.CancelledError:
            logger.info("Redis listener cancelled — shutting down")
            return
        except Exception as exc:
            logger.error("Redis listener error: %s — retrying in 5 s", exc)
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@router.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    """
    Live parking updates pushed every ~60 s.

    On connect: sends an immediate snapshot of all current live data.
    Thereafter: receives one JSON message per location per scrape cycle.
    """
    await manager.connect(ws)

    # Immediately send all current data so the client doesn't start blank
    try:
        redis: aioredis.Redis = ws.app.state.redis
        current = await get_all_live(redis)
        if current:
            await ws.send_text(json.dumps({
                "type":    "snapshot",
                "payload": current,
            }))
    except Exception as exc:
        logger.warning("Could not send initial snapshot: %s", exc)

    # Keep the connection alive — actual updates come via manager.broadcast()
    try:
        while True:
            # We don't expect messages from the client, but we need to keep
            # the receive loop running so disconnects are detected promptly.
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception as exc:
        logger.warning("WebSocket error: %s", exc)
        manager.disconnect(ws)
