"""
main.py — FastAPI application for the Belgrade Parking API.

Lifespan
--------
    On startup:
        1. Create asyncpg connection pool → app.state.pool
        2. Create Redis client           → app.state.redis
        3. Launch Redis pub/sub listener as background task

    On shutdown:
        Cancel the listener task, close Redis and DB connections cleanly.

Routers
-------
    /locations*   → routers/garages.py  (REST)
    /events*      → routers/garages.py  (REST)
    /ws/live      → routers/websocket.py (WebSocket)
"""

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db import create_pool
from cache import create_client, close_client
from routers.garages import router as garages_router
from routers.websocket import router as ws_router, redis_listener

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("api.main")


# ---------------------------------------------------------------------------
# Lifespan — startup and shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("Belgrade Parking API starting …")

    app.state.pool  = await create_pool()
    app.state.redis = await create_client()

    # Start the Redis pub/sub → WebSocket broadcast loop in the background
    listener_task = asyncio.create_task(
        redis_listener(app.state.redis),
        name="redis_listener",
    )
    logger.info("API ready")

    yield   # ← application runs here

    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("Belgrade Parking API shutting down …")
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass

    await close_client(app.state.redis)
    await app.state.pool.close()
    logger.info("API shut down cleanly")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Belgrade Parking API",
    description=(
        "Real-time parking availability for Belgrade, Serbia. "
        "Data scraped from JKP Parking Servis every 60 seconds."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Allow all origins in development — tighten this for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(garages_router)
app.include_router(ws_router)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
async def health():
    """Simple liveness check. Returns 200 if the API is up."""
    return {"status": "ok"}
