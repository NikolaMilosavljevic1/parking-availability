"""
weather.py — Open-Meteo weather integration for Belgrade.

Called on every scrape cycle (~60 s) to attach current weather conditions
to each parking snapshot. Open-Meteo is completely free, requires no API key,
and has a Belgrade station available at lat=44.8176, lng=20.4633.

WMO weather code reference (subset relevant for parking):
  0        — Clear sky
  1,2,3    — Mainly clear, partly cloudy, overcast
  45,48    — Fog
  51-67    — Drizzle / rain (various intensities)
  71-77    — Snow
  80-82    — Rain showers
  85,86    — Snow showers
  95       — Thunderstorm
  96,99    — Thunderstorm with hail

Usage:
    from weather import get_current_weather, WeatherSnapshot
    weather = await get_current_weather()
    print(weather.temperature_c, weather.is_raining)
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Belgrade coordinates (city centre)
BELGRADE_LAT = 44.8176
BELGRADE_LNG = 20.4633

OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={BELGRADE_LAT}&longitude={BELGRADE_LNG}"
    "&current=temperature_2m,precipitation,weather_code"
    "&timezone=Europe%2FBelgrade"
)

# WMO codes that indicate rain or showers
_RAIN_CODES = {
    51, 53, 55,         # drizzle
    61, 63, 65,         # rain
    66, 67,             # freezing rain
    80, 81, 82,         # rain showers
    95, 96, 99,         # thunderstorm (with / without hail)
}


@dataclass
class WeatherSnapshot:
    temperature_c: float
    precipitation_mm: float
    weather_code: int
    is_raining: bool
    fetched_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @classmethod
    def from_api_response(cls, current: dict) -> "WeatherSnapshot":
        temp = float(current["temperature_2m"])
        precip = float(current["precipitation"])
        code = int(current["weather_code"])
        raining = precip > 0 or code in _RAIN_CODES
        return cls(
            temperature_c=temp,
            precipitation_mm=precip,
            weather_code=code,
            is_raining=raining,
        )

    def as_dict(self) -> dict:
        """Return a dict suitable for direct **unpacking into a DB insert."""
        return {
            "temperature_c": self.temperature_c,
            "precipitation_mm": self.precipitation_mm,
            "weather_code": self.weather_code,
            "is_raining": self.is_raining,
        }


async def get_current_weather(
    timeout: float = 8.0,
    retries: int = 2,
) -> Optional[WeatherSnapshot]:
    """
    Fetch current Belgrade weather from Open-Meteo.

    Returns a WeatherSnapshot on success, or None if the request fails
    (so callers can store NULL in the DB rather than crashing).

    Args:
        timeout: HTTP request timeout in seconds.
        retries: Number of retry attempts on transient failure.
    """
    for attempt in range(1, retries + 2):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(OPEN_METEO_URL)
                resp.raise_for_status()
                data = resp.json()

            current = data["current"]
            snapshot = WeatherSnapshot.from_api_response(current)
            logger.debug(
                "Weather: %.1f°C, %.2f mm, code %d, raining=%s",
                snapshot.temperature_c,
                snapshot.precipitation_mm,
                snapshot.weather_code,
                snapshot.is_raining,
            )
            return snapshot

        except httpx.TimeoutException:
            logger.warning("Open-Meteo request timed out (attempt %d/%d)", attempt, retries + 1)
        except httpx.HTTPStatusError as exc:
            logger.warning("Open-Meteo HTTP error %d (attempt %d/%d)", exc.response.status_code, attempt, retries + 1)
        except Exception as exc:
            logger.error("Unexpected weather fetch error: %s", exc)
            break  # Don't retry on unexpected errors

        if attempt <= retries:
            await asyncio.sleep(2 ** attempt)  # exponential back-off: 2s, 4s

    logger.error("All weather fetch attempts failed — snapshot will have NULL weather fields")
    return None


# ---------------------------------------------------------------------------
# Quick standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    async def _test() -> None:
        print("Fetching Belgrade weather from Open-Meteo …")
        w = await get_current_weather()
        if w:
            print(f"  Temperature : {w.temperature_c:.1f} °C")
            print(f"  Precipitation: {w.precipitation_mm:.2f} mm")
            print(f"  WMO code    : {w.weather_code}")
            print(f"  Is raining  : {w.is_raining}")
            print(f"  Fetched at  : {w.fetched_at.isoformat()}")
        else:
            print("  FAILED — check network connectivity")
            sys.exit(1)

    asyncio.run(_test())
