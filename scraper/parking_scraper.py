"""
parking_scraper.py — Playwright scraper for JKP Parking Servis.

Target URL: https://www.parking-servis.co.rs/lat/garaze-i-parkiralista

DOM structure (confirmed from live page inspection):
    <ul class="parking-count">
        <li>
            <a href="https://www.google.com/maps/place/{lat},{lng}">Garaža "Name"</a>
            <span class="count ">173</span>   ← free spots only (no total on page)
        </li>
        ...
    </ul>

Only free_spots is available live. total_spots comes from the DB seed data.

Public API
----------
    results = await scrape_parking()
    # → list[ParkingReading]
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

TARGET_URL = "https://www.parking-servis.co.rs/lat/garaze-i-parkiralista"

# ---------------------------------------------------------------------------
# Name → location_id mapping
#
# Keys are lowercase, diacritic-stripped fragments of the display name.
# Matched with `in` — order matters, put longer/more specific keys first.
# ---------------------------------------------------------------------------
_NAME_TO_ID: list[tuple[str, str]] = [
    ("baba vi",        "baba-visnjina"),
    ("botanicka",      "botanicka-basta"),
    ("botanička",      "botanicka-basta"),
    ("aleksandra",     "dr-aleksandra-kostica"),
    ("masarik",        "masarikova"),
    ("obili",          "obilicev-venac"),
    ("pinki",          "pinki"),
    ("pionir",         "pionirski-park"),
    ("vukov",          "vukov-spomenik"),
    ("zeleni venac",   "zeleni-venac"),
    ("ada\"",          "ada"),          # Parkiralište "Ada"
    ("belvil",         "belvil"),
    ("bezanijska",     "bezanijska-kosa"),
    ("bežanijska",     "bezanijska-kosa"),
    ("blok 43",        "blok-43"),
    ("cukarica",       "cukarica"),
    ("čukarica",       "cukarica"),
    ("cvetkova",       "cvetkova-pijaca"),
    ("donji grad",     "donji-grad"),
    ("kalemegdan",     "kalemegdan"),
    ("kamenicka",      "kamenicka"),
    ("kamenička",      "kamenicka"),
    ("ljermontova",    "ljermontova"),
    ("medjunarodni",   "medjunarodni-carinski"),
    ("međunarodni",    "medjunarodni-carinski"),
    ("muskatirovic",   "milan-gale-muskatirovic"),
    ("muškatirović",   "milan-gale-muskatirovic"),
    ("opstina nbgd",   "opstina-nbgd"),
    ("opština nbgd",   "opstina-nbgd"),
    ("politika",       "politika"),
    ("slavija",        "slavija"),
    ("vidin kapija",   "vidin-kapija"),
    ("viska",          "viska"),
    ("viška",          "viska"),
    ("vma",            "vma"),
]

# Regex to extract lat,lng from Google Maps href
# e.g. https://www.google.com/maps/place/44.801441,20.474145
_MAPS_COORD_RE = re.compile(r"maps/place/([\d.]+),([\d.]+)")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------
@dataclass
class ParkingReading:
    location_id: str
    name: str           # raw display name from site
    free_spots: int
    latitude: Optional[float]   # extracted from Maps URL
    longitude: Optional[float]  # extracted from Maps URL
    location_type: str          # 'garage' or 'parking_lot'


# ---------------------------------------------------------------------------
# Name resolution
# ---------------------------------------------------------------------------
def _resolve_id(raw_name: str) -> Optional[str]:
    lower = raw_name.lower()
    for fragment, loc_id in _NAME_TO_ID:
        if fragment in lower:
            return loc_id
    return None


def _resolve_type(raw_name: str) -> str:
    lower = raw_name.lower()
    if "garaža" in lower or "garaza" in lower:
        return "garage"
    return "parking_lot"


# ---------------------------------------------------------------------------
# Playwright fetch
# ---------------------------------------------------------------------------
async def _fetch_html(url: str, timeout_ms: int = 30_000) -> str:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="sr-Latn",
            )
            page: Page = await context.new_page()

            # Block images, fonts, media — only need DOM
            await page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in {"image", "media", "font"}
                else route.continue_(),
            )

            await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            await page.wait_for_timeout(2_000)
            html = await page.content()
            logger.debug("Fetched %d bytes from %s", len(html), url)
            return html

        except PWTimeout:
            logger.error("Playwright timed out after %dms on %s", timeout_ms, url)
            raise
        finally:
            await browser.close()


# ---------------------------------------------------------------------------
# HTML parsing — precise selectors based on confirmed DOM structure
# ---------------------------------------------------------------------------
def _parse_html(html: str) -> list[ParkingReading]:
    """
    Parse <ul class="parking-count"> — each <li> contains:
      <a href="...maps/place/{lat},{lng}">Display Name</a>
      <span class="count[...]">free_spots</span>
    """
    soup = BeautifulSoup(html, "lxml")

    ul = soup.find("ul", class_="parking-count")
    if not ul:
        logger.error(
            "Could not find <ul class='parking-count'> — "
            "the site structure may have changed"
        )
        return []

    results: list[ParkingReading] = []

    for li in ul.find_all("li"):
        a_tag = li.find("a")
        span_tag = li.find("span", class_="count")

        if not a_tag or not span_tag:
            logger.debug("Skipping <li> missing <a> or <span class='count'>: %s", li)
            continue

        raw_name = a_tag.get_text(strip=True)
        free_text = span_tag.get_text(strip=True)

        # Parse free spots
        if not free_text.isdigit():
            logger.debug("Non-numeric count %r for %r — skipping", free_text, raw_name)
            continue
        free_spots = int(free_text)

        # Extract coordinates from Maps href
        href = a_tag.get("href", "")
        lat, lng = None, None
        m = _MAPS_COORD_RE.search(href)
        if m:
            lat = float(m.group(1))
            lng = float(m.group(2))

        # Resolve to our location_id
        loc_id = _resolve_id(raw_name)
        if loc_id is None:
            # Still record it — we'll upsert with the raw name as a new location
            loc_id = _slugify(raw_name)
            logger.info("New/unmapped location %r → generated id %r", raw_name, loc_id)

        loc_type = _resolve_type(raw_name)

        results.append(ParkingReading(
            location_id=loc_id,
            name=raw_name,
            free_spots=free_spots,
            latitude=lat,
            longitude=lng,
            location_type=loc_type,
        ))

    logger.info("Parsed %d locations from parking-count list", len(results))
    return results


def _slugify(text: str) -> str:
    """Generate a URL-safe ID from a display name."""
    _MAP = str.maketrans("ćčšžđĆČŠŽĐ", "ccszdCCSZD")
    slug = text.lower().translate(_MAP)
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    return slug.strip('-')


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def scrape_parking() -> list[ParkingReading]:
    """
    Fetch and parse the JKP Parking Servis page.
    Returns a (possibly empty) list of ParkingReading objects.
    Never raises — errors are logged so the scheduler continues.
    """
    try:
        html = await _fetch_html(TARGET_URL)
    except Exception as exc:
        logger.error("Failed to fetch parking page: %s", exc)
        return []

    try:
        readings = _parse_html(html)
    except Exception as exc:
        logger.error("Failed to parse parking HTML: %s", exc)
        return []

    if not readings:
        logger.warning("Scrape returned 0 readings — no data will be written this cycle")

    return readings


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import asyncio
    import sys
    from pathlib import Path

    dump_mode = "--dump" in sys.argv

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    async def _test() -> None:
        print(f"Scraping: {TARGET_URL}\n")
        html = await _fetch_html(TARGET_URL)

        if dump_mode:
            out = Path("debug_page.html")
            out.write_text(html, encoding="utf-8")
            print(f"Raw HTML saved to: {out.resolve()}\n")

        readings = _parse_html(html)
        if not readings:
            print("No readings returned — check logs above.")
            sys.exit(1)

        print(f"{'Location ID':<32} {'Type':<12} {'Name':<45} {'Free':>5} {'Lat':>10} {'Lng':>10}")
        print("-" * 115)
        for r in readings:
            lat = f"{r.latitude:.4f}" if r.latitude else "N/A"
            lng = f"{r.longitude:.4f}" if r.longitude else "N/A"
            print(f"{r.location_id:<32} {r.location_type:<12} {r.name:<45} {r.free_spots:>5} {lat:>10} {lng:>10}")
        print(f"\nTotal locations scraped: {len(readings)}")

    asyncio.run(_test())
