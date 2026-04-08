"""
event_scraper.py — Daily city event scraping for Belgrade.

Sources
-------
    1. Belgrade Arena          https://arenabeograd.com/listadogadjaja/
    2. FK Crvena zvezda        JS-rendered — graceful fallback (use POST /events)
    3. FK Partizan             JS-rendered — graceful fallback (use POST /events)
    4. Hram Svetog Save        hardcoded Orthodox calendar dates
    5. Sava Center             https://www.savacenter.net/
    6. Narodno pozorište       https://www.narodnopozoriste.rs/repertoar/

All scrapers fail gracefully — if a source can't be parsed, it logs a
warning and returns an empty list. The other sources still run.

Public API
----------
    events = await scrape_all_events()   → list[dict]
    await save_events(pool, events)
"""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

import asyncpg
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared HTTP client settings
# ---------------------------------------------------------------------------
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sr,en;q=0.9",
}
_TIMEOUT = httpx.Timeout(15.0)


# ---------------------------------------------------------------------------
# Date parsing helpers
# Latin and Cyrillic Serbian month names, full and abbreviated
# ---------------------------------------------------------------------------
_SR_MONTHS: dict[str, int] = {
    # Latin full
    "januar": 1,  "januara": 1,
    "februar": 2, "februara": 2,
    "mart": 3,    "marta": 3,
    "april": 4,   "aprila": 4,
    "maj": 5,     "maja": 5,
    "jun": 6,     "juna": 6,    "juni": 6,
    "jul": 7,     "jula": 7,    "juli": 7,
    "avgust": 8,  "avgusta": 8,
    "septembar": 9,  "septembra": 9,  "sep": 9,  "sept": 9,
    "oktobar": 10,   "oktobra": 10,   "okt": 10,
    "novembar": 11,  "novembra": 11,  "nov": 11,
    "decembar": 12,  "decembra": 12,  "dec": 12,
    # Latin abbreviated
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "avg": 8,
    # Cyrillic full
    "јануар": 1,  "јануара": 1,
    "фебруар": 2, "фебруара": 2,
    "март": 3,    "марта": 3,
    "април": 4,   "априла": 4,
    "мај": 5,     "маја": 5,
    "јун": 6,     "јуна": 6,    "јуни": 6,
    "јул": 7,     "јула": 7,    "јули": 7,
    "август": 8,  "августа": 8,
    "септембар": 9,  "септембра": 9,
    "октобар": 10,   "октобра": 10,
    "новембар": 11,  "новембра": 11,
    "децембар": 12,  "децембра": 12,
    # Cyrillic abbreviated (as seen on narodnopozoriste.rs)
    "јан": 1, "феб": 2, "мар": 3, "апр": 4,
    "јун": 6, "јул": 7, "авг": 8, "сеп": 9,
    "окт": 10, "нов": 11, "дец": 12,
}


def _infer_year(month: int, day: int) -> int:
    """
    If the given month/day is in the past this year, return next year.
    Otherwise return this year. Used for Arena dates that have no year.
    """
    today = date.today()
    candidate = date(today.year, month, day)
    if candidate < today:
        return today.year + 1
    return today.year


def _parse_date_text(text: str) -> Optional[date]:
    """
    Parse Serbian date text in various formats:
      '15. april 2025'  'april 26'  '15.04.2025'  '2025-04-15'
      'Сре1апр'         'апр 26'    '26. апр 2025'
    Returns None if parsing fails.
    """
    text = text.strip()

    # ISO: 2025-04-15
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # DD.MM.YYYY or DD/MM/YYYY
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # "MonthName DD [YYYY]" or "DD. MonthName [YYYY]" or "DD MonthName" (Latin + Cyrillic)
    # Also handles no-space variants like "1апр" from Narodno pozoriste.
    # Build a pattern from all known month names (longest first to avoid partial matches)
    month_pattern = "|".join(sorted(_SR_MONTHS.keys(), key=len, reverse=True))
    # Matches: optional day (with optional dot/space) THEN month THEN optional day/year
    m = re.search(
        rf"(?:(\d{{1,2}})\.?\s*)?({month_pattern})\s*(\d{{1,2}})?(?:\s+(\d{{4}}))?",
        text,
        re.IGNORECASE,
    )
    if m:
        day_pre   = m.group(1)
        month_str = m.group(2).lower()
        day_post  = m.group(3)
        year_str  = m.group(4)

        # One of day_pre or day_post must be present
        if not day_pre and not day_post:
            return None
        day = int(day_pre) if day_pre else int(day_post)
        month_num = _SR_MONTHS.get(month_str)
        if month_num:
            year = int(year_str) if year_str else _infer_year(month_num, day)
            try:
                return date(year, month_num, day)
            except ValueError:
                pass

    return None


def _is_future(d: date, days_ahead: int = 90) -> bool:
    today = date.today()
    return today <= d <= today + timedelta(days=days_ahead)


# ---------------------------------------------------------------------------
# Source 1 — Belgrade Arena
# Confirmed DOM structure (live, April 2026):
#   .grandconference-event-grid
#     .portfolio-classic-content-wrapper
#       .portfolio-classic-grid-wrapper  (one per event)
#         .portfolio-classic-content
#           h3.portfolio-classic_title a  ← event name
#           .portfolio-classic-meta
#             .portfolio-classic-meta-data (first = date like "април 26")
# ---------------------------------------------------------------------------

async def _scrape_arena() -> list[dict]:
    url = "https://arenabeograd.com/listadogadjaja/"
    events = []

    try:
        async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("Arena scrape failed: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    grid = soup.find(class_="grandconference-event-grid")
    if not grid:
        logger.warning("Arena: grandconference-event-grid not found")
        return []

    for card in grid.select(".portfolio-classic-grid-wrapper"):
        try:
            name_el = card.select_one("h3.portfolio-classic_title a")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)

            # Date is in the first .portfolio-classic-meta-data span
            date_el = card.select_one(".portfolio-classic-meta-data")
            if not date_el:
                continue
            date_text = date_el.get_text(strip=True)
            event_date = _parse_date_text(date_text)
            if not event_date or not _is_future(event_date):
                continue

            events.append({
                "event_name":          name[:200],
                "event_type":          "concert",
                "venue_name":          "Belgrade Arena",
                "venue_lat":           44.8065,
                "venue_lng":           20.4084,
                "event_date":          event_date,
                "event_time":          None,
                "expected_attendance": 20000,
            })
        except Exception as exc:
            logger.debug("Arena: error parsing card: %s", exc)

    logger.info("Arena: scraped %d events", len(events))
    return events


# ---------------------------------------------------------------------------
# Source 2 — FK Crvena zvezda
# Site is JS-rendered (React). httpx gets only a skeleton page.
# Returns empty list — use POST /events to add matches manually.
# ---------------------------------------------------------------------------

async def _scrape_crvena_zvezda() -> list[dict]:
    logger.info("Crvena zvezda: JS-rendered site — skipping automatic scrape (use POST /events)")
    return []


# ---------------------------------------------------------------------------
# Source 3 — FK Partizan
# Site is JS-rendered (shows "Učitavanje utakmice…").
# Returns empty list — use POST /events to add matches manually.
# ---------------------------------------------------------------------------

async def _scrape_partizan() -> list[dict]:
    logger.info("Partizan: JS-rendered site — skipping automatic scrape (use POST /events)")
    return []


# ---------------------------------------------------------------------------
# Source 4 — Hram Svetog Save (hardcoded Orthodox calendar)
# ---------------------------------------------------------------------------

def _orthodox_easter(year: int) -> date:
    """Return Gregorian date of Orthodox Easter for the given year."""
    a = year % 4
    b = year % 7
    c = year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    f = d + e + 114
    month = f // 31
    day = (f % 31) + 1
    julian = date(year, month, day)
    return julian + timedelta(days=13)  # Julian → Gregorian (21st century)


def _get_hram_events() -> list[dict]:
    """
    Generate major Orthodox calendar events at Hram Svetog Save
    for the current year (and next if we're near year-end).
    """
    today = date.today()
    years = [today.year] if today.month < 11 else [today.year, today.year + 1]
    events = []

    for year in years:
        easter = _orthodox_easter(year)
        good_friday  = easter - timedelta(days=2)
        holy_saturday = easter - timedelta(days=1)

        fixed = [
            (date(year, 1, 7),   "Bozic (Orthodox Christmas)",        50000),
            (date(year, 1, 19),  "Bogojavljenje (Epiphany)",           10000),
            (date(year, 5, 21),  "Sveti Konstantin i Jelena",           5000),
            (date(year, 6, 28),  "Vidovdan (Battle of Kosovo Day)",    20000),
            (date(year, 8, 28),  "Velika Gospojina (Dormition)",        8000),
            (date(year, 11, 21), "Arandjelovdan (St. Michael's Day)",   5000),
            (date(year, 12, 19), "Sveti Nikola (St. Nicholas Day)",     5000),
            (good_friday,        "Veliki Petak (Good Friday)",         15000),
            (holy_saturday,      "Velika Subota (Holy Saturday)",      10000),
            (easter,             "Vaskrs (Orthodox Easter)",           50000),
        ]

        for event_date, name, attendance in fixed:
            if _is_future(event_date, days_ahead=365):
                events.append({
                    "event_name":          name,
                    "event_type":          "religious",
                    "venue_name":          "Hram Svetog Save",
                    "venue_lat":           44.7990,
                    "venue_lng":           20.4681,
                    "event_date":          event_date,
                    "event_time":          None,
                    "expected_attendance": attendance,
                })

    logger.info("Hram: generated %d Orthodox calendar events", len(events))
    return events


# ---------------------------------------------------------------------------
# Source 5 — Sava Center
# ---------------------------------------------------------------------------

async def _scrape_sava_center() -> list[dict]:
    urls = [
        "https://www.savacenter.net/dogadjaji/",
        "https://www.savacenter.net/events/",
        "https://www.savacenter.net/",
    ]
    html = None
    for url in urls:
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(url)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    html = resp.text
                    break
        except Exception as exc:
            logger.debug("Sava Center: failed %s: %s", url, exc)

    if not html:
        logger.warning("Sava Center: could not fetch events page")
        return []

    soup = BeautifulSoup(html, "lxml")
    events = []

    candidates = (
        soup.select("article.event") or
        soup.select(".event-item") or
        soup.select(".dogadjaj") or
        soup.select("article") or
        soup.select(".post")
    )

    for card in candidates:
        try:
            name_el = card.find(["h2", "h3", "h4", "strong", "a"])
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or len(name) < 3:
                continue

            date_found = None
            for el in card.find_all(["time", "span", "p", "div"]):
                raw = el.get("datetime", "") or el.get_text(strip=True)
                d = _parse_date_text(raw)
                if d and _is_future(d):
                    date_found = d
                    break
            if not date_found:
                continue

            events.append({
                "event_name":          name[:200],
                "event_type":          "other",
                "venue_name":          "Sava Centar",
                "venue_lat":           44.8034,
                "venue_lng":           20.4247,
                "event_date":          date_found,
                "event_time":          None,
                "expected_attendance": 2000,
            })
        except Exception as exc:
            logger.debug("Sava Center: error parsing card: %s", exc)

    logger.info("Sava Center: scraped %d events", len(events))
    return events


# ---------------------------------------------------------------------------
# Source 6 — Narodno pozoriste
# Confirmed DOM structure (live, April 2026):
#   .repertoarwide-entry  (one per show, ~55 per page)
#     .repertoarwide-entry-date  ← Cyrillic "Сре1апр" (weekday+day+month abbrev)
#     The show title is in the entry text after the date block.
# ---------------------------------------------------------------------------

# Cyrillic day abbreviations (weekdays) to strip from date strings
_CYR_WEEKDAYS = {"Пон", "Уто", "Сре", "Чет", "Пет", "Суб", "Нед"}

def _parse_np_date(date_text: str) -> Optional[date]:
    """
    Parse Narodno pozoriste date format: 'Сре1апр' → Wednesday 1 April.
    Strips Cyrillic weekday prefix, then parses day+month.
    """
    # Strip any Cyrillic weekday abbreviation (3 chars) at the start
    text = date_text.strip()
    for wd in _CYR_WEEKDAYS:
        if text.startswith(wd):
            text = text[len(wd):]
            break

    # Now text looks like "1апр" or "26 апр" or "1 апр 2025"
    # Try the general parser with year-inference for no-year cases
    return _parse_date_text(text)


async def _scrape_narodno_pozoriste() -> list[dict]:
    url = "https://www.narodnopozoriste.rs/repertoar/"
    events = []

    try:
        async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("Narodno pozoriste: fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    entries = soup.select(".repertoarwide-entry")

    if not entries:
        logger.warning("Narodno pozoriste: no .repertoarwide-entry found — site may have changed")
        return []

    for entry in entries:
        try:
            date_el = entry.select_one(".repertoarwide-entry-date")
            if not date_el:
                continue
            date_text = date_el.get_text(strip=True)
            event_date = _parse_np_date(date_text)
            if not event_date or not _is_future(event_date):
                continue

            # Remove the date block from the entry to isolate the show name
            date_el_copy = date_el.extract()
            full_text = entry.get_text(" ", strip=True)

            # Show name is typically the first substantial chunk before genre/venue info
            # Split on common separators
            parts = re.split(r"·|–|-\s|\d{2}:\d{2}", full_text)
            name = parts[0].strip() if parts else full_text.strip()
            if not name or len(name) < 3:
                continue

            events.append({
                "event_name":          name[:200],
                "event_type":          "theatre",
                "venue_name":          "Narodno pozoriste",
                "venue_lat":           44.8181,
                "venue_lng":           20.4575,
                "event_date":          event_date,
                "event_time":          None,
                "expected_attendance": 500,
            })
        except Exception as exc:
            logger.debug("Narodno pozoriste: error parsing entry: %s", exc)

    logger.info("Narodno pozoriste: scraped %d events", len(events))
    return events


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def scrape_all_events() -> list[dict]:
    """
    Run all event scrapers concurrently.
    Individual failures are logged and ignored — other sources still run.
    """
    logger.info("Starting daily event scrape from all sources ...")

    hram_events = _get_hram_events()

    results = await asyncio.gather(
        _scrape_arena(),
        _scrape_crvena_zvezda(),
        _scrape_partizan(),
        _scrape_sava_center(),
        _scrape_narodno_pozoriste(),
        return_exceptions=True,
    )

    all_events = list(hram_events)
    source_names = ["Arena", "Crvena zvezda", "Partizan", "Sava Center", "Narodno pozoriste"]

    for name, result in zip(source_names, results):
        if isinstance(result, Exception):
            logger.error("Event source '%s' raised: %s", name, result)
        elif isinstance(result, list):
            all_events.extend(result)

    logger.info("Event scrape complete: %d total events collected", len(all_events))
    return all_events


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

_INSERT_EVENT_SQL = """
INSERT INTO city_events
    (event_name, event_type, venue_name, venue_lat, venue_lng,
     event_date, event_time, expected_attendance, scraped_at)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM city_events
    WHERE event_name = $1
      AND event_date = $6
      AND COALESCE(venue_name, '') = COALESCE($3, '')
)
"""


async def save_events(pool: asyncpg.Pool, events: list[dict]) -> int:
    """
    Insert events that don't already exist (matched on name + date + venue).
    Returns the count of newly inserted rows.
    """
    inserted = 0
    async with pool.acquire() as conn:
        for event in events:
            try:
                result = await conn.execute(
                    _INSERT_EVENT_SQL,
                    event["event_name"],
                    event.get("event_type"),
                    event.get("venue_name"),
                    event.get("venue_lat"),
                    event.get("venue_lng"),
                    event["event_date"],
                    event.get("event_time"),
                    event.get("expected_attendance"),
                )
                if result.endswith("1"):
                    inserted += 1
            except Exception as exc:
                logger.error("Failed to insert event '%s': %s", event.get("event_name"), exc)

    logger.info("save_events: %d/%d new events inserted", inserted, len(events))
    return inserted


# ---------------------------------------------------------------------------
# Standalone test  (python event_scraper.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import io

    # Force UTF-8 on Windows console
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    async def _test():
        events = await scrape_all_events()
        print(f"\n{'Date':<12} {'Type':<12} {'Venue':<35} {'Attend':>8}  Name")
        print("-" * 100)
        for e in sorted(events, key=lambda x: x["event_date"]):
            print(
                f"{e['event_date'].isoformat():<12} "
                f"{(e['event_type'] or ''):<12} "
                f"{(e['venue_name'] or ''):<35} "
                f"{(e['expected_attendance'] or 0):>8}  "
                f"{e['event_name']}"
            )
        print(f"\nTotal: {len(events)} events")

    asyncio.run(_test())
