"""
event_scraper.py — Daily city event scraping for Belgrade.

Sources
-------
    1. Belgrade Arena          https://arenabeograd.com/listadogadjaja/
    2. FK Crvena zvezda        Playwright → crvenazvezdafk.com (home games at Marakana)
    3. FK Partizan             Playwright → partizan.rs/utakmice (JS-rendered)
    4. Hram Svetog Save        hardcoded Orthodox calendar dates
    5. Sava Center             https://www.savacenter.net/
    6. Narodno pozorište       https://www.narodnopozoriste.rs/repertoar/
    7. MTS Dvorana             https://tickets.rs/venue/mts_dvorana_21

Storage vs API
--------------
    Scrapers keep events up to MAX_STORAGE_DAYS ahead in the DB.
    The mobile API serves only the next 7 days (api/db.py get_events).

Football clubs
--------------
    Crvena zvezda and Partizan sites are JS-rendered; httpx alone returns
    skeleton HTML. Playwright is used (see parking_scraper.py). If a club
    site changes layout, scrapers return [] — use POST /events as fallback.

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

# DB may store events further out; API filters to 7 days for mobile.
MAX_STORAGE_DAYS = 365

_MARAKANA = (44.7836, 20.4722)
_PARTIZAN_STADIUM = (44.7863, 20.4480)


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

    # tickets.rs: "23. oktobar 2026 20:00"
    month_pattern = "|".join(sorted(_SR_MONTHS.keys(), key=len, reverse=True))
    m = re.search(
        rf"(\d{{1,2}})\.\s*({month_pattern})\s+(\d{{4}})",
        text,
        re.IGNORECASE,
    )
    if m:
        month_str = m.group(2).lower().rstrip(".")
        month_num = _SR_MONTHS.get(month_str)
        if month_num:
            try:
                return date(int(m.group(3)), month_num, int(m.group(1)))
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
        month_str = m.group(2).lower().rstrip(".")
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


def _is_upcoming(d: date, days_ahead: int = MAX_STORAGE_DAYS) -> bool:
    """True if event is today or later, within DB storage horizon."""
    today = date.today()
    return today <= d <= today + timedelta(days=days_ahead)


# Back-compat alias used in a few call sites
_is_future = _is_upcoming


_SPORTS_RE = re.compile(
    r"\b(košark|kosark|basketball|aba\s*liga|euroleague|euro\s*cup|"
    r"fudbal|football|utakmic|meč|mec|derbi|superliga|šampion|sampion|"
    r"liga|odbojk|rukomet|hokej|tenis|tennis|mma|boks|boxing|atletik|"
    r"vs\.?|protiv)\b",
    re.IGNORECASE,
)
_THEATRE_RE = re.compile(
    r"\b(opera|balet|ballet|pozorišt|pozorist|predstav|theatre|theater|"
    r"drama|musical|musikal)\b",
    re.IGNORECASE,
)
_CONCERT_RE = re.compile(
    r"\b(koncert|concert|tour|live|gig|recital|soprano|belcanto)\b",
    re.IGNORECASE,
)
_FESTIVAL_RE = re.compile(
    r"\b(festival|fest)\b",
    re.IGNORECASE,
)
_RELIGIOUS_RE = re.compile(
    r"\b(božić|bozic|vaskrs|uskrs|slava|liturgij|crkva|hram|orthodox|"
    r"epiphany|vidovdan|gospojin)\b",
    re.IGNORECASE,
)


def _infer_event_type(name: str, default: str = "other") -> str:
    """Guess event_type from title keywords; falls back to default."""
    if _SPORTS_RE.search(name):
        return "sports"
    if _RELIGIOUS_RE.search(name):
        return "religious"
    if _THEATRE_RE.search(name):
        return "theatre"
    if _FESTIVAL_RE.search(name):
        return "festival"
    if _CONCERT_RE.search(name):
        return "concert"
    return default


async def _fetch_html_playwright(url: str, timeout_ms: int = 30_000) -> Optional[str]:
    """Fetch JS-rendered page HTML via Playwright (Chromium)."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed — cannot fetch %s", url)
        return None

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                context = await browser.new_context(
                    user_agent=_HEADERS["User-Agent"],
                    locale="sr-Latn",
                )
                page = await context.new_page()
                await page.route(
                    "**/*",
                    lambda route: route.abort()
                    if route.request.resource_type in {"image", "media", "font"}
                    else route.continue_(),
                )
                await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
                await page.wait_for_timeout(2_000)
                return await page.content()
            finally:
                await browser.close()
    except Exception as exc:
        logger.warning("Playwright fetch failed for %s: %s", url, exc)
        return None


_FK_DATE_RE = re.compile(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})")


def _parse_fk_dot_date(text: str) -> Optional[date]:
    """Parse first DD.MM.YYYY in text (negative lookbehind avoids 22.5 → 2.5)."""
    m = _FK_DATE_RE.search(text)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _fk_date_chunks(text: str) -> list[str]:
    """Split text into blocks starting at each standalone DD.MM.YYYY date."""
    matches = list(_FK_DATE_RE.finditer(text))
    chunks: list[str] = []
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        chunks.append(text[m.start():end])
    return chunks


def _extract_fk_opponent(lines: list[str], club_upper: str) -> Optional[str]:
    skip_words = {
        club_upper, "KUPI KARTE", "KUPI KARTE", "MEČ U TOKU", "MEC U TOKU",
        "RASPORED", "REZULTATI", "SUPERLIGA", "KUP", "PRIJATELJSKA UTAKMICA",
        "KUPI KARTE", "LIVE", "FT", "HT",
    }
    score_token = re.compile(r"^[\d\-:.'()\s]+$")

    for i, ln in enumerate(lines):
        if club_upper not in ln.upper():
            continue
        for j in range(i + 1, min(i + 12, len(lines))):
            cand = lines[j]
            cand_up = cand.upper()
            if (
                len(cand) >= 3
                and club_upper not in cand_up
                and not score_token.fullmatch(cand)
                and cand_up not in skip_words
                and "STADION" not in cand_up
                and "KUPI" not in cand_up
            ):
                return cand
    return None


def _parse_home_football_fixtures(
    html: str,
    *,
    club_name: str,
    home_stadium_markers: tuple[str, ...],
    venue_name: str,
    venue_lat: float,
    venue_lng: float,
    attendance: int = 15000,
) -> list[dict]:
    """
    Parse home fixtures from FK club schedule HTML (after Playwright render).
    Looks for date + home stadium marker + club name in the same text block.
    """
    events: list[dict] = []
    seen: set[tuple[date, str]] = set()
    text = BeautifulSoup(html, "lxml").get_text("\n", strip=True)
    club_upper = club_name.upper()

    for chunk in _fk_date_chunks(text):
        upper = chunk.upper()
        if club_upper not in upper:
            continue
        if not any(marker in upper for marker in home_stadium_markers):
            continue

        event_date = _parse_fk_dot_date(chunk)
        if not event_date or not _is_upcoming(event_date):
            continue

        lines = [ln.strip() for ln in chunk.split("\n") if ln.strip()]
        opponent = _extract_fk_opponent(lines, club_upper)
        if not opponent:
            continue

        key = (event_date, opponent.lower())
        if key in seen:
            continue
        seen.add(key)

        title = f"{club_name} vs {opponent}"
        events.append({
            "event_name":          title[:200],
            "event_type":          _infer_event_type(title, "sports"),
            "venue_name":          venue_name,
            "venue_lat":           venue_lat,
            "venue_lng":           venue_lng,
            "event_date":          event_date,
            "event_time":          None,
            "expected_attendance": attendance,
        })

    return events


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
            if not event_date or not _is_upcoming(event_date):
                continue

            events.append({
                "event_name":          name[:200],
                "event_type":          _infer_event_type(name, "concert"),
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
# Source 2 — FK Crvena zvezda (Playwright — site is JS-rendered)
# Schedule: https://www.crvenazvezdafk.com/sr-latn/raspored-rezultati
# Only home games at Rajko Mitic (Marakana) are stored.
# ---------------------------------------------------------------------------

async def _scrape_crvena_zvezda() -> list[dict]:
    url = "https://www.crvenazvezdafk.com/sr-latn/raspored-rezultati"
    html = await _fetch_html_playwright(url)
    if not html:
        logger.info(
            "Crvena zvezda: Playwright unavailable or fetch failed — "
            "use POST /events for manual entry",
        )
        return []

    events = _parse_home_football_fixtures(
        html,
        club_name="Crvena zvezda",
        home_stadium_markers=("STADION RAJKO MITI", "RAJKO MITIĆ", "RAJKO MITIC"),
        venue_name="Stadion Rajko Mitic (Marakana)",
        venue_lat=_MARAKANA[0],
        venue_lng=_MARAKANA[1],
        attendance=25000,
    )
    logger.info("Crvena zvezda: scraped %d home fixtures", len(events))
    return events


# ---------------------------------------------------------------------------
# Source 3 — FK Partizan (Playwright — partizan.rs/utakmice is JS-rendered)
# Only home games at Stadion Partizana are stored.
# ---------------------------------------------------------------------------

async def _scrape_partizan() -> list[dict]:
    url = "https://partizan.rs/utakmice"
    html = await _fetch_html_playwright(url)
    if not html:
        logger.info(
            "Partizan: Playwright unavailable or fetch failed — "
            "use POST /events for manual entry",
        )
        return []

    events = _parse_home_football_fixtures(
        html,
        club_name="Partizan",
        home_stadium_markers=(
            "STADION PARTIZANA",
            "STADION FK PARTIZANA",
            "PARTIZAN STADIUM",
            "STADION PARTIZAN",
        ),
        venue_name="Stadion Partizana",
        venue_lat=_PARTIZAN_STADIUM[0],
        venue_lng=_PARTIZAN_STADIUM[1],
        attendance=18000,
    )
    logger.info("Partizan: scraped %d home fixtures", len(events))
    return events


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
            if _is_upcoming(event_date):
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
                if d and _is_upcoming(d):
                    date_found = d
                    break
            if not date_found:
                continue

            events.append({
                "event_name":          name[:200],
                "event_type":          _infer_event_type(name, "other"),
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
            if not event_date or not _is_upcoming(event_date):
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
# Source 7 — MTS Dvorana (via tickets.rs mirror — mtsdvorana.rs blocks bots)
# tickets.rs: title in <h5>, date as "DD. mesec YYYY HH:MM" in parent block
# ---------------------------------------------------------------------------

_MTS_VENUE = (44.8133, 20.4628)
_MTS_SKIP_TITLES = {
    "top izbor", "ostala mesta u blizini", "mts dvorana",
    "akademsko pozorište krsmanac", "lisabon bar", "pop up galerija",
}


def _parse_mts_from_h5(h5, seen: set[tuple[str, date]]) -> Optional[dict]:
    import datetime as dt_mod

    name = h5.get_text(strip=True)
    if not name or len(name) < 3:
        return None
    if name.lower() in _MTS_SKIP_TITLES:
        return None

    block = h5.parent
    date_found = None
    time_found = None
    raw_block = ""

    for _ in range(4):
        if block is None:
            break
        raw_block = block.get_text(" ", strip=True)
        date_found = _parse_date_text(raw_block)
        if date_found:
            t_match = re.search(r"\b(\d{1,2}):(\d{2})\b", raw_block)
            if t_match:
                try:
                    time_found = dt_mod.time(
                        int(t_match.group(1)), int(t_match.group(2)),
                    )
                except ValueError:
                    pass
            break
        block = block.parent

    if not date_found or not _is_upcoming(date_found):
        return None

    key = (name.lower(), date_found)
    if key in seen:
        return None
    seen.add(key)

    return {
        "event_name":          name[:200],
        "event_type":          _infer_event_type(name, "concert"),
        "venue_name":          "MTS Dvorana",
        "venue_lat":           _MTS_VENUE[0],
        "venue_lng":           _MTS_VENUE[1],
        "event_date":          date_found,
        "event_time":          time_found,
        "expected_attendance": 2500,
    }


async def _scrape_mts_dvorana() -> list[dict]:
    url = "https://tickets.rs/venue/mts_dvorana_21"
    events: list[dict] = []
    seen: set[tuple[str, date]] = set()

    try:
        async with httpx.AsyncClient(
            headers=_HEADERS, timeout=_TIMEOUT, follow_redirects=True,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("MTS Dvorana (tickets.rs): fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Primary path: h5 titles (tickets.rs listing layout)
    for h5 in soup.find_all("h5"):
        try:
            ev = _parse_mts_from_h5(h5, seen)
            if ev:
                events.append(ev)
        except Exception as exc:
            logger.debug("MTS Dvorana: error parsing h5: %s", exc)

    logger.info("MTS Dvorana: scraped %d events", len(events))
    return events


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def scrape_all_events() -> list[dict]:
    """
    Run all event scrapers concurrently.
    Individual failures are logged and ignored — other sources still run.
    """
    events, _counts = await scrape_all_events_with_counts()
    return events


async def scrape_all_events_with_counts() -> tuple[list[dict], dict[str, int]]:
    """
    Run all event scrapers concurrently.
    Returns (all_events, per_source_counts).
    """
    logger.info("Starting daily event scrape from all sources ...")

    hram_events = _get_hram_events()
    counts: dict[str, int] = {"Hram": len(hram_events)}

    results = await asyncio.gather(
        _scrape_arena(),
        _scrape_crvena_zvezda(),
        _scrape_partizan(),
        _scrape_sava_center(),
        _scrape_narodno_pozoriste(),
        _scrape_mts_dvorana(),
        return_exceptions=True,
    )

    source_names = [
        "Arena", "Crvena zvezda", "Partizan",
        "Sava Center", "Narodno pozoriste", "MTS Dvorana",
    ]

    all_events = list(hram_events)
    for name, result in zip(source_names, results):
        if isinstance(result, Exception):
            logger.error("Event source '%s' raised: %s", name, result)
            counts[name] = 0
        elif isinstance(result, list):
            counts[name] = len(result)
            all_events.extend(result)

    logger.info(
        "Event scrape complete: %d total (%s)",
        len(all_events),
        ", ".join(f"{k}={v}" for k, v in counts.items()),
    )
    return all_events, counts


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

_INSERT_EVENT_SQL = """
INSERT INTO city_events
    (event_name, event_type, venue_name, venue_lat, venue_lng,
     event_date, event_time, expected_attendance, scraped_at)
SELECT $1::varchar, $2::varchar, $3::varchar, $4, $5, $6, $7, $8, NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM city_events
    WHERE event_name = $1::varchar
      AND event_date = $6
      AND COALESCE(venue_name, '') = COALESCE($3::varchar, '')
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
    import argparse
    import os
    import sys
    import io

    from dotenv import load_dotenv

    # Force UTF-8 on Windows console
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    load_dotenv()

    parser = argparse.ArgumentParser(description="Scrape Belgrade city events")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Persist scraped events to PostgreSQL (requires DATABASE_URL)",
    )
    args = parser.parse_args()
    should_save = args.save or bool(os.environ.get("DATABASE_URL"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    async def _test():
        events, counts = await scrape_all_events_with_counts()
        print("\nCounts per source:")
        for source, n in counts.items():
            print(f"  {source:<22} {n:>4}")
        print(f"  {'TOTAL':<22} {len(events):>4}")

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

        if should_save:
            from db import create_pool

            pool = await create_pool()
            try:
                inserted = await save_events(pool, events)
                print(f"\nSaved to DB: {inserted}/{len(events)} new events inserted")
            finally:
                await pool.close()
        elif args.save:
            print("\n--save requested but DATABASE_URL is not set — skipped DB write")

    asyncio.run(_test())
