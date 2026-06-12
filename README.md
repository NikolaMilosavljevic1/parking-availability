# Belgrade Parking

Real-time parking availability for Belgrade, Serbia. Scrapes live data from [JKP Parking Servis](https://www.parking-servis.co.rs/lat/garaze-i-parkiralista), stores history in PostgreSQL, serves it via FastAPI, and displays it in a React Native mobile app.

**Disclaimer:** This project is not affiliated with or endorsed by JKP Parking Servis.

---

## What it does

- Tracks **27 locations** (9 garages + 18 open parking lots) across Belgrade
- Scrapes availability every **60 seconds** (Playwright + APScheduler)
- Enriches snapshots with **weather** (Open-Meteo) and **city events** (concerts, sports, religious gatherings)
- Serves live data via **REST** and **WebSocket** (Redis cache)
- Mobile app shows free spots, occupancy bars, GPS distance, and directions
- Streamlit dashboard for analytics (optional)

---

## Prerequisites


| Tool                                                              | Version | Purpose                                       |
| ----------------------------------------------------------------- | ------- | --------------------------------------------- |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Latest  | Backend stack (Postgres, Redis, API, scraper) |
| [Node.js](https://nodejs.org/)                                    | 18+     | Mobile app                                    |
| [Expo Go](https://expo.dev/go)                                    | —       | Run app on a physical phone (optional)        |


---

## Quick start

### 1. Configure environment

```bash
cp .env.example .env          # Linux / macOS
copy .env.example .env        # Windows
```

Edit `.env` and set a strong `POSTGRES_PASSWORD`.

### 2. Start the backend

```bash
docker compose up -d
```

Wait until all services are healthy (~30 seconds on first run; Postgres applies `db/init.sql` automatically).


| Service       | URL                                                          |
| ------------- | ------------------------------------------------------------ |
| API (Swagger) | [http://localhost:8000/docs](http://localhost:8000/docs)     |
| API health    | [http://localhost:8000/health](http://localhost:8000/health) |
| Dashboard     | [http://localhost:8501](http://localhost:8501)               |


Check scraper logs:

```bash
docker compose logs -f scraper
```

You should see parking scrape cycles every 60 seconds.

### 3. Run the mobile app

```bash
cd mobile
npm install
npx expo start
```

Scan the QR code with Expo Go (Android) or the Camera app (iOS).

---

## Mobile API URL

The app reads the API host from environment variables. Copy the template and set your machine's address:

```bash
cd mobile
cp .env.example .env    # Linux / macOS
copy .env.example .env  # Windows
```

Edit `mobile/.env`:


| Environment                  | `EXPO_PUBLIC_API_URL`       |
| ---------------------------- | --------------------------- |
| iOS Simulator / same machine | `http://localhost:8000`     |
| Android Emulator             | `http://10.0.2.2:8000`      |
| Physical phone (same WiFi)   | `http://<your-lan-ip>:8000` |


Set `EXPO_PUBLIC_WS_URL` to the same host with `ws://` and path `/ws/live`.

Find your LAN IP:

```bash
ipconfig          # Windows — look for IPv4 Address
ifconfig          # macOS / Linux
```

Restart Expo after changing `.env`:

```bash
npx expo start --clear
```

---

## Deploy APK (use off home WiFi)

To install a standalone Android app that works on mobile data (not Expo Go):

1. Run backend: `docker compose up -d`
2. Expose API: `.\scripts\start-tunnel.ps1` (free Cloudflare Tunnel)
3. Set EAS env vars with your tunnel URL
4. Build APK: `cd mobile && eas build --platform android --profile preview`
5. Download APK from [expo.dev](https://expo.dev) and install on your phone

Full step-by-step: **[DEPLOY.md](DEPLOY.md)**

---

## Architecture

```
parking-servis.co.rs ──► scraper/ ──► PostgreSQL (history)
                          │              Redis (live cache)
                          │
                     Open-Meteo         FastAPI api/ :8000
                     Event sites    ◄── mobile/ (REST + WebSocket)
                                          dashboard/ (Streamlit)
```

**Data flow (every 60s):**

1. Scraper fetches parking counts + weather
2. Writes snapshot to PostgreSQL (with ML features: time, weather, events)
3. Updates Redis live keys and publishes to `parking:updates`
4. API WebSocket pushes updates to connected mobile clients

---

## Project structure

```
parking-project/
├── scraper/       Python scraper (parking, weather, events)
├── api/           FastAPI REST + WebSocket
├── mobile/        React Native + Expo + TypeScript
├── dashboard/     Streamlit analytics
├── db/init.sql    Schema + 27 location seeds
└── docker-compose.yml
```

---

## API endpoints


| Method | Path                                        | Description                        |
| ------ | ------------------------------------------- | ---------------------------------- |
| GET    | `/locations`                                | All locations with live free spots |
| GET    | `/locations/{id}`                           | Single location + elevated-demand hint |
| GET    | `/locations/{id}/history?period=24h|7d|30d` | Hourly occupancy history           |
| GET    | `/locations/{id}/predict`                   | ML prediction (503 until trained)  |
| GET    | `/events?days=7`                            | Upcoming city events               |
| POST   | `/events`                                   | Manually add an event              |
| WS     | `/ws/live`                                  | Live availability updates          |


Full interactive docs: [http://localhost:8000/docs](http://localhost:8000/docs)

`GET /locations/{id}` includes rule-based demand fields when a large event is within **2 km** and the current time falls in the pre/post-event window: `elevated_demand`, `demand_event_type`, `demand_venue_name`, and `demand_event_name`. The mobile app shows a short hint (event type + venue) without listing all nearby events.

---

## Adding a new event source

1. Open `scraper/event_scraper.py`
2. Add an async scraper function that returns a list of dicts:

```python
{
    "event_name": "Concert XYZ",
    "event_type": "concert",       # concert | sports | theatre | religious | festival | other
    "venue_name": "Belgrade Arena",
    "venue_lat": 44.8065,
    "venue_lng": 20.4084,
    "event_date": date(2026, 6, 15),
    "event_time": time(20, 0),     # optional
    "expected_attendance": 20000,
}
```

1. Register it in `scrape_all_events()` via `asyncio.gather`
2. If the site is JS-rendered, use Playwright (see `scraper/parking_scraper.py`)
3. **Fallback:** `POST /events` on the API for manual entry

Current sources: Belgrade Arena, Hram Svetog Save (hardcoded calendar), Sava Center, Narodno pozorište, MTS Dvorana (tickets.rs), FK Crvena zvezda and FK Partizan (Playwright — home fixtures only). The scraper stores events up to 365 days ahead; the API serves 7 days to mobile. Football scrapers need Playwright/Chromium (see `scraper/Dockerfile`); if a club site changes layout, use `POST /events` as fallback.

The scraper container runs `save_events()` on startup and again daily at 03:00 UTC. Scraping alone does not update the database — events must be persisted via `save_events()`.

### Refreshing events manually

```bash
docker compose exec scraper python -c "
import asyncio
from db import create_pool
from event_scraper import scrape_all_events, save_events

async def run():
    pool = await create_pool()
    events = await scrape_all_events()
    inserted = await save_events(pool, events)
    print(f'Scraped {len(events)}, inserted {inserted} new')
    await pool.close()

asyncio.run(run())
"
```

Or run the standalone scraper with `--save` inside the container:

```bash
docker compose exec scraper python event_scraper.py --save
```

---

## Data retention

At 60-second intervals across 27 locations, the database grows by roughly **1.4 million rows per month**. A retention policy (raw snapshots for 90 days, then hourly rollups) is planned for production.

---

## Development tips

**Rebuild a single service after dependency changes:**

```bash
docker compose build scraper && docker compose up -d scraper
```

**Run parking scraper standalone (outside Docker):**

```bash
cd scraper
pip install -r requirements.txt
playwright install chromium
python parking_scraper.py
```

**Reset database (destroys all history):**

```bash
docker compose down -v
docker compose up -d
```

---

## Privacy

- No user accounts in v1
- GPS is used on-device only to show distance to parking locations
- No personal data is stored

---

## License

Portfolio / educational project. Parking data belongs to JKP Parking Servis.
