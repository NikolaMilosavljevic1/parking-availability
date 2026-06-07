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

For the full implementation roadmap, see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

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

The app must reach your machine's API. Edit `mobile/config.ts`:


| Environment                  | `API_URL`                   |
| ---------------------------- | --------------------------- |
| iOS Simulator / same machine | `http://localhost:8000`     |
| Android Emulator             | `http://10.0.2.2:8000`      |
| Physical phone (same WiFi)   | `http://<your-lan-ip>:8000` |


Find your LAN IP:

```bash
ipconfig          # Windows — look for IPv4 Address
ifconfig          # macOS / Linux
```

Set `WS_URL` to the same host with `ws://` and path `/ws/live`.

> **Note:** Env-based config (`EXPO_PUBLIC_API_URL`) is planned in Phase B.

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
├── docker-compose.yml
└── IMPLEMENTATION_PLAN.md
```

---

## API endpoints


| Method | Path                                        | Description                        |
| ------ | ------------------------------------------- | ---------------------------------- |
| GET    | `/locations`                                | All locations with live free spots |
| GET    | `/locations/{id}`                           | Single location                    |
| GET    | `/locations/{id}/history?period=24h|7d|30d` | Hourly occupancy history           |
| GET    | `/locations/{id}/predict`                   | ML prediction (503 until trained)  |
| GET    | `/events?days=7`                            | Upcoming city events               |
| POST   | `/events`                                   | Manually add an event              |
| WS     | `/ws/live`                                  | Live availability updates          |


Full interactive docs: [http://localhost:8000/docs](http://localhost:8000/docs)

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

Current sources: Belgrade Arena, Hram Svetog Save (hardcoded calendar), Sava Center, Narodno pozorište, MTS Dvorana. FK Crvena zvezda and FK Partizan are stubbed (Phase C).

---

## Data retention

At 60-second intervals across 27 locations, the database grows by roughly **1.4 million rows per month**. A retention policy (raw snapshots for 90 days, then hourly rollups) is planned for production — see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) Phase F.

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