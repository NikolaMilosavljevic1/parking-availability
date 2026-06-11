-- =============================================================================
-- Belgrade Parking — PostgreSQL schema
-- Runs once on first container start via docker-entrypoint-initdb.d/
-- Seed data verified against live parking-servis.co.rs page.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Static location info: garages AND open parking lots
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parking_locations (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR NOT NULL,
    address         VARCHAR,
    location_type   VARCHAR NOT NULL
                    CHECK (location_type IN ('garage', 'parking_lot')),
    total_spots     INTEGER,           -- NULL until confirmed from official source
    latitude        FLOAT,             -- from Google Maps href on the site
    longitude       FLOAT,
    neighborhood    VARCHAR,

    -- Pre-computed distances to major event venues (km, static)
    dist_to_arena_km              FLOAT,
    dist_to_hram_km               FLOAT,
    dist_to_marakana_km           FLOAT,
    dist_to_partizan_km           FLOAT,
    dist_to_narodno_pozoriste_km  FLOAT,
    dist_to_sava_centar_km        FLOAT,

    -- JKP cenovnik (slow-changing; see db/migrations/003_add_pricing.sql)
    price_first_hour_rsd          INTEGER,
    price_extra_hour_rsd          INTEGER,
    price_daily_rsd               INTEGER,
    hours_note                    VARCHAR,
    pricing_note                  VARCHAR
);

-- ---------------------------------------------------------------------------
-- Every 60-second snapshot
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parking_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    location_id     VARCHAR REFERENCES parking_locations(id) ON DELETE CASCADE,
    free_spots      INTEGER,
    total_spots     INTEGER,           -- copied from parking_locations at insert time
    occupancy_pct   FLOAT,             -- NULL when total_spots is unknown
    scraped_at      TIMESTAMPTZ NOT NULL,

    hour_of_day     SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_public_holiday BOOLEAN NOT NULL DEFAULT FALSE,

    temperature_c       FLOAT,
    precipitation_mm    FLOAT,
    weather_code        SMALLINT,
    is_raining          BOOLEAN,

    hours_to_next_event             FLOAT,
    nearest_event_venue             VARCHAR,
    nearest_event_type              VARCHAR,
    nearest_event_attendance_est    INTEGER,
    nearest_event_distance_km       FLOAT
);

CREATE INDEX IF NOT EXISTS idx_snapshots_location_time
    ON parking_snapshots (location_id, scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_scraped_at
    ON parking_snapshots (scraped_at DESC);

-- ---------------------------------------------------------------------------
-- City events scraped from venues daily
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS city_events (
    id                  SERIAL PRIMARY KEY,
    event_name          VARCHAR NOT NULL,
    event_type          VARCHAR,
    venue_name          VARCHAR,
    venue_lat           FLOAT,
    venue_lng           FLOAT,
    event_date          DATE NOT NULL,
    event_time          TIME,
    expected_attendance INTEGER,
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_date
    ON city_events (event_date);

-- =============================================================================
-- Seed data — verified from live site (parking-servis.co.rs)
-- Addresses from official facility detail pages (garaza-* / parkiraliste-*).
-- Coordinates extracted from Google Maps hrefs on the listing page.
-- total_spots from official JKP documentation where available.
-- =============================================================================

INSERT INTO parking_locations
    (id, name, address, location_type, total_spots, latitude, longitude, neighborhood)
VALUES
    -- -------------------------------------------------------------------------
    -- GARAGES (Garaže) — 9 confirmed on live site
    -- -------------------------------------------------------------------------
    ('baba-visnjina',
     'Garaža "Baba Višnjina"',                'Baba Višnjina 38-42',           'garage', 351,  44.801441, 20.474145, 'Vračar'),

    ('botanicka-basta',
     'Garaža "Botanička bašta"',              'Vojvode Dobrnjca 10',           'garage', 244,  44.816025, 20.474593, 'Vračar'),

    ('dr-aleksandra-kostica',
     'Garaža "Dr Aleksandra Kostića"',        'Dr Aleksandra Kostića 15',      'garage', 59,   44.804726, 20.454868, 'Savski Venac'),

    ('masarikova',
     'Garaža "Masarikova"',                   'Masarikova 4',                  'garage', 457,  44.806871, 20.463281, 'Stari Grad'),

    ('obilicev-venac',
     'Garaža "Obilićev venac"',               'Obilićev venac 14-16',          'garage', 804,  44.815755, 20.457341, 'Stari Grad'),

    ('pinki',
     'Garaža "Pinki"',                        'Nemanjina 4a, Zemun',           'garage', 150,  44.840923, 20.411643, 'Zemun'),

    ('pionirski-park',
     'Garaža "Pionirski park"',               'Dragoslava Jovanovića 2',       'garage', 466,  44.811354, 20.463101, 'Stari Grad'),

    ('vukov-spomenik',
     'Garaža "Vukov spomenik"',               'Ugao ulica Kraljice Marije i Ruzveltove', 'garage', 120,  44.805372, 20.478827, 'Zvezdara'),

    ('zeleni-venac',
     'Garaža "Zeleni venac"',                 'Kraljice Natalije 13',          'garage', 304,  44.812041, 20.459228, 'Stari Grad'),

    -- -------------------------------------------------------------------------
    -- PARKING LOTS (Parkirališta) — 18 confirmed on live site
    -- -------------------------------------------------------------------------
    ('ada',
     'Parkiralište "Ada"',                    'Ada Ciganlija bb',              'parking_lot', 1023, 44.786757, 20.413043, 'Čukarica'),

    ('belvil',
     'Parkiralište "Belvil"',                 'Ugao ulica Jurija Gagarina, Bulevar Crvene armije i Marka Hristića', 'parking_lot', 413,  44.805451, 20.412065, 'Novi Beograd'),

    ('bezanijska-kosa',
     'Parkiralište "Bežanijska kosa"',        'Ugao ulica Dr Huga Klajna i Ismeta Mujezinovića', 'parking_lot', 85,   44.816044, 20.375018, 'Novi Beograd'),

    ('cukarica',
     'Parkiralište "Čukarica"',               'Šumadijski trg, ispred SO Čukarica', 'parking_lot', 76,   44.782623, 20.415853, 'Čukarica'),

    ('cvetkova-pijaca',
     'Parkiralište "Cvetkova pijaca"',        'Živka Davidovića bb',           'parking_lot', 81,   44.792080, 20.507312, 'Palilula'),

    ('donji-grad',
     'Parkiralište "Donji grad"',             'Karađorđeva 2-4',               'parking_lot', 387,  44.819175, 20.448810, 'Stari Grad'),

    ('kalemegdan',
     'Parkiralište "Kalemegdan"',             'Ulica Mali Kalemegdan bb',      'parking_lot', 113,  44.824869, 20.455307, 'Stari Grad'),

    ('kamenicka',
     'Parkiralište "Kamenička"',              'Kamenička 8',                   'parking_lot', 92,   44.811306, 20.454778, 'Stari Grad'),

    ('ljermontova',
     'Parkiralište "Ljermontova"',            'Ljermontova 12v',               'parking_lot', 68,   44.782967, 20.490112, 'Zvezdara'),

    ('medjunarodni-carinski',
     'Parkiralište "Međunarodni carinski terminal"', 'Prekonoška 12, Blok 43 (Međunarodni terminal)', 'parking_lot', 219,  44.828269, 20.359824, 'Novi Beograd'),

    ('milan-gale-muskatirovic',
     'Parkiralište "Milan Gale Muškatirović"','Tadeuša Kościuške 63',          'parking_lot', 302,  44.829521, 20.454848, 'Palilula'),

    ('opstina-nbgd',
     'Parkiralište "Opština NBGD"',           'Bulevar Mihajla Pupina 167, ispred SO Novi Beograd', 'parking_lot', 122,  44.822591, 20.414157, 'Novi Beograd'),

    ('politika',
     'Parkiralište "Politika"',               'Makedonska 29',                 'parking_lot', 60,   44.815752, 20.464650, 'Stari Grad'),

    ('slavija',
     'Parkiralište "Slavija"',                'Prote Mateje 1 i 2',            'parking_lot', 183,  44.802339, 20.468049, 'Vračar'),

    ('vidin-kapija',
     'Parkiralište "Vidin kapija"',           'Bulevar vojvode Bojovića bb',   'parking_lot', 71,   44.827353, 20.452506, 'Stari Grad'),

    ('viska',
     'Parkiralište "Viška"',                  'Maksima Gorkog 2, ispred SO Vračar', 'parking_lot', 48,   44.800917, 20.475278, 'Vračar'),

    ('vma',
     'Parkiralište "VMA"',                    'Crnotravska 17',                'parking_lot', 574,  44.763723, 20.470734, 'Voždovac')

ON CONFLICT (id) DO NOTHING;

-- Pricing (JKP Parking Servis cenovnik)
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 200, price_daily_rsd = 1750, hours_note = 'Open 24 hours' WHERE id = 'baba-visnjina';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'botanicka-basta';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'vukov-spomenik';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'dr-aleksandra-kostica';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'masarikova';
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'obilicev-venac';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'pinki';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'pionirski-park';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'zeleni-venac';
UPDATE parking_locations SET price_daily_rsd = 200, hours_note = 'Open 24 hours', pricing_note = 'Daily pass only (no hourly rate listed)' WHERE id = 'ada';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1000, hours_note = 'Open 24 hours' WHERE id = 'bezanijska-kosa';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'belvil';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'cukarica';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'cvetkova-pijaca';
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'donji-grad';
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'kalemegdan';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'kamenicka';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1000, hours_note = 'Open 24 hours' WHERE id = 'ljermontova';
UPDATE parking_locations SET price_daily_rsd = NULL, hours_note = 'Customs terminal', pricing_note = '1,900 RSD for the first 12 commenced hours, then 700 RSD per additional 12 hours (customs procedure).' WHERE id = 'medjunarodni-carinski';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'milan-gale-muskatirovic';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'opstina-nbgd';
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'politika';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'slavija';
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 150, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'vidin-kapija';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'viska';
UPDATE parking_locations SET price_first_hour_rsd = 45,  price_extra_hour_rsd = 45,  price_daily_rsd = NULL, hours_note = 'Open 24 hours', pricing_note = '7-day single-entry pass: 3,500 RSD' WHERE id = 'vma';
