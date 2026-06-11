-- Parking rates from JKP Parking Servis cenovnik (parking-servis.co.rs/lat/korisnicki-servis).
-- Run on existing databases:
--   docker compose exec -T db psql -U parking -d parking < db/migrations/003_add_pricing.sql

ALTER TABLE parking_locations
    ADD COLUMN IF NOT EXISTS price_first_hour_rsd INTEGER,
    ADD COLUMN IF NOT EXISTS price_extra_hour_rsd  INTEGER,
    ADD COLUMN IF NOT EXISTS price_daily_rsd       INTEGER,
    ADD COLUMN IF NOT EXISTS hours_note            VARCHAR,
    ADD COLUMN IF NOT EXISTS pricing_note          VARCHAR;

-- Garages
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 200, price_daily_rsd = 1750, hours_note = 'Open 24 hours' WHERE id = 'baba-visnjina';
UPDATE parking_locations SET price_first_hour_rsd = 50,  price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'botanicka-basta';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 100, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'vukov-spomenik';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'dr-aleksandra-kostica';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'masarikova';
UPDATE parking_locations SET price_first_hour_rsd = 150, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'obilicev-venac';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 1500, hours_note = 'Open 24 hours' WHERE id = 'pinki';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'pionirski-park';
UPDATE parking_locations SET price_first_hour_rsd = 100, price_extra_hour_rsd = 150, price_daily_rsd = 2000, hours_note = 'Open 24 hours' WHERE id = 'zeleni-venac';

-- Parking lots
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
