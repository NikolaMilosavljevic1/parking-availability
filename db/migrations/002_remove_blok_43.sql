-- Remove Parkiralište "Blok 43" — reserved/non-public, not shown in the app.
-- Run on existing databases:
--   docker compose exec -T db psql -U parking -d parking < db/migrations/002_remove_blok_43.sql

DELETE FROM parking_snapshots
WHERE location_id IN ('blok-43', 'parkiraliste-blok-43')
   OR location_id LIKE '%blok-43%';

DELETE FROM parking_locations
WHERE id IN ('blok-43', 'parkiraliste-blok-43')
   OR id LIKE '%blok-43%'
   OR name ILIKE '%blok 43%';
