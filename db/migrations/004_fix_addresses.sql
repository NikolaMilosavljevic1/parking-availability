-- Correct parking_locations.address values from official Parking Servis detail pages
-- Apply on existing DBs (use docker cp so UTF-8 č/ć/š/đ/ž are preserved):
--   docker compose cp db/migrations/004_fix_addresses.sql db:/tmp/004_fix_addresses.sql
--   docker compose exec db psql -U parking -d parking -f /tmp/004_fix_addresses.sql
-- (parking-servis.co.rs/garaza-* and parkiraliste-*), Latin script.

UPDATE parking_locations SET address = 'Vojvode Dobrnjca 10'
WHERE id = 'botanicka-basta';

UPDATE parking_locations SET address = 'Masarikova 4'
WHERE id = 'masarikova';

UPDATE parking_locations SET address = 'Nemanjina 4a, Zemun'
WHERE id = 'pinki';

UPDATE parking_locations SET address = 'Ugao ulica Kraljice Marije i Ruzveltove'
WHERE id = 'vukov-spomenik';

UPDATE parking_locations SET address = 'Kraljice Natalije 13'
WHERE id = 'zeleni-venac';

UPDATE parking_locations SET address = 'Ugao ulica Jurija Gagarina, Bulevar Crvene armije i Marka Hristića'
WHERE id = 'belvil';

UPDATE parking_locations SET address = 'Ugao ulica Dr Huga Klajna i Ismeta Mujezinovića'
WHERE id = 'bezanijska-kosa';

UPDATE parking_locations SET address = 'Šumadijski trg, ispred SO Čukarica'
WHERE id = 'cukarica';

UPDATE parking_locations SET address = 'Karađorđeva 2-4'
WHERE id = 'donji-grad';

UPDATE parking_locations SET address = 'Ulica Mali Kalemegdan bb'
WHERE id = 'kalemegdan';

UPDATE parking_locations SET address = 'Kamenička 8'
WHERE id = 'kamenicka';

UPDATE parking_locations SET address = 'Ljermontova 12v'
WHERE id = 'ljermontova';

UPDATE parking_locations SET address = 'Prekonoška 12, Blok 43 (Međunarodni terminal)'
WHERE id = 'medjunarodni-carinski';

UPDATE parking_locations SET address = 'Tadeuša Kościuške 63'
WHERE id = 'milan-gale-muskatirovic';

UPDATE parking_locations SET address = 'Bulevar Mihajla Pupina 167, ispred SO Novi Beograd'
WHERE id = 'opstina-nbgd';

UPDATE parking_locations SET address = 'Makedonska 29'
WHERE id = 'politika';

UPDATE parking_locations SET address = 'Prote Mateje 1 i 2'
WHERE id = 'slavija';

UPDATE parking_locations SET address = 'Bulevar vojvode Bojovića bb'
WHERE id = 'vidin-kapija';

UPDATE parking_locations SET address = 'Maksima Gorkog 2, ispred SO Vračar'
WHERE id = 'viska';
