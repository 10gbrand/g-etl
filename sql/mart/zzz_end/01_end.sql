-- End: H3-cellstabell med alla dataset från staging_2
-- Denna fil körs av pipeline_runner som genererar dynamisk SQL
-- baserat på vilka tabeller som finns i staging_2-schemat
--
-- Tabellstruktur (vertikal/long format):
--   h3_cell       - H3-cellens ID
--   dataset       - Tabellnamn från staging_2
--   leverantor    - Dataleverantör (t.ex. sks, nvv, sgu)
--   klass         - Klass (t.ex. biotopskydd, nationalpark)
--   classification - grupp.typ värdet
--
-- OBS: Själva INSERT-satserna genereras dynamiskt av pipeline_runner
--      baserat på vilka tabeller som faktiskt finns i staging_2.

-- Skapa tom tabell med rätt struktur (trunkeras om den finns)
CREATE OR REPLACE TABLE mart.h3_cells (
    h3_cell VARCHAR NOT NULL,
    dataset VARCHAR NOT NULL,
    leverantor VARCHAR,
    klass VARCHAR,
    classification VARCHAR
);
