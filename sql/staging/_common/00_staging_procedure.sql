-- Staging: Gemensam procedur för att skapa staging-tabeller
-- Körs INTE automatiskt - definierar bara makrot
--
-- Varje dataset kör sitt eget staging-skript som anropar detta makro
-- H3-index och A5-index beräknas i Python-steget efteråt
--
-- VIKTIGT: Geometrikolumner normaliseras till 'geom' av pipeline_runner
-- innan staging SQL körs. Alternativa namn (geometry, shape, geometri)
-- döps om automatiskt.

-- Makro för att hitta geometrikolumn (returnerar första som matchar)
CREATE OR REPLACE MACRO find_geom_column(tbl) AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema || '.' || table_name = tbl
      AND (data_type = 'GEOMETRY' OR column_name IN ('geom', 'geometry', 'the_geom', 'wkb_geometry', 'shape'))
    LIMIT 1
);
