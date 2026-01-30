-- Migration: Staging Procedure
-- Version: 004
-- Skapar en generisk staging-procedur som kan anropas för vilket dataset som helst
--
-- Användning (från Python):
--   CALL run_staging('avverkningsanmalningar', 'beteckn');
--
-- Proceduren:
-- 1. Läser från raw.{dataset_id}
-- 2. Validerar geometri
-- 3. Beräknar MD5-hashar (geometri, attribut, käll-ID)
-- 4. Beräknar centroid i WGS84
-- 5. Beräknar H3-index (centroid och polyfill)
-- 6. Skapar staging.{dataset_id}

-- migrate:up

-- PROJ4-strängar för koordinattransformation (undviker DuckDB EPSG-bugg)
CREATE OR REPLACE MACRO proj4_sweref99() AS
    '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs';

CREATE OR REPLACE MACRO proj4_wgs84() AS
    '+proj=longlat +datum=WGS84 +no_defs';

-- Hjälpmakro: Transformera geometri till WGS84
CREATE OR REPLACE MACRO to_wgs84(geom) AS
    ST_Transform(geom, proj4_sweref99(), proj4_wgs84());

-- Hjälpmakro: Hämta centroid lat/lng i WGS84
CREATE OR REPLACE MACRO wgs84_centroid_lat(geom) AS
    ST_Y(ST_Centroid(to_wgs84(geom)));

CREATE OR REPLACE MACRO wgs84_centroid_lng(geom) AS
    ST_X(ST_Centroid(to_wgs84(geom)));

-- Hjälpmakro: Validera och fixa geometri
CREATE OR REPLACE MACRO validate_geom(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        WHEN ST_IsValid(geom) THEN geom
        ELSE ST_MakeValid(geom)
    END;

-- Hjälpmakro: Beräkna H3 centroid-index (resolution 13 ≈ 43m²)
CREATE OR REPLACE MACRO h3_centroid(geom) AS
    h3_latlng_to_cell_string(
        wgs84_centroid_lat(geom),
        wgs84_centroid_lng(geom),
        13
    );

-- Hjälpmakro: Beräkna H3 polyfill (resolution 11 ≈ 2149m²)
CREATE OR REPLACE MACRO h3_polyfill(geom) AS
    to_json(h3_polygon_wkt_to_cells_string(
        ST_AsText(to_wgs84(geom)),
        11
    ))::VARCHAR;

-- Hjälpmakro: Rensa JSON från geometri-fält
CREATE OR REPLACE MACRO json_without_geom(row_json) AS
    regexp_replace(row_json::VARCHAR, ',"geom":"[^"]*"', '');

-- migrate:down

DROP MACRO IF EXISTS json_without_geom;
DROP MACRO IF EXISTS h3_polyfill;
DROP MACRO IF EXISTS h3_centroid;
DROP MACRO IF EXISTS validate_geom;
DROP MACRO IF EXISTS wgs84_centroid_lng;
DROP MACRO IF EXISTS wgs84_centroid_lat;
DROP MACRO IF EXISTS to_wgs84;
DROP MACRO IF EXISTS proj4_wgs84;
DROP MACRO IF EXISTS proj4_sweref99;
