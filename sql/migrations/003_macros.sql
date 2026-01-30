-- Migration: DuckDB Makron
-- Version: 003
-- Återanvändbara makron för geometrihantering och datatransformation

-- migrate:up

-- =============================================================================
-- Geometri-makron
-- =============================================================================

-- Validera och reparera geometri
CREATE OR REPLACE MACRO validate_and_fix_geometry(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        WHEN ST_IsValid(geom) THEN geom
        ELSE ST_MakeValid(geom)
    END;

-- Projicera om till SWEREF99 TM (EPSG:3006)
CREATE OR REPLACE MACRO reproject_to_sweref99(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Transform(geom, 'EPSG:4326', 'EPSG:3006')
    END;

-- Projicera om till WGS84 (EPSG:4326)
CREATE OR REPLACE MACRO reproject_to_wgs84(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Transform(geom, 'EPSG:3006', 'EPSG:4326')
    END;

-- Beräkna area i hektar (kräver SWEREF99-projicerad geometri)
CREATE OR REPLACE MACRO area_ha(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Area(geom) / 10000.0
    END;

-- Skapa en förenklad geometri för visning
CREATE OR REPLACE MACRO simplify_for_display(geom, tolerance) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Simplify(geom, tolerance)
    END;

-- =============================================================================
-- Data-makron
-- =============================================================================

-- Rensa och standardisera textsträngar
CREATE OR REPLACE MACRO clean_text(txt) AS
    CASE
        WHEN txt IS NULL THEN NULL
        ELSE TRIM(REGEXP_REPLACE(txt, '\s+', ' ', 'g'))
    END;

-- Konvertera tom sträng till NULL
CREATE OR REPLACE MACRO empty_to_null(val) AS
    CASE
        WHEN val IS NULL THEN NULL
        WHEN TRIM(CAST(val AS VARCHAR)) = '' THEN NULL
        ELSE val
    END;

-- =============================================================================
-- Staging-makron (för metadata och hashing)
-- =============================================================================

-- Beräkna MD5-hash av geometri (som WKT)
CREATE OR REPLACE MACRO geom_md5(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE MD5(ST_AsText(geom))
    END;

-- Hämta centroid-koordinater i WGS84
CREATE OR REPLACE MACRO centroid_lat(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Y(ST_Centroid(ST_Transform(geom, 'EPSG:3006', 'EPSG:4326')))
    END;

CREATE OR REPLACE MACRO centroid_lng(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_X(ST_Centroid(ST_Transform(geom, 'EPSG:3006', 'EPSG:4326')))
    END;

-- migrate:down

-- Ta bort makron (DuckDB stödjer DROP MACRO)
DROP MACRO IF EXISTS centroid_lng;
DROP MACRO IF EXISTS centroid_lat;
DROP MACRO IF EXISTS geom_md5;
DROP MACRO IF EXISTS empty_to_null;
DROP MACRO IF EXISTS clean_text;
DROP MACRO IF EXISTS simplify_for_display;
DROP MACRO IF EXISTS area_ha;
DROP MACRO IF EXISTS reproject_to_wgs84;
DROP MACRO IF EXISTS reproject_to_sweref99;
DROP MACRO IF EXISTS validate_and_fix_geometry;
