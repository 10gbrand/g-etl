-- DuckDB Makron
-- Återanvändbara makron för geometrihantering och datatransformation
--
-- Användning i dataset-specifika SQL-filer:
--   SELECT validate_and_fix_geometry(geom) AS geometry FROM ...
--   SELECT reproject_to_sweref99(geom) AS geometry FROM ...

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
