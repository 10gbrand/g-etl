-- Common: Geometrifunktioner
-- Skapar återanvändbara makron för geometrihantering
--
-- Användning i dataset-specifika SQL-filer:
--   SELECT validate_and_fix_geometry(geom) AS geometry FROM ...
--   SELECT reproject_to_sweref99(geom) AS geometry FROM ...

-- Makro för att validera och reparera geometri
CREATE OR REPLACE MACRO validate_and_fix_geometry(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        WHEN ST_IsValid(geom) THEN geom
        ELSE ST_MakeValid(geom)
    END;

-- Makro för att projicera om till SWEREF99 TM (EPSG:3006)
CREATE OR REPLACE MACRO reproject_to_sweref99(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Transform(geom, 'EPSG:4326', 'EPSG:3006')
    END;

-- Makro för att projicera om till WGS84 (EPSG:4326)
CREATE OR REPLACE MACRO reproject_to_wgs84(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Transform(geom, 'EPSG:3006', 'EPSG:4326')
    END;

-- Makro för att beräkna area i hektar (kräver SWEREF99-projicerad geometri)
CREATE OR REPLACE MACRO area_ha(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Area(geom) / 10000.0
    END;

-- Makro för att skapa en förenklad geometri för visning
CREATE OR REPLACE MACRO simplify_for_display(geom, tolerance) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Simplify(geom, tolerance)
    END;
