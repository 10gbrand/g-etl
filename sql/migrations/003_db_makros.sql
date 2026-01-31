-- DuckDB Makros
-- Återanvändbara makron för geometrihantering, datatransformation och staging

-- migrate:up

-- =============================================================================
-- Projektionsmakron (PROJ4-strängar för koordinattransformation)
-- =============================================================================

CREATE OR REPLACE MACRO proj4_sweref99() AS
    '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs';

CREATE OR REPLACE MACRO proj4_wgs84() AS
    '+proj=longlat +datum=WGS84 +no_defs';

CREATE OR REPLACE MACRO to_wgs84(geom) AS
    ST_Transform(geom, proj4_sweref99(), proj4_wgs84());

-- =============================================================================
-- Geometri-makron
-- =============================================================================

CREATE OR REPLACE MACRO validate_and_fix_geometry(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        WHEN ST_IsValid(geom) THEN geom
        ELSE ST_MakeValid(geom)
    END;

CREATE OR REPLACE MACRO validate_geom(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        WHEN ST_IsValid(geom) THEN geom
        ELSE ST_MakeValid(geom)
    END;

CREATE OR REPLACE MACRO reproject_to_sweref99(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Transform(geom, 'EPSG:4326', 'EPSG:3006')
    END;

CREATE OR REPLACE MACRO reproject_to_wgs84(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Transform(geom, 'EPSG:3006', 'EPSG:4326')
    END;

CREATE OR REPLACE MACRO area_ha(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Area(geom) / 10000.0
    END;

CREATE OR REPLACE MACRO simplify_for_display(geom, tolerance) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Simplify(geom, tolerance)
    END;

-- =============================================================================
-- Centroid-makron (WGS84)
-- =============================================================================

CREATE OR REPLACE MACRO wgs84_centroid_lat(geom) AS
    ST_Y(ST_Centroid(to_wgs84(geom)));

CREATE OR REPLACE MACRO wgs84_centroid_lng(geom) AS
    ST_X(ST_Centroid(to_wgs84(geom)));

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

-- =============================================================================
-- H3-makron
-- =============================================================================

CREATE OR REPLACE MACRO h3_centroid(geom) AS
    h3_latlng_to_cell_string(
        wgs84_centroid_lat(geom),
        wgs84_centroid_lng(geom),
        13
    );

CREATE OR REPLACE MACRO h3_polyfill(geom) AS
    to_json(h3_polygon_wkt_to_cells_string(
        ST_AsText(to_wgs84(geom)),
        11
    ))::VARCHAR;

-- =============================================================================
-- Data-makron
-- =============================================================================

CREATE OR REPLACE MACRO clean_text(txt) AS
    CASE
        WHEN txt IS NULL THEN NULL
        ELSE TRIM(REGEXP_REPLACE(txt, '\s+', ' ', 'g'))
    END;

CREATE OR REPLACE MACRO empty_to_null(val) AS
    CASE
        WHEN val IS NULL THEN NULL
        WHEN TRIM(CAST(val AS VARCHAR)) = '' THEN NULL
        ELSE val
    END;

CREATE OR REPLACE MACRO geom_md5(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE MD5(ST_AsText(geom))
    END;

CREATE OR REPLACE MACRO json_without_geom(row_json) AS
    regexp_replace(row_json::VARCHAR, ',"geom":"[^"]*"', '');

-- =============================================================================
-- Hjälp-makron
-- =============================================================================

CREATE OR REPLACE MACRO find_geom_column(tbl) AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema || '.' || table_name = tbl
      AND (data_type = 'GEOMETRY' OR column_name IN ('geom', 'geometry', 'the_geom', 'wkb_geometry', 'shape'))
    LIMIT 1
);

CREATE OR REPLACE MACRO add_source_metadata(provider, dataset_id) AS
    struct_pack(
        provider := provider,
        dataset := dataset_id,
        loaded_at := CURRENT_TIMESTAMP
    );

CREATE OR REPLACE MACRO generate_id(seed_value) AS
    md5(CAST(seed_value AS VARCHAR) || CAST(CURRENT_TIMESTAMP AS VARCHAR));

CREATE OR REPLACE MACRO format_date_iso(d) AS
    CASE
        WHEN d IS NULL THEN NULL
        ELSE strftime(d, '%Y-%m-%d')
    END;

-- migrate:down
-- Makron kan inte enkelt tas bort, men de skrivs över vid nästa migrate:up
