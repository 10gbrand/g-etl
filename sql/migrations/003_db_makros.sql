-- DuckDB Makros för G-ETL
-- Återanvändbara makron för geometrihantering, datatransformation och staging
--
-- Alla makron har prefixet g_ för att särskilja från standard SQL-funktioner.

-- migrate:up

-- =============================================================================
-- PROJ4-strängar för koordinattransformation
-- =============================================================================

CREATE OR REPLACE MACRO g_proj4_sweref99() AS
    '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs';

CREATE OR REPLACE MACRO g_proj4_wgs84() AS
    '+proj=longlat +datum=WGS84 +no_defs +type=crs';

-- =============================================================================
-- Geometri-transformation
-- =============================================================================

CREATE OR REPLACE MACRO g_to_wgs84(geom) AS
    ST_Transform(geom, g_proj4_sweref99(), g_proj4_wgs84());

CREATE OR REPLACE MACRO g_validate_geom(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        WHEN ST_IsValid(geom) THEN geom
        ELSE ST_MakeValid(geom)
    END;

CREATE OR REPLACE MACRO g_area_ha(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE ST_Area(geom) / 10000.0
    END;

-- =============================================================================
-- Centroid (WGS84)
-- =============================================================================

CREATE OR REPLACE MACRO g_centroid_wgs84(geom) AS
    ST_Centroid(g_to_wgs84(geom));

CREATE OR REPLACE MACRO g_centroid_lat(geom) AS
    ST_Y(g_centroid_wgs84(geom));

CREATE OR REPLACE MACRO g_centroid_lng(geom) AS
    ST_X(g_centroid_wgs84(geom));

-- =============================================================================
-- H3-index (med konfigurerbar resolution)
-- =============================================================================

-- H3-cell för centroid
CREATE OR REPLACE MACRO g_h3_center(geom, resolution) AS
    h3_latlng_to_cell_string(
        g_centroid_lat(geom),
        g_centroid_lng(geom),
        resolution
    );

-- H3-celler för polygon (polyfill)
CREATE OR REPLACE MACRO g_h3_polygon_cells(geom, resolution) AS
    to_json(h3_polygon_wkt_to_cells_string(
        ST_AsText(g_to_wgs84(geom)),
        resolution
    ))::VARCHAR;

-- H3-celler för linje (buffrad till polygon)
CREATE OR REPLACE MACRO g_h3_line_cells(geom, buffer_meters, resolution) AS
    to_json(h3_polygon_wkt_to_cells_string(
        ST_AsText(g_to_wgs84(ST_Buffer(geom, buffer_meters))),
        resolution
    ))::VARCHAR;

-- H3-cell för punkt (som array)
CREATE OR REPLACE MACRO g_h3_point_cells(geom, resolution) AS
    to_json([h3_latlng_to_cell_string(
        g_centroid_lat(geom),
        g_centroid_lng(geom),
        resolution
    )])::VARCHAR;

-- H3-cell till geometri (SWEREF99 TM)
-- Konverterar en H3-cell sträng till polygon i SWEREF99
CREATE OR REPLACE MACRO g_h3_cell_to_geom(h3_cell) AS
    ST_Transform(
        ST_GeomFromText(h3_cell_to_boundary_wkt(h3_cell)),
        g_proj4_wgs84(),
        g_proj4_sweref99()
    );

-- =============================================================================
-- Data-makron
-- =============================================================================

CREATE OR REPLACE MACRO g_clean_text(txt) AS
    CASE
        WHEN txt IS NULL THEN NULL
        ELSE TRIM(REGEXP_REPLACE(txt, '\s+', ' ', 'g'))
    END;

CREATE OR REPLACE MACRO g_empty_to_null(val) AS
    CASE
        WHEN val IS NULL THEN NULL
        WHEN TRIM(CAST(val AS VARCHAR)) = '' THEN NULL
        ELSE val
    END;

CREATE OR REPLACE MACRO g_geom_md5(geom) AS
    CASE
        WHEN geom IS NULL THEN NULL
        ELSE MD5(ST_AsText(geom))
    END;

CREATE OR REPLACE MACRO g_json_without_geom(row_json) AS
    regexp_replace(row_json::VARCHAR, ',"geom":"[^"]*"', '');

-- =============================================================================
-- Hjälp-makron
-- =============================================================================

CREATE OR REPLACE MACRO g_generate_id(seed_value) AS
    md5(CAST(seed_value AS VARCHAR) || CAST(CURRENT_TIMESTAMP AS VARCHAR));

CREATE OR REPLACE MACRO g_format_date_iso(d) AS
    CASE
        WHEN d IS NULL THEN NULL
        ELSE strftime(d, '%Y-%m-%d')
    END;

-- =============================================================================
-- H3 Query-makron (för polygon-baserade queries)
-- =============================================================================

-- Konvertera WKT polygon (SWEREF99) till H3-celler för query
CREATE OR REPLACE MACRO g_h3_query_cells(polygon_wkt, resolution) AS
    h3_polygon_wkt_to_cells_string(
        ST_AsText(ST_Transform(
            ST_GeomFromText(polygon_wkt),
            g_proj4_sweref99(),
            g_proj4_wgs84()
        )),
        resolution
    );

-- Skapa en tabell med query-celler från polygon (för JOIN)
-- Användning: SELECT * FROM g_h3_query_table('POLYGON((x y, ...))', 8)
CREATE OR REPLACE MACRO g_h3_query_table(polygon_wkt, resolution) AS TABLE
    SELECT UNNEST(g_h3_query_cells(polygon_wkt, resolution)) AS h3_cell;

-- =============================================================================
-- Bakåtkompatibla alias (utan prefix) - kan tas bort senare
-- =============================================================================

CREATE OR REPLACE MACRO proj4_sweref99() AS g_proj4_sweref99();
CREATE OR REPLACE MACRO proj4_wgs84() AS g_proj4_wgs84();
CREATE OR REPLACE MACRO to_wgs84(geom) AS g_to_wgs84(geom);
CREATE OR REPLACE MACRO validate_geom(geom) AS g_validate_geom(geom);
CREATE OR REPLACE MACRO wgs84_centroid_lat(geom) AS g_centroid_lat(geom);
CREATE OR REPLACE MACRO wgs84_centroid_lng(geom) AS g_centroid_lng(geom);
CREATE OR REPLACE MACRO h3_centroid(geom) AS g_h3_center(geom, 13);
CREATE OR REPLACE MACRO h3_polyfill(geom) AS g_h3_polygon_cells(geom, 11);
CREATE OR REPLACE MACRO json_without_geom(row_json) AS g_json_without_geom(row_json);
CREATE OR REPLACE MACRO geom_md5(geom) AS g_geom_md5(geom);

-- migrate:down
-- Makron kan inte enkelt tas bort, men de skrivs över vid nästa migrate:up
