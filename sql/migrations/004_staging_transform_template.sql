-- Staging-mall för {{ dataset_id }}
-- Genereras automatiskt från datasets.yml
--
-- Använder makron från 003_db_makros.sql (prefixade med g_)

-- migrate:up

-- Staging-kolumner:
--   geom             - Validerad geometri
--   _imported_at     - Tidsstämpel för import
--   _geom_md5        - MD5-hash av geometrin
--   _attr_md5        - MD5-hash av alla attribut
--   _json_data       - JSON av alla originalkolumner
--   _source_id_md5   - MD5-hash av käll-ID
--   _centroid_lat    - Centroid latitud (WGS84)
--   _centroid_lng    - Centroid longitud (WGS84)
--   _h3_index        - H3-cell för centroid
--   _h3_cells        - Alla H3-celler (polygon: polyfill, linje: buffrad, punkt: enkel cell)
--   _a5_index        - A5-cell (reserverad)

CREATE OR REPLACE TABLE staging.{{ dataset_id }} AS
WITH source_data AS (
    SELECT * FROM raw.{{ dataset_id }}
    WHERE geom IS NOT NULL
)
SELECT
    -- Alla originalkolumner exklusive geometri
    s.* EXCLUDE (geom),

    -- === VALIDERAD GEOMETRI ===
    g_validate_geom(s.geom) AS geom,

    -- === STAGING METADATA ===
    CURRENT_TIMESTAMP AS _imported_at,
    g_geom_md5(s.geom) AS _geom_md5,
    MD5(to_json(s)::VARCHAR) AS _attr_md5,
    g_json_without_geom(to_json(s)) AS _json_data,
    MD5(CAST(s.{{ source_id_column }} AS VARCHAR)) AS _source_id_md5,

    -- === CENTROID (WGS84) ===
    g_centroid_lat(s.geom) AS _centroid_lat,
    g_centroid_lng(s.geom) AS _centroid_lng,

    -- === H3 INDEX ===
    g_h3_center(s.geom, {{ h3_center_resolution }}) AS _h3_index,

    -- H3 cells - anpassat efter geometrityp (polygon, linje, punkt)
    CASE
        WHEN ST_GeometryType(s.geom) IN ('POLYGON', 'MULTIPOLYGON')
            THEN g_h3_polygon_cells(s.geom, {{ h3_polyfill_resolution }})
        WHEN ST_GeometryType(s.geom) IN ('LINESTRING', 'MULTILINESTRING')
            THEN g_h3_line_cells(s.geom, {{ h3_line_buffer_meters }}, {{ h3_line_resolution }})
        WHEN ST_GeometryType(s.geom) IN ('POINT', 'MULTIPOINT')
            THEN g_h3_point_cells(s.geom, {{ h3_point_resolution }})
        ELSE NULL
    END AS _h3_cells,

    -- A5 (reserverad)
    NULL::VARCHAR AS _a5_index

FROM source_data s;

-- migrate:down
DROP TABLE IF EXISTS staging.{{ dataset_id }};
