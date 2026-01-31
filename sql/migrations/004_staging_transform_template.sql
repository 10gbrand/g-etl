-- Staging-mall för {{ dataset_id }}
-- Genereras automatiskt från datasets.yml

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
--   _h3_cells        - Alla H3-celler inom ytan
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
    CASE
        WHEN s.geom IS NULL THEN NULL
        WHEN ST_IsValid(s.geom) THEN s.geom
        ELSE ST_MakeValid(s.geom)
    END AS geom,

    -- === STAGING METADATA ===
    CURRENT_TIMESTAMP AS _imported_at,
    MD5(ST_AsText(s.geom)) AS _geom_md5,
    MD5(to_json(s)::VARCHAR) AS _attr_md5,
    regexp_replace(to_json(s)::VARCHAR, ',"geom":"[^"]*"', '')::VARCHAR AS _json_data,
    MD5(CAST(s.{{ source_id_column }} AS VARCHAR)) AS _source_id_md5,

    -- === CENTROID (WGS84) ===
    -- Använder PROJ-strängar då DuckDB tolkar EPSG:3006 felaktigt
    ST_Y(ST_Centroid(ST_Transform(s.geom,
        '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs',
        '+proj=longlat +datum=WGS84 +no_defs +type=crs'
    ))) AS _centroid_lat,
    ST_X(ST_Centroid(ST_Transform(s.geom,
        '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs',
        '+proj=longlat +datum=WGS84 +no_defs +type=crs'
    ))) AS _centroid_lng,

    -- === H3 INDEX ===
    h3_latlng_to_cell_string(
        ST_Y(ST_Centroid(ST_Transform(s.geom,
            '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs',
            '+proj=longlat +datum=WGS84 +no_defs +type=crs'
        ))),
        ST_X(ST_Centroid(ST_Transform(s.geom,
            '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs',
            '+proj=longlat +datum=WGS84 +no_defs +type=crs'
        ))),
        {{ h3_center_resolution }}
    ) AS _h3_index,

    -- H3 polyfill - alla celler inom polygonen
    to_json(h3_polygon_wkt_to_cells_string(
        ST_AsText(ST_Transform(s.geom,
            '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs',
            '+proj=longlat +datum=WGS84 +no_defs +type=crs'
        )),
        {{ h3_polyfill_resolution }}
    ))::VARCHAR AS _h3_cells,

    -- A5 (reserverad)
    NULL::VARCHAR AS _a5_index

FROM source_data s;

-- migrate:down
DROP TABLE IF EXISTS staging.{{ dataset_id }};
