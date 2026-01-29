-- Staging: Skog_med_hoga_naturvarden
-- Skog med höga naturvärden (SMHN) från Skogsstyrelsen
-- Källa: skogsstyrelsen
--
-- Staging-kolumner som läggs till:
--   geom             - Standardiserat geometrikolumnnamn (validerad)
--   _imported_at     - Tidsstämpel för import
--   _geom_md5        - MD5-hash av geometrin (WKT)
--   _attr_md5        - MD5-hash av alla attribut (JSON)
--   _json_data       - JSON av alla originalkolumner (exkl. geometri)
--   _source_id_md5   - MD5-hash av ID-fält från källan
--   _centroid_lat    - Centroid latitud (WGS84)
--   _centroid_lng    - Centroid longitud (WGS84)
--   _h3_index        - H3-cell för centroid (beräknas i Python)
--   _h3_cells        - Alla H3-celler inom ytan (beräknas i Python)
--   _a5_index        - A5-cell (beräknas i Python när tillgängligt)
--
-- OBS: Geometrikolumnen normaliseras till 'geom' av pipeline_runner
--      innan denna SQL körs. Alternativa namn hanteras automatiskt.

CREATE OR REPLACE TABLE staging.skog_med_hoga_naturvarden AS
WITH source_data AS (
    SELECT * FROM raw.skog_med_hoga_naturvarden
    WHERE geom IS NOT NULL
)
SELECT
    -- Alla originalkolumner EXKLUSIVE geometrivarianter
    -- (geometry, shape, geometri kan finnas kvar som tomma efter rename)
    s.* EXCLUDE (geom),

    -- === STANDARDISERAD GEOMETRI ===
    -- Validerad och rensad geometri med standardnamn
    CASE
        WHEN s.geom IS NULL THEN NULL
        WHEN ST_IsValid(s.geom) THEN s.geom
        ELSE ST_MakeValid(s.geom)
    END AS geom,

    -- === STAGING METADATA ===

    -- Importdatum
    CURRENT_TIMESTAMP AS _imported_at,

    -- MD5-hash av geometri
    MD5(ST_AsText(s.geom)) AS _geom_md5,

    -- MD5-hash av alla attribut (exklusive geometri)
    -- OBS: Använder to_json på hela raden, geometrin serialiseras som WKT
    MD5(to_json(s)::VARCHAR) AS _attr_md5,
    
    -- JSON av alla originalkolumner (geometri exkluderas via regexp_replace)
    regexp_replace(to_json(s)::VARCHAR, ',"geom":"[^"]*"', '')::VARCHAR AS _json_data,
    
    -- MD5-hash av käll-ID (Skogsstyrelsen använder objektid)
    MD5(CAST(s.objektid AS VARCHAR)) AS _source_id_md5,
    
    -- Centroid i WGS84
    ST_Y(ST_Centroid(ST_Transform(s.geom, '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', '+proj=longlat +datum=WGS84 +no_defs'))) AS _centroid_lat,
    ST_X(ST_Centroid(ST_Transform(s.geom, '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', '+proj=longlat +datum=WGS84 +no_defs'))) AS _centroid_lng,
    
    -- H3 centroid (resolution 13 ≈ 43m²)
    h3_latlng_to_cell_string(
        ST_Y(ST_Centroid(ST_Transform(s.geom, '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', '+proj=longlat +datum=WGS84 +no_defs'))),
        ST_X(ST_Centroid(ST_Transform(s.geom, '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', '+proj=longlat +datum=WGS84 +no_defs'))),
        13
    ) AS _h3_index,

    -- H3 polyfill (resolution 11 ≈ 2149m²) - alla celler inom polygonen
    to_json(h3_polygon_wkt_to_cells_string(
        ST_AsText(ST_Transform(s.geom, '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', '+proj=longlat +datum=WGS84 +no_defs')),
        11
    ))::VARCHAR AS _h3_cells,

    -- A5 (reserverad)
    NULL::VARCHAR AS _a5_index

FROM source_data s;
