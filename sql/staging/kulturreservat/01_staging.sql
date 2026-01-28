-- Staging: Kulturreservat
-- Kulturreservat från Naturvårdsverket
-- Källa: naturvardsverket
--
-- Staging-kolumner som läggs till:
--   _imported_at     - Tidsstämpel för import
--   _geometry        - Validerad geometri
--   _geom_md5        - MD5-hash av geometrin (WKT)
--   _attr_md5        - MD5-hash av alla attribut (JSON)
--   _source_id_md5   - MD5-hash av ID-fält från källan
--   _centroid_lat    - Centroid latitud (WGS84)
--   _centroid_lng    - Centroid longitud (WGS84)
--   _h3_index        - H3-cell (beräknas i Python)
--   _a5_index        - A5-cell (beräknas i Python när tillgängligt)

CREATE OR REPLACE TABLE staging.kulturreservat AS
WITH source_data AS (
    SELECT * FROM raw.kulturreservat
    WHERE geom IS NOT NULL
)
SELECT
    -- Alla originalkolumner
    s.*,

    -- === STAGING METADATA ===
    
    -- Importdatum
    CURRENT_TIMESTAMP AS _imported_at,
    
    -- Validerad geometri
    CASE
        WHEN s.geom IS NULL THEN NULL
        WHEN ST_IsValid(s.geom) THEN s.geom
        ELSE ST_MakeValid(s.geom)
    END AS _geometry,
    
    -- MD5-hash av geometri
    MD5(ST_AsText(s.geom)) AS _geom_md5,
    
    -- MD5-hash av alla attribut (exklusive geometri)
    MD5(to_json(s)::VARCHAR) AS _attr_md5,
    
    -- MD5-hash av ID-fält (försöker hitta vanliga ID-kolumner)
    MD5(COALESCE(
        TRY_CAST(s."id" AS VARCHAR),
        TRY_CAST(s."objektid" AS VARCHAR),
        TRY_CAST(s."objekt_id" AS VARCHAR),
        TRY_CAST(s."object_id" AS VARCHAR),
        TRY_CAST(s."objectid" AS VARCHAR),
        TRY_CAST(s."fid" AS VARCHAR),
        TRY_CAST(s."gid" AS VARCHAR),
        TRY_CAST(s."uuid" AS VARCHAR),
        TRY_CAST(s."dnr" AS VARCHAR),
        TRY_CAST(s."diarienr" AS VARCHAR),
        TRY_CAST(s."diarienummer" AS VARCHAR),
        TRY_CAST(s."omradesid" AS VARCHAR),
        TRY_CAST(s."omrades_id" AS VARCHAR),
        TRY_CAST(s."area_id" AS VARCHAR),
        TRY_CAST(s."nvrid" AS VARCHAR),
        TRY_CAST(s."naturvardsid" AS VARCHAR),
        CAST(ROW_NUMBER() OVER () AS VARCHAR)
    )) AS _source_id_md5,
    
    -- Centroid i WGS84
    ST_Y(ST_Centroid(ST_Transform(s.geom, 'EPSG:3006', 'EPSG:4326'))) AS _centroid_lat,
    ST_X(ST_Centroid(ST_Transform(s.geom, 'EPSG:3006', 'EPSG:4326'))) AS _centroid_lng,
    
    -- H3 och A5 (fylls i av Python-steget)
    NULL::VARCHAR AS _h3_index,
    NULL::VARCHAR AS _a5_index

FROM source_data s;
