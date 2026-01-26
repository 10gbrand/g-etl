-- Staging: Naturreservat
-- Rensning och standardisering av naturreservat från raw-schema

CREATE OR REPLACE TABLE staging.naturreservat AS
SELECT
    -- Identifierare
    COALESCE(id, CAST(ROW_NUMBER() OVER () AS VARCHAR)) AS id,

    -- Grundläggande attribut
    NULLIF(TRIM(namn), '') AS namn,
    NULLIF(TRIM(beskrivning), '') AS beskrivning,

    -- Datum
    TRY_CAST(beslutsdatum AS DATE) AS beslutsdatum,

    -- Areal (konvertera till hektar om i m²)
    CASE
        WHEN areal_m2 IS NOT NULL THEN areal_m2 / 10000.0
        ELSE areal_ha
    END AS areal_ha,

    -- Geometri
    geom AS geometry,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.naturreservat
WHERE geom IS NOT NULL;
