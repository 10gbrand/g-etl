-- Staging: Avverkningsanmälningar
-- Rensning och standardisering av avverkningsanmälningar från Skogsstyrelsen

CREATE OR REPLACE TABLE staging.avverkningsanmalningar AS
SELECT
    -- Identifierare
    COALESCE(CAST(betession AS VARCHAR), CAST(ROW_NUMBER() OVER () AS VARCHAR)) AS id,

    -- Attribut (anpassa kolumnnamn efter faktisk data)
    betession AS beteckning,
    inkomdatum AS inkom_datum,
    avverktyp AS avverkningstyp,
    skogstyp,
    areal AS areal_ha,

    -- Geometri - använd common-makron
    validate_and_fix_geometry(geom) AS geometry,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.avverkningsanmalningar
WHERE geom IS NOT NULL;
