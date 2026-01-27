-- Staging: Avverkningsanmälningar
-- Rensning och standardisering av avverkningsanmälningar från Skogsstyrelsen

CREATE OR REPLACE TABLE staging.avverkningsanmalningar AS
SELECT
   *,

    -- Geometri - använd common-makron
    validate_and_fix_geometry(geom) AS geom,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.avverkningsanmalningar
WHERE geom IS NOT NULL;
