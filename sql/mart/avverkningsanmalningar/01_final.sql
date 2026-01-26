-- Mart: Avverkningsanmälningar
-- Färdigt dataset för avverkningsanmälningar

CREATE OR REPLACE TABLE mart.avverkningsanmalningar AS
SELECT
    id,
    beteckning,
    inkom_datum,
    avverkningstyp,
    skogstyp,
    areal_ha,
    geometry,
    _loaded_at
FROM staging.avverkningsanmalningar;
