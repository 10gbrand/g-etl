-- Mart: Skyddad natur
-- Sammanställning av alla skyddade naturområden

CREATE OR REPLACE TABLE mart.skyddad_natur AS

-- Naturreservat
SELECT
    id,
    namn,
    'Naturreservat' AS skyddstyp,
    areal_ha,
    geometry,
    _loaded_at
FROM staging.naturreservat

-- Lägg till fler skyddstyper med UNION ALL när de finns:
-- UNION ALL
-- SELECT ... FROM staging.natura2000
-- UNION ALL
-- SELECT ... FROM staging.nationalparker
;
