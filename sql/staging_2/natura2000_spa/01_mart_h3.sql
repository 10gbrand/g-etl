-- Mart: natura2000_spa
-- Natura2000 SPA från Naturvårdsverket
-- Körs efter H3-beräkning i staging-steget

CREATE OR REPLACE TABLE staging_2.natura2000_spa AS
SELECT 
    _source_id_md5 AS id
    , '' AS source_id
    , 'natura2000_spa' AS klass
    , '' AS grupp
    , '' AS typ
    , 'nvv' AS leverantor
    , _h3_index AS h3_center
    , _h3_cells AS h3_cells
    , _json_data AS json_data
    , '' AS data_1
    , '' AS data_2
    , '' AS data_3
    , '' AS data_4
    , '' AS data_5
    , geom
FROM staging.natura2000_spa
