-- Mart: biotopskydd_sks
-- Biotopskydd från Skogsstyrelsen
-- Körs efter H3-beräkning i staging-steget

CREATE OR REPLACE TABLE staging_2.biotopskydd_sks AS
SELECT 
    _source_id_md5 AS id
    , '' AS source_id
    , 'biotopskydd' AS klass
    , '' AS grupp
    , '' AS typ
    , 'sks' AS leverantor
    , _h3_index AS h3_center
    , _h3_cells AS h3_cells
    , _json_data AS json_data
    , '' AS data_1
    , '' AS data_2
    , '' AS data_3
    , '' AS data_4
    , '' AS data_5
    , geom
FROM staging.biotopskydd_sks
