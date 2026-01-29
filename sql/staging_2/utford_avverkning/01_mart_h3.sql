-- Mart: utford_avverkning
-- Utförda avverkningar
-- Körs efter H3-beräkning i staging-steget

CREATE OR REPLACE TABLE staging_2.utford_avverkning AS
SELECT 
    _source_id_md5 AS id
    , '' AS source_id
    , 'avverkning' AS klass
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
FROM staging.utford_avverkning
