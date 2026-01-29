-- Mart: giss_gavd
-- GAVD från GISS
-- Körs efter H3-beräkning i staging-steget

CREATE OR REPLACE TABLE staging_2.giss_gavd AS
SELECT 
    _source_id_md5 AS id
    , '' AS source_id
    , 'gavd' AS klass
    , '' AS grupp
    , '' AS typ
    , 'giss' AS leverantor
    , _h3_index AS h3_center
    , _h3_cells AS h3_cells
    , _json_data AS json_data
    , '' AS data_1
    , '' AS data_2
    , '' AS data_3
    , '' AS data_4
    , '' AS data_5
    , geom
FROM staging.giss_gavd
