-- Mart: sksbiotopskydd_h3
-- Biotopskydd från Skogsstyrelsen med H3-index
-- Körs efter H3-beräkning i staging-steget

CREATE OR REPLACE TABLE staging_2.avverkningsanmalningar AS
select 
    _source_id_md5 as id
    , beteckn as source_id
    , 'avverkningsanmalan' as klass
    , '' as grupp
    , '' as typ
    ,'sks' as leverantor
    , _h3_index as h3_center
    , _h3_cells as h3_cells
    , _json_data as json_data
    , '' as data_1
    , '' as data_2
    , '' as data_3
    , '' as data_4
    , '' as data_5
    , geom
from staging.avverkningsanmalningar
