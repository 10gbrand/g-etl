-- Staging-normalisering för {{ dataset_id }}
-- Pipeline: ext_restr (aab_ext_restr/001)
-- Schema: {{ schema }}
-- Källa: {{ prev_schema }}.{{ dataset_id }}
-- Normaliserar till enhetlig struktur
-- Genereras automatiskt från datasets.yml

-- migrate:up

CREATE OR REPLACE TABLE {{ schema }}.{{ dataset_id }} AS
SELECT
    s._source_id_md5 AS id,
    {{ source_id_expr }} AS source_id,
    '{{ klass }}' AS klass,
    {{ grupp_expr }} AS grupp,
    {{ typ_expr }} AS typ,
    '{{ leverantor }}' AS leverantor,
    s._h3_index AS h3_center,
    s._h3_cells AS h3_cells,
    s._json_data AS json_data,
    {{ data_1_expr }} AS data_1,
    {{ data_2_expr }} AS data_2,
    {{ data_3_expr }} AS data_3,
    {{ data_4_expr }} AS data_4,
    {{ data_5_expr }} AS data_5,
    s.geom,
    ST_PointOnSurface(s.geom) AS centerpoint
FROM {{ prev_schema }}.{{ dataset_id }} s;

-- migrate:down
DROP TABLE IF EXISTS {{ schema }}.{{ dataset_id }};
