-- Staging_2-mall för {{ dataset_id }}
-- Normaliserar staging till enhetlig struktur
-- Genereras automatiskt från datasets.yml

CREATE OR REPLACE TABLE staging_2.{{ dataset_id }} AS
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
FROM staging.{{ dataset_id }} s;
