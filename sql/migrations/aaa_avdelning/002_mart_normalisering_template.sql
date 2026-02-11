-- Staging-normalisering för {{ dataset_id }}
-- Pipeline: ext_restr (aab_ext_restr/001)
-- Schema: {{ schema }}
-- Källa: {{ prev_schema }}.{{ dataset_id }}
-- Normaliserar till enhetlig struktur
-- Genereras automatiskt från datasets.yml

-- migrate:up

CREATE OR REPLACE TABLE {{ schema }}.{{ dataset_id }} AS
SELECT
    *
FROM {{ prev_schema }}.{{ dataset_id }} s;

-- migrate:down
DROP TABLE IF EXISTS {{ schema }}.{{ dataset_id }};
