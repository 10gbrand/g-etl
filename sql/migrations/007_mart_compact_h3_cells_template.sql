-- Mart kompakterade H3-cells för {{ dataset_id }}
-- Schema: {{ schema }}
-- Källa: {{ prev_schema }}.{{ dataset_id }}
-- Skapar en tabell per dataset med kompakterade H3-celler
-- Genereras automatiskt från datasets.yml

-- migrate:up

CREATE OR REPLACE TABLE {{ schema }}.{{ dataset_id }}_h3_compact AS
SELECT
    * EXCLUDE (h3_cells, centerpoint),
    -- Konvertera JSON-sträng till array, kompaktera, konvertera tillbaka till JSON
    to_json(h3_compact_cells(from_json(h3_cells, '["VARCHAR"]'))) as h3_cells
FROM {{ prev_schema }}.{{ dataset_id }}
WHERE h3_cells IS NOT NULL AND h3_cells != '[]';

-- migrate:down
DROP TABLE IF EXISTS {{ schema }}.compact_{{ dataset_id }};
