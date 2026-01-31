-- Mart kompakterade H3-cells för {{ dataset_id }}
-- Skapar en tabell per dataset med kompakterade H3-celler
-- Genereras automatiskt från datasets.yml

-- migrate:up

CREATE OR REPLACE TABLE mart.compact_{{ dataset_id }} AS
SELECT
    * EXCLUDE (h3_cells),
    -- Konvertera JSON-sträng till array, kompaktera, konvertera tillbaka till JSON
    to_json(h3_compact_cells(from_json(h3_cells, '["VARCHAR"]'))) as h3_cells
FROM staging_2.{{ dataset_id }}
WHERE h3_cells IS NOT NULL AND h3_cells != '[]';

-- migrate:down
DROP TABLE IF EXISTS mart.compact_{{ dataset_id }};
