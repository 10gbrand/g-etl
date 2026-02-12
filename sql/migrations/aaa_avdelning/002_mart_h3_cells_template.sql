-- Mart H3-cells för {{ dataset_id }}
-- Pipeline: ext_restr (aab_ext_restr/002)
-- Schema: {{ schema }}
-- Källa: {{ prev_schema }}.{{ dataset_id }}
-- Skapar en tabell per dataset med exploderade H3-celler
-- Genereras automatiskt från datasets.yml

-- migrate:up

CREATE OR REPLACE TABLE {{ schema }}.{{ dataset_id }}_h3 AS
SELECT
    id,
    '{{ dataset_id }}' AS dataset,
    leverantor,
    klass,
    area,
    volym,
    COALESCE(NULLIF(grupp, ''), '-') || '.' || COALESCE(NULLIF(typ, ''), '-') AS classification,
    unnest(from_json(h3_cells, '["VARCHAR"]')) AS h3_cell,
    h3_cell_to_latlng(unnest(from_json(h3_cells, '["VARCHAR"]'))) AS latlng,
    g_h3_cell_to_geom(unnest(from_json(h3_cells, '["VARCHAR"]'))) AS geom
FROM {{ prev_schema }}.{{ dataset_id }}
WHERE h3_cells IS NOT NULL AND h3_cells != '[]';

-- migrate:down
DROP TABLE IF EXISTS {{ schema }}.{{ dataset_id }};
