-- Mart H3-cells för {{ dataset_id }}
-- Skapar en tabell per dataset med exploderade H3-celler
-- Genereras automatiskt från datasets.yml

CREATE OR REPLACE TABLE mart.{{ dataset_id }} AS
SELECT
    id,
    '{{ dataset_id }}' AS dataset,
    leverantor,
    klass,
    COALESCE(NULLIF(grupp, ''), '-') || '.' || COALESCE(NULLIF(typ, ''), '-') AS classification,
    unnest(from_json(h3_cells, '["VARCHAR"]')) AS h3_cell,
    h3_cell_to_latlng(unnest(from_json(h3_cells, '["VARCHAR"]'))) AS latlng,
    ST_Transform(
        ST_GeomFromText(h3_cell_to_boundary_wkt(unnest(from_json(h3_cells, '["VARCHAR"]')))),
        '+proj=longlat +datum=WGS84 +no_defs +type=crs',
        '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs'
    ) AS geom
FROM staging_2.{{ dataset_id }}
WHERE h3_cells IS NOT NULL AND h3_cells != '[]';
