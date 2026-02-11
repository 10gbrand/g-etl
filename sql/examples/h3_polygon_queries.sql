-- ============================================================================
-- Exempel: H3-baserade Polygon-queries
-- ============================================================================
--
-- Visar hur man gör snabba spatial queries med H3-index.
-- Använder mart.h3_index och mart.h3_stats tabellerna.
--
-- Fördelar med H3-baserad query:
-- 1. Mycket snabbare än traditionell ST_Intersects (index lookup vs full scan)
-- 2. Kan utnyttja pre-beräknad aggregering
-- 3. Skalbart till miljoner objekt
--
-- Workflow:
-- 1. Konvertera query-polygon till H3-celler (polyfill)
-- 2. JOIN med h3_index på h3_cell
-- 3. Aggregera resultat per dataset/klass/etc.

-- ============================================================================
-- EXEMPEL 1: Hitta alla objekt inom ett område (polygon)
-- ============================================================================

-- Definiera query-polygon (exempel: område i Stockholm, SWEREF99 TM)
WITH query_polygon AS (
    SELECT ST_GeomFromText('POLYGON((
        674000 6580000,
        676000 6580000,
        676000 6582000,
        674000 6582000,
        674000 6580000
    ))') AS geom
),

-- Konvertera polygon till H3-celler (resolution 8)
query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(ST_AsText(geom), 8)) AS h3_cell
    FROM query_polygon
)

-- Hitta alla objekt som har överlappande H3-celler
SELECT
    h.dataset_id,
    h.klass,
    COUNT(*) AS object_count,
    COUNT(DISTINCT h.id) AS unique_objects,
    LIST(DISTINCT h.grupp) AS grupper
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
GROUP BY h.dataset_id, h.klass
ORDER BY object_count DESC;


-- ============================================================================
-- EXEMPEL 2: Statistik per dataset inom polygon
-- ============================================================================

WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))',
        8
    )) AS h3_cell
)

SELECT
    h.dataset_id,
    h.leverantor,
    COUNT(DISTINCT h.id) AS antal_objekt,
    COUNT(DISTINCT h.h3_cell) AS antal_h3_celler,
    LIST(DISTINCT h.klass) AS klasser
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
GROUP BY h.dataset_id, h.leverantor
ORDER BY antal_objekt DESC;


-- ============================================================================
-- EXEMPEL 3: Heatmap-data (aggregerad per H3-cell)
-- ============================================================================

-- För en query-polygon, få ut alla H3-celler med antal objekt
-- Perfekt för att skapa heatmaps i Kepler.gl eller QGIS

WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))',
        8
    )) AS h3_cell
)

SELECT
    q.h3_cell,
    s.object_count,
    s.dataset_count,
    s.datasets,
    s.klasser,
    -- Konvertera H3-cell till geometri för visualisering
    g_h3_cell_to_geom(q.h3_cell) AS geom,
    -- WKT för export
    h3_cell_to_boundary_wkt(q.h3_cell) AS geom_wkt
FROM query_h3 q
LEFT JOIN mart.h3_stats s ON q.h3_cell = s.h3_cell
ORDER BY s.object_count DESC;


-- ============================================================================
-- EXEMPEL 4: Filtrera på specifika klasser/datasets
-- ============================================================================

-- Hitta bara naturreservat och nationalparker inom området

WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))',
        8
    )) AS h3_cell
)

SELECT
    h.dataset_id,
    h.klass,
    h.source_id,
    h.data_1 AS namn,
    h.data_2 AS areal_ha
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
WHERE h.klass IN ('naturreservat', 'nationalpark')
GROUP BY h.dataset_id, h.klass, h.source_id, h.data_1, h.data_2;


-- ============================================================================
-- EXEMPEL 5: Multi-polygon query (flera områden samtidigt)
-- ============================================================================

-- Query mot flera områden (t.ex. alla kommuner i ett län)

WITH query_polygons AS (
    -- Exempel: två olika områden
    SELECT 'stockholm' AS area_name,
           ST_GeomFromText('POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))') AS geom
    UNION ALL
    SELECT 'uppsala',
           ST_GeomFromText('POLYGON((653000 6645000, 655000 6645000, 655000 6647000, 653000 6647000, 653000 6645000))') AS geom
),

query_h3 AS (
    SELECT
        qp.area_name,
        UNNEST(g_h3_query_cells(ST_AsText(qp.geom), 8)) AS h3_cell
    FROM query_polygons qp
)

SELECT
    q.area_name,
    h.dataset_id,
    COUNT(DISTINCT h.id) AS antal_objekt
FROM query_h3 q
INNER JOIN mart.h3_index h ON h.h3_cell = q.h3_cell
GROUP BY q.area_name, h.dataset_id
ORDER BY q.area_name, antal_objekt DESC;


-- ============================================================================
-- EXEMPEL 6: Buffrad punkt-query (cirkel)
-- ============================================================================

-- Hitta alla objekt inom 5 km från en punkt

WITH query_point AS (
    SELECT ST_Point(675000, 6581000) AS geom  -- Punkt i SWEREF99 TM
),

query_polygon AS (
    SELECT ST_Buffer(geom, 5000) AS geom  -- 5 km buffer
    FROM query_point
),

query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(ST_AsText(geom), 8)) AS h3_cell
    FROM query_polygon
)

SELECT
    h.dataset_id,
    h.klass,
    COUNT(DISTINCT h.id) AS antal_objekt,
    ROUND(AVG(ST_Distance(
        ST_Point(675000, 6581000),
        h.geom
    ))) AS avg_distance_m
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
GROUP BY h.dataset_id, h.klass
ORDER BY antal_objekt DESC;


-- ============================================================================
-- EXEMPEL 7: Export för Kepler.gl (CSV med H3-index)
-- ============================================================================

-- Exportera H3-heatmap för visualisering i Kepler.gl

WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))',
        8
    )) AS h3_cell
)

SELECT
    q.h3_cell AS hex_id,  -- Kepler.gl läser 'hex_id' för H3
    s.object_count AS value,
    s.dataset_count,
    LIST_SORT(s.datasets) AS datasets
FROM query_h3 q
LEFT JOIN mart.h3_stats s ON q.h3_cell = s.h3_cell
WHERE s.object_count IS NOT NULL
ORDER BY s.object_count DESC;

-- Spara till CSV:
-- COPY (...query...) TO 'output/h3_heatmap.csv' WITH (HEADER, DELIMITER ',');


-- ============================================================================
-- EXEMPEL 8: Performance-jämförelse (H3 vs traditionell ST_Intersects)
-- ============================================================================

-- H3-baserad query (SNABB - index lookup)
EXPLAIN ANALYZE
WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))',
        8
    )) AS h3_cell
)
SELECT COUNT(*) FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell;

-- Traditionell spatial query (LÅNGSAM - full table scan med geometry checks)
-- OBS: Kör detta bara på små datasets eller med WHERE-filter!
-- EXPLAIN ANALYZE
-- SELECT COUNT(*) FROM staging_005.naturreservat
-- WHERE ST_Intersects(
--     geom,
--     ST_GeomFromText('POLYGON((674000 6580000, 676000 6580000, 676000 6582000, 674000 6582000, 674000 6580000))')
-- );


-- ============================================================================
-- TIPS: Välja rätt H3-resolution
-- ============================================================================

-- Resolution bestämmer granularitet vs prestanda:
--
-- Res 6:  ~36 km²   - Lämplig för län/regioner
-- Res 7:  ~5.2 km²  - Lämplig för kommuner
-- Res 8:  ~0.74 km² - Lämplig för stadsdelar (DEFAULT i G-ETL)
-- Res 9:  ~0.10 km² - Lämplig för kvarter
-- Res 10: ~0.015 km²- Lämplig för enskilda byggnader
--
-- Högre resolution = mer exakt men fler celler att processa
-- Lägre resolution = snabbare men grövre granularitet
--
-- Använd SAMMA resolution som data har indexerats med för bästa resultat!
