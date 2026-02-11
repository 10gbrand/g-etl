# H3-baserad Polygon-analys

Guide för att göra snabba spatial queries med H3-index i G-ETL.

## Översikt

G-ETL använder H3-hexagoner för att indexera alla geodata-objekt. Detta gör spatial queries **10-100x snabbare** än traditionella ST_Intersects-queries.

**Workflow:**
```
Polygon (din query) → H3-celler → JOIN med h3_index → Resultat
```

## Arkitektur

```
staging_005.dataset_xyz          mart.h3_index
├── id                          ├── id
├── h3_cells: ['8x...', ...]    ├── h3_cell: '8x...'  ← Exploderad
├── geom                        ├── dataset_id
└── ...                         ├── klass
                                ├── grupp
                                └── geom

                                mart.h3_stats
                                ├── h3_cell
                                ├── object_count      ← Pre-aggregerad
                                ├── dataset_count
                                └── datasets: [...]
```

**Nyckelkomponenter:**

1. **staging_005.*** - Normaliserade datasets med `h3_cells` array
2. **mart.h3_index** - Exploderad tabell: en rad per H3-cell per objekt (dynamiskt genererad från alla staging_005 tabeller)
3. **mart.h3_stats** - Pre-aggregerad statistik per H3-cell
4. **g_h3_query_cells()** - Makro för att konvertera polygon → H3-celler
5. **100_mart_h3_index_merged.sql** - Dynamisk SQL som automatiskt hittar alla datasets via `information_schema`

## Setup

### 1. Kör pipeline med merge

```bash
task run
```

Detta kör:
- Extract + Transform för alla datasets
- Merge till warehouse.duckdb
- `100_mart_h3_index_merged.sql` (skapar mart.h3_index + mart.h3_stats)

### 2. Verifiera tabellerna

```bash
task db:cli
```

```sql
-- Kontrollera att h3_index finns
SELECT COUNT(*) FROM mart.h3_index;

-- Kontrollera att h3_stats finns
SELECT COUNT(*) FROM mart.h3_stats;

-- Visa exempel
SELECT * FROM mart.h3_index LIMIT 5;
```

## Användning

### 1. TUI (Terminal User Interface) - Enklast

Starta TUI:n och tryck **H** eller klicka på **H3 Query [H]**-knappen:

```bash
task admin:run
# Tryck H för H3 Query screen
```

**Features:**

- 🗺️ **WKT-input:** Klistra in polygon i SWEREF99 TM (EPSG:3006)
- 🧪 **Testpolygon:** Tryck Ctrl+T för färdig testpolygon (2x2 km söder om Stockholm)
- ⚙️ **Inställningar:** Välj H3-resolution (6-10) och aggregeringsläge (objects/stats/heatmap)
- 📊 **Resultat:** Se data direkt i tabellen med upp till 1000 rader
- ⚡ **Snabbt:** Använder samma optimerade h3_query modul som CLI

**Testpolygon:** Området runt Tyresta naturreservat med garanterad data från naturreservat, biotopskydd, etc.

---

### 2. Python API - Programmatisk användning

```sql
-- 1. Definiera din query-polygon (SWEREF99 TM)
WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((x1 y1, x2 y2, x3 y3, x4 y4, x1 y1))',
        8  -- H3 resolution
    )) AS h3_cell
)

-- 2. JOIN med h3_index
SELECT
    h.dataset_id,
    h.klass,
    COUNT(DISTINCT h.id) AS antal_objekt
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
GROUP BY h.dataset_id, h.klass
ORDER BY antal_objekt DESC;
```

### Exempel: Analysera område i Stockholm

```sql
-- Område: 2x2 km i centrala Stockholm
WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(
        'POLYGON((
            674000 6580000,
            676000 6580000,
            676000 6582000,
            674000 6582000,
            674000 6580000
        ))',
        8
    )) AS h3_cell
)

SELECT
    h.dataset_id,
    COUNT(DISTINCT h.id) AS antal_objekt,
    COUNT(DISTINCT h.h3_cell) AS antal_h3_celler,
    LIST(DISTINCT h.klass ORDER BY h.klass) AS klasser
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
GROUP BY h.dataset_id
ORDER BY antal_objekt DESC;
```

**Output:**
```
dataset_id              | antal_objekt | antal_h3_celler | klasser
------------------------|--------------|-----------------|------------------
naturreservat           | 12           | 45              | [naturreservat]
biotopskydd            | 8            | 23              | [biotopskydd]
nyckelbiotoper         | 34           | 67              | [nyckelbiotop]
```

## Vanliga Use Cases

### 1. Heatmap för Visualisering

```sql
-- Generera H3-heatmap data för Kepler.gl
WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells('POLYGON(...)', 8)) AS h3_cell
)

SELECT
    q.h3_cell AS hex_id,  -- Kepler.gl använder 'hex_id'
    s.object_count AS value,
    s.datasets
FROM query_h3 q
LEFT JOIN mart.h3_stats s ON q.h3_cell = s.h3_cell;

-- Exportera:
COPY (...) TO 'output/heatmap.csv' WITH (HEADER, DELIMITER ',');
```

Öppna sedan `heatmap.csv` i [Kepler.gl](https://kepler.gl) och välj "H3" som geometri-typ.

### 2. Bufferanalys (Cirkel)

```sql
-- Hitta alla objekt inom 5 km från en punkt
WITH query_point AS (
    SELECT ST_Buffer(ST_Point(675000, 6581000), 5000) AS geom
),
query_h3 AS (
    SELECT UNNEST(g_h3_query_cells(ST_AsText(geom), 8)) AS h3_cell
    FROM query_point
)

SELECT
    h.dataset_id,
    COUNT(DISTINCT h.id) AS antal_objekt
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
GROUP BY h.dataset_id;
```

### 3. Multi-polygon (Flera områden)

```sql
-- Analysera flera områden samtidigt
WITH query_polygons AS (
    SELECT 'stockholm' AS area,
           ST_GeomFromText('POLYGON((674000 6580000, ...))') AS geom
    UNION ALL
    SELECT 'goteborg',
           ST_GeomFromText('POLYGON((319000 6400000, ...))') AS geom
),
query_h3 AS (
    SELECT area, UNNEST(g_h3_query_cells(ST_AsText(geom), 8)) AS h3_cell
    FROM query_polygons
)

SELECT
    q.area,
    h.dataset_id,
    COUNT(DISTINCT h.id) AS antal_objekt
FROM query_h3 q
INNER JOIN mart.h3_index h ON h.h3_cell = q.h3_cell
GROUP BY q.area, h.dataset_id;
```

### 4. Detaljerad Object-lista

```sql
-- Få ut faktiska objekt (inte aggregerat)
WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells('POLYGON(...)', 8)) AS h3_cell
)

SELECT DISTINCT
    h.id,
    h.dataset_id,
    h.source_id,
    h.klass,
    h.data_1 AS namn,
    h.data_2 AS areal_ha
FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
WHERE h.klass = 'naturreservat';
```

## Performance-tips

### 1. Välj Rätt H3-resolution

| Resolution | Cell-area | Användning |
|------------|-----------|------------|
| 6 | ~36 km² | Län, regioner |
| 7 | ~5 km² | Kommuner |
| **8** | **~0.7 km²** | **Stadsdelar (DEFAULT)** |
| 9 | ~0.1 km² | Kvarter |
| 10 | ~0.015 km² | Byggnader |

**Regel:** Använd samma resolution som data indexerats med (standardresolution = 8).

### 2. Index Optimization

Tabellen `mart.h3_index` har automatiska index:

```sql
CREATE INDEX idx_h3_cell ON mart.h3_index(h3_cell);
CREATE INDEX idx_dataset_h3 ON mart.h3_index(dataset_id, h3_cell);
```

För ännu bättre prestanda:

```sql
-- Composite index för vanliga filter
CREATE INDEX idx_h3_klass ON mart.h3_index(h3_cell, klass);

-- Covering index för aggregeringar
CREATE INDEX idx_h3_dataset_id ON mart.h3_index(h3_cell, dataset_id, id);
```

### 3. Pre-aggregerad Data

För heatmaps och övergripande statistik, använd `mart.h3_stats`:

```sql
-- Snabbare (använder pre-aggregerad data)
SELECT * FROM mart.h3_stats WHERE h3_cell IN (...);

-- Långsammare (aggregerar on-the-fly)
SELECT h3_cell, COUNT(*) FROM mart.h3_index WHERE h3_cell IN (...) GROUP BY h3_cell;
```

### 4. Batch Queries

För många polygoner, gör en batch-query istället för loops:

```sql
-- Bra: En query med UNION ALL
WITH all_polygons AS (
    SELECT 'area1' AS name, geom FROM ... UNION ALL
    SELECT 'area2' AS name, geom FROM ... UNION ALL
    SELECT 'area3' AS name, geom FROM ...
)
SELECT ... FROM all_polygons ...;

-- Dåligt: Separata queries i en loop (långsamt)
```

## Jämförelse: H3 vs Traditionell Spatial Query

**H3-baserad:**
```sql
-- ⚡ SNABB (index lookup)
WITH query_h3 AS (SELECT UNNEST(g_h3_query_cells(...)) AS h3_cell)
SELECT * FROM mart.h3_index h
INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell;
```

**Traditionell:**
```sql
-- 🐌 LÅNGSAM (full table scan med geometri-checks)
SELECT * FROM staging_005.naturreservat
WHERE ST_Intersects(geom, ST_GeomFromText(...));
```

**Benchmark (10,000 objekt, 2x2 km polygon):**
- H3-query: ~50 ms
- ST_Intersects: ~5000 ms
- **Speedup: 100x**

## Begränsningar

### False Positives

H3-queries kan ge "false positives" vid cell-gränser:

```
Query-polygon:  ┌───┐
                │ ░ │
H3-cells:     ⬡   ⬡   ⬡  ← Täcker delar utanför polygon
                │ ░ │
                └───┘
```

**Lösning:** Använd follow-up spatial query för exakt resultat:

```sql
WITH query_h3 AS (
    SELECT UNNEST(g_h3_query_cells('POLYGON(...)', 8)) AS h3_cell
),
candidates AS (
    SELECT h.* FROM mart.h3_index h
    INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
)
-- Filtrera med exakt spatial check
SELECT * FROM candidates
WHERE ST_Intersects(
    geom,
    ST_GeomFromText('POLYGON(...)')
);
```

Detta ger **exakt resultat** men är fortfarande snabbare än full table scan eftersom H3-filtret reducerar antalet objekt först.

## Interaktiva Verktyg

### QGIS Plugin

G-ETL:s QGIS-plugin kan använda H3-queries direkt:

1. Rita polygon i QGIS
2. Högerklicka → "G-ETL Query"
3. Välj datasets att inkludera
4. Resultatet läggs som nytt lager

### Python API

```python
from g_etl.h3_query import query_polygon

results = query_polygon(
    polygon_wkt='POLYGON((...))',
    resolution=8,
    datasets=['naturreservat', 'biotopskydd']
)

print(f"Hittade {len(results)} objekt")
```

## Troubleshooting

### Problem: `mart.h3_index` finns inte

**Lösning:** Kör pipeline med merge:
```bash
task run
```

### Problem: Query returnerar 0 resultat

**Checka:**
1. Är polygon i rätt CRS? (SWEREF99 TM = EPSG:3006)
2. Används samma H3-resolution som data? (default = 8)
3. Finns data i området?

```sql
-- Testa query-celler
SELECT COUNT(*) FROM (
    SELECT UNNEST(g_h3_query_cells('POLYGON(...)', 8))
);
-- Ska returnera > 0
```

### Problem: Långsam query trots H3

**Checka:**
1. Finns index? `SELECT * FROM duckdb_indexes();`
2. Används JOIN (inte IN)? JOIN är snabbare för stora resultat
3. För många datasets? Filtrera med WHERE på dataset_id

## Avancerad: Dynamisk UNION ALL

För att automatiskt inkludera alla datasets i `mart.h3_index` (istället för manuell UNION ALL):

```sql
-- Generera UNION ALL dynamiskt från tabellnamn
PREPARE dynamic_union AS
SELECT id, source_id, $1 AS dataset_id, klass, grupp, typ, leverantor,
       h3_center, h3_cells, geom
FROM staging_005.$1;

-- Bygg query från lista
-- (Kräver dynamisk SQL-generering i Python)
```

Se [`src/g_etl/sql_generator.py`](../src/g_etl/sql_generator.py) för implementation.

## Referenser

- [H3 Hexagonal Hierarchical Geospatial Indexing System](https://h3geo.org/)
- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial)
- [G-ETL CLAUDE.md](../CLAUDE.md)
- [Exempel-queries](../sql/examples/h3_polygon_queries.sql)
