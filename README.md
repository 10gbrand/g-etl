# G-ETL – DuckDB ETL-stack för svenska geodata

En ETL-stack för svenska geodata med DuckDB som analytisk motor, H3 spatial indexering och plugin-baserad datahämtning.

## Pipeline-översikt

```mermaid
flowchart TB
    subgraph Extract["1. EXTRACT (Plugins)"]
        direction LR
        A1[zip_geopackage] --> R[(raw.*)]
        A2[zip_shapefile] --> R
        A3[wfs] --> R
        A4[geoparquet] --> R
        A5[lantmateriet] --> R
        A6[mssql] --> R
    end

    subgraph Staging["2. STAGING (SQL + H3)"]
        direction TB
        R --> S1["sql/staging/_common/<br/>00_staging_procedure.sql"]
        S1 --> S2["sql/staging/{dataset}/<br/>01_staging.sql<br/>(inkl. H3 via DuckDB extension)"]
        S2 --> ST[(staging.* med H3)]
    end

    subgraph Staging2["3. STAGING_2 (Normalisering)"]
        direction TB
        ST --> M1["sql/staging_2/_common/<br/>01_output_functions.sql"]
        M1 --> M2["sql/staging_2/{dataset}/<br/>01_mart_h3.sql"]
        M2 --> ST2[(staging_2.*)]
    end

    subgraph Mart["4. MART (Aggregering)"]
        direction TB
        ST2 --> M3["sql/mart/zzz_end/<br/>01_end.sql"]
        M3 --> MF[(mart.h3_cells)]
    end

    Extract --> Staging --> Staging2 --> Mart

    style Extract fill:#e1f5fe
    style Staging fill:#fff3e0
    style Staging2 fill:#fce4ec
    style Mart fill:#e8f5e9
```

## Detaljerat pipeline-flöde

### Steg 1: Extract (Plugins → raw.*)

Plugins laddar ner och läser in geodata till `raw`-schemat i DuckDB.

| Plugin | Källa | Format |
|--------|-------|--------|
| `zip_geopackage` | URL eller lokal fil | Zippad GeoPackage |
| `zip_shapefile` | URL eller lokal fil | Zippad Shapefile |
| `wfs` | OGC WFS-tjänst | GML/JSON |
| `geoparquet` | URL, S3 eller lokal fil | GeoParquet |
| `lantmateriet` | Lantmäteriets API | JSON |
| `mssql` | Microsoft SQL Server | ODBC |

**Resultat:** `raw.{dataset}` – rådata med originalkolumner och geometri.

### Steg 2: Staging (SQL + H3 → staging.*)

#### 2a. Common SQL
`sql/staging/_common/00_staging_procedure.sql` körs först och definierar makron.

#### 2b. Dataset SQL
`sql/staging/{dataset}/01_staging.sql` skapar staging-tabellen med:

- Validerad geometri (`geom`)
- Metadata: `_imported_at`, `_geom_md5`, `_attr_md5`, `_json_data`
- Centroid i WGS84: `_centroid_lat`, `_centroid_lng`
- **H3-index** (beräknas direkt i SQL via DuckDB H3 extension):
  - `_h3_index` (res 13, ~43 m²) – centroid-cell
  - `_h3_cells` (res 11, ~2149 m²) – alla celler inom polygonen
- Käll-ID: `_source_id_md5`

H3-beräkningen sker med DuckDB:s community extension:
```sql
h3_latlng_to_cell_string(lat, lng, 13) AS _h3_index,
to_json(h3_polygon_wkt_to_cells_string(wkt, 11)) AS _h3_cells
```

**Resultat:** `staging.{dataset}` – standardiserad data med H3-index.

### Steg 3: Staging_2 (SQL → staging_2.*)

Normaliserar alla dataset till en enhetlig struktur.

#### 3a. Common SQL
`sql/staging_2/_common/01_output_functions.sql` definierar makron för output.

#### 3b. Dataset SQL
`sql/staging_2/{dataset}/01_mart_h3.sql` skapar den normaliserade tabellen med:

```sql
SELECT
    _source_id_md5 AS id,
    source_id,           -- Käll-ID (t.ex. beteckn)
    klass,               -- Typ av skydd (biotopskydd, naturreservat, etc.)
    grupp,               -- Undergrupp
    typ,                 -- Specifik typ
    leverantor,          -- Dataleverantör (sks, nvv, sgu)
    h3_center,           -- H3-cell för centroid
    h3_cells,            -- Alla H3-celler inom polygonen
    json_data,           -- Originaldata som JSON
    data_1..data_5,      -- Extra datafält
    geom
FROM staging.{dataset}
```

**Resultat:** `staging_2.{dataset}` – normaliserade dataset med enhetlig struktur.

### Steg 4: Mart (SQL → mart.*)

Aggregerar alla dataset till en gemensam H3-tabell.

#### 4a. End SQL (zzz_end)
`sql/mart/zzz_end/01_end.sql` körs sist och skapar `mart.h3_cells`:

- Kombinerar alla `staging_2.*`-tabeller
- En rad per H3-cell
- Kolumner per dataset med klassificering

**Resultat:** `mart.h3_cells` – aggregerad tabell redo för analys och export.

## Körordning för SQL-filer

SQL-filer körs i **alfabetisk ordning** baserat på sökväg:

```
sql/staging/_common/00_staging_procedure.sql       ← Först (underscore sorteras före a-z)
sql/staging/avverkningsanmalningar/01_staging.sql
sql/staging/biotopskydd/01_staging.sql
...
sql/staging/vso/01_staging.sql

sql/staging_2/_common/01_output_functions.sql      ← Först i staging_2
sql/staging_2/avverkningsanmalningar/01_mart_h3.sql
sql/staging_2/biotopskydd/01_mart_h3.sql
...
sql/staging_2/vso/01_mart_h3.sql

sql/mart/zzz_end/01_end.sql                        ← Sist (zzz sorteras efter a-z)
```

## DuckDB-scheman

| Schema | Syfte |
|--------|-------|
| `raw` | Rå ingesterad data direkt från plugins |
| `staging` | Validerad geometri, metadata och H3-index |
| `staging_2` | Normaliserade dataset med enhetlig struktur |
| `mart` | Aggregerade tabeller (h3_cells) redo för analys och export |

## Projektstruktur

```
g-etl/
├── config/
│   ├── datasets.yml       # Dataset-konfiguration
│   └── settings.py        # Centrala inställningar (H3, CRS, etc.)
├── plugins/               # Datakälla-plugins
│   ├── base.py            # Basklass för plugins
│   ├── zip_geopackage.py  # Zippad GeoPackage (URL/lokal)
│   ├── zip_shapefile.py   # Zippad Shapefile
│   ├── wfs.py             # OGC WFS-tjänster
│   ├── geoparquet.py      # GeoParquet-filer
│   ├── lantmateriet.py    # Lantmäteriets API
│   └── mssql.py           # Microsoft SQL Server
├── sql/
│   ├── _init/             # Databas-initiering
│   ├── staging/           # Staging SQL per dataset
│   │   ├── _common/       # Gemensamma makron
│   │   └── {dataset}/     # Dataset-specifik SQL
│   ├── staging_2/         # Normalisering per dataset
│   │   ├── _common/       # Gemensamma funktioner
│   │   └── {dataset}/     # Dataset-specifik SQL
│   └── mart/              # Aggregering
│       └── zzz_end/       # Slutskript (körs sist)
├── scripts/
│   ├── admin/             # Textual TUI-applikation
│   │   └── services/
│   │       ├── pipeline_runner.py    # Pipeline-körare
│   │       └── staging_processor.py  # Staging-processor
│   ├── pipeline.py        # CLI pipeline-runner
│   └── export_h3.py       # Export av H3-data
├── data/                  # DuckDB-databaser
└── logs/                  # Pipeline-loggar
```

## Komma igång

### Förutsättningar

- Python 3.11+
- UV (pakethanterare)
- Docker (valfritt)

### Installation

```bash
# Installera dependencies
task py:install

# Initiera databas
task db:init
```

### Köra pipelinen

```bash
# Hela pipelinen (extract + transform)
task run

# Endast extract
task pipeline:extract

# Endast transform (staging + staging_2 + mart SQL)
task pipeline:transform

# Specifikt dataset
task pipeline:dataset -- sksbiotopskydd

# Datasets av viss typ
task pipeline:type -- skogsstyrelsen_gpkg

# Lista tillgängliga typer
task pipeline:types
```

### Exportera H3-data

```bash
# CSV för Kepler.gl
task pipeline:export:kepler

# GeoJSON med H3-polygoner
task pipeline:export:geojson

# Interaktiv HTML-karta (Folium)
task pipeline:export:html

# GeoParquet för QGIS/analys
task pipeline:export:parquet
```

### Admin TUI

```bash
# Starta TUI
task admin:run

# Med mockdata (för test)
task admin:mock
```

### DuckDB CLI

```bash
# Öppna REPL
task db:cli

# Exempel-queries
SELECT COUNT(*) FROM mart.h3_cells;
SELECT * FROM mart.sksbiotopskydd LIMIT 10;
```

## Dataset

Dataset konfigureras i `config/datasets.yml`:

```yaml
datasets:
  - id: sksbiotopskydd
    name: Biotopskydd SKS
    description: Biotopskydd från Skogsstyrelsen
    plugin: zip_geopackage
    url: https://geodpags.skogsstyrelsen.se/.../sksBiotopskydd_gpkg.zip
    enabled: true

  - id: giss_gavd
    name: GISS GAVD
    plugin: zip_geopackage
    url: /Volumes/T9/spring/GISS.gpkg.zip  # Lokal fil
    layer: GAVD
    enabled: false
```

## H3 Spatial Index

Projektet använder [H3](https://h3geo.org/) för spatial indexering:

| Inställning | Värde | Cellstorlek |
|-------------|-------|-------------|
| `H3_RESOLUTION` | 13 | ~43 m² (centroid) |
| `H3_POLYFILL_RESOLUTION` | 11 | ~2149 m² (polyfill) |

H3-celler möjliggör snabba spatial joins och aggregeringar utan geometriberäkningar.

## Koordinatsystem

**OBS:** DuckDB:s spatial extension har en bugg med EPSG-koder. Använd PROJ4-strängar:

```sql
-- Korrekt (PROJ4)
ST_Transform(geom,
    '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs',
    '+proj=longlat +datum=WGS84 +no_defs')

-- Fel (EPSG) - ger felaktiga koordinater!
ST_Transform(geom, 'EPSG:3006', 'EPSG:4326')
```

## Utveckling

### Lägga till nytt dataset

1. Lägg till konfiguration i `config/datasets.yml`
2. Skapa `sql/staging/{dataset}/01_staging.sql`
3. Skapa `sql/staging_2/{dataset}/01_mart_h3.sql`
4. Uppdatera `sql/mart/zzz_end/01_end.sql` om det ska ingå i h3_cells

### Lägga till ny plugin

1. Skapa `plugins/{namn}.py` som ärver från `SourcePlugin`
2. Implementera `extract(config, conn, on_log, on_progress)`
3. Registrera i `plugins/__init__.py`

Se `plugins/README.md` för detaljerad dokumentation.
