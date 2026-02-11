# Arkitektur

Pipeline-översikt och detaljerad dokumentation av G-ETL:s arkitektur.

## Pipeline-översikt

```mermaid
flowchart TB
    subgraph Extract["1. EXTRACT (Parallellt)"]
        direction LR
        A1[zip_geopackage] --> P1[(parquet)]
        A2[zip_shapefile] --> P2[(parquet)]
        A3[geoparquet] --> P3[(parquet)]
    end

    subgraph Transform["2. PARALLELL TRANSFORM (temp-DB per dataset)"]
        direction TB
        P1 --> T1["dataset1.duckdb<br/>raw→staging_004→[pipeline]→mart"]
        P2 --> T2["dataset2.duckdb<br/>raw→staging_004→[pipeline]→mart"]
        P3 --> T3["dataset3.duckdb<br/>raw→staging_004→[pipeline]→mart"]
    end

    subgraph Merge["3. MERGE"]
        direction TB
        T1 --> W[(warehouse.duckdb)]
        T2 --> W
        T3 --> W
    end

    subgraph PostMerge["4. POST-MERGE SQL"]
        direction TB
        W --> PM["pipeline *_merged.sql"]
        PM --> XP["x*.sql post-pipeline"]
    end

    Extract --> Transform --> Merge --> PostMerge

    style Extract fill:#e1f5fe
    style Transform fill:#fff3e0
    style Merge fill:#fce4ec
    style PostMerge fill:#e8f5e9
```

## Parallell arkitektur

Varje dataset processas i en **egen temporär DuckDB-fil** för äkta parallelism utan fillåsning:

| Fas        | Parallelism        | Beskrivning                              |
| ---------- | ------------------ | ---------------------------------------- |
| Extract    | `cpu_count()`      | I/O-bound, alla kärnor                   |
| Transform  | `cpu_count() // 2` | CPU-bound, DuckDB paralleliserar internt |
| Merge      | Sekventiell        | Kombinerar temp-DBs                      |
| Post-merge | Sekventiell        | Aggregeringar över alla datasets         |

```text
┌─────────────────────────────────────────────────────────────────┐
│  EXTRACT (parallellt, cpu_count() workers)                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ dataset1 │  │ dataset2 │  │ dataset3 │  │ dataset4 │  ...    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
│       │             │             │             │               │
│       ▼             ▼             ▼             ▼               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ .parquet │  │ .parquet │  │ .parquet │  │ .parquet │         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
└───────┼─────────────┼─────────────┼─────────────┼───────────────┘
        │             │             │             │
        ▼             ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────┐
│  TRANSFORM (parallellt, cpu_count()//2 workers)                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ temp1.db │  │ temp2.db │  │ temp3.db │  │ temp4.db │  ...    │
│  │004+pipe  │  │004+pipe  │  │004+pipe  │  │004+pipe  │         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
└───────┼─────────────┼─────────────┼─────────────┼───────────────┘
        │             │             │             │
        └─────────────┴──────┬──────┴─────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  MERGE (sekventiell)                                            │
│  └── warehouse.duckdb ← alla temp-DBs                           │
└───────────────────────────────┬─────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  POST-MERGE (sekventiell)                                       │
│  ├── pipeline/*_merged.sql → per-pipeline aggregeringar         │
│  └── x*.sql → post-pipeline globala aggregeringar               │
└─────────────────────────────────────────────────────────────────┘
```

## Detaljerat pipeline-flöde

### Steg 1: Extract (Plugins → raw.*)

Plugins laddar ner och läser in geodata till `raw`-schemat i DuckDB.

| Plugin | Källa | Format | status |
|--------|-------|--------|--------|
| `zip_geopackage` | URL eller lokal fil | Zippad GeoPackage | Klart |
| `zip_shapefile` | URL eller lokal fil | Zippad Shapefile | Klart |
| `wfs` | OGC WFS-tjänst | GML/JSON | Plan |
| `geoparquet` | URL, S3 eller lokal fil | GeoParquet | Plan |
| `lantmateriet` | Lantmäteriets API | JSON | Plan |
| `mssql` | Microsoft SQL Server | ODBC | Plan |

**Resultat:** `raw.{dataset}` – rådata med originalkolumner och geometri.

### Steg 2: Staging (SQL + H3 → staging.*)

#### 2a. Makron från migrering
`sql/migrations/003_db_makros.sql` definierar makron för H3-beräkning och
geometrihantering. Dessa installeras automatiskt när pipelinen körs.

#### 2b. Genererad SQL
`004_staging_transform_template.sql` renderas med värden från `field_mapping:` i `datasets.yml`.
Staging-tabellen skapas med:

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

**Resultat:** `staging_004.{dataset}` – standardiserad data med H3-index.

### Steg 3: Pipeline-templates (SQL → staging_{pipeline}_NNN.*, mart.*)

Varje dataset tillhör en pipeline (t.ex. `ext_restr`) angiven i `datasets.yml`.
Pipeline-specifika templates körs efter delade root-templates.

**ext_restr-pipelinen:**
- `001_staging_normalisering_template.sql` → `staging_ext_restr_001`: Normaliserad struktur
- `002_mart_h3_cells_template.sql` → `mart`: Exploderade H3-celler
- `003_mart_compact_h3_cells_template.sql` → `mart`: Kompakterade H3-celler

**Resultat:** `staging_ext_restr_001.{dataset}` + `mart.{dataset}_h3`

### Steg 4: Merge + Post-merge

1. **Merge:** Kopierar `raw` och `mart` från alla temp-DBs till warehouse.duckdb
2. **Pipeline-merged:** `aab_ext_restr/100_merged.sql` → H3-index aggregering
3. **Post-pipeline:** `x*.sql` filer (globala aggregeringar)

**Resultat:** `mart.*` – aggregerade tabeller redo för analys och export.

## Transform-pipeline

Transformationer körs parallellt med separata temp-databaser per dataset:

```
1. Extract (parallellt)
   └── Plugins → parquet-filer (en per dataset)

2. Parallell Transform (temp-DB per dataset)
   ├── dataset1.duckdb ──┐  Root: 004_staging_transform_template
   ├── dataset2.duckdb ──┼── Pipeline: aab_ext_restr/001 → 002 → 003
   └── dataset3.duckdb ──┘

3. Merge
   └── Kombinera alla temp-DBs → warehouse.duckdb

4. Post-merge SQL
   ├── aab_ext_restr/100_merged.sql → Pipeline-specifika aggregeringar
   └── x*.sql → Globala post-pipeline
```

### SQL-templates

Templates (`*_template.sql`) körs automatiskt per dataset:

**Root (alla datasets):**

| Fil                                       | Schema       | Beskrivning               |
| ----------------------------------------- | ------------ | ------------------------- |
| `004_staging_transform_template.sql`      | staging_004  | Validering, MD5, H3-index |

**Pipeline ext_restr:**

| Fil                                       | Schema                | Beskrivning               |
| ----------------------------------------- | --------------------- | ------------------------- |
| `aab_ext_restr/001_staging_normalisering_template.sql` | staging_ext_restr_001 | Normaliserad struktur     |
| `aab_ext_restr/002_mart_h3_cells_template.sql`         | mart                  | Exploderade H3-celler     |
| `aab_ext_restr/003_mart_compact_h3_cells_template.sql` | mart                  | Kompakterade H3-celler    |

```text
Template-rendering:

┌─────────────────────────────────────────────────────────────────┐
│  datasets.yml                                                   │
│  └── field_mapping:                                             │
│        source_id_column: $beteckn                               │
│        klass: biotopskydd                                       │
│        grupp: $Biotyp                                           │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  SQLGenerator.render_template()                                 │
│  ├── Läs template: 004_staging_transform_template.sql           │
│  ├── Ersätt {{ dataset_id }} → "sksbiotopskydd"                 │
│  ├── Ersätt {{ source_id_column }} → "beteckn"                  │
│  ├── Ersätt {{ klass }} → "biotopskydd"                         │
│  └── Ersätt {{ grupp_expr }} → "COALESCE(s.Biotyp::VARCHAR,'')" │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Renderad SQL                                                   │
│  └── CREATE TABLE staging.sksbiotopskydd AS ...                 │
└─────────────────────────────────────────────────────────────────┘
```

### Post-merge SQL

Tre typer körs i ordning efter merge:

1. **Pipeline-merged** (`aab_ext_restr/100_merged.sql`): Per pipeline-katalog
2. **Root-merged** (`100_merged.sql`): Root-nivå
3. **Post-pipeline** (`x01_*.sql`): Globala aggregeringar, körs sist

## DuckDB-scheman

| Schema                    | Syfte                                          |
| ------------------------- | ---------------------------------------------- |
| `raw`                     | Rå ingesterad data direkt från plugins          |
| `staging_004`             | Validerad geometri, metadata och H3-index       |
| `staging_{pipeline}_{N}`  | Pipeline-specifik staging (t.ex. normalisering) |
| `mart`                    | Aggregerade tabeller redo för analys och export  |

```text
Dataflöde genom scheman (per dataset, pipeline=ext_restr):

┌─────────────────────────────────────────────────────────────────┐
│  raw.{dataset}                                                  │
│  └── Originaldata från plugin (alla kolumner, geometri)        │
└───────────────────────────────┬─────────────────────────────────┘
                                │ 004_staging_transform_template.sql
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  staging_004.{dataset}                                          │
│  ├── Validerad geometri (geom)                                  │
│  ├── Metadata (_imported_at, _geom_md5, _attr_md5)              │
│  ├── H3-index (_h3_index, _h3_cells)                            │
│  └── JSON-data (_json_data)                                     │
└───────────────────────────────┬─────────────────────────────────┘
                                │ aab_ext_restr/001_staging_normalisering
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  staging_ext_restr_001.{dataset}                                │
│  ├── Normaliserade fält (klass, grupp, typ, leverantor)         │
│  ├── H3 (h3_center, h3_cells)                                   │
│  └── Extra data (data_1..data_5)                                │
└───────────────────────────────┬─────────────────────────────────┘
                                │ aab_ext_restr/002+003_mart_*
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  mart.{dataset}_h3                                              │
│  └── En rad per H3-cell med geometri                            │
│                                                                 │
│  mart.{dataset}_h3_compact                                      │
│  └── Kompakterade H3-celler (färre rader)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Projektstruktur

```
g-etl/
├── config/
│   └── datasets.yml       # Dataset-konfiguration
├── src/g_etl/             # Python-paket
│   ├── settings.py        # Inställningar (H3, CRS, parallelism)
│   ├── plugins/           # Datakälla-plugins
│   │   ├── base.py        # Basklass för plugins
│   │   ├── zip_geopackage.py
│   │   ├── zip_shapefile.py
│   │   ├── wfs.py
│   │   ├── geoparquet.py
│   │   ├── lantmateriet.py
│   │   └── mssql.py
│   ├── admin/             # Textual TUI-applikation
│   │   ├── screens/       # TUI-skärmar
│   │   └── services/
│   │       └── pipeline_runner.py  # GEMENSAM parallell pipeline-logik
│   ├── migrations/        # Migreringssystem
│   │   ├── cli.py
│   │   └── migrator.py
│   ├── pipeline.py        # CLI (anropar PipelineRunner)
│   ├── sql_generator.py   # Genererar SQL från templates
│   └── export.py          # Export av data
├── sql/
│   └── migrations/        # Alla SQL-filer
│       ├── 001_db_extensions.sql           # Init: extensions
│       ├── 002_db_schemas.sql              # Init: scheman
│       ├── 003_db_makros.sql               # Init: makron
│       ├── 004_staging_transform_template.sql  # Delad staging
│       ├── aaa_avdelning/                  # Pipeline "avdelning"
│       ├── aab_ext_restr/                  # Pipeline "ext_restr"
│       │   ├── 001_staging_normalisering_template.sql
│       │   ├── 002_mart_h3_cells_template.sql
│       │   ├── 003_mart_compact_h3_cells_template.sql
│       │   └── 100_mart_h3_index_merged.sql
│       └── x*.sql                          # Post-pipeline
├── data/
│   ├── raw/               # Parquet-filer från extract
│   ├── temp/              # Temporära per-dataset DBs
│   └── warehouse.duckdb   # Slutlig databas
└── logs/                  # Pipeline-loggar
```

**CLI och TUI delar samma `PipelineRunner`** i `src/g_etl/admin/services/pipeline_runner.py`.
Det finns ingen duplicerad pipeline-logik.

```text
CLI/TUI-arkitektur:

┌──────────────────────┐     ┌──────────────────────┐
│  CLI (pipeline.py)   │     │  TUI (admin/app.py)  │
│  └── task run        │     │  └── Textual UI      │
└──────────┬───────────┘     └──────────┬───────────┘
           │                            │
           └────────────┬───────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│  PipelineRunner (admin/services/pipeline_runner.py)             │
│  ├── run_parallel_extract()    # Parallell datahämtning        │
│  ├── run_parallel_transform()  # Parallell SQL-transformering  │
│  ├── merge_databases()         # Kombinera temp-DBs            │
│  └── run_merged_sql()          # Post-merge aggregeringar      │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Plugins + SQLGenerator + Migrator                              │
│  └── Gemensam logik för all pipeline-körning                    │
└─────────────────────────────────────────────────────────────────┘
```

## H3 Spatial Index

Projektet använder [H3](https://h3geo.org/) för spatial indexering:

| Inställning | Värde | Cellstorlek |
|-------------|-------|-------------|
| `H3_RESOLUTION` | 13 | ~43 m² (centroid) |
| `H3_POLYFILL_RESOLUTION` | 11 | ~2149 m² (polyfill) |

H3-celler möjliggör snabba spatial joins och aggregeringar utan geometriberäkningar.

## Parallelism

Parallelliteten auto-detekteras baserat på antal CPU-kärnor (`src/g_etl/settings.py`):

```python
MAX_CONCURRENT_EXTRACTS = cpu_count()      # I/O-bound: alla kärnor
MAX_CONCURRENT_SQL = cpu_count() // 2      # CPU-bound: halva (DuckDB paralleliserar internt)
```

Temporära databaser sparas i `data/temp/` och rensas automatiskt efter merge.

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
