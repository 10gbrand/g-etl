# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

G-ETL är en containerbaserad ETL-stack för svenska geodata och HTTP-baserade källor. Projektet använder DuckDB som analytisk SQL-motor med plugin-baserad datahämtning och ren SQL för transformationer. Dokumentation och kommentarer skrivs på svenska.

## Commands

Projektet använder Go Task som task runner. Alla kommandon körs med `task <kommando>`.

**Pipeline:**
- `task run` - Kör hela pipelinen (extract + transform)
- `task pipeline:extract` - Kör bara extract (hämta data)
- `task pipeline:transform` - Kör bara SQL-transformationer
- `task pipeline:dataset -- <id>` - Kör specifikt dataset
- `task pipeline:type -- <typ>` - Kör datasets av viss typ
- `task pipeline:types` - Lista tillgängliga dataset-typer

**Export:**

- `task pipeline:export:kepler` - Exportera H3-data till CSV för Kepler.gl
- `task pipeline:export:geojson` - Exportera H3-data till GeoJSON
- `task pipeline:export:html` - Exportera H3-data till interaktiv HTML-karta
- `task pipeline:export:parquet` - Exportera H3-data till GeoParquet
- `task pipeline:export:gpkg` - Exportera H3-data till GeoPackage (bäst för QGIS)
- `task pipeline:export:fgb` - Exportera H3-data till FlatGeobuf (snabb streaming)

**Python:**
- `task py:install` - Installera dependencies med UV
- `task py:lint` - Kör ruff linting
- `task py:format` - Formatera kod med ruff
- `task py:fix` - Fixa formatering och lint-fel automatiskt
- `task py:test` - Kör alla tester
- `task py:ci` - Kör samma kontroller som CI (format check, lint, test)
- `task py:precommit` - **Kör innan commit** (fix → test)

**Database:**
- `task db:cli` - Öppna DuckDB REPL
- `task db:migrate` - Kör väntande statiska migrationer (001-003)
- `task db:migrate:status` - Visa status för alla migrationer
- `task db:migrate:rollback` - Rulla tillbaka senaste statiska migreringen
- `task db:migrate:create -- <namn>` - Skapa ny migreringsfil

**Admin TUI:**
- `task admin:run` - Starta admin TUI
- `task admin:mock` - Starta med mockdata (för test)
- `task admin:docker` - Starta admin TUI i Docker
- `task admin:huey` - Starta Huey (DuckDB data explorer) i Docker
- `task admin:huey:stop` - Stoppa Huey
- `task admin:huey:rebuild` - Bygg om Huey-imagen

**Docker:**
- `task docker:up` - Starta containers
- `task docker:down` - Stoppa containers
- `task docker:build` - Bygg images

**Release:**
- `task release:check` - Kontrollera status innan release
- `task release:new -- v0.1.3` - Skapa GitHub Release (triggar Nuitka-bygge)
- `task release:trigger -- v0.1.2` - Trigga bygge för befintlig tag
- `task release:delete -- v0.1.2` - Ta bort release och tag

**QGIS Plugin:**

- `task qgis:build` - Bygg plugin-zip för distribution
- `task qgis:install` - Installera plugin direkt till QGIS (för utveckling)
- `task qgis:check` - Kontrollera plugin-installation

## Architecture

Tre gränssnitt delar samma `PipelineRunner` för parallell exekvering:

```text
CLI (pipeline.py) ─────┐
                       │
TUI (admin/app.py) ────┼──→ PipelineRunner → Plugins → DuckDB
                       │                          ↓
QGIS Plugin ───────────┘                   Parallell Transform
                                           (temp-DB per dataset)
                                                  ↓
                                           Merge → warehouse.duckdb
                                                  ↓
                                           Post-merge SQL (*_merged.sql)
                                                  ↓
                                           Export (GeoPackage/Parquet/FGB)
```

**Styrkor per gränssnitt:**

| Gränssnitt | Styrka | Användning |
|------------|--------|------------|
| **CLI** | Schemaläggning, batch, CI/CD | `task run`, cron-jobb |
| **TUI** | Interaktiv kontroll, progress | `task admin:run` |
| **QGIS** | Kartanalys, visualisering | Plugin i QGIS |

**Parallell Transform-arkitektur:**

Varje dataset processas i en egen temporär DuckDB-fil för äkta parallelism.
Varje temp-DB initieras automatiskt med extensions, scheman och makron (001-003):

```text
Extract (parallellt):
├── dataset1 → parquet
├── dataset2 → parquet
└── dataset3 → parquet

Transform (parallellt, en temp-DB per dataset):
├── dataset1.duckdb: init(001-003) → raw → staging_004 → [pipeline-templates] → mart
├── dataset2.duckdb: init(001-003) → raw → staging_004 → [pipeline-templates] → mart
└── dataset3.duckdb: init(001-003) → raw → staging_004 → [pipeline-templates] → mart

Merge:
└── warehouse.duckdb ← alla temp-DBs (raw + mart)

Post-merge:
├── Pipeline-merged (*_merged.sql per pipeline-katalog)
└── Post-pipeline (x*.sql i roten)
```

**Dataflöde genom DuckDB-scheman:**
- `raw/` - Rå ingesterad data från plugins
- `staging_004/` - Validering, metadata och H3-indexering (delad staging)
- `staging_{pipeline}_{NNN}/` - Pipeline-specifika staging-scheman (t.ex. `staging_ext_restr_001`)
- `mart/` - Aggregerade tabeller (SQL)

OBS: Staging-scheman skapas dynamiskt. Root-templates genererar `staging_NNN`, pipeline-templates genererar `staging_{pipeline}_{NNN}`.

**Nyckelkomponenter:**

- `src/g_etl/admin/services/pipeline_runner.py` - **Gemensam pipeline-logik** (parallell extract/transform/merge)
- `src/g_etl/pipeline.py` - CLI-wrapper som anropar PipelineRunner
- `src/g_etl/admin/app.py` - TUI-applikation (Textual) som anropar PipelineRunner
- `src/g_etl/plugins/` - Datakälla-plugins (wfs, lantmateriet, geoparquet, zip_geopackage, zip_shapefile, mssql)
- `src/g_etl/sql_generator.py` - Genererar SQL från mallar + datasets.yml
- `src/g_etl/export.py` - Export (CSV, GeoJSON, HTML, Parquet, GeoPackage, FlatGeobuf)
- `qgis_plugin/` - QGIS-plugin (ren Python, installerar dependencies automatiskt)
- `src/g_etl/migrations/` - Migreringssystem (Migrator, CLI)
- `sql/migrations/` - Alla SQL-filer (init + templates)
- `config/datasets.yml` - Dataset-konfiguration med plugin-parametrar
- `src/g_etl/settings.py` - Centrala inställningar (H3-resolution, CRS, parallelism)

**Heatmap-visualisering (optional):**

Data Explorer i TUI:n stödjer heatmap-rendering med bakgrundskarta.
Installera `uv sync --extra viz` för att aktivera (matplotlib, contextily, rich-pixels, Pillow).
Tangent `h` i Explorer-skärmen växlar mellan braille-karta och heatmap.
Pipeline: DuckDB → matplotlib hexbin + contextily basemap → PNG → rich-pixels halfblock → Textual.

**Auto-detekterad parallelism (settings.py):**

```python
MAX_CONCURRENT_EXTRACTS  # = cpu_count() för I/O-bound extract
MAX_CONCURRENT_SQL       # = cpu_count() // 2 för CPU-bound SQL (DuckDB paralleliserar internt)
```

**Migreringsspårning:**

Pipeline:n integrerar med migreringssystemet för att spåra körda SQL-filer:

- **Statiska migrationer (001-003)**: Spåras som `001`, `002`, `003` i `_migrations`
- **Root-template-migrationer (004+)**: Spåras per dataset som `004:dataset_id`
- **Pipeline-template-migrationer**: Spåras som `aab_ext_restr/001:dataset_id`

När en template redan är körd för ett dataset hoppas den över (om inte `force=True`).

**SQL-struktur (multi-pipeline):**

```text
sql/migrations/
├── 001_db_extensions.sql                    # Init: DuckDB extensions
├── 002_db_schemas.sql                       # Init: bas-scheman raw, mart
├── 003_db_makros.sql                        # Init: SQL-makron
├── 004_staging_transform_template.sql       # Delad staging (alla datasets)
├── aaa_avdelning/                           # Pipeline "avdelning"
│   ├── 001_*_template.sql                   # Pipeline-specifika templates
│   └── 100_*_merged.sql                     # Pipeline-merged SQL
├── aab_ext_restr/                           # Pipeline "ext_restr"
│   ├── 001_staging_normalisering_template.sql
│   ├── 002_mart_h3_cells_template.sql
│   ├── 003_mart_compact_h3_cells_template.sql
│   └── 100_mart_h3_index_merged.sql
├── x01_*.sql                                # Post-pipeline (efter alla merges)
└── x02_*.sql
```

**Exekveringsordning per dataset:**
1. Init (001-003) → extensions, scheman, makron
2. Delad staging (004) → `staging_004`
3. Pipeline-templates → pipeline-specifika scheman
4. Merge → kopiera raw + mart till warehouse
5. Pipeline-merged → `*_merged.sql` per pipeline-katalog
6. Post-pipeline → `x*.sql` i roten (sist)

**Multi-pipeline:**

Varje dataset tillhör exakt en pipeline via `pipeline:`-fältet i `datasets.yml`.
Pipeline-kataloger namnges `{prefix}_{pipeline}` där prefix (t.ex. `aaa`, `aab`) styr körordning.

**Dynamiska staging-scheman:**

Schemanamnet genereras från SQL-filens nummer och pipeline:
- Root: `004_staging_*.sql` → schema `staging_004`
- Pipeline: `001_staging_*.sql` (ext_restr) → schema `staging_ext_restr_001`
- `NNN_mart_*.sql` → schema `mart` (alltid)

**SQL-mallar (generiskt system):**

Templates (`*_template.sql`) körs automatiskt per dataset med parametrar från `datasets.yml`.
Pipeline-fasen bestäms av template-namnet:

| Namnmönster               | Schema        | Beskrivning                                |
| ------------------------- | ------------- | ------------------------------------------ |
| `NNN_staging_*`           | staging_NNN   | Validering, metadata, H3-indexering        |
| `NNN_mart_*`              | mart          | Aggregerade tabeller, H3-celler            |

Nya templates plockas upp automatiskt utan kodändringar:

```sql
-- 008_mart_example_template.sql (fungerar automatiskt!)
CREATE OR REPLACE TABLE {{ schema }}.example_{{ dataset_id }} AS
SELECT * FROM {{ prev_schema }}.{{ dataset_id }}
WHERE '{{ klass }}' = 'naturreservat';
```

**Post-merge SQL (`*_merged.sql` och `x*.sql`):**

Tre typer av post-merge SQL körs i ordning:
1. **Pipeline-merged** (`aab_ext_restr/100_merged.sql`): Per pipeline-katalog
2. **Root-merged** (`100_merged.sql`): Root-nivå (bakåtkompatibilitet)
3. **Post-pipeline** (`x01_*.sql`): Globala aggregeringar, körs sist

**Tillgängliga variabler:**

- `{{ schema }}` - Aktuellt schema (t.ex. `staging_004`, `staging_ext_restr_001`, `mart`)
- `{{ prev_schema }}` - Föregående schema (t.ex. `raw`, `staging_004`, `staging_ext_restr_001`)
- `{{ dataset_id }}` - Dataset-ID
- `{{ source_id_column }}` - Kolumn för käll-ID
- `{{ klass }}`, `{{ leverantor }}` - Från field_mapping (literaler)
- `{{ grupp_expr }}`, `{{ typ_expr }}` - SQL-uttryck (kolumnref eller literal)
- `{{ h3_center_resolution }}`, `{{ h3_polyfill_resolution }}` - H3-inställningar

**field_mapping syntax i datasets.yml:**

```yaml
pipeline: ext_restr            # Pipeline (matchar underkatalog, t.ex. aab_ext_restr/)
field_mapping:
  source_id_column: $beteckn   # $prefix = kolumnreferens
  klass: biotopskydd           # utan prefix = literal sträng
  grupp: $Biotyp               # $prefix = hämta från kolumn "Biotyp"
  typ: skyddad_natur           # literal sträng "skyddad_natur"
  leverantor: sks
```

**SQL-makron (003_db_makros.sql):**

Återanvändbara makron med prefix `g_` för att särskilja från standard SQL:

| Makro                                  | Beskrivning                              |
| -------------------------------------- | ---------------------------------------- |
| `g_to_wgs84(geom)`                     | Transformera från SWEREF99 TM till WGS84 |
| `g_validate_geom(geom)`                | Validera/reparera geometri               |
| `g_centroid_lat/lng(geom)`             | Hämta centroid-koordinater i WGS84       |
| `g_h3_center(geom, res)`               | H3-cell för centroid                     |
| `g_h3_polygon_cells(geom, res)`        | H3-celler för polygon (polyfill)         |
| `g_h3_line_cells(geom, buffer, res)`   | H3-celler för linje (med buffer)         |
| `g_h3_cell_to_geom(h3_cell)`           | Konvertera H3-cell till SWEREF99-polygon |

## Plugins

Nya datakällor läggs till som plugins i `src/g_etl/plugins/`. Varje plugin:
1. Ärver från `SourcePlugin` i `g_etl.plugins.base`
2. Implementerar `extract(config, conn, on_log)` metoden
3. Registreras i `g_etl/plugins/__init__.py`

**Trådsäker URL-hantering:**

Plugins som laddar ner filer (t.ex. `zip_geopackage`) hanterar parallella nedladdningar trådsäkert:

- Samma URL laddas bara ner en gång (med `threading.Lock()` per URL)
- Varje dataset extraherar till sin egen katalog för att undvika race conditions
- Cache rensas automatiskt efter körning

## QGIS Plugin

QGIS-pluginet (`qgis_plugin/`) är ett självständigt plugin som inkluderar all nödvändig kod.

**Installation:**

1. Ladda ner `g_etl_qgis-<version>.zip` från GitHub Releases
2. I QGIS: Tillägg → Hantera och installera tillägg → Installera från ZIP

**Arkitektur:**

- Ren Python (ingen binär)
- Installerar dependencies automatiskt vid första körning (duckdb, h3, etc.)
- Delar samma `PipelineRunner` som CLI/TUI
- Exporterar till GeoPackage/GeoParquet/FlatGeobuf för QGIS-kompatibilitet

**Utveckling:**

```text
qgis_plugin/
├── __init__.py         # Plugin entry point
├── metadata.txt        # QGIS plugin metadata (version uppdateras vid bygge)
├── g_etl_plugin.py     # Huvudklass, meny, toolbar
├── dialog.py           # Qt-dialoger
├── deps.py             # Dependency-hantering
└── runner.py           # Wrapper för PipelineRunner
```

Vid release byggs pluginet automatiskt och laddas upp till GitHub Releases.
Version synkas automatiskt med release-taggen.

## Huey (DuckDB Data Explorer)

Huey är en webbbaserad DuckDB-utforskare som körs i Docker för att enkelt inspektera data.

**Starta:**

```bash
task admin:huey
```

Öppnar på <http://localhost:8080>. Dra-och-släpp filer för att utforska:

- `/data/warehouse.duckdb` - Hela datalagret
- `/data/output/*.parquet` - Exporterade Parquet-filer

**Docker-setup:**

- [docker/huey/Dockerfile](docker/huey/Dockerfile) - Minimal nginx:alpine med Huey från GitHub
- Data-katalogen monteras som `/data` (read-only)

## Working Guidelines

- Presentera alltid ett förslag på lösning innan du genomför ändringar
- Uppdatera dokumentation löpande
- Lägg till användbara kommandon i taskfiles löpande
- Använd GeoParquet som lagringsformat för geodata om inte annat anges
