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

**Docker:**
- `task docker:up` - Starta containers
- `task docker:down` - Stoppa containers
- `task docker:build` - Bygg images

**Release:**
- `task release:check` - Kontrollera status innan release
- `task release:new -- v0.1.3` - Skapa GitHub Release (triggar Nuitka-bygge)
- `task release:trigger -- v0.1.2` - Trigga bygge för befintlig tag
- `task release:delete -- v0.1.2` - Ta bort release och tag

## Architecture

CLI och TUI delar samma `PipelineRunner` för parallell exekvering:

```text
CLI (pipeline.py) ─────┐
                       ├──→ PipelineRunner → Plugins → DuckDB
TUI (admin/app.py) ────┘                          ↓
                                           Parallell Transform
                                           (temp-DB per dataset)
                                                  ↓
                                           Merge → warehouse.duckdb
                                                  ↓
                                           Post-merge SQL (*_merged.sql)
```

**Parallell Transform-arkitektur:**

Varje dataset processas i en egen temporär DuckDB-fil för äkta parallelism.
Varje temp-DB initieras automatiskt med extensions, scheman och makron (001-003):

```text
Extract (parallellt):
├── dataset1 → parquet
├── dataset2 → parquet
└── dataset3 → parquet

Transform (parallellt, en temp-DB per dataset):
├── dataset1.duckdb: init(001-003) → raw → staging → staging_2 → mart
├── dataset2.duckdb: init(001-003) → raw → staging → staging_2 → mart
└── dataset3.duckdb: init(001-003) → raw → staging → staging_2 → mart

Merge:
└── warehouse.duckdb ← alla temp-DBs

Post-merge:
└── Kör *_merged.sql (aggregeringar över alla datasets)
```

**Dataflöde genom DuckDB-scheman:**
- `raw/` - Rå ingesterad data från plugins
- `staging/` - Validering, metadata och H3-indexering (SQL)
- `staging_2/` - Normaliserade dataset med enhetlig struktur (SQL)
- `mart/` - Aggregerade tabeller (SQL)

**Nyckelkomponenter:**

- `src/g_etl/admin/services/pipeline_runner.py` - **Gemensam pipeline-logik** (parallell extract/transform/merge)
- `src/g_etl/pipeline.py` - CLI-wrapper som anropar PipelineRunner
- `src/g_etl/admin/app.py` - TUI-applikation (Textual) som anropar PipelineRunner
- `src/g_etl/plugins/` - Datakälla-plugins (wfs, lantmateriet, geoparquet, zip_geopackage, zip_shapefile, mssql)
- `src/g_etl/sql_generator.py` - Genererar SQL från mallar + datasets.yml
- `src/g_etl/export_h3.py` - Export av H3-data (CSV, GeoJSON, HTML, Parquet)
- `src/g_etl/migrations/` - Migreringssystem (Migrator, CLI)
- `sql/migrations/` - Alla SQL-filer (init + templates)
- `config/datasets.yml` - Dataset-konfiguration med plugin-parametrar
- `src/g_etl/settings.py` - Centrala inställningar (H3-resolution, CRS, parallelism)

**Auto-detekterad parallelism (settings.py):**

```python
MAX_CONCURRENT_EXTRACTS  # = cpu_count() för I/O-bound extract
MAX_CONCURRENT_SQL       # = cpu_count() // 2 för CPU-bound SQL (DuckDB paralleliserar internt)
```

**Migreringsspårning:**

Pipeline:n integrerar med migreringssystemet för att spåra körda SQL-filer:

- **Statiska migrationer (001-003)**: Spåras som `001`, `002`, `003` i `_migrations`
- **Template-migrationer (004+)**: Spåras per dataset som `004:dataset_id`, `005:dataset_id`, etc.

När en template redan är körd för ett dataset hoppas den över (om inte `force=True`).

**SQL-struktur:**

Alla SQL-filer samlade i `sql/migrations/` med namnmönstret `{löpnr}_{beskrivning}_template.sql`:

```text
sql/migrations/
├── 001_db_extensions.sql                    # Installerar DuckDB extensions (körs vid init)
├── 002_db_schemas.sql                       # Skapar scheman (körs vid init)
├── 003_db_makros.sql                        # SQL-makron (körs vid init)
├── 004_staging_transform_template.sql       # Staging: validering, MD5, H3-index
├── 005_staging2_normalisering_template.sql  # Staging_2: normaliserad struktur
├── 006_mart_h3_cells_template.sql           # Mart: exploderade H3-celler med geometri
└── 007_mart_compact_h3_cells_template.sql   # Mart: kompakterade H3-celler
```

**SQL-mallar (generiskt system):**

Templates (`*_template.sql`) körs automatiskt per dataset med parametrar från `datasets.yml`.
Pipeline-fasen bestäms av template-namnet:

| Namnmönster               | Fas       | Beskrivning                                |
| ------------------------- | --------- | ------------------------------------------ |
| `*_staging_transform_*`   | Staging   | Validering, metadata, H3-indexering        |
| `*_staging2_*`            | Staging_2 | Normalisering till enhetlig struktur       |
| `*_mart_*`                | Mart      | Aggregerade tabeller, H3-exploderade celler|

Nya templates plockas upp automatiskt utan kodändringar:

```sql
-- 008_mart_example_template.sql (fungerar automatiskt!)
CREATE OR REPLACE TABLE mart.example_{{ dataset_id }} AS
SELECT * FROM staging_2.{{ dataset_id }}
WHERE '{{ klass }}' = 'naturreservat';
```

**Post-merge SQL (`*_merged.sql`):**

Filer med suffix `_merged.sql` körs EFTER att alla datasets slagits ihop till warehouse.
Använd detta för aggregeringar över alla datasets:

```sql
-- 100_all_h3_cells_merged.sql
CREATE OR REPLACE TABLE mart.all_h3_cells AS
SELECT * FROM mart.naturreservat
UNION ALL SELECT * FROM mart.biotopskyddsomraden
UNION ALL SELECT * FROM mart.vattenskyddsomraden;
```

**Tillgängliga variabler:**

- `{{ dataset_id }}` - Dataset-ID
- `{{ source_id_column }}` - Kolumn för käll-ID
- `{{ klass }}`, `{{ leverantor }}` - Från field_mapping (literaler)
- `{{ grupp_expr }}`, `{{ typ_expr }}` - SQL-uttryck (kolumnref eller literal)
- `{{ h3_center_resolution }}`, `{{ h3_polyfill_resolution }}` - H3-inställningar

**field_mapping syntax i datasets.yml:**

```yaml
field_mapping:
  source_id_column: $beteckn   # $prefix = kolumnreferens
  klass: biotopskydd           # utan prefix = literal sträng
  grupp: $Biotyp               # $prefix = hämta från kolumn "Biotyp"
  typ: skyddad_natur           # literal sträng "skyddad_natur"
  leverantor: sks
```

## Plugins

Nya datakällor läggs till som plugins i `src/g_etl/plugins/`. Varje plugin:
1. Ärver från `SourcePlugin` i `g_etl.plugins.base`
2. Implementerar `extract(config, conn, on_log)` metoden
3. Registreras i `g_etl/plugins/__init__.py`

## Working Guidelines

- Presentera alltid ett förslag på lösning innan du genomför ändringar
- Uppdatera dokumentation löpande
- Lägg till användbara kommandon i taskfiles löpande
- Använd GeoParquet som lagringsformat för geodata om inte annat anges
