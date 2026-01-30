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

**Database:**
- `task db:cli` - Öppna DuckDB REPL
- `task db:init` - Initiera databas med extensions (legacy)

**Migrationer:**
- `task db:migrate` - Kör alla väntande migrationer
- `task db:migrate:status` - Visa status för migrationer
- `task db:migrate:rollback` - Rulla tillbaka senaste migreringen
- `task db:migrate:create -- "namn"` - Skapa ny migrering

**Admin TUI:**
- `task admin:run` - Starta admin TUI
- `task admin:mock` - Starta med mockdata (för test)
- `task admin:docker` - Starta admin TUI i Docker

**Docker:**
- `task docker:up` - Starta containers
- `task docker:down` - Stoppa containers
- `task docker:build` - Bygg images

## Architecture

```text
Admin TUI (Textual) → Pipeline Runner → Plugins → DuckDB
                                              ↓
                                       SQL-transformationer
                                       (staging/ → staging_2/ → mart/)
```

**Dataflöde genom DuckDB-scheman:**
- `raw/` - Rå ingesterad data från plugins
- `staging/` - Validering, metadata och H3-indexering (SQL)
- `staging_2/` - Normaliserade dataset med enhetlig struktur (SQL)
- `mart/` - Aggregerade tabeller (SQL)

**Nyckelkomponenter:**

- `plugins/` - Datakälla-plugins (wfs, lantmateriet, geoparquet, zip_geopackage, zip_shapefile, mssql)
- `sql/migrations/` - Versionerade databasmigrationer (up/down)
- `sql/_init/` - Databas-initiering (legacy, ersätts av migrations)
- `sql/staging/` - SQL för validering, metadata och H3-indexering
- `sql/staging_2/` - SQL för normalisering till enhetlig struktur
- `sql/mart/` - SQL för aggregering (zzz_end/01_end.sql skapar mart.h3_cells)
- `scripts/migrations/` - Migreringsmotor och CLI
- `scripts/pipeline.py` - Pipeline-runner (CLI)
- `scripts/export_h3.py` - Export av H3-data (CSV, GeoJSON, HTML, Parquet)
- `scripts/db.py` - Gemensamma databasverktyg
- `scripts/admin/app.py` - Textual TUI-applikation
- `scripts/admin/screens/migrations.py` - TUI för migrationshantering
- `config/datasets.yml` - Dataset-konfiguration med plugin-parametrar
- `config/settings.py` - Centrala inställningar (H3-resolution, CRS, etc.)

**Migrationer (sql/migrations/):**

SQL-baserat migreringssystem som fungerar med både DuckDB och PostgreSQL. Migrationer spåras i tabellen `_migrations`.

Filformat:
```sql
-- migrate:up
CREATE SCHEMA raw;

-- migrate:down
DROP SCHEMA raw CASCADE;
```

Befintliga migrationer:
- `001_extensions.sql` - Installerar DuckDB extensions (spatial, parquet, httpfs, json, h3)
- `002_schemas.sql` - Skapar scheman (raw, staging, staging_2, mart)
- `003_macros.sql` - Definierar återanvändbara SQL-makron

**DuckDB-initiering (sql/_init/) [Legacy]:**

Körs automatiskt vid varje databasanslutning i alfabetisk ordning (ersätts successivt av migrationer):

- `01_extensions.sql` - Installerar och laddar extensions (spatial, parquet, httpfs, json, h3)
- `02_schemas.sql` - Skapar scheman (raw, staging, staging_2, mart)
- `03_macros.sql` - Definierar återanvändbara makron (validate_and_fix_geometry, etc.)

## Plugins

Nya datakällor läggs till som plugins i `plugins/`. Varje plugin:
1. Ärver från `SourcePlugin` i `plugins/base.py`
2. Implementerar `extract(config, conn, on_log)` metoden
3. Registreras i `plugins/__init__.py`

## Working Guidelines

- Presentera alltid ett förslag på lösning innan du genomför ändringar
- Uppdatera dokumentation löpande
- Lägg till användbara kommandon i taskfiles löpande
- Använd GeoParquet som lagringsformat för geodata om inte annat anges
