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

**Python:**
- `task py:install` - Installera dependencies med UV
- `task py:lint` - Kör ruff linting
- `task py:format` - Formatera kod med ruff

**Database:**
- `task db:cli` - Öppna DuckDB REPL
- `task db:init` - Initiera databas med extensions
- `task db:migrate` - Kör alla migrationer

**Admin TUI:**
- `task admin:run` - Starta admin TUI
- `task admin:mock` - Starta med mockdata (för test)

**Web Interface:**

- `task web:dev` - Starta webbgränssnitt lokalt (med auto-reload)
- `task web:docker` - Starta via Docker (`http://localhost:8000`)
- `task web:docker-bg` - Starta i bakgrunden

**Docker:**
- `task docker:up` - Starta containers
- `task docker:down` - Stoppa containers
- `task docker:build` - Bygg images

## Architecture

```text
Web UI (FastAPI)  ─┐
                   ├→ Pipeline Runner → Plugins → DuckDB
Admin TUI (Textual)┘                        ↓
                                     SQL-transformationer
                                     (staging/ → mart/)
```

**Dataflöde genom DuckDB-scheman:**
- `raw/` - Rå ingesterad data från plugins
- `staging/` - Mellanliggande transformationer (SQL)
- `mart/` - Färdiga dataset (SQL)

**Nyckelkomponenter:**

- `plugins/` - Datakälla-plugins (wfs, lantmateriet, geopackage, geoparquet, zip_geopackage)
- `sql/_init/` - Databas-initiering (extensions, scheman, makron)
- `sql/staging/` - SQL för rensning och standardisering
- `sql/mart/` - SQL för färdiga dataset
- `scripts/pipeline.py` - Pipeline-runner (CLI)
- `scripts/db.py` - Gemensamma databasverktyg
- `scripts/admin/app.py` - Textual TUI-applikation
- `scripts/web/app.py` - FastAPI webbgränssnitt
- `config/datasets.yml` - Dataset-konfiguration med plugin-parametrar

**DuckDB-initiering (sql/_init/):**

Körs automatiskt vid varje databasanslutning i alfabetisk ordning:

- `01_extensions.sql` - Installerar och laddar extensions (spatial, parquet, httpfs, json)
- `02_schemas.sql` - Skapar scheman (raw, staging, mart)
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
