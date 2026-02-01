# Integration: g-etl + h3-api

Detta dokument beskriver hur g-etl och h3-api hänger ihop.

## Översikt

g-etl är ETL-pipelinen som samlar och indexerar geodata. h3-api är REST API:t som exponerar datan för frontends.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA PIPELINE                                │
│                                                                     │
│  ┌─────────────┐                      ┌─────────────────┐           │
│  │   g-etl     │                      │    h3-api       │           │
│  │  (detta     │                      │   (FastAPI)     │           │
│  │   projekt)  │                      │                 │           │
│  │             │                      │  /hexbin        │           │
│  │  Extract    │   warehouse.duckdb   │  /heatmap       │           │
│  │     ↓       │ ──────────────────►  │  /cell/{id}    │           │
│  │  Transform  │    (läses av API)    │                 │           │
│  │     ↓       │                      │  GeoJSON out    │           │
│  │  mart.*     │                      └────────┬────────┘           │
│  └─────────────┘                               │                     │
│                                                ▼                     │
│                                       ┌─────────────────┐           │
│                                       │  Frontend Map   │           │
│                                       └─────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

## Vad g-etl producerar

### Databas: `data/warehouse.duckdb`

| Schema | Tabell | Beskrivning |
|--------|--------|-------------|
| `mart` | `h3_cells` | Alla H3-celler (exploderade) |
| `mart` | `{dataset}` | Dataset-specifika celler |
| `mart` | `{dataset}_compact` | Kompakterade H3-celler |

### H3-data format

Varje rad i `mart.h3_cells`:

```sql
SELECT
    h3_cell,      -- H3 cell ID (varchar)
    klass,        -- Klassificering (naturreservat, biotopskydd, etc)
    grupp,        -- Undergrupp
    typ,          -- Specifik typ
    leverantor,   -- Dataleverantör (sks, nvv, etc)
    source_id,    -- Ursprunglig ID
    geom          -- H3-cellens geometri
FROM mart.h3_cells
```

## Hur h3-api använder datan

h3-api läser direkt från `warehouse.duckdb`:

```python
# h3-api/src/h3_api/services/db.py
conn = duckdb.connect("../g-etl/data/warehouse.duckdb", read_only=True)
```

### API endpoints som konsumerar datan

| Endpoint | SQL | Beskrivning |
|----------|-----|-------------|
| `/hexbin` | `SELECT FROM mart.h3_cells WHERE ...` | Celler i bbox |
| `/cell/{id}` | `SELECT FROM mart.h3_cells WHERE h3_cell = ?` | Enskild cell |

## Körning

### Alternativ 1: Separat (utveckling)

```bash
# Kör g-etl pipeline
cd g-etl
task pipeline:run

# I annan terminal: Starta API
cd ../h3-api
docker compose up
```

### Alternativ 2: Docker Compose (produktion)

Skapa en kombinerad docker-compose i parent-katalogen:

```yaml
# ../docker-compose.yml
services:
  api:
    build: ./h3-api
    ports:
      - "8000:8000"
    volumes:
      - ./g-etl/data:/app/data:ro
    environment:
      - H3_API_DATABASE_PATH=/app/data/warehouse.duckdb
```

### Alternativ 3: Export till h3-api

```bash
# Exportera H3-data som parquet
task pipeline:export:parquet

# Kopiera till h3-api
cp data/export/*.parquet ../h3-api/data/
```

## Schemaläggning

Rekommenderat körschema för produktion:

```
┌──────────────────────────────────────────────────────────────┐
│  Natt (02:00)                                                │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  g-etl pipeline                                      │   │
│  │  - Extract från myndigheter                          │   │
│  │  - Transform (H3-indexering)                         │   │
│  │  - Uppdatera warehouse.duckdb                        │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Dag (24/7)                                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  h3-api                                              │   │
│  │  - Serverar requests                                 │   │
│  │  - Läser från warehouse.duckdb (read-only)           │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

## Relaterade filer

| Projekt | Fil | Beskrivning |
|---------|-----|-------------|
| g-etl | `config/datasets.yml` | Dataset-konfiguration |
| g-etl | `sql/migrations/006_mart_h3_cells_template.sql` | H3-cell SQL |
| h3-api | `src/h3_api/services/db.py` | Databaskoppling |
| h3-api | `INTEGRATION.md` | Motsvarande dok i h3-api |

## Se även

- [h3-api README](../h3-api/README.md)
- [h3-api INTEGRATION.md](../h3-api/INTEGRATION.md)
