# Docker

Docker-konfigurationer för G-ETL. Tre olika upplägg beroende på behov.

## Översikt

```text
docker/
├── Dockerfile.admin              # Full Python-image med TUI
├── Dockerfile.runtime            # Minimal image med förbyggd binär
├── docker-compose.yml            # Compose för utveckling (bygger lokalt)
├── docker-compose.runtime.yml    # Compose för runtime (GitHub Release)
└── huey/
    └── Dockerfile                # DuckDB data explorer (webbgränssnitt)
```

## Images

| Image | Bas | Storlek | Syfte |
| ----- | --- | ------- | ----- |
| `admin` | python:3.12-slim | ~500 MB | Full TUI med alla dependencies |
| `runtime` | debian:bookworm-slim | ~100 MB | Minimal med Nuitka-binär |
| `huey` | nginx:alpine | ~30 MB | Webbbaserad DuckDB-utforskare |

```text
┌─────────────────────────────────────────────────────────────────┐
│  Dockerfile.admin (utveckling/produktion)                       │
│  ├── python:3.12-slim                                           │
│  ├── uv sync (alla Python-deps)                                 │
│  ├── src/, config/, sql/                                        │
│  └── ENTRYPOINT: uv run python -m g_etl.admin.app              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Dockerfile.runtime (minimal)                                   │
│  ├── debian:bookworm-slim                                       │
│  ├── curl → Laddar ner binär från GitHub Release                │
│  └── CMD: ./g_etl                                               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  huey/Dockerfile (datautforskning)                              │
│  ├── nginx:alpine                                               │
│  ├── Huey webapp från GitHub (statisk HTML/JS)                  │
│  └── Data monteras som /data (read-only)                        │
└─────────────────────────────────────────────────────────────────┘
```

## Användning

### Admin (TUI)

Interaktiv TUI för att köra pipelinen. Används vid daglig drift.

```bash
# Med publicerad image (rekommenderat)
docker compose run --rm admin

# Bygga lokalt (för utveckling)
docker compose -f docker/docker-compose.yml run --rm admin

# Med mockdata
docker compose -f docker/docker-compose.yml run --rm admin --mock
```

**Volymer:**

```text
┌──────────────────┐      ┌────────────────────┐
│  Host             │      │  Container          │
├──────────────────┤      ├────────────────────┤
│  ./data/          │ ───► │  /app/data          │  Resultat (warehouse.duckdb)
│  ./config/        │ ───► │  /app/config   (ro) │  Dataset-konfiguration
│  ./sql/           │ ───► │  /app/sql      (ro) │  SQL-templates
│  ./input_data/    │ ───► │  /app/input_data(ro)│  Lokala geodatafiler
└──────────────────┘      └────────────────────┘
```

### Runtime (förbyggd binär)

Minimal image som laddar ner förbyggd Nuitka-binär från GitHub Release.
Kräver ingen Python-installation.

```bash
# Bygg (standard: linux-arm64, latest)
docker compose -f docker/docker-compose.runtime.yml build

# Bygg för specifik arkitektur/version
ARCH=linux-x86_64 VERSION=v0.1.0 \
  docker compose -f docker/docker-compose.runtime.yml build

# Kör interaktivt
docker compose -f docker/docker-compose.runtime.yml run --rm g-etl
```

**Tillgängliga arkitekturer:**

| ARCH | Plattform |
| ---- | --------- |
| `linux-arm64` | Raspberry Pi, AWS Graviton (default) |
| `linux-x86_64` | Vanliga servrar |
| `macos-arm64` | Apple Silicon |

### Huey (DuckDB Data Explorer)

Webbbaserad DuckDB-utforskare för att inspektera pipeline-resultat.
Öppnar på http://localhost:8080.

```bash
# Starta
task admin:huey

# Stoppa
task admin:huey:stop

# Bygg om image
task admin:huey:rebuild
```

```text
┌──────────────────┐      ┌────────────────────┐
│  Host             │      │  Container          │
├──────────────────┤      ├────────────────────┤
│  ./data/          │ ───► │  /data        (ro)  │
│    warehouse.duckdb│      │    warehouse.duckdb │
│    output/*.parquet│      │    output/*.parquet │
└──────────────────┘      └────────────────────┘
                                    │
                                    ▼
                          ┌────────────────────┐
                          │  nginx:alpine       │
                          │  port 8080 → 80     │
                          │  Huey webapp        │
                          └────────────────────┘
                                    │
                                    ▼
                          ┌────────────────────┐
                          │  Webbläsare         │
                          │  http://localhost:8080│
                          │                     │
                          │  Dra-och-släpp:      │
                          │  /data/warehouse.duckdb│
                          │  /data/output/*.parquet│
                          └────────────────────┘
```

## Compose-filer

| Fil | Syfte |
| --- | ----- |
| `docker-compose.yml` (i docker/) | Utveckling – bygger image lokalt från källkod |
| `docker-compose.runtime.yml` | Runtime – laddar ner förbyggd binär |
| `docker-compose.yml` (i rot/) | Slutanvändare – använder publicerad image från GHCR |

```text
Vilken compose-fil ska jag använda?

┌───────────────────────────────────────┐
│  Vill du utveckla G-ETL?              │
│  ├── Ja  → docker/docker-compose.yml  │
│  └── Nej                              │
│      ├── Har du Python?               │
│      │   ├── Ja  → rot/docker-compose.yml (publicerad image)
│      │   └── Nej → docker-compose.runtime.yml (binär)
│      └── Vill du bara utforska data?  │
│          └── task admin:huey          │
└───────────────────────────────────────┘
```

## Miljövariabler

| Variabel | Beskrivning | Krävs |
| -------- | ----------- | ----- |
| `LM_API_KEY` | Lantmäteriet API-nyckel | Nej (bara för LM-dataset) |
| `LM_API_SECRET` | Lantmäteriet API-hemlighet | Nej (bara för LM-dataset) |
| `ARCH` | Arkitektur för runtime-image | Nej (default: linux-arm64) |
| `VERSION` | Release-version för runtime | Nej (default: latest) |

Miljövariabler läses från `.env` i projektets rot (om filen finns).
