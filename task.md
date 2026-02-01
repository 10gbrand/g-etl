# Task-kommandon

## Aktiva kommandon

### Python (py:)
| Kommando | Beskrivning |
|----------|-------------|
| `task py:install` | Installera dependencies med UV |
| `task py:test` | Kör alla tester |
| `task py:lint` | Kör linting med ruff |
| `task py:format` | Formatera kod med ruff |
| `task py:fix` | Fixa formatering och lint-fel automatiskt |
| `task py:ci` | Kör samma kontroller som CI |

### Admin TUI (admin:)
| Kommando | Beskrivning |
|----------|-------------|
| `task admin:run` | Starta admin TUI lokalt |
| `task admin:mock` | Starta admin TUI i mock-läge |
| `task admin:build` | Bygg lokal binär med Nuitka i Docker |

### Pipeline (pipeline:)
| Kommando | Beskrivning |
|----------|-------------|
| `task pipeline:run` | Kör hela pipelinen (extract + transform) |
| `task pipeline:extract` | Kör bara extract |
| `task pipeline:transform` | Kör bara transform |
| `task pipeline:dataset -- <id>` | Kör specifikt dataset |
| `task pipeline:type -- <typ>` | Kör datasets av viss typ |
| `task pipeline:types` | Lista tillgängliga typer |

### Verktyg
| Kommando | Beskrivning |
|----------|-------------|
| `task hq` | Starta Harlequin (DuckDB GUI) |
| `task sf` | Starta Superfile (filhanterare) |

---

## Borttagna kommandon

### Python (py:) - borttagna
- `py:add` - Lägg till paket
- `py:remove` - Ta bort paket
- `py:run` - Kör Python-script
- `py:shell` - Python REPL
- `py:lock` - Uppdatera uv.lock
- `py:test:fast` - Snabba tester
- `py:test:cov` - Tester med coverage
- `py:test:watch` - Tester i watch-mode

### Admin (admin:) - borttagna
- `admin:docker` - TUI i Docker
- `admin:docker-mock` - TUI i Docker (mock)
- `admin:docker-build` - Bygg Docker-image
- `admin:cleanup` - Rensa gamla databaser
- `admin:list-db` - Lista session-databaser
- `admin:build-clean` - Rensa build-artefakter

### Pipeline (pipeline:) - borttagna
- `pipeline:export:kepler` - Export till CSV
- `pipeline:export:geojson` - Export till GeoJSON
- `pipeline:export:html` - Export till HTML
- `pipeline:export:parquet` - Export till Parquet

### DuckDB (db:) - borttagna
- `db:cli` - DuckDB CLI
- `db:cli-raw` - DuckDB CLI utan init
- `db:query` - Kör SQL-fråga
- `db:init` - Initiera databas
- `db:run-sql` - Kör SQL-fil
- `db:migrate` - Kör migrationer
- `db:migrate:rollback` - Rollback
- `db:migrate:status` - Status
- `db:migrate:create` - Skapa migrering
- `db:extensions` - Visa extensions
- `db:schemas` - Visa schemas
- `db:tables` - Visa tabeller

### Docker (docker:) - borttagna
- `docker:up` - Starta containrar
- `docker:down` - Stoppa containrar
- `docker:build` - Bygg images
- `docker:logs` - Visa loggar
- `docker:ps` - Visa containrar
- `docker:clean` - Rensa allt

### Huvudnivå - borttagna
- `init` - Initiera projekt
- `run` - Alias för pipeline:run
- `clean` - Rensa cache
