# G-ETL – DuckDB ETL-stack för geodata

En containerbaserad ETL-stack för geodata och HTTP(S)-baserade källor.

## Stack

* **DuckDB** – analytisk SQL-motor med inbyggt stöd för geodata
* **Python + Plugins** – datakälla-plugins för WFS, GeoPackage, GeoParquet, Lantmäteriet
* **SQL-transformationer** – ren SQL i staging/ och mart/
* **Docker** – reproducerbar körmiljö
* **.env** – hantering av känslig information

## Datakällor

Stacken är särskilt anpassad för:

* WFS-tjänster (Naturvårdsverket, Skogsstyrelsen, etc.)
* GeoPackage
* GeoParquet
* Skyddade datakällor (t.ex. Lantmäteriets geodataportal)

## Lagringsformat

GeoParquet används som lagringsformat för geodata om inte annat anges.

## Projektstruktur

```text
g-etl/
├── plugins/           # Datakälla-plugins (wfs, lantmateriet, geopackage, geoparquet)
├── sql/
│   ├── staging/       # SQL för rensning och standardisering
│   └── mart/          # SQL för färdiga dataset
├── scripts/
│   ├── pipeline.py    # Pipeline-runner
│   └── admin/         # Admin TUI
├── config/
│   └── datasets.yml   # Dataset-konfiguration
├── data/              # DuckDB-databas och filer
├── migrations/        # DuckDB-schemamigrationer
└── taskfiles/         # Task-kommandon
```

## Utvecklingsmiljö

### Grundkrav

* Om på Windows: WSL2
* Jetify devbox
* Docker

### Komma igång

```bash
# Starta devbox-miljön
devbox shell

# Initiera projektet
task init

# Kör pipelinen
task run

# Eller kör Admin TUI
task admin:run
```

### Vanliga kommandon

```bash
task                     # Visa alla kommandon
task run                 # Kör hela pipelinen
task pipeline:extract    # Kör bara extract
task pipeline:transform  # Kör bara SQL-transformationer
task admin:run           # Starta Admin TUI
task admin:mock          # Admin TUI med mockdata
task db:cli              # Öppna DuckDB REPL
```

## Claude

För stöd i kodning används Claude i detta projekt.

### Instruktioner till Claude

* Shift + Enter används för radbrytning i prompten
* Presentera alltid ett förslag på lösning som vi kan förfina innan du genomför ändringen
* Uppdatera dokumentationen löpande
* Lägg till användbara kommandon i taskfiles löpande
