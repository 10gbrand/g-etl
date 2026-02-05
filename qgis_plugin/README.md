# G-ETL QGIS Plugin

QGIS-plugin för G-ETL – kör ETL-pipeline för svenska geodata direkt i QGIS.

![QGIS Plugin](../docs/images/qgis-plugin.png)

## Installation

### Alternativ 1: Från GitHub Releases (rekommenderat)

1. Ladda ner `g_etl_qgis-<version>.zip` från [GitHub Releases](https://github.com/10gbrand/g-etl/releases)
2. I QGIS: **Tillägg** → **Hantera och installera tillägg** → **Installera från ZIP**
3. Välj den nedladdade zip-filen
4. Klicka **Installera tillägg**

### Alternativ 2: Från QGIS Plugin Repository

1. I QGIS: **Tillägg** → **Hantera och installera tillägg** → **Inställningar**
2. Klicka **Lägg till...** och ange:
   - **Namn:** G-ETL
   - **URL:** `https://raw.githubusercontent.com/10gbrand/g-etl/main/plugins.xml`
3. Gå till **Alla** och sök efter "G-ETL"
4. Klicka **Installera tillägg**

## Första körningen

Vid första körningen installeras automatiskt nödvändiga Python-paket:

- `duckdb` – Analytisk SQL-motor
- `h3` – H3 spatial indexering
- `pyyaml` – YAML-parser
- `jinja2` – Template-motor
- `requests` – HTTP-klient

Detta tar några sekunder och sker endast en gång.

## Användning

1. Klicka på **G-ETL**-ikonen i verktygsfältet eller välj **Tillägg** → **G-ETL** → **Kör G-ETL Pipeline...**

2. **Välj datasets** – Markera de datasets du vill hämta och transformera

3. **Välj exportformat:**
   - **GeoPackage** (.gpkg) – Rekommenderat för QGIS
   - **GeoParquet** (.parquet) – Bra för stora dataset
   - **FlatGeobuf** (.fgb) – Snabb streaming

4. **Välj output-katalog** – Där resultatfilerna sparas

5. **Välj faser:**
   - **Staging** – Validering, metadata, H3-indexering
   - **Staging 2** – Normalisering till enhetlig struktur
   - **Mart** – Aggregerade H3-celler

6. Klicka **Kör pipeline**

7. Resultatet importeras automatiskt till QGIS-projektet

## Arkitektur

```
QGIS Plugin
    │
    ├── Dialog (Qt) ──────────────── Välj datasets, format, faser
    │
    ├── Runner ───────────────────── Wrapper för PipelineRunner
    │   └── PipelineRunner ──────── Samma logik som CLI/TUI
    │       ├── Parallell Extract
    │       ├── Parallell Transform
    │       ├── Merge
    │       └── Export
    │
    └── Layer Import ─────────────── Ladda resultat till QGIS
```

Pluginet använder **samma PipelineRunner** som CLI och TUI. All pipeline-logik är delad.

## Filstruktur

```
qgis_plugin/
├── __init__.py         # Plugin entry point
├── metadata.txt        # QGIS plugin metadata
├── g_etl_plugin.py     # Huvudklass (meny, toolbar, lager-import)
├── dialog.py           # Qt-dialoger (dataset-val, progress)
├── deps.py             # Automatisk installation av dependencies
├── runner.py           # Wrapper för PipelineRunner
├── config/
│   └── datasets.yml    # Dataset-konfiguration
└── sql/
    └── migrations/     # SQL-templates
```

## Felsökning

### "Kunde inte installera nödvändiga paket"

Försök installera manuellt i QGIS Python-konsol:

```python
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "h3", "pyyaml", "jinja2", "requests"])
```

### "Inga datasets konfigurerade"

Kontrollera att `config/datasets.yml` finns i plugin-katalogen. Den bör ha skapats automatiskt vid installation.

### DuckDB extensions installeras inte

DuckDB extensions (spatial, h3) installeras vid första körning. Om det misslyckas, försök:

```python
import duckdb
conn = duckdb.connect(":memory:")
conn.execute("INSTALL spatial")
conn.execute("INSTALL h3")
```

## Utveckling

### Bygga plugin lokalt

```bash
# Från projektets rotkatalog
mkdir -p g_etl_qgis/core

# Kopiera plugin-filer
cp qgis_plugin/*.py g_etl_qgis/
cp qgis_plugin/metadata.txt g_etl_qgis/

# Kopiera core-moduler
cp -r src/g_etl/admin g_etl_qgis/core/
cp -r src/g_etl/plugins g_etl_qgis/core/
cp -r src/g_etl/migrations g_etl_qgis/core/
cp src/g_etl/settings.py g_etl_qgis/core/
cp src/g_etl/sql_generator.py g_etl_qgis/core/

# Kopiera config och SQL
cp -r config g_etl_qgis/
cp -r sql g_etl_qgis/

# Skapa zip
zip -r g_etl_qgis.zip g_etl_qgis/
```

### Testa i QGIS

1. Kopiera `g_etl_qgis/` till QGIS plugin-katalog:
   - **Linux:** `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/`
   - **macOS:** `~/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/`
   - **Windows:** `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins\`

2. Starta om QGIS eller använd **Plugin Reloader**

## Licens

MIT License – samma som G-ETL.

## Länkar

- [G-ETL GitHub](https://github.com/10gbrand/g-etl)
- [Releases](https://github.com/10gbrand/g-etl/releases)
- [Dokumentation](https://github.com/10gbrand/g-etl#readme)
- [Rapportera problem](https://github.com/10gbrand/g-etl/issues)
