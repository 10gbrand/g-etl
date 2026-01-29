# G-ETL Plugins

Plugins för att hämta data från olika källor till DuckDB:s `raw`-schema.

## Tillgängliga plugins

| Plugin | Beskrivning |
|--------|-------------|
| `wfs` | Hämtar geodata från WFS-tjänster |
| `geoparquet` | Läser GeoParquet-filer (lokalt eller URL) |
| `zip_geopackage` | Läser zippade GeoPackage-filer (URL eller lokal fil) |
| `zip_shapefile` | Läser zippade Shapefile-filer (URL eller lokal fil) |
| `lantmateriet` | Hämtar data från Lantmäteriets API |
| `mssql` | Hämtar data från Microsoft SQL Server |

---

## wfs

Hämtar geodata från OGC WFS-tjänster (Web Feature Service).

### Parametrar

| Parameter | Obligatorisk | Default | Beskrivning |
|-----------|--------------|---------|-------------|
| `id` | Ja | - | Tabellnamn i DuckDB |
| `url` | Ja | - | WFS-tjänstens bas-URL |
| `layer` | Ja | - | Lagrets namn (typename) |
| `srs` | Nej | `EPSG:3006` | Koordinatsystem |
| `max_features` | Nej | - | Max antal features att hämta |

### Exempel

```yaml
- id: avverkningsanmalningar
  name: Avverkningsanmälningar
  plugin: wfs
  url: https://geodpags.skogsstyrelsen.se/geodataport/services/sksAvverkAnm_WFS
  layer: sksAvverkAnm
  srs: EPSG:3006
  max_features: 10000
  enabled: true
```

---

## geoparquet

Läser GeoParquet-filer från lokal disk eller URL (inklusive S3).

### Parametrar

| Parameter | Obligatorisk | Default | Beskrivning |
|-----------|--------------|---------|-------------|
| `id` | Ja | - | Tabellnamn i DuckDB |
| `path` | Ja | - | Sökväg eller URL till parquet-filen |

### Exempel

```yaml
# Lokal fil
- id: byggnader
  name: Byggnader
  plugin: geoparquet
  path: /data/byggnader.parquet
  enabled: true

# Från URL
- id: overpass_buildings
  name: OSM Byggnader
  plugin: geoparquet
  path: https://example.com/data/buildings.parquet
  enabled: true

# Från S3
- id: s3_data
  name: S3 Data
  plugin: geoparquet
  path: s3://bucket-name/path/to/file.parquet
  enabled: true
```

---

## zip_geopackage

Läser zippade GeoPackage-filer från URL eller lokal disk, extraherar och läser in.

### Parametrar

| Parameter | Obligatorisk | Default | Beskrivning |
|-----------|--------------|---------|-------------|
| `id` | Ja | - | Tabellnamn i DuckDB |
| `url` | Ja | - | URL eller lokal sökväg till zip-filen |
| `layer` | Nej | Första lagret | Specifikt lager att läsa |
| `gpkg_filename` | Nej | Hittas automatiskt | Filnamn på .gpkg i arkivet |

### Exempel

```yaml
# Från URL
- id: naturreservat
  name: Naturreservat
  plugin: zip_geopackage
  url: https://geodata.naturvardsverket.se/nedladdning/naturreservat.zip
  layer: naturreservat_polygon
  enabled: true

# Från lokal fil
- id: giss_data
  name: GISS Data
  plugin: zip_geopackage
  url: /Volumes/T9/spring/giss.geopackage.zip
  gpkg_filename: giss.gpkg
  enabled: true
```

---

## zip_shapefile

Laddar ner zippade Shapefile-filer från URL, extraherar och läser in. Använder `geopandas` för korrekt hantering av teckenkodning.

### Parametrar

| Parameter | Obligatorisk | Default | Beskrivning |
|-----------|--------------|---------|-------------|
| `id` | Ja | - | Tabellnamn i DuckDB |
| `url` | Ja | - | URL till zip-filen |
| `shp_filename` | Nej | Hittas automatiskt | Filnamn på .shp i arkivet |
| `encoding` | Nej | `LATIN1` | Teckenkodning för DBF-filen |

### Exempel

```yaml
- id: natura2000_sci
  name: Natura 2000 SCI
  plugin: zip_shapefile
  url: https://geodata.naturvardsverket.se/nedladdning/naturvardsregistret/SCI_Rikstackande.zip
  encoding: LATIN1
  enabled: true

# Om zip-arkivet innehåller flera .shp-filer
- id: specifik_shp
  name: Specifik Shapefile
  plugin: zip_shapefile
  url: https://example.com/data.zip
  shp_filename: subfolder/specific_file.shp
  encoding: UTF-8
  enabled: true
```

### Vanliga encoding-värden

| Encoding | Användning |
|----------|------------|
| `LATIN1` | Svenska myndighetsdata (default) |
| `UTF-8` | Moderna filer |
| `CP1252` | Windows-baserade filer |

---

## lantmateriet

Hämtar geodata från Lantmäteriets API. Kräver API-nyckel.

### Miljövariabler

```bash
LM_API_KEY=din-api-nyckel
```

### Parametrar

| Parameter | Obligatorisk | Default | Beskrivning |
|-----------|--------------|---------|-------------|
| `id` | Ja | - | Tabellnamn i DuckDB |
| `endpoint` | Ja | - | API-endpoint |
| `params` | Nej | `{}` | Extra query-parametrar |

### Exempel

```yaml
- id: lm_naturreservat
  name: LM Naturreservat
  plugin: lantmateriet
  endpoint: /referensdata/v1/naturreservat
  params:
    format: geojson
  enabled: true
```

---

## mssql

Hämtar data från Microsoft SQL Server via ODBC.

### Krav

- ODBC Driver 18 for SQL Server (eller annan ODBC-drivrutin)
- `pyodbc` Python-paket (installeras automatiskt)

### Parametrar

| Parameter | Obligatorisk | Default | Beskrivning |
|-----------|--------------|---------|-------------|
| `id` | Ja | - | Tabellnamn i DuckDB |
| `query` | Ja | - | SQL-fråga att köra |
| `connection_string` | Nej* | - | Komplett ODBC-anslutningssträng |
| `server` | Nej* | - | Servernamn |
| `database` | Nej* | - | Databasnamn |
| `username` | Nej | - | Användarnamn (för SQL-autentisering) |
| `password` | Nej | - | Lösenord (för SQL-autentisering) |
| `driver` | Nej | `ODBC Driver 18 for SQL Server` | ODBC-drivrutin |
| `trust_server_certificate` | Nej | `yes` | Lita på servercertifikat |
| `geometry_column` | Nej | - | Namn på geometrikolumn |

*Antingen `connection_string` ELLER `server` + `database` krävs.

### Exempel

```yaml
# Med connection_string
- id: kunder
  name: Kunder
  plugin: mssql
  connection_string: "DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql.example.com;DATABASE=prod;UID=user;PWD=pass"
  query: "SELECT id, namn, adress FROM dbo.Kunder WHERE aktiv = 1"
  enabled: true

# Med individuella parametrar och SQL-autentisering
- id: platser
  name: Platser med geometri
  plugin: mssql
  server: localhost
  database: geodata
  username: sa
  password: ${MSSQL_PASSWORD}
  query: "SELECT id, namn, geom.STAsText() as geom_wkt FROM dbo.Platser"
  enabled: true

# Med Windows-autentisering (inga credentials)
- id: intern_data
  name: Intern data
  plugin: mssql
  server: intern-server.domain.local
  database: produktion
  query: "SELECT * FROM dbo.Rapporter"
  enabled: true
```

---

## Skapa ett nytt plugin

1. Skapa en ny fil i `plugins/`, t.ex. `plugins/mitt_plugin.py`
2. Ärv från `SourcePlugin` och implementera `extract()`-metoden
3. Registrera pluginet i `plugins/__init__.py`

### Exempelstruktur

```python
"""Plugin för min datakälla."""

from collections.abc import Callable
import duckdb
from plugins.base import ExtractResult, SourcePlugin


class MittPlugin(SourcePlugin):
    """Beskrivning av pluginet."""

    @property
    def name(self) -> str:
        return "mitt_plugin"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data och laddar till raw-schema.

        Config-parametrar:
            id: Tabellnamn i DuckDB
            param1: Beskrivning...
        """
        table_name = config.get("id")

        # Logga och rapportera progress
        self._log("Startar...", on_log)
        self._progress(0.1, "Startar...", on_progress)

        try:
            # Din logik här...

            # Skapa tabell i raw-schema
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS
                SELECT ...
            """)

            # Hämta radantal
            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._progress(1.0, f"Läste {rows_count} rader", on_progress)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Läste {rows_count} rader",
            )

        except Exception as e:
            return ExtractResult(success=False, message=str(e))
```

### Registrera i `__init__.py`

```python
from plugins.mitt_plugin import MittPlugin

PLUGINS: dict[str, type[SourcePlugin]] = {
    # ... befintliga plugins
    "mitt_plugin": MittPlugin,
}
```
