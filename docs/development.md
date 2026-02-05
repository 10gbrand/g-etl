# Utveckling

Guide för att utöka G-ETL med nya dataset och plugins.

## Lägga till nytt dataset

1. Lägg till konfiguration i `config/datasets.yml` med `field_mapping:` block
2. Kör pipelinen - SQL genereras automatiskt baserat på konfigurationen

Alla SQL-transformationer genereras via `SQLGenerator` som renderar templates med
värden från `field_mapping`. Du behöver inte skapa SQL-filer för varje dataset.

### Exempel på dataset-konfiguration

```yaml
datasets:
  - id: sksbiotopskydd
    name: Biotopskydd SKS
    description: Biotopskydd från Skogsstyrelsen
    typ: skogsstyrelsen_gpkg
    plugin: zip_geopackage
    url: https://geodpags.skogsstyrelsen.se/.../sksBiotopskydd_gpkg.zip
    enabled: true
    field_mapping:
      source_id_column: $beteckn   # $prefix = kolumnreferens
      klass: biotopskydd           # utan prefix = literal sträng
      grupp: $Biotyp               # hämta värde från kolumn "Biotyp"
      typ: $Naturtyp               # hämta värde från kolumn "Naturtyp"
      leverantor: sks              # literal sträng
```

Se [config/readme.md](../config/readme.md) för fullständig dokumentation av konfigurationen.

## SQL-generator

Pipelinen använder `g_etl.sql_generator` för att rendera SQL-templates:

```python
from g_etl.sql_generator import SQLGenerator

generator = SQLGenerator()

# Lista alla templates
templates = generator.list_templates()
# ['004_staging_transform_template.sql', '005_staging2_normalisering_template.sql', ...]

# Rendera en specifik template
sql = generator.render_template(
    "004_staging_transform_template.sql",
    "sksbiotopskydd",
    {"field_mapping": {"source_id_column": "$beteckn", "klass": "biotopskydd"}}
)

# Rendera alla templates för ett dataset
for template_name, sql in generator.render_all_templates("sksbiotopskydd", config):
    conn.execute(sql)
```

### Skapa ny SQL-template

Templates (`*_template.sql`) plockas upp automatiskt och körs per dataset:

```sql
-- sql/migrations/008_mart_example_template.sql (fungerar automatiskt!)
CREATE OR REPLACE TABLE mart.example_{{ dataset_id }} AS
SELECT * FROM staging_2.{{ dataset_id }}
WHERE '{{ klass }}' = 'naturreservat';
```

### Tillgängliga variabler

| Variabel | Beskrivning |
| -------- | ----------- |
| `{{ dataset_id }}` | Dataset-ID |
| `{{ source_id_column }}` | Kolumn för käll-ID |
| `{{ klass }}`, `{{ leverantor }}` | Från field_mapping (literaler) |
| `{{ grupp_expr }}`, `{{ typ_expr }}` | SQL-uttryck (kolumnref eller literal) |
| `{{ h3_center_resolution }}`, `{{ h3_polyfill_resolution }}` | H3-inställningar |

## Makron

SQL-templates använder makron definierade i `sql/migrations/003_db_makros.sql`.
Alla makron har prefixet `g_` för att särskilja från standard SQL-funktioner.

| Makro                                     | Beskrivning                           |
| ----------------------------------------- | ------------------------------------- |
| `g_validate_geom(geom)`                   | Validera och fixa geometri            |
| `g_to_wgs84(geom)`                        | Transformera till WGS84               |
| `g_centroid_lat(geom)`                    | Centroid latitud i WGS84              |
| `g_centroid_lng(geom)`                    | Centroid longitud i WGS84             |
| `g_h3_center(geom, resolution)`           | H3-cell för centroid                  |
| `g_h3_polygon_cells(geom, resolution)`    | H3-celler för polygon (polyfill)      |
| `g_h3_line_cells(geom, buffer, res)`      | H3-celler för linje (buffrad)         |
| `g_h3_point_cells(geom, resolution)`      | H3-cell för punkt (som array)         |
| `g_geom_md5(geom)`                        | MD5-hash av geometri                  |
| `g_json_without_geom(json)`               | Ta bort geometri från JSON            |
| `g_h3_cell_to_geom(h3_cell)`              | Konvertera H3-cell till polygon       |

Bakåtkompatibla alias utan prefix finns också (`validate_geom`, `h3_centroid`, etc.).

## Lägga till ny plugin

1. Skapa `src/g_etl/plugins/{namn}.py` som ärver från `SourcePlugin`
2. Implementera `extract(config, conn, on_log, on_progress)`
3. Registrera i `src/g_etl/plugins/__init__.py`

### Plugin-exempel

```python
from g_etl.plugins.base import SourcePlugin

class MyPlugin(SourcePlugin):
    """Min nya datakälla."""

    def extract(
        self,
        config: dict,
        conn,
        on_log=None,
        on_progress=None
    ) -> dict:
        """Ladda ner och läs in data."""
        table_name = config["id"]
        url = config.get("url")

        # 1. Ladda ner data
        self._log("Laddar ner data...", on_log)
        self._progress(0.2, "Laddar ner...", on_progress)

        # 2. Läs och transformera
        df = self._read_data(url)

        # 3. Skriv till parquet
        parquet_path = self._write_parquet(df, table_name)

        return {
            "rows": len(df),
            "parquet_path": parquet_path
        }
```

### Plugin-arkitektur

```text
┌─────────────────────────────────────────────────────────────────┐
│  datasets.yml                                                   │
│  └── plugin: zip_geopackage                                     │
│      url: https://example.com/data.zip                          │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  PipelineRunner.run_parallel_extract()                          │
│  └── get_plugin("zip_geopackage") → ZipGeoPackagePlugin         │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Plugin.extract(config, conn, on_log, on_progress)              │
│  ├── 1. Ladda ner fil (URL → lokal cache)                       │
│  ├── 2. Läs geodata (GeoPackage/Shapefile/etc.)                 │
│  ├── 3. Skriv till parquet (data/raw/{dataset}.parquet)         │
│  └── 4. Returnera metadata (rows, columns, etc.)                │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  data/raw/{dataset}.parquet                                     │
│  └── Redo för transform-fasen                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Tillgängliga plugins

| Plugin | Källa | Format |
|--------|-------|--------|
| `zip_geopackage` | URL eller lokal fil | Zippad GeoPackage |
| `zip_shapefile` | URL eller lokal fil | Zippad Shapefile |
| `wfs` | OGC WFS-tjänst | GML/JSON |
| `geoparquet` | URL, S3 eller lokal fil | GeoParquet |
| `geopackage` | URL eller lokal fil | GeoPackage (ej zippat) |
| `lantmateriet` | Lantmäteriets API | JSON |
| `mssql` | Microsoft SQL Server | ODBC |

## Test

```bash
# Kör alla tester
task py:test

# Kör med coverage
task py:test -- --cov

# Kör specifikt test
task py:test -- tests/test_sql_generator.py -v
```

## Linting och formatering

```bash
# Kontrollera formatering
task py:lint

# Fixa automatiskt
task py:fix

# Kör samma kontroller som CI
task py:ci

# Innan commit
task py:precommit
```
