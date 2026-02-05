# Config

Konfigurationsfiler för G-ETL pipeline.

## Filer

| Fil | Beskrivning |
| --- | ----------- |
| [datasets.yml](datasets.yml) | Dataset-konfiguration |
| [datasets.md](datasets.md) | Dokumentation av varje dataset |

## datasets.yml

Huvudkonfiguration för alla dataset som kan hämtas av pipelinen.

### Struktur

```yaml
datasets:
  - id: unikt_id           # Obligatoriskt - används som tabellnamn i DuckDB
    name: Visningsnamn     # Obligatoriskt - läsbart namn
    description: ...       # Beskrivning av datasetet
    typ: kategori          # Gruppering (t.ex. skogsstyrelsen_gpkg, naturvardsverket_shp)
    plugin: plugin_namn    # Plugin för datahämtning
    url: https://...       # URL till data
    enabled: true/false    # Om datasetet ska köras
    field_mapping:         # Fältmappning för staging
      source_id_column: $kolumn
      klass: literal_eller_$kolumn
      grupp: literal_eller_$kolumn
      typ: literal_eller_$kolumn
      leverantor: leverantor_kod
```

### Plugins

| Plugin | Beskrivning | Extra parametrar |
| ------ | ----------- | ---------------- |
| `zip_geopackage` | Zippade GeoPackage-filer | `layer`, `gpkg_filename` |
| `zip_shapefile` | Zippade Shapefile-filer | `shp_filename` |
| `geopackage` | GeoPackage direkt (ej zippat) | `layer` |
| `wfs` | WFS-tjänster | `layer` (obligatoriskt) |
| `geoparquet` | GeoParquet-filer | - |

### Field Mapping

Fältmappning styr hur data transformeras i staging-fasen:

```yaml
field_mapping:
  source_id_column: $beteckn   # $-prefix = kolumnreferens
  klass: biotopskydd           # utan prefix = literal sträng
  grupp: $Biotyp               # kolumnvärde från data
  typ: skyddad_natur           # fast värde
  leverantor: sks              # leverantörskod
```

**Syntax:**

- `$kolumnnamn` - Hämtar värde från angiven kolumn i källdata
- `värde` - Literal sträng som används direkt

### Leverantörskoder

| Kod | Leverantör |
| --- | ---------- |
| `sks` | Skogsstyrelsen |
| `nvv` | Naturvårdsverket |
| `sgu` | Sveriges Geologiska Undersökning |
| `hav` | Havs- och vattenmyndigheten |
| `raa` | Riksantikvarieämbetet |
| `slu` | Sveriges lantbruksuniversitet |

### Aktivera/Inaktivera

- `enabled: true` - Datasetet körs vid `task run`
- `enabled: false` - Datasetet hoppas över
- `status: källan finns ej` - Dokumentation för varför datasetet är inaktiverat
