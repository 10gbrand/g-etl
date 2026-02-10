# WFS Troubleshooting Guide

Guide för att hantera problem med WFS-tjänster (Web Feature Service).

## Vanliga Problem

### 1. "Unterminated object" - Trasigt JSON-svar

**Symptom:**
```
GDAL Error (1): At line 1, character XXXXX: Unterminated object
```

**Orsak:** WFS-servern returnerar ofullständigt/trasigt JSON-svar.

**Lösningar:**

#### A. Använd GeoPandas-baserad WFS-plugin (BÄST)

Ändra plugin-typ i `config/datasets.yml`:

```yaml
- id: problematic_dataset
  plugin: wfs_geopandas  # ⬅️ Ändra från "wfs" till "wfs_geopandas"
  url: https://...
  layer: layer_name
  page_size: 50          # Börja med litet värde
  max_features: 500      # Begränsa för test
```

**Fördelar:**
- ✅ Bättre felhantering än GDAL/ST_Read
- ✅ Kan hantera trasiga WFS-servrar
- ✅ Robustare paginering
- ✅ Automatisk retry-logik

#### B. Minska chunk-storlek

För standard WFS-plugin:

```yaml
- id: dataset
  plugin: wfs
  url: https://...
  layer: layer_name
  paginate: true
  page_size: 50          # ⬅️ Minska från 1000 till 50
  max_features: 500      # ⬅️ Testa med litet värde först
```

#### C. Inaktivera datasetet tillfälligt

```yaml
- id: dataset
  enabled: false  # ⬅️ Hoppa över dataset
  plugin: wfs
  # ...
```

### 2. Timeout vid stora dataset

**Symptom:** Request tar för lång tid och timeout:as.

**Lösning:** Aktivera paginering

```yaml
- id: large_dataset
  plugin: wfs
  url: https://...
  layer: layer_name
  paginate: true       # ⬅️ Aktivera paginering
  page_size: 1000      # Antal features per request
```

### 3. WFS-server stöder inte paginering

Vissa gamla WFS-servrar stödjer inte `startIndex`-parametern.

**Lösning:** Använd `max_features` istället:

```yaml
- id: dataset
  plugin: wfs
  url: https://...
  layer: layer_name
  paginate: false      # ⬅️ Inaktivera paginering
  max_features: 5000   # Begränsa totalt antal
```

## Plugin-jämförelse

| Plugin | Användning | Fördelar | Nackdelar |
|--------|------------|----------|-----------|
| **wfs** | Standard WFS | - Snabb (GDAL)<br>- Inbyggd i DuckDB | - Känslig för trasiga servrar<br>- Mindre felhantering |
| **wfs_geopandas** | Trasiga WFS-servrar | - Robust felhantering<br>- Kan reparera trasigt JSON<br>- Bättre paginering | - Lite långsammare<br>- Kräver GeoPandas |

## Rekommenderade Inställningar per Datakälla

### Naturvårdsverket WFS
```yaml
plugin: wfs
paginate: false
max_features: 100000  # Oftast små-medelstora dataset
```

### HAV (Havs- och vattenmyndigheten)
```yaml
plugin: wfs_geopandas  # ⬅️ Trasig WFS-server
page_size: 50
max_features: 1000     # Testa först
```

### Lantmäteriet
```yaml
plugin: lantmateriet   # ⬅️ Använd dedikerad plugin
```

### Skogsstyrelsen
```yaml
plugin: zip_geopackage  # ⬅️ Använd ZIP-nedladdning istället för WFS
```

## Debugging

### Testa WFS-URL manuellt

```bash
# Hämta första 10 features
curl "https://wfs-server.se/ows?service=WFS&version=2.0.0&request=GetFeature&typename=layer&count=10&outputFormat=application/json" | jq '.'
```

### Kontrollera vad som faktiskt hämtas

```bash
# Öppna DuckDB
task db:cli

# Kolla raw-tabell
SELECT COUNT(*), ST_GeometryType(geom), ST_SRID(geom)
FROM raw.dataset_name
GROUP BY ST_GeometryType(geom), ST_SRID(geom);
```

### Logga WFS-requests

Aktivera debug-logging i plugin:

```python
# I wfs.py eller wfs_geopandas.py
self._log(f"WFS URL: {wfs_url}", on_log)  # Lägg till denna rad
```

## Alternativa Lösningar

### 1. Ladda ner manuellt till GeoPackage

```bash
# Ladda ner WFS till GeoPackage med QGIS eller ogr2ogr
ogr2ogr -f GPKG output.gpkg "WFS:https://wfs-server.se/ows" layer_name

# Använd sedan GeoPackage-plugin istället
```

```yaml
- id: dataset
  plugin: geopackage
  path: data/manual_downloads/output.gpkg
```

### 2. Använd annan datakälla

Många myndigheter erbjuder data via:
- **Direktnedladdning** (ZIP med Shapefile/GeoPackage)
- **GeoParquet** (modernare format)
- **API:er** (REST/JSON)

### 3. Kontakta datakällan

Om WFS-servern är konsekvent trasig, kontakta leverantören:

```
Till: support@myndighet.se
Ämne: WFS returnerar trasigt JSON

Vi får felmeddelandet "Unterminated object" vid hämtning från:
https://geodata.myndighet.se/ows?service=WFS&...

Detta påverkar automatiserad datahämtning.
Kan ni undersöka er WFS-implementation?
```

## Best Practices

1. **Börja med litet `max_features`** för att testa att WFS fungerar
2. **Använd paginering** för dataset >1000 features
3. **Välj rätt plugin** baserat på WFS-serverns kvalitet
4. **Logga alltid WFS-URLs** för debugging
5. **Ha fallback** (manuell nedladdning) för kritiska dataset

## Kända Problem per Datakälla

| Datakälla | Status | Lösning |
|-----------|--------|---------|
| Naturvårdsverket WFS | ✅ Fungerar | Standard WFS-plugin |
| HAV WFS | ⚠️ Trasig | `wfs_geopandas` med små chunks |
| Lantmäteriet API | ✅ Fungerar | Dedikerad `lantmateriet`-plugin |
| Skogsstyrelsen | ✅ Fungerar | `zip_geopackage` istället för WFS |
| SGU WFS | ✅ Fungerar | Standard WFS-plugin |

## Felsökning Checklista

- [ ] Testa WFS-URL manuellt i webbläsare/curl
- [ ] Kontrollera att `typename` är korrekt
- [ ] Försök med `max_features: 10` för att isolera problemet
- [ ] Testa både `wfs` och `wfs_geopandas` plugins
- [ ] Minska `page_size` till 50 eller 100
- [ ] Kontrollera logs för exakt felmeddelande
- [ ] Sök efter alternativa datakällor (ZIP, GeoParquet)
- [ ] Rapportera problem till datakällans support

## Se Även

- [CLAUDE.md](../CLAUDE.md) - Projektöversikt
- [H3_POLYGON_ANALYSIS.md](H3_POLYGON_ANALYSIS.md) - H3-baserade spatial queries
- [WFS Standard](https://www.ogc.org/standards/wfs) - OGC WFS Specification
