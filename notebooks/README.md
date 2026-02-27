# Notebooks

Interaktiva [marimo](https://marimo.io/)-notebooks för G-ETL. Marimo-notebooks sparas som rena `.py`-filer och är Git-vänliga.

## Tillgängliga notebooks

| Notebook | Beskrivning |
|----------|-------------|
| [pipeline.py](pipeline.py) | Kör ETL-pipelinen interaktivt med dataset-val, SQL-utforskning och datastatistik |

## Starta

### Editor (full interaktivitet)

```bash
task py:marimo
```

### Webapp (read-only, widgets fungerar)

```bash
task py:marimo:run
```

### Direkt med uv

```bash
uv run marimo edit notebooks/pipeline.py
```

## Pipeline-notebook

Notebook:en innehåller följande sektioner:

1. **Välj datasets** - Filtrera på pipeline/typ och välj specifika datasets via multiselect
2. **Alternativ** - Faser (extract/transform), keep-staging, save-sql, auto-export
3. **Kör pipeline** - Startar PipelineRunner med progress-spinner och loggning
4. **Resultat** - Körstatistik och komplett logg
5. **Warehouse-innehåll** - Visar alla tabeller och scheman i warehouse.duckdb
6. **SQL Explorer** - Skriv fria SQL-frågor mot warehouse och se resultat som tabell
7. **Datastatistik** - Databaser, parquet-filer och storlekar

## Skapa nya notebooks

```bash
uv run marimo edit notebooks/min_notebook.py
```

Importera G-ETL-moduler direkt i cellerna:

```python
@app.cell
def _():
    from g_etl.services.pipeline_runner import PipelineRunner
    from g_etl.config_loader import load_datasets_config
    from g_etl.settings import settings
    return PipelineRunner, load_datasets_config, settings
```
