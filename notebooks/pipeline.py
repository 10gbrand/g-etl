"""G-ETL Pipeline - Interaktiv notebook.

Kör ETL-pipelinen interaktivt med dataset-val, progress och SQL-utforskning.
Starta med: uv run marimo edit notebooks/pipeline.py
"""

import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def imports():
    import marimo as mo

    return (mo,)


@app.cell
def header(mo):
    mo.md("""
    # G-ETL Pipeline

    Interaktiv pipeline-körning och datautforskning.
    Välj datasets, konfigurera alternativ och kör ETL-flödet.
    """)
    return


@app.cell
def load_config():
    from g_etl.config_loader import load_datasets_config, load_pipelines_config

    all_datasets = load_datasets_config()
    all_pipelines = load_pipelines_config()

    # Bygg uppslagningar
    types = sorted({ds.get("typ", "") for ds in all_datasets if ds.get("typ")})
    pipeline_ids = sorted({ds.get("pipeline", "") for ds in all_datasets if ds.get("pipeline")})
    enabled_datasets = [ds for ds in all_datasets if ds.get("enabled", True)]
    return enabled_datasets, pipeline_ids, types


@app.cell
def dataset_filters(mo, pipeline_ids, types):
    pipeline_filter = mo.ui.dropdown(
        options={"Alla pipelines": ""} | {p: p for p in pipeline_ids},
        label="Pipeline",
    )
    type_filter = mo.ui.dropdown(
        options={"Alla typer": ""} | {t: t for t in types},
        label="Typ",
    )
    return pipeline_filter, type_filter


@app.cell
def dataset_selector(enabled_datasets, mo, pipeline_filter, type_filter):
    # Bygg lista med tillgängliga datasets baserat på filter
    filtered = enabled_datasets
    if pipeline_filter.value:
        filtered = [ds for ds in filtered if ds.get("pipeline") == pipeline_filter.value]
    if type_filter.value:
        filtered = [ds for ds in filtered if ds.get("typ") == type_filter.value]

    dataset_options = {f"{ds['id']} ({ds.get('plugin', '?')})": ds["id"] for ds in filtered}

    dataset_select = mo.ui.multiselect(
        options=dataset_options,
        label="Datasets att köra",
    )

    mo.md(
        f"""
        ## Välj datasets

        {mo.hstack([pipeline_filter, type_filter], gap=1)}

        {dataset_select}

        **{len(filtered)} datasets tillgängliga** (av {len(enabled_datasets)} aktiverade totalt)
        """
    )
    return (dataset_select,)


@app.cell
def pipeline_options(mo):
    extract_phase = mo.ui.checkbox(label="Extract (hämta data)", value=True)
    transform_phase = mo.ui.checkbox(label="Transform (SQL)", value=True)
    keep_staging = mo.ui.checkbox(label="Behåll staging-tabeller")
    save_sql = mo.ui.checkbox(label="Spara renderade SQL")
    auto_export = mo.ui.checkbox(label="Auto-export (GeoParquet)")

    mo.md(
        f"""
        ## Alternativ

        **Faser:**
        {extract_phase} {transform_phase}

        **Debug:**
        {keep_staging} {save_sql} {auto_export}
        """
    )
    return auto_export, extract_phase, keep_staging, save_sql, transform_phase


@app.cell
def run_pipeline_cell(dataset_select, mo):
    run_button = mo.ui.run_button(label="Kör pipeline")

    mo.md(
        f"""
        ## Kör

        Valda datasets: **{len(dataset_select.value)}**
        {"(alla aktiverade körs om inget väljs)" if not dataset_select.value else ""}

        {run_button}
        """
    )
    return (run_button,)


@app.cell
def execute_pipeline(
    auto_export,
    dataset_select,
    extract_phase,
    keep_staging,
    mo,
    run_button,
    save_sql,
    transform_phase,
):
    mo.stop(not run_button.value, mo.md("*Tryck 'Kör pipeline' för att starta.*"))

    from pathlib import Path as _Path

    from g_etl.pipeline import Pipeline
    from g_etl.services.pipeline_runner import PipelineEvent
    from g_etl.utils.logging import FileLogger

    # Filloggning (samma som CLI/TUI)
    _file_logger = FileLogger(logs_dir=_Path("logs"), prefix="notebook")
    _log_file = _file_logger.start()
    mo.output.append(mo.md(f"**Loggfil:** `{_log_file}`"))

    # Spåra dataset-status per fas för multikolumn-progress
    # {ds_id: {"Extract": {"status": "completed", "detail": "1234 rader"}, ...}}
    _dataset_progress: dict[str, dict[str, dict]] = {}
    _current_phase = [""]
    _phase_columns = ["Extract", "Transform", "Merge"]
    _icons = {"pending": "·", "running": "⟳", "completed": "✓", "failed": "✗"}

    def _render_progress():
        """Rendera multikolumn progress-tabell."""
        if not _dataset_progress:
            return
        # Kolumnrubriker
        phase_headers = " | ".join(_phase_columns)
        phase_sep = " | ".join(["---"] * len(_phase_columns))
        header = f"| Dataset | {phase_headers} |\n|---------|{phase_sep}|"

        rows = []
        for ds_id, phases in _dataset_progress.items():
            cells = []
            for phase in _phase_columns:
                info = phases.get(phase)
                if not info:
                    cells.append("·")
                else:
                    icon = _icons.get(info["status"], "?")
                    detail = info.get("detail", "")
                    cells.append(f"{icon} {detail}" if detail else icon)
            rows.append(f"| **{ds_id}** | {' | '.join(cells)} |")

        table = header + "\n" + "\n".join(rows)
        mo.output.replace_at_index(mo.md(table), idx=1)

    # Placeholder för progress-tabell (index 1)
    mo.output.append(mo.md("*Startar pipeline...*"))

    def on_log(msg):
        _file_logger.log(msg)
        # Detektera fasbyten från loggmeddelanden
        if "=== EXTRACT" in msg:
            _current_phase[0] = "Extract"
        elif "=== TRANSFORM" in msg:
            _current_phase[0] = "Transform"
        elif "=== MERGE" in msg:
            _current_phase[0] = "Merge"
        elif "=== POST-MERGE" in msg:
            _current_phase[0] = "Post-merge"

    def on_event(event: PipelineEvent):
        ds = event.dataset
        if not ds:
            return

        phase = _current_phase[0]
        if not phase:
            return

        if ds not in _dataset_progress:
            _dataset_progress[ds] = {}

        if event.event_type == "dataset_started":
            _dataset_progress[ds][phase] = {"status": "running", "detail": ""}
        elif event.event_type == "progress" and event.progress is not None:
            current = _dataset_progress[ds].get(phase, {})
            if current.get("status") not in ("completed", "failed"):
                pct = int(event.progress * 100)
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                _dataset_progress[ds][phase] = {
                    "status": "running",
                    "detail": f"`{bar}` {pct}%",
                }
        elif event.event_type == "dataset_completed":
            detail = f"{event.rows_count} rader" if event.rows_count else "Klar"
            _dataset_progress[ds][phase] = {"status": "completed", "detail": detail}
        elif event.event_type == "dataset_failed":
            _dataset_progress[ds][phase] = {
                "status": "failed",
                "detail": event.message or "",
            }

        _render_progress()

    pipeline = Pipeline()
    selected_ids = list(dataset_select.value) if dataset_select.value else None

    extract_only = extract_phase.value and not transform_phase.value
    transform_only = transform_phase.value and not extract_phase.value

    try:
        # Använd _run_async direkt — marimo kör redan en event loop
        result = await pipeline._run_async(
            datasets=selected_ids,
            extract_only=extract_only,
            transform_only=transform_only,
            on_log=on_log,
            on_event=on_event,
            keep_staging=keep_staging.value,
            save_sql=save_sql.value,
            auto_export=auto_export.value,
        )
    finally:
        pipeline.close()
        _file_logger.close()

    # Visa slutresultat
    status_icon = "✓ OK" if result.success else "✗ FEL"
    status_color = "green" if result.success else "red"

    mo.output.append(
        mo.md(
            f"""
---
## Resultat

**Status:** <span style="color: {status_color}; font-weight: bold;">{status_icon}</span>

| Mätvärde | Antal |
|----------|-------|
| Datasets körda | {result.datasets_run} |
| Datasets misslyckade | {result.datasets_failed} |
| SQL-filer körda | {result.sql_files_run} |

**Logg sparad till:** `{_log_file}`
"""
        )
    )
    return


@app.cell
def warehouse_db(mo):
    from pathlib import Path

    import duckdb

    db_path = Path("data/warehouse.duckdb")

    if not db_path.exists():
        mo.stop(True, mo.md("*Ingen warehouse.duckdb hittad. Kör pipelinen först.*"))

    def open_conn():
        conn = duckdb.connect(str(db_path), read_only=True)
        # Ladda extensions som behövs för geo-makron (g_centroid_lat etc.)
        for ext in ("spatial", "h3"):
            try:
                conn.execute(f"LOAD {ext}")
            except Exception:
                pass
        return conn

    return db_path, open_conn


@app.cell
def warehouse_explorer(db_path, mo, open_conn):
    _conn = open_conn()

    # Hämta alla scheman och tabeller
    tables = _conn.execute(
        """
        SELECT schema_name, table_name,
               estimated_size as rows
        FROM duckdb_tables()
        ORDER BY schema_name, table_name
        """
    ).fetchdf()

    _conn.close()

    tables_widget = mo.ui.table(
        tables,
        label="Tabeller i warehouse (välj en rad för preview)",
        selection="single",
    )

    mo.md(
        f"""
        ## Warehouse-innehåll

        **Databas:** `{db_path}` ({db_path.stat().st_size / 1024 / 1024:.1f} MB)

        {tables_widget}
        """
    )
    return (tables_widget,)


@app.cell
def table_preview(mo, open_conn, tables_widget):
    # Kräv att en rad är vald
    selected = tables_widget.value
    mo.stop(
        selected.empty if hasattr(selected, "empty") else not selected,
        mo.md("*Välj en tabell ovan för att se preview och karta.*"),
    )

    _conn = open_conn()

    _schema = selected.iloc[0]["schema_name"]
    _table = selected.iloc[0]["table_name"]
    _full_name = f"{_schema}.{_table}"

    try:
        # Kolumninfo
        _columns = _conn.execute(f"DESCRIBE {_full_name}").fetchdf()
        _col_names = set(_columns["column_name"].tolist())
        _row_count = _conn.execute(f"SELECT COUNT(*) FROM {_full_name}").fetchone()[0]

        # Detektera geo-typ och bygg lat/lng-query
        _geo_query = None
        _geo_type = None
        if "_centroid_lat" in _col_names and "_centroid_lng" in _col_names:
            _geo_type = "centroids"
            _geo_query = f"""
                SELECT _centroid_lat as lat, _centroid_lng as lng
                FROM {_full_name}
                WHERE _centroid_lat IS NOT NULL
                USING SAMPLE 5000
            """
        elif "latlng" in _col_names:
            _geo_type = "latlng"
            _geo_query = f"""
                SELECT latlng[1] as lat, latlng[2] as lng
                FROM {_full_name}
                WHERE latlng IS NOT NULL
                USING SAMPLE 5000
            """
        elif "geom" in _col_names:
            _geo_type = "geom"
            _geo_query = f"""
                SELECT g_centroid_lat(geom) as lat, g_centroid_lng(geom) as lng
                FROM {_full_name}
                WHERE geom IS NOT NULL
                USING SAMPLE 5000
            """

        # Karta
        _map_html = None
        if _geo_query:
            _points = _conn.execute(_geo_query).fetchall()
            if _points:
                import folium

                _m = folium.Map(
                    location=[63, 17], zoom_start=5, tiles="CartoDB positron"
                )
                for _lat, _lng in _points:
                    if _lat and _lng:
                        folium.CircleMarker(
                            [_lat, _lng],
                            radius=3,
                            color="#377eb8",
                            fill=True,
                            fill_opacity=0.6,
                            weight=1,
                        ).add_to(_m)
                _map_html = _m._repr_html_()

        # Sample-data (exkludera geometri-kolumner)
        _non_geo_cols = [c for c in _col_names if c not in ("geom", "geometry")]
        _sample_cols = ", ".join(f'"{c}"' for c in sorted(_non_geo_cols)[:15])
        _sample = _conn.execute(
            f"SELECT {_sample_cols} FROM {_full_name} LIMIT 10"
        ).fetchdf()

        # Förpopulerad SQL-query
        _suggested_sql = f"SELECT * FROM {_full_name} LIMIT 100"

        # Kolumnlista formaterad
        _col_list = ", ".join(
            f"`{row['column_name']}` ({row['column_type']})"
            for _, row in _columns.iterrows()
        )

    finally:
        _conn.close()

    # Rendera output
    _output_parts = [
        mo.md(
            f"""
## Preview: `{_full_name}`

**Rader:** {_row_count:,} | **Kolumner:** {len(_col_names)}
| **Geo:** {_geo_type or 'ingen'}

{_col_list}

**SQL:**
```sql
{_suggested_sql}
```
"""
        ),
    ]

    if _map_html:
        _output_parts.append(mo.Html(_map_html))
    elif _geo_type:
        _output_parts.append(mo.md("*Ingen geodata att visa (inga koordinater).*"))

    _output_parts.append(mo.ui.table(_sample, label="Sample (10 rader)"))

    mo.vstack(_output_parts)
    return


@app.cell
def sql_cell(mo):
    sql_input = mo.ui.text_area(
        value="SELECT schema_name, table_name FROM duckdb_tables() ORDER BY 1, 2",
        label="SQL-fråga",
        full_width=True,
    )
    query_button = mo.ui.run_button(label="Kör SQL")

    mo.md(
        f"""
        ## SQL Explorer

        {sql_input}
        {query_button}
        """
    )
    return query_button, sql_input


@app.cell
def sql_result(mo, open_conn, query_button, sql_input):
    mo.stop(not query_button.value, mo.md("*Skriv en SQL-fråga och klicka 'Kör SQL'.*"))

    _conn = open_conn()

    try:
        df = _conn.execute(sql_input.value).fetchdf()
        output = mo.ui.table(df, label=f"Resultat ({len(df)} rader)")
    except Exception as e:
        output = mo.md(f"**Fel:** {e}")
    finally:
        _conn.close()

    output
    return


@app.cell
def data_stats(mo):
    from g_etl.services.db_session import get_data_stats

    stats = get_data_stats()

    mo.md(
        f"""
        ## Datastatistik

        | Egenskap | Värde |
        |----------|-------|
        | Databaser | {stats.get("database_count", 0)} |
        | Total storlek | {stats.get("total_size_mb", 0):.1f} MB |
        | Parquet-filer | {stats.get("parquet_count", 0)} |
        | Parquet storlek | {stats.get("parquet_size_mb", 0):.1f} MB |
        | Loggfiler | {stats.get("log_count", 0)} |
        """
    )
    return


if __name__ == "__main__":
    app.run()
