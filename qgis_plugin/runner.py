"""Pipeline runner för QGIS.

Wrapper runt g_etl core som hanterar asynkron körning och export.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path

# Core-moduler importeras efter dependency-check
core_imported = False


def _ensure_core_imports():
    """Importera core-moduler (lazy loading efter dependency-check)."""
    global core_imported
    if core_imported:
        return

    global PipelineRunner, PipelineEvent, settings, yaml

    import yaml as _yaml

    from .core.admin.services.pipeline_runner import PipelineEvent, PipelineRunner
    from .core.settings import settings

    yaml = _yaml
    core_imported = True


class QGISPipelineRunner:
    """Pipeline runner anpassad för QGIS."""

    def __init__(self, plugin_dir: Path):
        """Initiera runner.

        Args:
            plugin_dir: Sökväg till plugin-katalogen (innehåller config/, sql/).
        """
        _ensure_core_imports()

        self.plugin_dir = plugin_dir
        self.config_dir = plugin_dir / "config"
        self.sql_dir = plugin_dir / "sql"

        # Överskrid settings för att använda plugin-kataloger
        settings.CONFIG_DIR = self.config_dir
        settings.SQL_DIR = self.sql_dir

    def list_datasets(self) -> list[dict]:
        """Hämta tillgängliga datasets från config.

        Returns:
            Lista med dataset-konfigurationer.
        """
        _ensure_core_imports()

        config_path = self.config_dir / "datasets.yml"
        if not config_path.exists():
            return []

        with open(config_path) as f:
            config = yaml.safe_load(f)

        return config.get("datasets", [])

    def list_dataset_types(self) -> list[str]:
        """Hämta unika dataset-typer.

        Returns:
            Lista med unika typ-värden.
        """
        datasets = self.list_datasets()
        types = set()
        for ds in datasets:
            if "type" in ds:
                types.add(ds["type"])
        return sorted(types)

    def run_pipeline(
        self,
        dataset_ids: list[str],
        output_dir: Path,
        export_format: str = "gpkg",
        phases: tuple[bool, bool, bool] = (True, True, True),
        on_progress: Callable[[str, float], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> Path | None:
        """Kör pipeline för valda datasets.

        Args:
            dataset_ids: Lista med dataset-ID:n att köra.
            output_dir: Katalog för output-filer.
            export_format: Exportformat (gpkg, geoparquet, fgb).
            phases: Tuple med (staging, staging2, mart) - vilka faser som ska köras.
            on_progress: Callback för progress (message, percent 0-100).
            on_log: Callback för loggmeddelanden.

        Returns:
            Sökväg till exporterad fil, eller None vid fel.
        """
        _ensure_core_imports()

        # Kör async pipeline synkront
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = loop.run_until_complete(
                self._run_pipeline_async(
                    dataset_ids=dataset_ids,
                    output_dir=output_dir,
                    export_format=export_format,
                    phases=phases,
                    on_progress=on_progress,
                    on_log=on_log,
                )
            )
            return result
        finally:
            loop.close()

    async def _run_pipeline_async(
        self,
        dataset_ids: list[str],
        output_dir: Path,
        export_format: str,
        phases: tuple[bool, bool, bool],
        on_progress: Callable[[str, float], None] | None,
        on_log: Callable[[str], None] | None,
    ) -> Path | None:
        """Intern async implementation av pipeline."""
        _ensure_core_imports()

        output_dir.mkdir(parents=True, exist_ok=True)
        db_path = output_dir / "warehouse.duckdb"

        runner = PipelineRunner(
            db_path=str(db_path),
            sql_path=self.sql_dir,
        )

        try:
            # Hämta dataset-configs
            all_datasets = self.list_datasets()
            dataset_configs = [ds for ds in all_datasets if ds.get("id") in dataset_ids]

            if not dataset_configs:
                if on_log:
                    on_log("Inga datasets valda")
                return None

            # Steg 1: Extract
            if on_progress:
                on_progress("Extraherar data...", 10)

            def log_wrapper(msg: str):
                if on_log:
                    on_log(msg)

            extract_result = await runner.run_parallel_extract(
                dataset_configs=dataset_configs,
                output_dir=output_dir / "raw",
                on_log=log_wrapper,
            )

            if not extract_result.success:
                if on_log:
                    on_log(f"Extract misslyckades: {extract_result.failed}")
                return None

            # Steg 2: Transform
            if on_progress:
                on_progress("Transformerar data...", 40)

            temp_dbs = await runner.run_parallel_transform(
                parquet_files=extract_result.parquet_files,
                phases=phases,
                on_log=log_wrapper,
            )

            # Steg 3: Merge
            if on_progress:
                on_progress("Slår ihop databaser...", 70)

            await runner.merge_databases(temp_dbs, on_log=log_wrapper)

            # Steg 4: Post-merge SQL
            await runner.run_merged_sql(on_log=log_wrapper)

            # Steg 5: Export
            if on_progress:
                on_progress("Exporterar resultat...", 90)

            output_path = self._export_result(
                db_path=db_path,
                output_dir=output_dir,
                export_format=export_format,
                on_log=log_wrapper,
            )

            if on_progress:
                on_progress("Klar!", 100)

            return output_path

        finally:
            runner.close()

    def _export_result(
        self,
        db_path: Path,
        output_dir: Path,
        export_format: str,
        on_log: Callable[[str], None] | None,
    ) -> Path | None:
        """Exportera resultat till valt format.

        Args:
            db_path: Sökväg till DuckDB-databasen.
            output_dir: Katalog för output.
            export_format: Format (gpkg, geoparquet, fgb).
            on_log: Callback för loggmeddelanden.

        Returns:
            Sökväg till exporterad fil.
        """
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)

        try:
            # Ladda extensions
            conn.execute("LOAD spatial")
            conn.execute("LOAD h3")

            # Kolla om mart.h3_cells finns
            tables = conn.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'mart'
            """).fetchall()

            if not tables:
                if on_log:
                    on_log("Inga tabeller i mart-schemat")
                return None

            # Bestäm output-fil och driver
            format_config = {
                "gpkg": (".gpkg", "GPKG"),
                "geoparquet": (".parquet", None),  # Använd PARQUET format
                "fgb": (".fgb", "FlatGeobuf"),
            }

            ext, driver = format_config.get(export_format, (".gpkg", "GPKG"))
            output_path = output_dir / f"g_etl_export{ext}"

            # Bygg export-SQL
            # Försök först med h3_cells, annars ta första mart-tabellen
            source_table = "mart.h3_cells"
            has_h3_cells = any(t[0] == "h3_cells" for t in tables)

            if not has_h3_cells:
                source_table = f"mart.{tables[0][0]}"
                if on_log:
                    on_log(f"Använder {source_table} (h3_cells saknas)")

            # Kolla vilka kolumner som finns
            columns = conn.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema || '.' || table_name = '{source_table}'
            """).fetchall()
            col_names = [c[0] for c in columns]

            # Bygg SELECT med geometri
            select_cols = []
            for col in col_names:
                if col.lower() not in ("geom", "geometry"):
                    select_cols.append(col)

            # Lägg till geometri från H3 om h3_cell finns
            if "h3_cell" in col_names:
                select_cols.append("ST_GeomFromText(h3_cell_to_boundary_wkt(h3_cell)) as geometry")
            elif "geom" in col_names:
                select_cols.append("geom as geometry")
            elif "geometry" in col_names:
                select_cols.append("geometry")

            select_sql = ", ".join(select_cols)

            # Exportera
            if driver:
                # GDAL-format (GeoPackage, FlatGeobuf)
                sql = f"""
                    COPY (SELECT {select_sql} FROM {source_table})
                    TO '{output_path}' (FORMAT GDAL, DRIVER '{driver}')
                """
            else:
                # GeoParquet
                sql = f"""
                    COPY (SELECT {select_sql} FROM {source_table})
                    TO '{output_path}' (FORMAT PARQUET)
                """

            conn.execute(sql)

            count = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            if on_log:
                on_log(f"Exporterade {count} rader till {output_path.name}")

            return output_path

        except Exception as e:
            if on_log:
                on_log(f"Export-fel: {e}")
            return None

        finally:
            conn.close()
