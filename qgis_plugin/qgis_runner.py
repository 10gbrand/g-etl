"""Pipeline runner för QGIS.

Wrapper runt g_etl core som hanterar asynkron körning och export.

Kompatibel med Python 3.9+ (QGIS LTR).
"""

import asyncio
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

# Core-moduler importeras efter dependency-check
core_imported = False


def _ensure_core_imports():
    """Importera core-moduler (lazy loading efter dependency-check).

    Lägger runner/ i sys.path så att bundlade moduler i runner/g_etl/
    hittas som paketet 'g_etl'. Alla interna imports (from g_etl.xxx)
    fungerar då utan ändringar.
    """
    global core_imported
    if core_imported:
        return

    import sys

    runner_path = str(Path(__file__).parent / "runner")

    # VIKTIGT: Rensa g_etl från sys.modules om den är importerad från fel plats
    # QGIS laddar plugin-rooten som g_etl, men vi vill ha runner/g_etl/
    if "g_etl" in sys.modules:
        g_etl_module = sys.modules["g_etl"]
        g_etl_file = getattr(g_etl_module, "__file__", "")
        # Om g_etl är importerad från plugin-rooten (inte runner), rensa den
        if "runner" not in g_etl_file:
            # Rensa alla g_etl-submoduler från cache
            modules_to_remove = [key for key in sys.modules.keys() if key.startswith("g_etl")]
            for module_name in modules_to_remove:
                del sys.modules[module_name]

    if runner_path not in sys.path:
        sys.path.insert(0, runner_path)

    global PipelineRunner, PipelineEvent, settings, yaml, export_mart_tables, FileLogger

    import yaml as _yaml

    from g_etl.admin.services.pipeline_runner import (
        PipelineEvent,
        PipelineRunner,
    )
    from g_etl.export import export_mart_tables
    from g_etl.settings import settings
    from g_etl.utils.logging import FileLogger

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

        # Använd användarens hem-katalog för data (skrivbar plats)
        user_data_dir = Path.home() / ".g_etl"
        settings.DATA_DIR = user_data_dir / "data"
        settings.RAW_DIR = user_data_dir / "data" / "raw"
        settings.TEMP_DIR = user_data_dir / "data" / "temp"
        settings.INPUT_DATA_DIR = user_data_dir / "input_data"
        settings.LOGS_DIR = user_data_dir / "logs"

        # Skapa kataloger
        settings.ensure_dirs()

    def list_datasets(self) -> List[Dict]:
        """Hämta tillgängliga datasets från config.

        Stödjer både nytt format (pipelines: [...]) och gammalt (datasets: [...]).

        Returns:
            Lista med dataset-konfigurationer (platt, med 'pipeline' injicerat).
        """
        _ensure_core_imports()

        config_path = self.config_dir / "datasets.yml"
        if not config_path.exists():
            return []

        with open(config_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return []

        # Nytt format: pipelines-grupperat
        if "pipelines" in data:
            result = []
            for pipeline in data["pipelines"]:
                pipeline_id = pipeline.get("id", "")
                for ds in pipeline.get("datasets", []):
                    ds["pipeline"] = pipeline_id
                    result.append(ds)
            return result

        # Gammalt format: platt lista
        return data.get("datasets", [])

    def list_dataset_types(self) -> List[str]:
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
        dataset_ids: List[str],
        output_dir: Path,
        export_format: str = "gpkg",
        phases: Tuple[bool, bool] = (True, True),
        on_progress: Optional[Callable[[str, float], None]] = None,
        on_log: Optional[Callable[[str], None]] = None,
    ) -> Optional[Path]:
        """Kör pipeline för valda datasets.

        Args:
            dataset_ids: Lista med dataset-ID:n att köra.
            output_dir: Katalog för output-filer.
            export_format: Exportformat (gpkg, geoparquet, fgb).
            phases: Tuple med (staging, mart) - vilka faser som ska köras.
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
        dataset_ids: List[str],
        output_dir: Path,
        export_format: str,
        phases: Tuple[bool, bool],
        on_progress: Optional[Callable[[str, float], None]],
        on_log: Optional[Callable[[str], None]],
    ) -> Optional[Path]:
        """Intern async implementation av pipeline."""
        _ensure_core_imports()

        output_dir.mkdir(parents=True, exist_ok=True)
        db_path = output_dir / "warehouse.duckdb"

        # Starta filloggning (använder centraliserad FileLogger från core)
        file_logger = FileLogger(
            logs_dir=settings.LOGS_DIR,
            prefix="qgis_pipeline",
            title="G-ETL QGIS Plugin Log",
            max_log_files=None,  # Ingen automatisk rotation i QGIS
        )
        log_file = file_logger.start()

        def log_wrapper(msg: str):
            """Logga till både fil och callback."""
            file_logger.log(msg)
            if on_log:
                on_log(msg)

        runner = PipelineRunner(
            db_path=str(db_path),
            sql_path=self.sql_dir,
        )

        try:
            log_wrapper(f"Loggar till: {log_file}")
            log_wrapper(f"Kör pipeline för {len(dataset_ids)} dataset(s): {', '.join(dataset_ids)}")
            log_wrapper(f"Output-katalog: {output_dir}")

            # Hämta dataset-configs
            all_datasets = self.list_datasets()
            dataset_configs = [ds for ds in all_datasets if ds.get("id") in dataset_ids]

            if not dataset_configs:
                log_wrapper("Inga datasets valda")
                return None

            # Steg 1: Extract
            if on_progress:
                on_progress("Extraherar data...", 10)

            extract_result = await runner.run_parallel_extract(
                dataset_configs=dataset_configs,
                output_dir=output_dir / "raw",
                on_log=log_wrapper,
            )

            if not extract_result.success:
                log_wrapper(f"Extract misslyckades: {extract_result.failed}")
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

            # Stäng runner innan export för att frigöra databasanslutningen
            runner.close()

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

            log_wrapper("Pipeline slutförd!")
            return output_path

        except Exception as e:
            # Säkerställ att runner stängs vid fel
            runner.close()
            log_wrapper(f"Pipeline misslyckades: {e}")
            raise
        finally:
            file_logger.close()

    def _export_result(
        self,
        db_path: Path,
        output_dir: Path,
        export_format: str,
        on_log: Optional[Callable[[str], None]],
    ) -> Optional[Path]:
        """Exportera resultat till valt format.

        Exporterar varje tabell i mart-schemat till en egen fil.

        Args:
            db_path: Sökväg till DuckDB-databasen.
            output_dir: Katalog för output.
            export_format: Format (gpkg, geoparquet, fgb).
            on_log: Callback för loggmeddelanden.

        Returns:
            Sökväg till output-katalogen.
        """
        _ensure_core_imports()
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)

        try:
            # Ladda extensions
            conn.execute("LOAD spatial")
            conn.execute("LOAD h3")

            # Använd generell export-funktion
            exported_files = export_mart_tables(
                conn=conn,
                output_dir=output_dir,
                export_format=export_format,
                on_log=on_log,
            )

            if not exported_files:
                return None

            # Returnera första compact-filen (dessa fungerar bäst med QGIS)
            # eftersom de inte har JSON-arrayer som orsakar "Unsupported field type"
            compact_files = [f for f in exported_files if "compact" in f.stem]
            if compact_files:
                return compact_files[0]

            # Annars returnera första filen
            return exported_files[0]

        except Exception as e:
            if on_log:
                import traceback

                on_log(f"Export-fel: {e}")
                on_log(traceback.format_exc())
            return None

        finally:
            conn.close()
