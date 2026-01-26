"""Pipeline-runner för G-ETL.

Kör extract (via plugins) och transform (via SQL-filer).
"""

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import yaml
from dotenv import load_dotenv

from plugins import get_plugin
from plugins.base import ExtractResult

load_dotenv()


@dataclass
class PipelineResult:
    """Resultat från en pipeline-körning."""

    success: bool
    datasets_run: int = 0
    datasets_failed: int = 0
    sql_files_run: int = 0
    message: str = ""


class Pipeline:
    """Huvudklass för att köra ETL-pipeline."""

    def __init__(
        self,
        db_path: str = "data/warehouse.duckdb",
        config_path: str = "config/datasets.yml",
        sql_path: str = "sql",
    ):
        self.db_path = db_path
        self.config_path = Path(config_path)
        self.sql_path = Path(sql_path)
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Hämta eller skapa databasanslutning."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            # Säkerställ att extensions och scheman finns
            self._init_database()
        return self._conn

    def _init_database(self):
        """Initiera databas med extensions och scheman."""
        conn = self._conn
        if conn is None:
            return

        # Ladda extensions
        for ext in ["spatial", "parquet", "httpfs", "json"]:
            try:
                conn.execute(f"INSTALL {ext}")
                conn.execute(f"LOAD {ext}")
            except Exception:
                pass  # Extension kanske redan laddad

        # Skapa scheman
        for schema in ["raw", "staging", "mart"]:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def load_config(self) -> list[dict]:
        """Ladda dataset-konfiguration från YAML."""
        if not self.config_path.exists():
            return []

        with open(self.config_path) as f:
            data = yaml.safe_load(f)

        return data.get("datasets", [])

    def extract_dataset(
        self,
        dataset: dict,
        on_log: Callable[[str], None] | None = None,
    ) -> ExtractResult:
        """Kör extract för ett dataset."""
        plugin_name = dataset.get("plugin")
        if not plugin_name:
            return ExtractResult(success=False, message="Saknar plugin i config")

        try:
            plugin = get_plugin(plugin_name)
        except ValueError as e:
            return ExtractResult(success=False, message=str(e))

        conn = self._get_connection()
        return plugin.extract(dataset, conn, on_log)

    def run_sql_files(
        self,
        folder: str,
        on_log: Callable[[str], None] | None = None,
    ) -> int:
        """Kör alla SQL-filer i en mapp i alfabetisk ordning."""
        sql_folder = self.sql_path / folder
        if not sql_folder.exists():
            return 0

        conn = self._get_connection()
        files_run = 0

        for sql_file in sorted(sql_folder.glob("*.sql")):
            if on_log:
                on_log(f"Kör {sql_file.name}...")

            try:
                sql = sql_file.read_text()
                conn.execute(sql)
                files_run += 1
            except Exception as e:
                if on_log:
                    on_log(f"Fel i {sql_file.name}: {e}")

        return files_run

    def run(
        self,
        datasets: list[str] | None = None,
        typ: str | None = None,
        extract_only: bool = False,
        transform_only: bool = False,
        on_log: Callable[[str], None] | None = None,
    ) -> PipelineResult:
        """Kör pipeline.

        Args:
            datasets: Lista med dataset-ID:n att köra (None = alla enabled)
            typ: Filtrera på dataset-typ (t.ex. naturvardsverket_wfs)
            extract_only: Kör bara extract, inte transform
            transform_only: Kör bara transform, inte extract
            on_log: Callback för loggmeddelanden
        """
        config = self.load_config()
        datasets_run = 0
        datasets_failed = 0
        sql_files_run = 0

        # Extract
        if not transform_only:
            for dataset in config:
                # Filtrera på typ om specificerat
                if typ and dataset.get("typ") != typ:
                    continue

                # Filtrera på ID om specificerat
                if datasets and dataset.get("id") not in datasets:
                    continue

                # Hoppa över disabled datasets
                if not dataset.get("enabled", True):
                    continue

                if on_log:
                    on_log(f"Extraherar {dataset.get('name', dataset.get('id'))}...")

                result = self.extract_dataset(dataset, on_log)

                if result.success:
                    datasets_run += 1
                else:
                    datasets_failed += 1
                    if on_log:
                        on_log(f"Misslyckades: {result.message}")

        # Transform
        if not extract_only:
            if on_log:
                on_log("Kör staging-transformationer...")
            sql_files_run += self.run_sql_files("staging", on_log)

            if on_log:
                on_log("Kör mart-transformationer...")
            sql_files_run += self.run_sql_files("mart", on_log)

        success = datasets_failed == 0
        message = f"Extract: {datasets_run} OK, {datasets_failed} fel. Transform: {sql_files_run} SQL-filer."

        if on_log:
            on_log(message)

        return PipelineResult(
            success=success,
            datasets_run=datasets_run,
            datasets_failed=datasets_failed,
            sql_files_run=sql_files_run,
            message=message,
        )

    def close(self):
        """Stäng databasanslutning."""
        if self._conn:
            self._conn.close()
            self._conn = None


def main():
    """CLI för pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="G-ETL Pipeline Runner")
    parser.add_argument("--dataset", "-d", action="append", help="Specifikt dataset att köra")
    parser.add_argument("--type", "-t", dest="typ", help="Kör datasets av viss typ (t.ex. naturvardsverket_wfs)")
    parser.add_argument("--extract-only", action="store_true", help="Kör bara extract")
    parser.add_argument("--transform-only", action="store_true", help="Kör bara transform")
    parser.add_argument("--list-types", action="store_true", help="Lista tillgängliga typer")
    parser.add_argument("--db", default="data/warehouse.duckdb", help="Sökväg till databas")
    parser.add_argument("--config", default="config/datasets.yml", help="Sökväg till config")

    args = parser.parse_args()

    pipeline = Pipeline(db_path=args.db, config_path=args.config)

    # Lista typer om --list-types
    if args.list_types:
        config = pipeline.load_config()
        types = set(d.get("typ", "") for d in config if d.get("typ"))
        print("Tillgängliga typer:")
        for t in sorted(types):
            count = sum(1 for d in config if d.get("typ") == t)
            print(f"  {t} ({count} datasets)")
        return

    def log(msg: str):
        print(msg)

    try:
        result = pipeline.run(
            datasets=args.dataset,
            typ=args.typ,
            extract_only=args.extract_only,
            transform_only=args.transform_only,
            on_log=log,
        )
        exit(0 if result.success else 1)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
