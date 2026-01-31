"""Pipeline-runner för G-ETL.

Kör extract (via plugins) och transform (via SQL-filer).
"""

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import yaml
from dotenv import load_dotenv

from config.settings import settings
from plugins import clear_download_cache, get_plugin
from plugins.base import ExtractResult

load_dotenv()


# =============================================================================
# Loggning
# =============================================================================

MAX_LOG_FILES = 20  # Behåll de senaste N loggfilerna


class FileLogger:
    """Hanterar loggning till fil med automatisk rotation."""

    def __init__(self, logs_dir: Path | None = None, prefix: str = "pipeline"):
        self.logs_dir = logs_dir or settings.LOGS_DIR
        self.prefix = prefix
        self.log_file: Path | None = None
        self._file_handle = None

    def start(self) -> Path:
        """Startar en ny loggfil och returnerar sökvägen."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Skapa filnamn med timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.log_file = self.logs_dir / f"{self.prefix}_{timestamp}.log"

        # Öppna filen
        self._file_handle = open(self.log_file, "w", encoding="utf-8")

        # Skriv header
        self._file_handle.write(f"# G-ETL Pipeline Log\n")
        self._file_handle.write(f"# Startad: {datetime.now().isoformat()}\n")
        self._file_handle.write(f"# {'=' * 58}\n\n")
        self._file_handle.flush()

        # Rensa gamla loggfiler
        self._cleanup_old_logs()

        return self.log_file

    def log(self, message: str):
        """Skriv ett meddelande till loggfilen."""
        if self._file_handle:
            timestamp = datetime.now().strftime("%H:%M:%S")
            self._file_handle.write(f"[{timestamp}] {message}\n")
            self._file_handle.flush()

    def close(self):
        """Stänger loggfilen."""
        if self._file_handle:
            self._file_handle.write(f"\n# {'=' * 58}\n")
            self._file_handle.write(f"# Avslutad: {datetime.now().isoformat()}\n")
            self._file_handle.close()
            self._file_handle = None

    def _cleanup_old_logs(self):
        """Ta bort gamla loggfiler, behåll de senaste MAX_LOG_FILES."""
        log_files = sorted(
            self.logs_dir.glob(f"{self.prefix}_*.log"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )

        # Ta bort filer utöver MAX_LOG_FILES
        for old_file in log_files[MAX_LOG_FILES:]:
            try:
                old_file.unlink()
            except OSError:
                pass  # Ignorera om filen inte kan tas bort


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

    def _get_connection(
        self, on_log: Callable[[str], None] | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Hämta eller skapa databasanslutning."""
        if self._conn is None:
            if on_log:
                on_log(f"[DB] Ansluter till {self.db_path}...")
            self._conn = duckdb.connect(self.db_path)
            # Säkerställ att extensions och scheman finns
            self._init_database(on_log)
            if on_log:
                on_log(f"[DB] Anslutning etablerad")
        return self._conn

    def _init_database(self, on_log: Callable[[str], None] | None = None):
        """Initiera databas med extensions, scheman och makron från sql/migrations/."""
        conn = self._conn
        if conn is None:
            return

        def log(msg: str):
            if on_log:
                on_log(msg)
            else:
                print(msg)

        init_folder = self.sql_path / "migrations"
        if not init_folder.exists():
            log(f"[Init] Varning: Mappen {init_folder} finns inte")
            return

        log(f"[Init] Initierar databas från {init_folder}...")

        # Kör alla SQL-filer i _init i alfabetisk ordning
        for sql_file in sorted(init_folder.glob("*.sql")):
            log(f"[Init]   Kör {sql_file.name}...")
            statements_run = 0
            try:
                sql = sql_file.read_text()
                # Kör varje statement separat (separerade med semikolon)
                # DuckDB hanterar SQL-kommentarer själv
                for statement in sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        conn.execute(statement)
                        statements_run += 1
                log(f"[Init]   ✓ {sql_file.name}: {statements_run} statements")
            except Exception as e:
                # Logga men fortsätt - vissa kommandon kan redan vara körda
                log(f"[Init]   ✗ {sql_file.name}: {e}")

        log("[Init] Databasinitiering klar")

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
        dataset_id = dataset.get("id", "okänt")
        plugin_name = dataset.get("plugin")

        if on_log:
            on_log(f"[Extract] Dataset: {dataset_id}, Plugin: {plugin_name}")

        if not plugin_name:
            msg = "Saknar plugin i config"
            if on_log:
                on_log(f"[Extract] ✗ {msg}")
            return ExtractResult(success=False, message=msg)

        try:
            plugin = get_plugin(plugin_name)
            if on_log:
                on_log(f"[Extract] Laddar plugin: {plugin.__class__.__name__}")
        except ValueError as e:
            if on_log:
                on_log(f"[Extract] ✗ Plugin-fel: {e}")
            return ExtractResult(success=False, message=str(e))

        conn = self._get_connection(on_log)
        result = plugin.extract(dataset, conn, on_log)

        if on_log:
            if result.success:
                rows_info = f" ({result.rows_count} rader)" if result.rows_count else ""
                on_log(f"[Extract] ✓ {dataset_id}{rows_info}")
            else:
                on_log(f"[Extract] ✗ {dataset_id}: {result.message}")

        return result

    def run_sql_folder(
        self,
        folder: Path,
        on_log: Callable[[str], None] | None = None,
    ) -> int:
        """Kör alla SQL-filer i en specifik mapp i alfabetisk ordning."""
        if not folder.exists():
            if on_log:
                on_log(f"[SQL] Mappen finns inte: {folder}")
            return 0

        conn = self._get_connection(on_log)
        files_run = 0
        sql_files = sorted(folder.glob("*.sql"))

        if not sql_files:
            if on_log:
                on_log(f"[SQL] Inga SQL-filer i {folder}")
            return 0

        if on_log:
            on_log(f"[SQL] Hittade {len(sql_files)} SQL-filer i {folder.name}/")

        for sql_file in sql_files:
            if on_log:
                on_log(f"[SQL]   Kör {sql_file.name}...")

            try:
                sql = sql_file.read_text()
                result = conn.execute(sql)

                # Försök hämta antal påverkade rader om det finns
                try:
                    row_count = result.fetchone()
                    if row_count and on_log:
                        on_log(f"[SQL]   ✓ {sql_file.name}")
                except Exception:
                    if on_log:
                        on_log(f"[SQL]   ✓ {sql_file.name}")

                files_run += 1
            except Exception as e:
                if on_log:
                    on_log(f"[SQL]   ✗ {sql_file.name}: {e}")

        return files_run

    def run_sql_for_dataset(
        self,
        dataset_id: str,
        stage: str,
        on_log: Callable[[str], None] | None = None,
    ) -> int:
        """Kör SQL-filer för ett specifikt dataset.

        Körordning:
        1. _common/ mappen (om den finns)
        2. <dataset_id>/ mappen (om den finns)

        Args:
            dataset_id: Dataset-ID som matchar mappnamn
            stage: "staging" eller "mart"
            on_log: Callback för loggmeddelanden
        """
        if on_log:
            on_log(f"[Transform] Steg: {stage}, Dataset: {dataset_id}")

        stage_folder = self.sql_path / stage
        if not stage_folder.exists():
            if on_log:
                on_log(f"[Transform] Varning: Mappen {stage}/ finns inte")
            return 0

        files_run = 0

        # Kör _common först
        common_folder = stage_folder / "_common"
        if common_folder.exists():
            if on_log:
                on_log(f"[Transform] Kör {stage}/_common/...")
            common_files = self.run_sql_folder(common_folder, on_log)
            files_run += common_files
            if on_log:
                on_log(f"[Transform] _common: {common_files} filer körda")
        else:
            if on_log:
                on_log(f"[Transform] Ingen _common/ mapp i {stage}/")

        # Kör dataset-specifik mapp
        dataset_folder = stage_folder / dataset_id
        if dataset_folder.exists():
            if on_log:
                on_log(f"[Transform] Kör {stage}/{dataset_id}/...")
            dataset_files = self.run_sql_folder(dataset_folder, on_log)
            files_run += dataset_files
            if on_log:
                on_log(f"[Transform] {dataset_id}: {dataset_files} filer körda")
        else:
            if on_log:
                on_log(f"[Transform] Varning: Ingen mapp {stage}/{dataset_id}/")

        if on_log:
            on_log(f"[Transform] Totalt för {stage}/{dataset_id}: {files_run} SQL-filer")

        return files_run

    def run_sql_files(
        self,
        folder: str,
        on_log: Callable[[str], None] | None = None,
    ) -> int:
        """Kör alla SQL-filer i en mapp (legacy-metod för bakåtkompatibilitet).

        Kör _common först, sedan alla dataset-mappar i alfabetisk ordning.
        """
        stage_folder = self.sql_path / folder
        if not stage_folder.exists():
            return 0

        files_run = 0

        # Kör _common först
        common_folder = stage_folder / "_common"
        if common_folder.exists():
            if on_log:
                on_log(f"Kör {folder}/_common...")
            files_run += self.run_sql_folder(common_folder, on_log)

        # Kör alla dataset-mappar
        for subfolder in sorted(stage_folder.iterdir()):
            if subfolder.is_dir() and not subfolder.name.startswith("_"):
                if on_log:
                    on_log(f"Kör {folder}/{subfolder.name}...")
                files_run += self.run_sql_folder(subfolder, on_log)

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
        if on_log:
            on_log("=" * 60)
            on_log("[Pipeline] Startar pipeline...")
            mode = "extract + transform"
            if extract_only:
                mode = "endast extract"
            elif transform_only:
                mode = "endast transform"
            on_log(f"[Pipeline] Läge: {mode}")
            if datasets:
                on_log(f"[Pipeline] Specificerade datasets: {', '.join(datasets)}")
            if typ:
                on_log(f"[Pipeline] Filtrerat på typ: {typ}")
            on_log("=" * 60)

        config = self.load_config()
        if on_log:
            on_log(f"[Pipeline] Laddade {len(config)} datasets från config")

        datasets_run = 0
        datasets_failed = 0
        sql_files_run = 0

        # Samla dataset som ska köras
        datasets_to_run = []
        for dataset in config:
            # Filtrera på typ om specificerat
            if typ and dataset.get("typ") != typ:
                continue

            # Filtrera på ID om specificerat
            if datasets and dataset.get("id") not in datasets:
                continue

            # Hoppa över disabled datasets
            if not dataset.get("enabled", True):
                if on_log:
                    on_log(f"[Pipeline] Hoppar över (disabled): {dataset.get('id')}")
                continue

            datasets_to_run.append(dataset)

        if on_log:
            on_log(f"[Pipeline] {len(datasets_to_run)} dataset att köra:")
            for ds in datasets_to_run:
                on_log(f"[Pipeline]   - {ds.get('id')} ({ds.get('plugin')})")
            on_log("-" * 60)

        # Kör extract + transform per dataset
        for i, dataset in enumerate(datasets_to_run, 1):
            dataset_id = dataset.get("id")
            dataset_name = dataset.get("name", dataset_id)

            if on_log:
                on_log("")
                on_log(f"[Pipeline] === Dataset {i}/{len(datasets_to_run)}: {dataset_id} ===")

            # Extract
            if not transform_only:
                if on_log:
                    on_log(f"[Pipeline] Steg 1: Extract ({dataset_name})...")

                result = self.extract_dataset(dataset, on_log)

                if result.success:
                    datasets_run += 1
                    if on_log:
                        on_log(f"[Pipeline] Extract klar för {dataset_id}")
                else:
                    datasets_failed += 1
                    if on_log:
                        on_log(f"[Pipeline] ✗ Extract misslyckades: {result.message}")
                    continue  # Hoppa över transform om extract misslyckades

            # Transform för detta dataset
            if not extract_only:
                if on_log:
                    on_log(f"[Pipeline] Steg 2: Transform (staging + mart)...")
                staging_files = self.run_sql_for_dataset(dataset_id, "staging", on_log)
                mart_files = self.run_sql_for_dataset(dataset_id, "mart", on_log)
                sql_files_run += staging_files + mart_files
                if on_log:
                    on_log(f"[Pipeline] Transform klar: {staging_files} staging + {mart_files} mart filer")

        # Om bara transform körs utan specifika datasets, kör alla SQL-filer
        if transform_only and not datasets:
            if on_log:
                on_log("[Pipeline] Kör alla staging-transformationer...")
            sql_files_run += self.run_sql_files("staging", on_log)

            if on_log:
                on_log("[Pipeline] Kör alla mart-transformationer...")
            sql_files_run += self.run_sql_files("mart", on_log)

        # Rensa nedladdningscache (friggör temp-filer)
        clear_download_cache()

        success = datasets_failed == 0
        message = f"Extract: {datasets_run} OK, {datasets_failed} fel. Transform: {sql_files_run} SQL-filer."

        if on_log:
            on_log("")
            on_log("=" * 60)
            on_log("[Pipeline] SAMMANFATTNING")
            on_log("=" * 60)
            on_log(f"[Pipeline] Datasets extraherade: {datasets_run}")
            on_log(f"[Pipeline] Datasets misslyckade: {datasets_failed}")
            on_log(f"[Pipeline] SQL-filer körda: {sql_files_run}")
            status = "✓ LYCKADES" if success else "✗ MISSLYCKADES"
            on_log(f"[Pipeline] Status: {status}")
            on_log("=" * 60)

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
    parser.add_argument("--verbose", "-v", action="store_true", help="Visa detaljerad loggning")
    parser.add_argument("--quiet", "-q", action="store_true", help="Visa minimal output")
    parser.add_argument("--no-log-file", action="store_true", help="Inaktivera loggning till fil")
    parser.add_argument("--log-dir", default="logs", help="Mapp för loggfiler (default: logs/)")

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

    # Starta fil-loggning om inte inaktiverad
    file_logger: FileLogger | None = None
    if not args.no_log_file:
        file_logger = FileLogger(logs_dir=Path(args.log_dir))
        log_file = file_logger.start()
        print(f"[Logg] Skriver till: {log_file}")

    def log(msg: str):
        """Loggfunktion med stöd för verbose/quiet och fil-loggning."""
        # Skriv alltid till fil (verbose)
        if file_logger:
            file_logger.log(msg)

        # Konsol-output baserat på verbose/quiet
        if args.quiet:
            # I quiet-läge, visa bara sammanfattning och fel
            if "SAMMANFATTNING" in msg or "✗" in msg or "Status:" in msg or "===" in msg:
                print(msg)
        elif args.verbose:
            # I verbose-läge, visa allt
            print(msg)
        else:
            # Standard-läge: visa huvudsteg men inte alla detaljer
            # Filtrera bort [SQL] och [Init] detaljer om de inte är fel
            if msg.startswith("[SQL]   ") and "✗" not in msg:
                return  # Hoppa över SQL-fildetaljer
            if msg.startswith("[Init]   ") and "✗" not in msg:
                return  # Hoppa över init-detaljer
            print(msg)

    try:
        result = pipeline.run(
            datasets=args.dataset,
            typ=args.typ,
            extract_only=args.extract_only,
            transform_only=args.transform_only,
            on_log=log,
        )

        if file_logger and file_logger.log_file:
            print(f"[Logg] Fullständig logg: {file_logger.log_file}")

        exit(0 if result.success else 1)
    finally:
        pipeline.close()
        if file_logger:
            file_logger.close()


if __name__ == "__main__":
    main()
