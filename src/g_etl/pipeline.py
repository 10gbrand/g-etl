"""Pipeline-runner för G-ETL CLI.

Använder samma PipelineRunner som TUI:n för parallell körning.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import yaml

# Importera den gemensamma PipelineRunner
from g_etl.admin.services.pipeline_runner import PipelineEvent, PipelineRunner
from g_etl.settings import settings
from g_etl.utils.logging import FileLogger

# Re-exportera FileLogger för bakåtkompatibilitet (används av TUI)
__all__ = ["FileLogger", "Pipeline", "PipelineResult"]


@dataclass
class PipelineResult:
    """Resultat från en pipeline-körning."""

    success: bool
    datasets_run: int = 0
    datasets_failed: int = 0
    sql_files_run: int = 0
    message: str = ""


class Pipeline:
    """CLI-wrapper för PipelineRunner.

    Kör samma parallella logik som TUI:n via asyncio.run().
    """

    def __init__(
        self,
        db_path: str = "data/warehouse.duckdb",
        config_path: str = "config/datasets.yml",
        sql_path: str = "sql",
    ):
        self.db_path = db_path
        self.config_path = Path(config_path)
        self.sql_path = Path(sql_path)
        # Använd gemensam PipelineRunner
        self._runner = PipelineRunner(db_path=db_path, sql_path=sql_path)

    def load_config(self) -> list[dict]:
        """Ladda dataset-konfiguration från YAML."""
        if not self.config_path.exists():
            return []

        with open(self.config_path) as f:
            data = yaml.safe_load(f)

        return data.get("datasets", [])

    def run(
        self,
        datasets: list[str] | None = None,
        typ: str | None = None,
        extract_only: bool = False,
        transform_only: bool = False,
        on_log: Callable[[str], None] | None = None,
    ) -> PipelineResult:
        """Kör pipeline med parallell exekvering.

        Använder samma PipelineRunner som TUI:n.

        Args:
            datasets: Lista med dataset-ID:n att köra (None = alla enabled)
            typ: Filtrera på dataset-typ (t.ex. naturvardsverket_wfs)
            extract_only: Kör bara extract, inte transform
            transform_only: Kör bara transform, inte extract
            on_log: Callback för loggmeddelanden
        """
        # Kör async-metoden synkront
        return asyncio.run(
            self._run_async(
                datasets=datasets,
                typ=typ,
                extract_only=extract_only,
                transform_only=transform_only,
                on_log=on_log,
            )
        )

    async def _run_async(
        self,
        datasets: list[str] | None = None,
        typ: str | None = None,
        extract_only: bool = False,
        transform_only: bool = False,
        on_log: Callable[[str], None] | None = None,
    ) -> PipelineResult:
        """Asynkron pipeline-körning med parallella processer."""
        if on_log:
            on_log("=" * 60)
            on_log("[Pipeline] Startar pipeline (parallell)...")
            mode = "extract + transform"
            if extract_only:
                mode = "endast extract"
            elif transform_only:
                mode = "endast transform"
            on_log(f"[Pipeline] Läge: {mode}")
            on_log(f"[Pipeline] Max parallella extracts: {settings.MAX_CONCURRENT_EXTRACTS}")
            on_log(f"[Pipeline] Max parallella transforms: {settings.MAX_CONCURRENT_SQL}")
            if datasets:
                on_log(f"[Pipeline] Specificerade datasets: {', '.join(datasets)}")
            if typ:
                on_log(f"[Pipeline] Filtrerat på typ: {typ}")
            on_log("=" * 60)

        config = self.load_config()
        if on_log:
            on_log(f"[Pipeline] Laddade {len(config)} datasets från config")

        # Filtrera datasets
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

        if not datasets_to_run:
            if on_log:
                on_log("[Pipeline] Inga datasets att köra")
            return PipelineResult(success=True, message="Inga datasets att köra")

        if on_log:
            on_log(f"[Pipeline] {len(datasets_to_run)} dataset att köra:")
            for ds in datasets_to_run:
                on_log(f"[Pipeline]   - {ds.get('id')} ({ds.get('plugin')})")
            on_log("-" * 60)

        datasets_run = 0
        datasets_failed = 0
        parquet_files: list[tuple[str, str]] = []

        # Event-callback för progress
        def on_event(event: PipelineEvent) -> None:
            if on_log and event.message:
                prefix = f"[{event.dataset}]" if event.dataset else "[Pipeline]"
                on_log(f"{prefix} {event.message}")

        # === EXTRACT (parallellt) ===
        if not transform_only:
            if on_log:
                on_log("")
                on_log("[Pipeline] === EXTRACT (parallellt) ===")

            extract_result = await self._runner.run_parallel_extract(
                dataset_configs=datasets_to_run,
                max_concurrent=settings.MAX_CONCURRENT_EXTRACTS,
                on_event=on_event,
                on_log=on_log,
            )

            parquet_files = extract_result.parquet_files
            datasets_run = len(parquet_files)
            datasets_failed = len(extract_result.failed)

            if on_log:
                on_log(f"[Pipeline] Extract klar: {datasets_run} OK, {datasets_failed} misslyckade")

            if datasets_failed > 0 and on_log:
                for ds_id, error in extract_result.failed:
                    on_log(f"[Pipeline] ✗ {ds_id}: {error}")

        # Om bara extract, returnera här
        if extract_only:
            success = datasets_failed == 0
            return PipelineResult(
                success=success,
                datasets_run=datasets_run,
                datasets_failed=datasets_failed,
                message=f"Extract: {datasets_run} OK, {datasets_failed} fel",
            )

        # === TRANSFORM (parallellt med temp-DBs) ===
        if on_log:
            on_log("")
            on_log("[Pipeline] === TRANSFORM (parallellt) ===")

        # Om transform_only, ladda parquet-filer från raw-mappen
        if transform_only:
            dataset_ids = [ds.get("id") for ds in datasets_to_run]
            for ds_id in dataset_ids:
                parquet_path = settings.RAW_DIR / f"{ds_id}.parquet"
                if parquet_path.exists():
                    parquet_files.append((ds_id, str(parquet_path)))
                elif on_log:
                    on_log(f"[Pipeline] Varning: Ingen parquet för {ds_id}")

        if not parquet_files:
            if on_log:
                on_log("[Pipeline] Inga parquet-filer att transformera")
            return PipelineResult(
                success=datasets_failed == 0,
                datasets_run=datasets_run,
                datasets_failed=datasets_failed,
                message="Inga filer att transformera",
            )

        # Kör parallell transform (varje dataset i egen temp-DB)
        temp_dbs = await self._runner.run_parallel_transform(
            parquet_files=parquet_files,
            phases=(True, True, True),  # staging, staging2, mart
            on_log=on_log,
            on_event=on_event,
        )

        # === MERGE ===
        if on_log:
            on_log("")
            on_log("[Pipeline] === MERGE ===")

        merge_success = await self._runner.merge_databases(
            temp_dbs=temp_dbs,
            on_log=on_log,
            on_event=on_event,
        )

        # === POST-MERGE SQL ===
        if on_log:
            on_log("")
            on_log("[Pipeline] === POST-MERGE SQL ===")

        await self._runner.run_merged_sql(on_log=on_log)

        # Sammanfattning
        success = datasets_failed == 0 and merge_success
        message = (
            f"Extract: {datasets_run} OK, {datasets_failed} fel. "
            f"Transform: {len(temp_dbs)} datasets."
        )

        if on_log:
            on_log("")
            on_log("=" * 60)
            on_log("[Pipeline] SAMMANFATTNING")
            on_log("=" * 60)
            on_log(f"[Pipeline] Datasets extraherade: {datasets_run}")
            on_log(f"[Pipeline] Datasets misslyckade: {datasets_failed}")
            on_log(f"[Pipeline] Datasets transformerade: {len(temp_dbs)}")
            status = "✓ LYCKADES" if success else "✗ MISSLYCKADES"
            on_log(f"[Pipeline] Status: {status}")
            on_log("=" * 60)

        return PipelineResult(
            success=success,
            datasets_run=datasets_run,
            datasets_failed=datasets_failed,
            sql_files_run=len(temp_dbs),  # Antal transformerade datasets
            message=message,
        )

    def close(self):
        """Stäng databasanslutning."""
        self._runner.close()


def main():
    """CLI för pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="G-ETL Pipeline Runner (parallell)")
    parser.add_argument("--dataset", "-d", action="append", help="Specifikt dataset att köra")
    parser.add_argument(
        "--type", "-t", dest="typ", help="Kör datasets av viss typ (t.ex. naturvardsverket_wfs)"
    )
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
            # Filtrera bort detaljerade meddelanden
            if msg.startswith("  "):
                return  # Hoppa över indenterade detaljer
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
