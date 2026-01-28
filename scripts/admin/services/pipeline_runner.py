"""Pipeline runner service för Admin TUI."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from config.settings import settings
from plugins import get_plugin
from scripts.admin.services.staging_processor import process_all_to_staging


@dataclass
class PipelineEvent:
    """Event för pipeline-statusuppdateringar."""

    event_type: str  # "started", "progress", "completed", "failed"
    message: str
    dataset: str | None = None
    status: str | None = None
    rows_count: int | None = None
    progress: float | None = None  # 0.0 - 1.0
    current_step: int | None = None
    total_steps: int | None = None


@dataclass
class ParallelExtractResult:
    """Resultat från parallell extraktion."""

    success: bool
    parquet_files: list[tuple[str, str]] = field(default_factory=list)  # (dataset_id, path)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (dataset_id, error)


class PipelineRunner:
    """Asynkron pipeline-runner för TUI."""

    def __init__(
        self,
        db_path: str | None = None,
        sql_path: str | Path | None = None,
    ):
        self.db_path = db_path or str(settings.DATA_DIR / f"{settings.DB_PREFIX}{settings.DB_EXTENSION}")
        self.sql_path = Path(sql_path) if sql_path else settings.SQL_DIR
        self._running = False
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Hämta eller skapa databasanslutning."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            self._init_database()
        return self._conn

    def _init_database(self):
        """Initiera databas med extensions, scheman och makron."""
        conn = self._conn
        if conn is None:
            return

        # Kör alla init-skript i sql/_init/ i sorterad ordning
        init_folder = self.sql_path / "_init"
        if init_folder.exists():
            for sql_file in sorted(init_folder.glob("*.sql")):
                try:
                    sql = sql_file.read_text()
                    conn.execute(sql)
                except Exception:
                    # Ignorera fel (t.ex. om extension redan laddad)
                    pass
        else:
            # Fallback om _init-mappen inte finns
            for ext in ["spatial", "parquet", "httpfs", "json"]:
                try:
                    conn.execute(f"INSTALL {ext}")
                    conn.execute(f"LOAD {ext}")
                except Exception:
                    pass

            for schema in ["raw", "staging", "mart"]:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    async def run_dataset(
        self,
        dataset_config: dict,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör extract för ett dataset."""
        self._running = True
        dataset_name = dataset_config.get("name", dataset_config.get("id"))
        plugin_name = dataset_config.get("plugin")

        if not plugin_name:
            if on_log:
                on_log(f"Saknar plugin för {dataset_name}")
            return False

        try:
            plugin = get_plugin(plugin_name)
        except ValueError as e:
            if on_log:
                on_log(str(e))
            return False

        if on_event:
            on_event(PipelineEvent(
                event_type="dataset_started",
                message=f"Startar {dataset_name}",
                dataset=dataset_name,
            ))

        # Skapa on_progress callback som bryggar till on_event
        def on_progress(progress: float, message: str) -> None:
            if on_event:
                on_event(PipelineEvent(
                    event_type="progress",
                    message=message,
                    dataset=dataset_name,
                    progress=progress,
                ))

        # Kör extract i executor för att inte blocka event loop
        conn = self._get_connection()

        def do_extract():
            return plugin.extract(dataset_config, conn, on_log, on_progress=on_progress)

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, do_extract)

        if result.success:
            if on_event:
                on_event(PipelineEvent(
                    event_type="dataset_completed",
                    message=f"Klar: {dataset_name} ({result.rows_count} rader)",
                    dataset=dataset_name,
                    status="success",
                    rows_count=result.rows_count,
                ))
            return True
        else:
            if on_event:
                on_event(PipelineEvent(
                    event_type="dataset_failed",
                    message=f"Fel: {result.message}",
                    dataset=dataset_name,
                    status="error",
                ))
            return False

    async def run_transforms(
        self,
        dataset_ids: list[str] | None = None,
        folders: list[str] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör SQL-transformationer för angivna datasets.

        Args:
            dataset_ids: Lista med dataset-ID:n att köra transformationer för.
                         Om None körs alla. Mappar som börjar med _ (t.ex. _common)
                         körs alltid.
            folders: Lista med mappar att köra (t.ex. ["staging"] eller ["mart"]).
                     Om None körs både staging och mart.
            on_log: Callback för loggmeddelanden.
        """
        conn = self._get_connection()
        success = True

        # Bestäm vilka mappar som ska köras
        target_folders = folders if folders else ["staging", "mart"]

        for folder in target_folders:
            sql_folder = self.sql_path / folder
            if not sql_folder.exists():
                continue

            # Rekursiv sökning efter SQL-filer i undermappar
            for sql_file in sorted(sql_folder.glob("**/*.sql")):
                # Hämta första undermappens namn (dataset-ID eller _common)
                rel_to_folder = sql_file.relative_to(sql_folder)
                subfolder = rel_to_folder.parts[0] if rel_to_folder.parts else ""

                # Hoppa över om:
                # - dataset_ids är specificerat OCH
                # - mappen inte börjar med _ (t.ex. _common) OCH
                # - mappen inte matchar något dataset-ID
                if dataset_ids is not None:
                    if not subfolder.startswith("_") and subfolder not in dataset_ids:
                        continue

                # Visa relativ sökväg från sql-mappen
                rel_path = sql_file.relative_to(self.sql_path)
                if on_log:
                    on_log(f"Kör {rel_path}...")

                try:
                    sql = sql_file.read_text()

                    def do_sql():
                        conn.execute(sql)

                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, do_sql)

                except Exception as e:
                    if on_log:
                        on_log(f"Fel i {sql_file.name}: {e}")
                    success = False

        return success

    async def run_parallel_extract(
        self,
        dataset_configs: list[dict],
        output_dir: str | Path | None = None,
        max_concurrent: int = 4,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> ParallelExtractResult:
        """Kör extract för flera dataset parallellt till GeoParquet.

        Args:
            dataset_configs: Lista av dataset-konfigurationer
            output_dir: Mapp för parquet-filer
            max_concurrent: Max antal parallella extractions
            on_event: Callback för progress-events
            on_log: Callback för loggmeddelanden

        Returns:
            ParallelExtractResult med lyckade och misslyckade datasets
        """
        self._running = True
        output_path = Path(output_dir) if output_dir else settings.RAW_DIR
        output_path.mkdir(parents=True, exist_ok=True)
        concurrent = max_concurrent or settings.MAX_CONCURRENT_EXTRACTS

        semaphore = asyncio.Semaphore(concurrent)
        results: list[tuple[str, str | None, str | None]] = []  # (id, path, error)

        async def extract_one(config: dict) -> tuple[str, str | None, str | None]:
            """Extrahera ett dataset."""
            dataset_id = config.get("id", config.get("name"))
            plugin_name = config.get("plugin")

            if not plugin_name:
                return (dataset_id, None, f"Saknar plugin för {dataset_id}")

            try:
                plugin = get_plugin(plugin_name)
            except ValueError as e:
                return (dataset_id, None, str(e))

            async with semaphore:
                if not self._running:
                    return (dataset_id, None, "Avbruten")

                if on_event:
                    on_event(PipelineEvent(
                        event_type="dataset_started",
                        message=f"Startar {dataset_id}",
                        dataset=dataset_id,
                    ))

                def on_progress(progress: float, message: str) -> None:
                    if on_event:
                        on_event(PipelineEvent(
                            event_type="progress",
                            message=message,
                            dataset=dataset_id,
                            progress=progress,
                        ))

                def do_extract():
                    return plugin.extract_to_parquet(
                        config, output_path, on_log, on_progress
                    )

                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, do_extract)

                if result.success:
                    if on_event:
                        on_event(PipelineEvent(
                            event_type="dataset_completed",
                            message=f"Klar: {dataset_id} ({result.rows_count} rader)",
                            dataset=dataset_id,
                            status="success",
                            rows_count=result.rows_count,
                        ))
                    return (dataset_id, result.output_path, None)
                else:
                    if on_event:
                        on_event(PipelineEvent(
                            event_type="dataset_failed",
                            message=f"Fel: {result.message}",
                            dataset=dataset_id,
                            status="error",
                        ))
                    return (dataset_id, None, result.message)

        # Kör alla extraktioner parallellt
        tasks = [extract_one(config) for config in dataset_configs]
        results = await asyncio.gather(*tasks)

        # Sammanställ resultat
        parquet_files = [(r[0], r[1]) for r in results if r[1] is not None]
        failed = [(r[0], r[2]) for r in results if r[2] is not None]

        return ParallelExtractResult(
            success=len(failed) == 0,
            parquet_files=parquet_files,
            failed=failed,
        )

    async def load_parquet_to_db(
        self,
        parquet_files: list[tuple[str, str]],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Ladda parquet-filer till DuckDB raw-schema.

        Args:
            parquet_files: Lista av (dataset_id, parquet_path)
            on_log: Callback för loggmeddelanden
            on_event: Callback för progress-events

        Returns:
            True om alla laddades framgångsrikt
        """
        conn = self._get_connection()
        success = True

        for i, (dataset_id, parquet_path) in enumerate(parquet_files):
            if on_log:
                on_log(f"Laddar {dataset_id} till databas...")

            if on_event:
                on_event(PipelineEvent(
                    event_type="progress",
                    message=f"Laddar {dataset_id}...",
                    dataset=dataset_id,
                    progress=(i + 1) / len(parquet_files),
                ))

            try:

                def do_load():
                    conn.execute(f"""
                        CREATE OR REPLACE TABLE raw.{dataset_id} AS
                        SELECT * FROM read_parquet('{parquet_path}')
                    """)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, do_load)

            except Exception as e:
                if on_log:
                    on_log(f"Fel vid laddning av {dataset_id}: {e}")
                success = False

        return success

    async def run_staging(
        self,
        dataset_ids: list[str],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Kör staging-transformationer för datasets.

        Processerar raw-tabeller till staging med:
        - Geometri-validering
        - Importdatum
        - MD5-hashar (geometri, attribut, käll-ID)
        - H3-index för centroid
        - Centroid lat/lng (för framtida A5)

        Args:
            dataset_ids: Lista med dataset-ID:n att processa
            on_log: Callback för loggmeddelanden
            on_event: Callback för progress-events

        Returns:
            True om alla processades framgångsrikt
        """
        if on_log:
            on_log("=== Kör staging-transformationer ===")

        if on_event:
            on_event(PipelineEvent(
                event_type="progress",
                message="Startar staging-transformationer...",
                progress=0.0,
            ))

        conn = self._get_connection()
        success_count, fail_count = await process_all_to_staging(
            conn, dataset_ids, on_log
        )

        if on_log:
            on_log(f"Staging klar: {success_count} lyckade, {fail_count} misslyckade")

        if on_event:
            on_event(PipelineEvent(
                event_type="progress",
                message=f"Staging klar: {success_count} lyckade",
                progress=1.0,
            ))

        return fail_count == 0

    async def stop(self):
        """Stoppa pågående körning."""
        self._running = False

    def close(self):
        """Stäng databasanslutning."""
        if self._conn:
            self._conn.close()
            self._conn = None


class MockPipelineRunner(PipelineRunner):
    """Mock runner för att testa TUI utan riktig data."""

    async def run_dataset(
        self,
        dataset_config: dict,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        self._running = True
        dataset_name = dataset_config.get("name", dataset_config.get("id"))
        total_steps = 5

        if on_log:
            on_log(f"[MOCK] Startar {dataset_name}...")

        if on_event:
            on_event(PipelineEvent(
                event_type="dataset_started",
                message=f"Startar {dataset_name}",
                dataset=dataset_name,
                progress=0.0,
                current_step=0,
                total_steps=total_steps,
            ))

        # Simulera arbete med progress-uppdateringar
        steps = [
            "Ansluter till källa...",
            "Hämtar metadata...",
            "Laddar data...",
            "Validerar geometri...",
            "Sparar till databas...",
        ]

        for i, step_msg in enumerate(steps):
            if not self._running:
                return False

            if on_log:
                on_log(f"[MOCK] {step_msg}")

            if on_event:
                on_event(PipelineEvent(
                    event_type="progress",
                    message=step_msg,
                    dataset=dataset_name,
                    progress=(i + 1) / total_steps,
                    current_step=i + 1,
                    total_steps=total_steps,
                ))

            await asyncio.sleep(0.4)  # 0.4s per steg = 2s totalt

        rows = 1000 + hash(dataset_name) % 9000  # Varierande antal rader

        if on_log:
            on_log(f"[MOCK] Klar: {dataset_name} ({rows} rader)")

        if on_event:
            on_event(PipelineEvent(
                event_type="dataset_completed",
                message=f"Klar: {dataset_name}",
                dataset=dataset_name,
                status="success",
                rows_count=rows,
                progress=1.0,
            ))

        self._running = False
        return True

    async def run_transforms(
        self,
        dataset_ids: list[str] | None = None,
        folders: list[str] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        target_folders = folders if folders else ["staging", "mart"]
        transform_steps = []

        # Lägg till common-steg för valda mappar
        if "staging" in target_folders:
            transform_steps.append("staging/_common/00_staging_procedure.sql")
        if "mart" in target_folders:
            transform_steps.append("mart/_common/01_output_functions.sql")

        # Lägg till dataset-specifika steg
        if dataset_ids:
            for ds_id in dataset_ids:
                if "staging" in target_folders:
                    transform_steps.append(f"staging/{ds_id}/01_staging.sql")
                if "mart" in target_folders:
                    transform_steps.append(f"mart/{ds_id}/01_final.sql")

        for step in transform_steps:
            if on_log:
                on_log(f"[MOCK] Kör {step}...")
            await asyncio.sleep(0.2)

        folder_str = ", ".join(target_folders)
        if on_log:
            on_log(f"[MOCK] {folder_str} transformationer klara")
        return True

    async def run_parallel_extract(
        self,
        dataset_configs: list[dict],
        output_dir: str | Path | None = None,
        max_concurrent: int | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> ParallelExtractResult:
        """Mock parallell extraktion."""
        self._running = True
        output_path = Path(output_dir) if output_dir else settings.RAW_DIR
        concurrent = max_concurrent or settings.MAX_CONCURRENT_EXTRACTS
        semaphore = asyncio.Semaphore(concurrent)
        parquet_files: list[tuple[str, str]] = []

        async def extract_one(config: dict) -> tuple[str, str]:
            dataset_id = config.get("id", config.get("name"))

            async with semaphore:
                if not self._running:
                    return (dataset_id, "")

                if on_event:
                    on_event(PipelineEvent(
                        event_type="dataset_started",
                        message=f"Startar {dataset_id}",
                        dataset=dataset_id,
                    ))

                steps = ["Hämtar...", "Bearbetar...", "Sparar parquet..."]
                for i, step in enumerate(steps):
                    if on_log:
                        on_log(f"[MOCK] {dataset_id}: {step}")
                    if on_event:
                        on_event(PipelineEvent(
                            event_type="progress",
                            message=step,
                            dataset=dataset_id,
                            progress=(i + 1) / len(steps),
                        ))
                    await asyncio.sleep(0.3)

                rows = 1000 + hash(dataset_id) % 9000

                if on_event:
                    on_event(PipelineEvent(
                        event_type="dataset_completed",
                        message=f"Klar: {dataset_id} ({rows} rader)",
                        dataset=dataset_id,
                        status="success",
                        rows_count=rows,
                    ))

                return (dataset_id, str(output_path / f"{dataset_id}.parquet"))

        tasks = [extract_one(config) for config in dataset_configs]
        results = await asyncio.gather(*tasks)
        parquet_files = [(r[0], r[1]) for r in results if r[1]]

        return ParallelExtractResult(
            success=True,
            parquet_files=parquet_files,
            failed=[],
        )

    async def load_parquet_to_db(
        self,
        parquet_files: list[tuple[str, str]],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Mock laddning av parquet till databas."""
        for i, (dataset_id, _) in enumerate(parquet_files):
            if on_log:
                on_log(f"[MOCK] Laddar {dataset_id} till databas...")
            if on_event:
                on_event(PipelineEvent(
                    event_type="progress",
                    message=f"Laddar {dataset_id}...",
                    dataset=dataset_id,
                    progress=(i + 1) / len(parquet_files),
                ))
            await asyncio.sleep(0.1)

        if on_log:
            on_log("[MOCK] Alla parquet-filer laddade")
        return True

    async def run_staging(
        self,
        dataset_ids: list[str],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Mock staging-transformationer."""
        if on_log:
            on_log("[MOCK] === Kör staging-transformationer ===")

        for i, dataset_id in enumerate(dataset_ids):
            if on_log:
                on_log(f"[MOCK] Processar {dataset_id} till staging...")
                on_log(f"[MOCK]   Geometrikolumn: geom")
                on_log(f"[MOCK]   ID-kolumner: ['id', 'objektid']")
                on_log(f"[MOCK]   Beräknar H3-index...")
                on_log(f"[MOCK]   Skapade staging.{dataset_id} med 1234 rader")

            if on_event:
                on_event(PipelineEvent(
                    event_type="progress",
                    message=f"Staging {dataset_id}...",
                    dataset=dataset_id,
                    progress=(i + 1) / len(dataset_ids),
                ))

            await asyncio.sleep(0.2)

        if on_log:
            on_log(f"[MOCK] Staging klar: {len(dataset_ids)} lyckade, 0 misslyckade")
        return True
