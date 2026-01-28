"""Pipeline runner service för Admin TUI."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from plugins import get_plugin


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


class PipelineRunner:
    """Asynkron pipeline-runner för TUI."""

    def __init__(
        self,
        db_path: str = "data/warehouse.duckdb",
        sql_path: str = "sql",
    ):
        self.db_path = db_path
        self.sql_path = Path(sql_path)
        self._running = False
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Hämta eller skapa databasanslutning."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            self._init_database()
        return self._conn

    def _init_database(self):
        """Initiera databas med extensions och scheman."""
        conn = self._conn
        if conn is None:
            return

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
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör SQL-transformationer."""
        conn = self._get_connection()
        success = True

        for folder in ["staging", "mart"]:
            sql_folder = self.sql_path / folder
            if not sql_folder.exists():
                continue

            for sql_file in sorted(sql_folder.glob("*.sql")):
                if on_log:
                    on_log(f"Kör {sql_file.name}...")

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
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        transform_steps = [
            "staging/_common/01_validate.sql",
            "staging/_common/02_clean.sql",
            "mart/_common/01_aggregate.sql",
        ]

        for step in transform_steps:
            if on_log:
                on_log(f"[MOCK] Kör {step}...")
            await asyncio.sleep(0.3)

        if on_log:
            on_log("[MOCK] Transformationer klara")
        return True
