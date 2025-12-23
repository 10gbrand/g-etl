import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DbtEvent:
    event_type: str
    message: str
    model: str | None = None
    status: str | None = None
    execution_time: float | None = None
    rows_affected: int | None = None


class DbtRunner:
    def __init__(self, dbt_project_dir: Path | str = "dbt"):
        self.dbt_project_dir = Path(dbt_project_dir)
        self.process: asyncio.subprocess.Process | None = None
        self._running = False

    async def run_models(
        self,
        models: list[str],
        on_event: Callable[[DbtEvent], None] | None = None,
        on_progress: Callable[[int, int], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Run dbt models with progress tracking."""
        self._running = True
        total_models = len(models)
        completed = 0

        select_arg = " ".join(models)

        cmd = [
            "dbt",
            "run",
            "--select",
            select_arg,
            "--log-format",
            "json",
        ]

        try:
            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=self.dbt_project_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            async for line in self.process.stdout:
                if not self._running:
                    break

                decoded = line.decode().strip()
                if not decoded:
                    continue

                event = self._parse_log_line(decoded)

                if on_log:
                    on_log(event.message if event else decoded)

                if event and on_event:
                    on_event(event)

                if event and event.event_type == "model_completed":
                    completed += 1
                    if on_progress:
                        on_progress(completed, total_models)

            await self.process.wait()
            return self.process.returncode == 0

        except Exception as e:
            if on_log:
                on_log(f"Error: {e}")
            return False
        finally:
            self._running = False

    def _parse_log_line(self, line: str) -> DbtEvent | None:
        """Parse dbt JSON log line into DbtEvent."""
        try:
            data = json.loads(line)
            info = data.get("info", {})
            msg = info.get("msg", "")
            event_name = info.get("name", "")

            # Detect model completion
            if "Completed" in msg or event_name == "LogModelResult":
                return DbtEvent(
                    event_type="model_completed",
                    message=msg,
                    model=data.get("data", {}).get("node_info", {}).get("node_name"),
                    status="success",
                    execution_time=data.get("data", {}).get("execution_time"),
                    rows_affected=data.get("data", {}).get("adapter_response", {}).get(
                        "rows_affected"
                    ),
                )

            # Detect model start
            if event_name == "LogStartLine" or "Running" in msg:
                return DbtEvent(
                    event_type="model_started",
                    message=msg,
                    model=data.get("data", {}).get("node_info", {}).get("node_name"),
                )

            # Detect errors
            if info.get("level") == "error":
                return DbtEvent(
                    event_type="error",
                    message=msg,
                    status="error",
                )

            # Generic event
            return DbtEvent(
                event_type="log",
                message=msg,
            )

        except json.JSONDecodeError:
            # Non-JSON line, return as generic log
            return DbtEvent(
                event_type="log",
                message=line,
            )

    async def stop(self):
        """Stop the running dbt process."""
        self._running = False
        if self.process:
            self.process.terminate()
            await self.process.wait()


class MockDbtRunner(DbtRunner):
    """Mock runner for testing the TUI without dbt."""

    async def run_models(
        self,
        models: list[str],
        on_event: Callable[[DbtEvent], None] | None = None,
        on_progress: Callable[[int, int], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        self._running = True
        total_models = len(models)

        for i, model in enumerate(models):
            if not self._running:
                break

            # Simulate model start
            if on_log:
                on_log(f"Running model {model}...")
            if on_event:
                on_event(DbtEvent(event_type="model_started", message=f"Running {model}", model=model))

            # Simulate work
            await asyncio.sleep(2)

            # Simulate completion
            if on_log:
                on_log(f"Completed model {model}")
            if on_event:
                on_event(
                    DbtEvent(
                        event_type="model_completed",
                        message=f"Completed {model}",
                        model=model,
                        status="success",
                        execution_time=2.0,
                        rows_affected=1000 + i * 500,
                    )
                )

            if on_progress:
                on_progress(i + 1, total_models)

        self._running = False
        return True
