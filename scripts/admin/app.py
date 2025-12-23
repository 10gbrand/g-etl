import asyncio
from datetime import datetime
from pathlib import Path

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Label,
    Log,
    ProgressBar,
    Static,
)

from scripts.admin.models.dataset import Dataset, DatasetConfig, DatasetStatus
from scripts.admin.services.dbt_runner import DbtRunner, MockDbtRunner


class DatasetItem(Static):
    """A single dataset item with checkbox and status."""

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()
        self.dataset = dataset

    def compose(self) -> ComposeResult:
        with Horizontal(classes="dataset-row"):
            yield Checkbox(
                self.dataset.name,
                value=self.dataset.enabled,
                id=f"cb-{self.dataset.id}",
            )
            yield Label(self.dataset.status_icon, id=f"status-{self.dataset.id}", classes="status-icon")

    def update_status(self, status: DatasetStatus):
        self.dataset.status = status
        status_label = self.query_one(f"#status-{self.dataset.id}", Label)
        status_label.update(self.dataset.status_icon)

        # Update styling based on status
        self.remove_class("running", "completed", "failed")
        if status == DatasetStatus.RUNNING:
            self.add_class("running")
        elif status == DatasetStatus.COMPLETED:
            self.add_class("completed")
        elif status == DatasetStatus.FAILED:
            self.add_class("failed")


class ProgressPanel(Static):
    """Panel showing current progress."""

    def compose(self) -> ComposeResult:
        yield Label("Väntar på jobb...", id="progress-label")
        yield ProgressBar(id="progress-bar", show_eta=True)
        yield Label("", id="progress-stats")

    def update_progress(self, current: int, total: int, model_name: str = ""):
        label = self.query_one("#progress-label", Label)
        bar = self.query_one("#progress-bar", ProgressBar)
        stats = self.query_one("#progress-stats", Label)

        if total > 0:
            bar.update(total=total, progress=current)
            label.update(f"Kör: {model_name}" if model_name else f"Progress: {current}/{total}")
            percent = int((current / total) * 100)
            stats.update(f"{current}/{total} ({percent}%)")
        else:
            label.update("Väntar på jobb...")
            bar.update(total=100, progress=0)
            stats.update("")

    def reset(self):
        self.update_progress(0, 0)


class AdminApp(App):
    """G-ETL Admin TUI Application."""

    CSS = """
    Screen {
        layout: grid;
        grid-size: 2 3;
        grid-columns: 1fr 1fr;
        grid-rows: auto 1fr auto;
    }

    #header-container {
        column-span: 2;
        height: auto;
        padding: 1;
        background: $primary-darken-2;
    }

    #datasets-panel {
        height: 100%;
        border: solid $primary;
        padding: 1;
    }

    #datasets-panel > Label {
        text-style: bold;
        padding-bottom: 1;
    }

    #right-panel {
        height: 100%;
    }

    #progress-panel {
        height: auto;
        min-height: 5;
        border: solid $secondary;
        padding: 1;
        margin-bottom: 1;
    }

    #log-panel {
        height: 100%;
        border: solid $accent;
    }

    #button-bar {
        column-span: 2;
        height: auto;
        padding: 1;
        align: center middle;
    }

    .dataset-row {
        height: 3;
        padding: 0 1;
    }

    .status-icon {
        width: 3;
        text-align: center;
    }

    .running {
        background: $warning 20%;
    }

    .completed {
        background: $success 20%;
    }

    .failed {
        background: $error 20%;
    }

    Button {
        margin: 0 1;
    }

    ProgressBar {
        padding: 1 0;
    }
    """

    BINDINGS = [
        Binding("r", "run_selected", "Kör valda"),
        Binding("a", "run_all", "Kör alla"),
        Binding("s", "stop", "Stoppa"),
        Binding("q", "quit", "Avsluta"),
        Binding("m", "toggle_mock", "Mock-läge"),
    ]

    def __init__(self, config_path: str = "config/datasets.yml", mock: bool = False):
        super().__init__()
        self.config = DatasetConfig.load(config_path)
        self.mock_mode = mock
        self.runner: DbtRunner | None = None
        self._running = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Container(id="header-container"):
            mode_text = " [MOCK]" if self.mock_mode else ""
            yield Label(f"G-ETL Admin{mode_text}", id="app-title")

        with Container(id="datasets-panel"):
            yield Label("Datasets")
            for dataset in self.config.datasets:
                yield DatasetItem(dataset)

        with Vertical(id="right-panel"):
            with Container(id="progress-panel"):
                yield ProgressPanel()

            with Container(id="log-panel"):
                yield Log(id="log", highlight=True)

        with Horizontal(id="button-bar"):
            yield Button("Kör valda [R]", id="btn-run", variant="primary")
            yield Button("Kör alla [A]", id="btn-all", variant="success")
            yield Button("Stoppa [S]", id="btn-stop", variant="error", disabled=True)

        yield Footer()

    def get_selected_datasets(self) -> list[Dataset]:
        """Get list of selected datasets."""
        selected = []
        for dataset in self.config.datasets:
            checkbox = self.query_one(f"#cb-{dataset.id}", Checkbox)
            if checkbox.value:
                selected.append(dataset)
        return selected

    def log_message(self, message: str):
        """Add a message to the log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log = self.query_one("#log", Log)
        log.write_line(f"[{timestamp}] {message}")

    def update_dataset_status(self, dataset_id: str, status: DatasetStatus):
        """Update the status of a dataset."""
        for item in self.query(DatasetItem):
            if item.dataset.id == dataset_id:
                item.update_status(status)
                break

    def set_running_state(self, running: bool):
        """Update UI for running/stopped state."""
        self._running = running
        self.query_one("#btn-run", Button).disabled = running
        self.query_one("#btn-all", Button).disabled = running
        self.query_one("#btn-stop", Button).disabled = not running

    @work(exclusive=True)
    async def run_datasets(self, datasets: list[Dataset]):
        """Run selected datasets."""
        if not datasets:
            self.log_message("Inga datasets valda")
            return

        self.set_running_state(True)

        # Reset all statuses
        for dataset in datasets:
            self.update_dataset_status(dataset.id, DatasetStatus.PENDING)

        progress_panel = self.query_one(ProgressPanel)
        progress_panel.reset()

        # Create runner
        if self.mock_mode:
            self.runner = MockDbtRunner()
        else:
            self.runner = DbtRunner()

        total = len(datasets)
        completed = 0

        self.log_message(f"Startar {total} dataset(s)...")

        for dataset in datasets:
            if not self._running:
                self.log_message("Avbrutet av användare")
                break

            self.update_dataset_status(dataset.id, DatasetStatus.RUNNING)
            progress_panel.update_progress(completed, total, dataset.name)
            self.log_message(f"Kör {dataset.name}...")

            def on_log(msg: str):
                self.log_message(msg)

            def on_progress(current: int, model_total: int):
                progress_panel.update_progress(completed, total, dataset.name)

            success = await self.runner.run_models(
                models=[dataset.dbt_model],
                on_log=on_log,
                on_progress=on_progress,
            )

            if success:
                self.update_dataset_status(dataset.id, DatasetStatus.COMPLETED)
                self.log_message(f"Klar: {dataset.name}")
            else:
                self.update_dataset_status(dataset.id, DatasetStatus.FAILED)
                self.log_message(f"Fel: {dataset.name}")

            completed += 1

        progress_panel.update_progress(completed, total)
        self.log_message(f"Klart! {completed}/{total} datasets körda.")
        self.set_running_state(False)

    async def action_run_selected(self):
        """Run selected datasets."""
        datasets = self.get_selected_datasets()
        self.run_datasets(datasets)

    async def action_run_all(self):
        """Run all enabled datasets."""
        datasets = self.config.get_enabled()
        self.run_datasets(datasets)

    async def action_stop(self):
        """Stop running jobs."""
        self._running = False
        if self.runner:
            await self.runner.stop()
        self.log_message("Stoppar...")
        self.set_running_state(False)

    def action_toggle_mock(self):
        """Toggle mock mode."""
        self.mock_mode = not self.mock_mode
        mode_text = "MOCK" if self.mock_mode else "LIVE"
        self.log_message(f"Växlade till {mode_text}-läge")
        title = self.query_one("#app-title", Label)
        title.update(f"G-ETL Admin [{mode_text}]")

    async def on_button_pressed(self, event: Button.Pressed):
        """Handle button presses."""
        if event.button.id == "btn-run":
            await self.action_run_selected()
        elif event.button.id == "btn-all":
            await self.action_run_all()
        elif event.button.id == "btn-stop":
            await self.action_stop()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="G-ETL Admin TUI")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode")
    parser.add_argument("--config", default="config/datasets.yml", help="Path to config file")
    args = parser.parse_args()

    app = AdminApp(config_path=args.config, mock=args.mock)
    app.run()


if __name__ == "__main__":
    main()
