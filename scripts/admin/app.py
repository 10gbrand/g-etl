from datetime import datetime

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, ScrollableContainer, Vertical
from textual.widgets import (
    Button,
    Footer,
    Header,
    Label,
    Log,
    ProgressBar,
    Select,
    Static,
)

from scripts.admin.models.dataset import Dataset, DatasetConfig, DatasetStatus
from scripts.admin.services.pipeline_runner import MockPipelineRunner, PipelineRunner


class DatasetRow(Static):
    """En rad för ett dataset."""

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()
        self.dataset = dataset
        self.selected = False

    def compose(self) -> ComposeResult:
        status = "○" if self.dataset.enabled else "◌"
        typ_short = self.dataset.typ.split("_")[0][:10] if self.dataset.typ else ""
        yield Label(
            f"{status} [{typ_short:10}] {self.dataset.name}",
            id=f"lbl-{self.dataset.id}",
        )

    def on_click(self) -> None:
        """Toggle selection on click."""
        if not self.dataset.enabled:
            return
        self.selected = not self.selected
        self._update_display()
        # Notify parent about selection change
        self.app.update_selection_count()

    def _update_display(self):
        """Update the display based on state."""
        self.remove_class("selected", "running", "completed", "failed", "disabled")

        if not self.dataset.enabled:
            self.add_class("disabled")
        elif self.selected:
            self.add_class("selected")

        if self.dataset.status == DatasetStatus.RUNNING:
            self.add_class("running")
        elif self.dataset.status == DatasetStatus.COMPLETED:
            self.add_class("completed")
        elif self.dataset.status == DatasetStatus.FAILED:
            self.add_class("failed")

    def update_status(self, status: DatasetStatus):
        """Update dataset status."""
        self.dataset.status = status
        label = self.query_one(f"#lbl-{self.dataset.id}", Label)

        icons = {
            DatasetStatus.PENDING: "○",
            DatasetStatus.RUNNING: "⟳",
            DatasetStatus.COMPLETED: "✓",
            DatasetStatus.FAILED: "✗",
        }
        icon = icons.get(status, "○")
        typ_short = self.dataset.typ.split("_")[0][:10] if self.dataset.typ else ""
        label.update(f"{icon} [{typ_short:10}] {self.dataset.name}")
        self._update_display()

    def set_selected(self, selected: bool):
        """Set selection state."""
        if self.dataset.enabled:
            self.selected = selected
            self._update_display()


class AdminApp(App):
    """G-ETL Admin TUI Application."""

    CSS = """
    Screen {
        layout: grid;
        grid-size: 2 4;
        grid-columns: 1fr 1fr;
        grid-rows: auto auto 1fr auto;
    }

    #header-container {
        column-span: 2;
        height: auto;
        padding: 1;
        background: $primary-darken-2;
    }

    #filter-bar {
        column-span: 2;
        height: auto;
        padding: 1;
        background: $surface;
    }

    #filter-bar Horizontal {
        height: auto;
    }

    #filter-bar Label {
        padding: 0 1;
    }

    #filter-bar Select {
        width: 40;
    }

    #datasets-panel {
        height: 100%;
        border: solid $primary;
    }

    #datasets-scroll {
        height: 100%;
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

    DatasetRow {
        height: 2;
        padding: 0 1;
    }

    DatasetRow:hover {
        background: $primary 20%;
    }

    DatasetRow.selected {
        background: $primary 40%;
    }

    DatasetRow.disabled {
        color: $text-muted;
    }

    DatasetRow.running {
        background: $warning 30%;
    }

    DatasetRow.completed {
        background: $success 30%;
    }

    DatasetRow.failed {
        background: $error 30%;
    }

    Button {
        margin: 0 1;
    }

    ProgressBar {
        padding: 1 0;
    }

    #selection-info {
        text-align: right;
        padding-right: 1;
    }
    """

    BINDINGS = [
        Binding("r", "run_selected", "Kör valda"),
        Binding("a", "run_all", "Kör alla"),
        Binding("t", "run_type", "Kör typ"),
        Binding("s", "stop", "Stoppa"),
        Binding("c", "clear_selection", "Rensa val"),
        Binding("q", "quit", "Avsluta"),
        Binding("m", "toggle_mock", "Mock-läge"),
    ]

    def __init__(self, config_path: str = "config/datasets.yml", mock: bool = False):
        super().__init__()
        self.config = DatasetConfig.load(config_path)
        self.mock_mode = mock
        self.runner: PipelineRunner | None = None
        self._running = False
        self.current_type_filter: str | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Container(id="header-container"):
            with Horizontal():
                mode_text = " [MOCK]" if self.mock_mode else ""
                yield Label(f"G-ETL Admin{mode_text}", id="app-title")
                yield Label("", id="selection-info")

        with Container(id="filter-bar"):
            with Horizontal():
                yield Label("Typ:")
                types = [("Alla typer", None)] + [
                    (t, t) for t in sorted(self.config.get_types())
                ]
                yield Select(types, id="type-filter", value=None)
                yield Label(f"({len(self.config.get_enabled())} aktiva datasets)")

        with Container(id="datasets-panel"):
            with ScrollableContainer(id="datasets-scroll"):
                for dataset in self.config.datasets:
                    yield DatasetRow(dataset)

        with Vertical(id="right-panel"):
            with Container(id="progress-panel"):
                yield Label("Väntar på jobb...", id="progress-label")
                yield ProgressBar(id="progress-bar", show_eta=True)
                yield Label("", id="progress-stats")

            with Container(id="log-panel"):
                yield Log(id="log", highlight=True)

        with Horizontal(id="button-bar"):
            yield Button("Kör valda [R]", id="btn-run", variant="primary")
            yield Button("Kör typ [T]", id="btn-type", variant="warning")
            yield Button("Kör alla [A]", id="btn-all", variant="success")
            yield Button("Stoppa [S]", id="btn-stop", variant="error", disabled=True)

        yield Footer()

    def on_mount(self) -> None:
        """Initialize display on mount."""
        for row in self.query(DatasetRow):
            row._update_display()
        self.update_selection_count()

    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle type filter change."""
        if event.select.id == "type-filter":
            self.current_type_filter = event.value
            self._filter_datasets()

    def _filter_datasets(self) -> None:
        """Filter datasets based on type."""
        for row in self.query(DatasetRow):
            if self.current_type_filter is None:
                row.display = True
            else:
                row.display = row.dataset.typ == self.current_type_filter

    def update_selection_count(self) -> None:
        """Update the selection count display."""
        selected = self.get_selected_datasets()
        info = self.query_one("#selection-info", Label)
        if selected:
            info.update(f"{len(selected)} valda")
        else:
            info.update("")

    def get_selected_datasets(self) -> list[Dataset]:
        """Get list of selected datasets."""
        return [row.dataset for row in self.query(DatasetRow) if row.selected and row.dataset.enabled]

    def get_filtered_datasets(self) -> list[Dataset]:
        """Get datasets matching current type filter."""
        if self.current_type_filter:
            return self.config.get_by_type(self.current_type_filter)
        return self.config.get_enabled()

    def log_message(self, message: str):
        """Add a message to the log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log = self.query_one("#log", Log)
        log.write_line(f"[{timestamp}] {message}")

    def update_dataset_status(self, dataset_id: str, status: DatasetStatus):
        """Update the status of a dataset."""
        for row in self.query(DatasetRow):
            if row.dataset.id == dataset_id:
                row.update_status(status)
                break

    def update_progress(self, current: int, total: int, name: str = ""):
        """Update progress display."""
        label = self.query_one("#progress-label", Label)
        bar = self.query_one("#progress-bar", ProgressBar)
        stats = self.query_one("#progress-stats", Label)

        if total > 0:
            bar.update(total=total, progress=current)
            label.update(f"Kör: {name}" if name else f"Progress: {current}/{total}")
            percent = int((current / total) * 100)
            stats.update(f"{current}/{total} ({percent}%)")
        else:
            label.update("Väntar på jobb...")
            bar.update(total=100, progress=0)
            stats.update("")

    def set_running_state(self, running: bool):
        """Update UI for running/stopped state."""
        self._running = running
        self.query_one("#btn-run", Button).disabled = running
        self.query_one("#btn-type", Button).disabled = running
        self.query_one("#btn-all", Button).disabled = running
        self.query_one("#btn-stop", Button).disabled = not running

    @work(exclusive=True)
    async def run_datasets(self, datasets: list[Dataset]):
        """Run selected datasets."""
        if not datasets:
            self.log_message("Inga datasets att köra")
            return

        self.set_running_state(True)

        # Reset statuses
        for dataset in datasets:
            self.update_dataset_status(dataset.id, DatasetStatus.PENDING)

        self.update_progress(0, 0)

        # Create runner
        if self.mock_mode:
            self.runner = MockPipelineRunner()
        else:
            self.runner = PipelineRunner()

        total = len(datasets)
        completed = 0

        self.log_message(f"Startar {total} dataset(s)...")

        for dataset in datasets:
            if not self._running:
                self.log_message("Avbrutet av användare")
                break

            self.update_dataset_status(dataset.id, DatasetStatus.RUNNING)
            self.update_progress(completed, total, dataset.name)
            self.log_message(f"Kör {dataset.name}...")

            def on_log(msg: str):
                self.log_message(msg)

            success = await self.runner.run_dataset(
                dataset_config=dataset.config,
                on_log=on_log,
            )

            if success:
                self.update_dataset_status(dataset.id, DatasetStatus.COMPLETED)
                self.log_message(f"Klar: {dataset.name}")
            else:
                self.update_dataset_status(dataset.id, DatasetStatus.FAILED)
                self.log_message(f"Fel: {dataset.name}")

            completed += 1

        # Kör transformationer
        if self._running and completed > 0:
            self.log_message("Kör SQL-transformationer...")
            await self.runner.run_transforms(on_log=lambda msg: self.log_message(msg))

        self.update_progress(completed, total)
        self.log_message(f"Klart! {completed}/{total} datasets körda.")
        self.set_running_state(False)

        if self.runner:
            self.runner.close()

    async def action_run_selected(self):
        """Run selected datasets."""
        datasets = self.get_selected_datasets()
        if not datasets:
            self.log_message("Välj datasets genom att klicka på dem")
            return
        self.run_datasets(datasets)

    async def action_run_type(self):
        """Run all datasets of current type filter."""
        datasets = self.get_filtered_datasets()
        type_name = self.current_type_filter or "alla typer"
        self.log_message(f"Kör {len(datasets)} datasets av typ: {type_name}")
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

    def action_clear_selection(self):
        """Clear all selections."""
        for row in self.query(DatasetRow):
            row.set_selected(False)
        self.update_selection_count()
        self.log_message("Rensade alla val")

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
        elif event.button.id == "btn-type":
            await self.action_run_type()
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
