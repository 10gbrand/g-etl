"""Pipeline screen med Docker-stil multi-progress."""

from datetime import datetime

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Label,
    Log,
    Select,
    Static,
)

from scripts.admin.models.dataset import Dataset, DatasetConfig, DatasetStatus
from scripts.admin.services.pipeline_runner import MockPipelineRunner, PipelineRunner
from scripts.admin.widgets.multi_progress import (
    MultiProgressWidget,
    TaskProgress,
    TaskStatus,
)


class DatasetRow(Static):
    """En rad för ett dataset i listan."""

    DEFAULT_CSS = """
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
    """

    def __init__(self, dataset: Dataset) -> None:
        super().__init__()
        self.dataset = dataset
        self.selected = False

    def compose(self) -> ComposeResult:
        """Bygg raden."""
        status = "○" if self.dataset.enabled else "◌"
        typ_short = self.dataset.typ.split("_")[0][:10] if self.dataset.typ else ""
        yield Label(
            f"{status} [{typ_short:10}] {self.dataset.name}",
            id=f"lbl-{self.dataset.id}",
        )

    def on_click(self) -> None:
        """Toggle selection vid klick."""
        if not self.dataset.enabled:
            return
        self.selected = not self.selected
        self._update_display()
        # Meddela parent om selection-ändring
        self.screen.update_selection_count()

    def _update_display(self) -> None:
        """Uppdatera visning baserat på state."""
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

    def update_status(self, status: DatasetStatus) -> None:
        """Uppdatera dataset status."""
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

    def set_selected(self, selected: bool) -> None:
        """Sätt selection state."""
        if self.dataset.enabled:
            self.selected = selected
            self._update_display()


class PipelineScreen(Screen):
    """Screen för att köra pipeline med Docker-stil progress."""

    BINDINGS = [
        Binding("r", "run_selected", "Kör valda"),
        Binding("a", "run_all", "Kör alla"),
        Binding("t", "run_type", "Kör typ"),
        Binding("s", "stop", "Stoppa"),
        Binding("c", "clear_selection", "Rensa val"),
        Binding("e", "app.push_screen('explorer')", "Explorer"),
        Binding("m", "toggle_mock", "Mock-läge"),
        Binding("q", "quit", "Avsluta"),
    ]

    CSS = """
    PipelineScreen {
        layout: vertical;
    }

    #main-content {
        layout: grid;
        grid-size: 2 1;
        grid-columns: 1fr 1fr;
        height: 1fr;
    }

    #header-container {
        height: auto;
        padding: 1;
        background: $primary-darken-2;
    }

    #header-container Horizontal {
        height: auto;
    }

    #filter-bar {
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
        min-height: 10;
        max-height: 50%;
    }

    #log-panel {
        height: 1fr;
        border: solid $accent;
    }

    #button-bar {
        height: auto;
        padding: 1;
        align: center middle;
    }

    Button {
        margin: 0 1;
    }

    #selection-info {
        text-align: right;
        padding-right: 1;
        width: 1fr;
    }
    """

    def __init__(
        self,
        config: DatasetConfig,
        mock: bool = False,
    ) -> None:
        super().__init__()
        self.config = config
        self.mock_mode = mock
        self.runner: PipelineRunner | None = None
        self._running = False
        self.current_type_filter: str | None = None

    def compose(self) -> ComposeResult:
        """Bygg screen-layouten."""
        yield Header(show_clock=True)

        with Container(id="header-container"):
            with Horizontal():
                mode_text = " [MOCK]" if self.mock_mode else ""
                yield Label(f"G-ETL Pipeline{mode_text}", id="app-title")
                yield Label("", id="selection-info")

        with Container(id="filter-bar"):
            with Horizontal():
                yield Label("Typ:")
                types = [("Alla typer", None)] + [
                    (t, t) for t in sorted(self.config.get_types())
                ]
                yield Select(types, id="type-filter", value=None)
                yield Label(f"({len(self.config.get_enabled())} aktiva datasets)")

        with Horizontal(id="main-content"):
            with Container(id="datasets-panel"):
                with ScrollableContainer(id="datasets-scroll"):
                    for dataset in self.config.datasets:
                        yield DatasetRow(dataset)

            with Vertical(id="right-panel"):
                with Container(id="progress-panel"):
                    yield MultiProgressWidget(title="Pipeline")

                with Container(id="log-panel"):
                    yield Log(id="log", highlight=True)

        with Horizontal(id="button-bar"):
            yield Button("Kör valda [R]", id="btn-run", variant="primary")
            yield Button("Kör typ [T]", id="btn-type", variant="warning")
            yield Button("Kör alla [A]", id="btn-all", variant="success")
            yield Button("Stoppa [S]", id="btn-stop", variant="error", disabled=True)
            yield Button("Explorer [E]", id="btn-explorer", variant="default")

        yield Footer()

    def on_mount(self) -> None:
        """Initiera display vid mount."""
        for row in self.query(DatasetRow):
            row._update_display()
        self.update_selection_count()

    def on_select_changed(self, event: Select.Changed) -> None:
        """Hantera typ-filter ändring."""
        if event.select.id == "type-filter":
            self.current_type_filter = event.value
            self._filter_datasets()

    def _filter_datasets(self) -> None:
        """Filtrera datasets baserat på typ."""
        for row in self.query(DatasetRow):
            if self.current_type_filter is None:
                row.display = True
            else:
                row.display = row.dataset.typ == self.current_type_filter

    def update_selection_count(self) -> None:
        """Uppdatera selection count display."""
        selected = self.get_selected_datasets()
        info = self.query_one("#selection-info", Label)
        if selected:
            info.update(f"{len(selected)} valda")
        else:
            info.update("")

    def get_selected_datasets(self) -> list[Dataset]:
        """Hämta lista av valda datasets."""
        return [
            row.dataset
            for row in self.query(DatasetRow)
            if row.selected and row.dataset.enabled
        ]

    def get_filtered_datasets(self) -> list[Dataset]:
        """Hämta datasets som matchar aktuellt typ-filter."""
        if self.current_type_filter:
            return self.config.get_by_type(self.current_type_filter)
        return self.config.get_enabled()

    def log_message(self, message: str) -> None:
        """Lägg till meddelande i loggen."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log = self.query_one("#log", Log)
        log.write_line(f"[{timestamp}] {message}")

    def update_dataset_status(self, dataset_id: str, status: DatasetStatus) -> None:
        """Uppdatera status för ett dataset."""
        for row in self.query(DatasetRow):
            if row.dataset.id == dataset_id:
                row.update_status(status)
                break

    def set_running_state(self, running: bool) -> None:
        """Uppdatera UI för körande/stoppad state."""
        self._running = running
        self.query_one("#btn-run", Button).disabled = running
        self.query_one("#btn-type", Button).disabled = running
        self.query_one("#btn-all", Button).disabled = running
        self.query_one("#btn-stop", Button).disabled = not running

    @work(exclusive=True)
    async def run_datasets(self, datasets: list[Dataset]) -> None:
        """Kör valda datasets med Docker-stil progress."""
        if not datasets:
            self.log_message("Inga datasets att köra")
            return

        self.set_running_state(True)
        progress = self.query_one(MultiProgressWidget)
        progress.reset()

        # Reset statuses och lägg till i progress
        for i, dataset in enumerate(datasets):
            self.update_dataset_status(dataset.id, DatasetStatus.PENDING)
            progress.add_task(
                TaskProgress(
                    id=dataset.id,
                    name=dataset.name,
                    status=TaskStatus.QUEUED,
                    queue_position=i + 1,
                )
            )

        # Skapa runner
        if self.mock_mode:
            self.runner = MockPipelineRunner()
        else:
            self.runner = PipelineRunner()

        progress.set_phase("Extract")
        self.log_message(f"Startar {len(datasets)} dataset(s)...")

        # Kör datasets sekventiellt
        for i, dataset in enumerate(datasets):
            if not self._running:
                self.log_message("Avbrutet av användare")
                # Markera återstående som skipped
                for remaining in datasets[i:]:
                    progress.update_task(remaining.id, status=TaskStatus.SKIPPED)
                break

            # Uppdatera status
            self.update_dataset_status(dataset.id, DatasetStatus.RUNNING)
            progress.update_task(
                dataset.id,
                status=TaskStatus.RUNNING,
                progress=0.0,
                message="Startar...",
            )

            # Callback för logg-meddelanden
            def on_log(msg: str, ds_id: str = dataset.id) -> None:
                self.log_message(msg)

            # Callback för progress-events
            def on_event(event, ds_id: str = dataset.id) -> None:
                if event.event_type == "progress":
                    progress.update_task(
                        ds_id,
                        progress=event.progress or 0.0,
                        message=event.message,
                    )
                elif event.event_type == "dataset_completed":
                    progress.update_task(
                        ds_id,
                        status=TaskStatus.COMPLETED,
                        progress=1.0,
                        rows_count=event.rows_count,
                    )
                elif event.event_type == "dataset_failed":
                    progress.update_task(
                        ds_id,
                        status=TaskStatus.FAILED,
                        error=event.message,
                    )

            self.log_message(f"Kör {dataset.name}...")

            success = await self.runner.run_dataset(
                dataset_config=dataset.config,
                on_event=on_event,
                on_log=on_log,
            )

            if success:
                self.update_dataset_status(dataset.id, DatasetStatus.COMPLETED)
                self.log_message(f"Klar: {dataset.name}")
            else:
                self.update_dataset_status(dataset.id, DatasetStatus.FAILED)
                self.log_message(f"Fel: {dataset.name}")

        # Kör transformationer
        if self._running:
            progress.set_phase("Transform")
            self.log_message("Kör SQL-transformationer...")
            progress.set_status("Kör SQL-transformationer...")

            await self.runner.run_transforms(
                on_log=lambda msg: self.log_message(msg)
            )

        self.log_message("Klart!")
        progress.set_status("")
        self.set_running_state(False)

        if self.runner:
            self.runner.close()

    async def action_run_selected(self) -> None:
        """Kör valda datasets."""
        datasets = self.get_selected_datasets()
        if not datasets:
            self.log_message("Välj datasets genom att klicka på dem")
            return
        self.run_datasets(datasets)

    async def action_run_type(self) -> None:
        """Kör alla datasets av aktuell typ."""
        datasets = self.get_filtered_datasets()
        type_name = self.current_type_filter or "alla typer"
        self.log_message(f"Kör {len(datasets)} datasets av typ: {type_name}")
        self.run_datasets(datasets)

    async def action_run_all(self) -> None:
        """Kör alla aktiverade datasets."""
        datasets = self.config.get_enabled()
        self.run_datasets(datasets)

    async def action_stop(self) -> None:
        """Stoppa pågående jobb."""
        self._running = False
        if self.runner:
            await self.runner.stop()
        self.log_message("Stoppar...")
        self.set_running_state(False)

    def action_clear_selection(self) -> None:
        """Rensa alla val."""
        for row in self.query(DatasetRow):
            row.set_selected(False)
        self.update_selection_count()
        self.log_message("Rensade alla val")

    def action_toggle_mock(self) -> None:
        """Växla mock-läge."""
        self.mock_mode = not self.mock_mode
        mode_text = "MOCK" if self.mock_mode else "LIVE"
        self.log_message(f"Växlade till {mode_text}-läge")
        title = self.query_one("#app-title", Label)
        title.update(f"G-ETL Pipeline [{mode_text}]")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Hantera knapptryckningar."""
        if event.button.id == "btn-run":
            await self.action_run_selected()
        elif event.button.id == "btn-type":
            await self.action_run_type()
        elif event.button.id == "btn-all":
            await self.action_run_all()
        elif event.button.id == "btn-stop":
            await self.action_stop()
        elif event.button.id == "btn-explorer":
            self.app.push_screen("explorer")
