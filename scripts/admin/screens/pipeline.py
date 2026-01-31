"""Pipeline screen med Docker-stil multi-progress."""

from datetime import datetime

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Label,
    Log,
    Select,
    Static,
)

from scripts.admin.models.dataset import Dataset, DatasetConfig, DatasetStatus
from scripts.admin.services.db_session import (
    cleanup_all_databases,
    cleanup_all_logs,
    cleanup_all_parquet,
    get_data_stats,
)
from scripts.admin.services.pipeline_runner import (
    MockPipelineRunner,
    PipelineRunner,
)
from scripts.admin.widgets.multi_progress import (
    MultiProgressWidget,
    TaskProgress,
    TaskStatus,
)
from scripts.pipeline import FileLogger


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
        Binding("d", "clear_data", "Rensa data"),
        Binding("e", "app.push_screen('explorer')", "Explorer"),
        Binding("g", "app.push_screen('migrations')", "Migrationer"),
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

    #filter-bar Checkbox {
        margin-left: 2;
    }

    #phase-bar {
        height: auto;
        padding: 0 1;
        background: $surface-darken-1;
    }

    #phase-bar Horizontal {
        height: auto;
    }

    #phase-bar Label {
        padding: 0 1;
    }

    #phase-bar Checkbox {
        margin-left: 1;
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
        db_path: str = "data/warehouse.duckdb",
    ) -> None:
        super().__init__()
        self.config = config
        self.mock_mode = mock
        self.db_path = db_path
        self.runner: PipelineRunner | None = None
        self._running = False
        self.current_type_filter: str | None = None
        self._file_logger: FileLogger | None = None

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
                yield Checkbox("Endast saknade", id="skip-existing", value=False)

        with Container(id="phase-bar"):
            with Horizontal():
                yield Label("Faser:")
                yield Checkbox("1. Extract", id="phase-extract", value=True)
                yield Checkbox("2. Staging", id="phase-staging", value=True)
                yield Checkbox("3. Staging_2", id="phase-staging2", value=True)
                yield Checkbox("4. Mart", id="phase-mart", value=True)

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

    def get_existing_parquet_ids(self) -> set[str]:
        """Hämta ID:n för datasets som har befintliga parquet-filer i data/raw/."""
        from pathlib import Path
        raw_dir = Path("data/raw")
        existing_ids = set()
        if raw_dir.exists():
            for parquet_file in raw_dir.glob("*.parquet"):
                # Filnamnet är dataset-ID
                dataset_id = parquet_file.stem
                existing_ids.add(dataset_id)
        return existing_ids

    def log_message(self, message: str) -> None:
        """Lägg till meddelande i loggen (UI + fil)."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log = self.query_one("#log", Log)
        log.write_line(f"[{timestamp}] {message}")

        # Skriv även till fil om loggning är aktiv
        if self._file_logger:
            self._file_logger.log(message)

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

    def get_selected_phases(self) -> tuple[bool, bool, bool, bool]:
        """Hämta vilka faser som är valda."""
        extract = self.query_one("#phase-extract", Checkbox).value
        staging = self.query_one("#phase-staging", Checkbox).value
        staging2 = self.query_one("#phase-staging2", Checkbox).value
        mart = self.query_one("#phase-mart", Checkbox).value
        return extract, staging, staging2, mart

    @work(exclusive=True)
    async def run_datasets(self, datasets: list[Dataset]) -> None:
        """Kör valda datasets med parallell extraktion och Docker-stil progress."""
        if not datasets:
            self.log_message("Inga datasets att köra")
            return

        # Hämta valda faser
        run_extract, run_staging, run_staging2, run_mart = self.get_selected_phases()
        if not any([run_extract, run_staging, run_staging2, run_mart]):
            self.log_message("Ingen fas vald - välj minst en fas att köra")
            return

        phases_str = ", ".join(
            name for name, enabled in [
                ("Extract", run_extract),
                ("Staging", run_staging),
                ("Staging_2", run_staging2),
                ("Mart", run_mart)
            ] if enabled
        )
        self.log_message(f"Kör faser: {phases_str}")

        # Starta filloggning
        self._file_logger = FileLogger(prefix="tui_pipeline")
        log_file = self._file_logger.start()
        self.log_message(f"Loggar till: {log_file}")

        self.set_running_state(True)
        progress = self.query_one(MultiProgressWidget)
        progress.reset()

        # Kolla om vi ska hoppa över befintliga parquet-filer
        skip_existing = self.query_one("#skip-existing", Checkbox).value
        existing_ids = self.get_existing_parquet_ids() if skip_existing else set()

        from pathlib import Path
        from scripts.admin.services.pipeline_runner import ParallelExtractResult

        # Skapa runner
        if self.mock_mode:
            self.runner = MockPipelineRunner()
        else:
            self.runner = PipelineRunner(db_path=self.db_path)

        # Lista med dataset-IDs som ska processas i staging/mart
        loaded_ids = [ds.id for ds in datasets]
        all_parquet_files = []

        # === FAS 1: EXTRACT (Plugins) ===
        if run_extract:
            # Filtrera datasets för extraktion (hoppa över befintliga om checkboxen är aktiv)
            datasets_to_extract = [
                ds for ds in datasets if ds.id not in existing_ids
            ]
            skipped_datasets = [ds for ds in datasets if ds.id in existing_ids]

            if skip_existing and skipped_datasets:
                self.log_message(
                    f"Hoppar över {len(skipped_datasets)} dataset med befintliga parquet-filer: "
                    f"{', '.join(ds.id for ds in skipped_datasets)}"
                )

            # Reset statuses och lägg till i progress (alla datasets)
            for i, dataset in enumerate(datasets):
                self.update_dataset_status(dataset.id, DatasetStatus.PENDING)
                # Markera skippade som "completed" direkt
                if dataset.id in existing_ids:
                    progress.add_task(
                        TaskProgress(
                            id=dataset.id,
                            name=dataset.name,
                            status=TaskStatus.COMPLETED,
                            message="Befintlig parquet",
                        )
                    )
                else:
                    progress.add_task(
                        TaskProgress(
                            id=dataset.id,
                            name=dataset.name,
                            status=TaskStatus.QUEUED,
                            queue_position=i + 1,
                        )
                    )

            progress.set_phase("1. Extract (parallell)")
            if datasets_to_extract:
                self.log_message(f"Startar parallell extraktion av {len(datasets_to_extract)} dataset(s)...")
            else:
                self.log_message("Alla valda datasets har befintliga parquet-filer, hoppar extraktion")

            # Callback för progress-events under extraktion
            def on_extract_event(event) -> None:
                ds_id = event.dataset
                if event.event_type == "dataset_started":
                    self.update_dataset_status(ds_id, DatasetStatus.RUNNING)
                    progress.update_task(
                        ds_id,
                        status=TaskStatus.RUNNING,
                        progress=0.0,
                        message="Startar...",
                    )
                elif event.event_type == "progress":
                    progress.update_task(
                        ds_id,
                        progress=event.progress or 0.0,
                        message=event.message,
                    )
                elif event.event_type == "dataset_completed":
                    self.update_dataset_status(ds_id, DatasetStatus.COMPLETED)
                    progress.update_task(
                        ds_id,
                        status=TaskStatus.COMPLETED,
                        progress=1.0,
                        rows_count=event.rows_count,
                    )
                    self.log_message(f"Klar: {ds_id} ({event.rows_count} rader)")
                elif event.event_type == "dataset_failed":
                    self.update_dataset_status(ds_id, DatasetStatus.FAILED)
                    progress.update_task(
                        ds_id,
                        status=TaskStatus.FAILED,
                        error=event.message,
                    )
                    self.log_message(f"Fel: {ds_id} - {event.message}")

            # Kör parallell extraktion (endast för datasets som ska extraheras)
            if datasets_to_extract:
                extract_result = await self.runner.run_parallel_extract(
                    dataset_configs=[ds.config for ds in datasets_to_extract],
                    output_dir="data/raw",
                    max_concurrent=4,
                    on_event=on_extract_event,
                    on_log=lambda msg: self.log_message(msg),
                )
            else:
                extract_result = ParallelExtractResult(success=True, parquet_files=[], failed=[])

            if not self._running:
                self.log_message("Avbrutet av användare")
                self.set_running_state(False)
                return

            # Lägg till befintliga parquet-filer för skippade datasets
            skipped_parquet_files = [
                (ds.id, str(Path("data/raw") / f"{ds.id}.parquet"))
                for ds in skipped_datasets
            ]

            # Kombinera extraherade och skippade filer
            all_parquet_files = extract_result.parquet_files + skipped_parquet_files

            # Rapportera resultat
            if datasets_to_extract:
                self.log_message(
                    f"Extraktion klar: {len(extract_result.parquet_files)} lyckade, "
                    f"{len(extract_result.failed)} misslyckade"
                )
            if skipped_datasets:
                self.log_message(f"Använder {len(skipped_parquet_files)} befintliga parquet-filer")

                loaded_ids = [ds_id for ds_id, _ in all_parquet_files]
        else:
            self.log_message("Hoppar över Extract-fasen")

        # === FAS 2: PARALLELL TRANSFORM ===
        # Kör alla templates (staging, staging_2, mart) parallellt per dataset
        # med separata temp-DBs för äkta parallelism
        run_any_sql = run_staging or run_staging2 or run_mart
        if run_any_sql and self._running and all_parquet_files:
            progress.set_phase("2. Parallell Transform")

            # Reset progress för transform-fasen
            for i, (ds_id, _) in enumerate(all_parquet_files):
                progress.update_task(
                    ds_id,
                    status=TaskStatus.QUEUED,
                    progress=0.0,
                    message="Väntar på transform...",
                    queue_position=i + 1,
                )

            def on_transform_event(event) -> None:
                if event.event_type == "dataset_started" and event.dataset:
                    progress.update_task(
                        event.dataset,
                        status=TaskStatus.RUNNING,
                        progress=0.2,
                        message="Transform...",
                    )
                elif event.event_type == "dataset_completed" and event.dataset:
                    progress.update_task(
                        event.dataset,
                        status=TaskStatus.COMPLETED,
                        progress=1.0,
                        message="Transform klar",
                    )
                elif event.event_type == "dataset_failed" and event.dataset:
                    progress.update_task(
                        event.dataset,
                        status=TaskStatus.FAILED,
                        progress=0.0,
                        message=event.message or "Fel",
                    )

            # Kör parallell transform (raw → staging → staging_2 → mart per dataset)
            temp_dbs = await self.runner.run_parallel_transform(
                parquet_files=all_parquet_files,
                on_log=lambda msg: self.log_message(msg),
                on_event=on_transform_event,
            )

            # === FAS 3: MERGE ===
            # Slå ihop temp-DBs till warehouse
            if temp_dbs and self._running:
                progress.set_phase("3. Merge → warehouse")
                self.log_message(f"Slår ihop {len(temp_dbs)} databaser...")

                await self.runner.merge_databases(
                    temp_dbs=temp_dbs,
                    on_log=lambda msg: self.log_message(msg),
                )

            # === FAS 4: POST-MERGE SQL ===
            # Kör *_merged.sql för aggregeringar över alla datasets
            if self._running:
                progress.set_phase("4. Post-merge SQL")
                await self.runner.run_merged_sql(
                    on_log=lambda msg: self.log_message(msg),
                )

        elif not run_any_sql:
            self.log_message("Hoppar över SQL-faserna")

        # Uppdatera den fasta warehouse.duckdb med senaste körningen
        from scripts.admin.services.db_session import update_current_db
        current_db = update_current_db()
        if current_db:
            self.log_message(f"Uppdaterade {current_db.name}")

        self.log_message("Klart!")
        progress.set_status("")
        self.set_running_state(False)

        if self.runner:
            self.runner.close()

        # Stäng filloggning
        if self._file_logger:
            self._file_logger.close()
            self._file_logger = None

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

        # Stäng filloggning vid stopp
        if self._file_logger:
            self._file_logger.close()
            self._file_logger = None

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

    def action_quit(self) -> None:
        """Avsluta applikationen."""
        self.app.exit()

    async def action_clear_data(self) -> None:
        """Rensa alla datafiler (databaser, parquet och loggar)."""
        # Visa aktuell statistik
        stats = get_data_stats()
        self.log_message(
            f"Nuvarande data: {stats['db_count']} databaser ({stats['db_size_mb']} MB), "
            f"{stats['parquet_count']} parquet ({stats['parquet_size_mb']} MB), "
            f"{stats['log_count']} loggar ({stats['log_size_mb']} MB)"
        )

        # Rensa databaser
        db_count, db_size = cleanup_all_databases()
        if db_count > 0:
            self.log_message(f"Tog bort {db_count} databasfiler ({db_size} MB)")

        # Rensa parquet-filer
        pq_count, pq_size = cleanup_all_parquet()
        if pq_count > 0:
            self.log_message(f"Tog bort {pq_count} parquet-filer ({pq_size} MB)")

        # Rensa loggfiler
        log_count, log_size = cleanup_all_logs()
        if log_count > 0:
            self.log_message(f"Tog bort {log_count} loggfiler ({log_size} MB)")

        total_count = db_count + pq_count + log_count
        if total_count == 0:
            self.log_message("Inga filer att rensa")
        else:
            total_size = db_size + pq_size + log_size
            self.log_message(f"Totalt rensat: {total_count} filer ({total_size} MB)")

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
