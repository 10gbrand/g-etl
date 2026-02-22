"""Pipeline screen med inline steg-prickar per dataset.

Varje dataset visas som en rad med 5 steg (Ext, Stg, Mrt, Mrg, Exp)
och en statustext som visar vad som pagar just nu.
"""

import time

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
    Select,
)

from g_etl.admin.models.dataset import Dataset, DatasetConfig
from g_etl.admin.widgets.detail_panel import DetailPanel
from g_etl.admin.widgets.pipeline_row import (
    PipelineRow,
    StepLegend,
    StepState,
    TotalProgressBar,
)
from g_etl.services.db_session import (
    cleanup_all_databases,
    cleanup_all_logs,
    cleanup_all_parquet,
    cleanup_data_subdirs,
    get_data_stats,
)
from g_etl.services.pipeline_runner import (
    MockPipelineRunner,
    PipelineRunner,
)
from g_etl.settings import settings
from g_etl.utils.logging import FileLogger

# Steg-index (matchar STEP_NAMES i pipeline_row.py)
STEP_EXT = 0
STEP_STG = 1
STEP_MRT = 2
STEP_MRG = 3
STEP_EXP = 4


class PipelineScreen(Screen):
    """Pipeline-vy med inline steg-prickar per dataset."""

    BINDINGS = [
        Binding("r", "run_selected", "Kor valda"),
        Binding("a", "run_all", "Kor alla"),
        Binding("t", "run_type", "Kor typ"),
        Binding("s", "stop", "Stoppa"),
        Binding("c", "clear_selection", "Rensa val"),
        Binding("d", "clear_data", "Rensa data"),
        Binding("escape", "close_panel", "Stang panel"),
        Binding("e", "app.push_screen('explorer')", "Explorer"),
        Binding("h", "app.push_screen('h3_query')", "H3 Query"),
        Binding("g", "app.push_screen('migrations')", "Migrationer"),
        Binding("m", "toggle_mock", "Mock-lage"),
        Binding("q", "quit", "Avsluta"),
    ]

    CSS = """
    PipelineScreen {
        layout: vertical;
    }

    #title-bar {
        height: auto;
        padding: 0 2;
        background: $primary-darken-2;
    }

    #title-bar Horizontal {
        height: auto;
    }

    #title-bar Label {
        padding: 0 1;
    }

    #title-text {
        text-style: bold;
    }

    #title-right {
        text-align: right;
        width: 1fr;
        color: $text-muted;
    }

    #settings-bar {
        height: auto;
        padding: 0 1;
        background: $surface-darken-1;
    }

    #settings-bar Horizontal {
        height: auto;
    }

    #settings-bar Label {
        padding: 0 1;
    }

    #settings-bar Select {
        width: 35;
    }

    #settings-bar Checkbox {
        margin-left: 1;
    }

    #settings-divider {
        margin-left: 2;
        color: $text-muted;
    }

    #step-legend-top {
        height: auto;
        padding: 1 0 0 0;
    }

    #main-area {
        height: 1fr;
    }

    #left-column {
        width: 1fr;
        height: 100%;
    }

    #datasets-scroll {
        height: 1fr;
        padding: 0 0;
    }

    #step-legend-bottom {
        height: auto;
        padding: 0 0;
    }

    #progress-bar-container {
        height: auto;
        padding: 0 0;
        border-top: solid $primary-darken-2;
    }

    #button-bar {
        height: auto;
        padding: 0 1;
        align: center middle;
        background: $surface-darken-1;
    }

    #button-bar Button {
        margin: 0 1;
    }

    #button-bar .btn-spacer {
        width: 2;
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
        self._start_time: float = 0.0

    def compose(self) -> ComposeResult:
        """Bygg den platta pipeline-layouten."""
        yield Header(show_clock=True)

        # Titelrad
        with Container(id="title-bar"):
            with Horizontal():
                mode = "MOCK" if self.mock_mode else "LIVE"
                yield Label("G-ETL Pipeline", id="title-text")
                n_enabled = len(self.config.get_enabled())
                n_total = len(self.config.datasets)
                yield Label(
                    f"{n_enabled} aktiva av {n_total} datasets \u00b7 {mode}",
                    id="title-right",
                )

        # Installningar - en rad
        with Container(id="settings-bar"):
            with Horizontal():
                yield Label("Typ:")
                types = [("Alla typer", None)] + [(t, t) for t in sorted(self.config.get_types())]
                yield Select(types, id="type-filter", value=None)
                yield Checkbox("Ext", id="phase-extract", value=True)
                yield Checkbox("Stg", id="phase-staging", value=True)
                yield Checkbox("Mrt", id="phase-mart", value=True)
                yield Checkbox("Exp", id="phase-export", value=False)
                yield Label("\u2502", id="settings-divider")
                yield Checkbox("Beh\u00e5ll staging", id="keep-staging", value=False)
                yield Checkbox("Spara SQL", id="save-sql", value=False)
                yield Checkbox("Saknade", id="skip-existing", value=False)

        # Steg-legend ovanfor listan
        with Container(id="step-legend-top"):
            yield StepLegend()

        # Mittdel: dataset-rader (vanster) + detaljpanel (hoger, dolj)
        with Horizontal(id="main-area"):
            with Vertical(id="left-column"):
                with ScrollableContainer(id="datasets-scroll"):
                    for dataset in self.config.datasets:
                        yield PipelineRow(dataset)

            yield DetailPanel(id="detail-panel")

        # Steg-legend under listan
        with Container(id="step-legend-bottom"):
            yield StepLegend()

        # Total progress
        with Container(id="progress-bar-container"):
            yield TotalProgressBar(id="total-progress")

        # Knappar
        with Horizontal(id="button-bar"):
            yield Button("\u25b6 Kor", id="btn-run", variant="primary")
            yield Button("\u25b6\u25b6 Alla", id="btn-all", variant="success")
            yield Button("\u25a0 Stopp", id="btn-stop", variant="error", disabled=True)

        yield Footer()

    def on_mount(self) -> None:
        """Initiera vid mount."""
        self.update_title_info()

    # --- Event handlers ---

    def on_select_changed(self, event: Select.Changed) -> None:
        """Hantera typ-filter."""
        if event.select.id == "type-filter":
            self.current_type_filter = event.value
            self._filter_datasets()

    def _filter_datasets(self) -> None:
        """Filtrera dataset-rader baserat pa typ."""
        for row in self.query(PipelineRow):
            if self.current_type_filter is None:
                row.display = True
            else:
                row.display = row.dataset.typ == self.current_type_filter

    def update_title_info(self) -> None:
        """Uppdatera titelradens info."""
        selected = len(self.get_selected_datasets())
        n_enabled = len(self.config.get_enabled())
        n_total = len(self.config.datasets)
        mode = "MOCK" if self.mock_mode else "LIVE"
        info = self.query_one("#title-right", Label)
        if selected > 0:
            info.update(f"{selected} valda \u00b7 {n_enabled} aktiva av {n_total} \u00b7 {mode}")
        else:
            info.update(f"{n_enabled} aktiva av {n_total} datasets \u00b7 {mode}")

    def on_pipeline_row_selection_changed(self, event: PipelineRow.SelectionChanged) -> None:
        """Reagera pa selection-andringar i rader."""
        self.update_title_info()

    def on_pipeline_row_step_clicked(self, event: PipelineRow.StepClicked) -> None:
        """Oppna detaljpanelen for ett klickat steg."""
        panel = self.query_one("#detail-panel", DetailPanel)
        panel.show_step(event.dataset, event.step_index, self.db_path)

    def action_close_panel(self) -> None:
        """Stang detaljpanelen."""
        panel = self.query_one("#detail-panel", DetailPanel)
        if panel.is_visible:
            panel.hide()
        else:
            # Om panelen redan ar stangd, lat Escape poppa screen
            self.app.pop_screen()

    # --- Hjalp-metoder ---

    def get_selected_datasets(self) -> list[Dataset]:
        """Hamta valda datasets."""
        return [
            row.dataset for row in self.query(PipelineRow) if row.selected and row.dataset.enabled
        ]

    def get_filtered_datasets(self) -> list[Dataset]:
        """Hamta datasets som matchar aktuellt typ-filter."""
        if self.current_type_filter:
            return self.config.get_by_type(self.current_type_filter)
        return self.config.get_enabled()

    def get_existing_parquet_ids(self) -> set[str]:
        """Hamta IDs for datasets med befintliga parquet-filer."""
        from pathlib import Path

        raw_dir = Path("data/raw")
        existing_ids = set()
        if raw_dir.exists():
            for pf in raw_dir.glob("*.parquet"):
                existing_ids.add(pf.stem)
        return existing_ids

    def get_row(self, dataset_id: str) -> PipelineRow | None:
        """Hamta PipelineRow for ett dataset-ID."""
        for row in self.query(PipelineRow):
            if row.dataset.id == dataset_id:
                return row
        return None

    def get_selected_phases(self) -> tuple[bool, bool, bool]:
        """Hamta vilka faser som ar valda (extract, staging, mart)."""
        extract = self.query_one("#phase-extract", Checkbox).value
        staging = self.query_one("#phase-staging", Checkbox).value
        mart = self.query_one("#phase-mart", Checkbox).value
        return extract, staging, mart

    def set_running_state(self, running: bool) -> None:
        """Uppdatera UI for korande/stoppad."""
        self._running = running
        self.query_one("#btn-run", Button).disabled = running
        self.query_one("#btn-all", Button).disabled = running
        self.query_one("#btn-stop", Button).disabled = not running

    def log_to_file(self, message: str) -> None:
        """Logga till fil (om aktiv)."""
        if self._file_logger:
            self._file_logger.log(message)

    def _reset_rows(self, datasets: list[Dataset]) -> None:
        """Aterstall alla valda rader for ny korning."""
        for ds in datasets:
            row = self.get_row(ds.id)
            if row:
                row.steps = [StepState.IDLE] * 5
                row.rows_count = None
                row.elapsed = 0.0
                row.error = None
                row.status_text = ""
                row.remove_class("row-done", "row-failed")
                row.refresh()

    # --- Pipeline-korning ---

    @work(exclusive=True)
    async def run_datasets(self, datasets: list[Dataset]) -> None:
        """Kor valda datasets med inline steg-uppdateringar."""
        if not datasets:
            return

        run_extract, run_staging, run_mart = self.get_selected_phases()
        run_export = self.query_one("#phase-export", Checkbox).value
        keep_staging = self.query_one("#keep-staging", Checkbox).value
        save_sql = self.query_one("#save-sql", Checkbox).value

        if not any([run_extract, run_staging, run_mart]):
            return

        # Starta filloggning
        self._file_logger = FileLogger(logs_dir=settings.LOGS_DIR, prefix="tui_pipeline")
        self._file_logger.start()
        self._start_time = time.monotonic()

        self.set_running_state(True)
        self._reset_rows(datasets)

        # Progress-bar
        total_bar = self.query_one("#total-progress", TotalProgressBar)
        total_bar.total = len(datasets)
        total_bar.completed = 0
        total_bar.failed = 0
        total_bar.finished = False
        total_bar.total_rows = 0
        total_bar.elapsed = 0.0

        # Kolla om vi ska hoppa over befintliga parquet-filer
        skip_existing = self.query_one("#skip-existing", Checkbox).value
        existing_ids = self.get_existing_parquet_ids() if skip_existing else set()

        from pathlib import Path

        from g_etl.services.pipeline_runner import ParallelExtractResult

        # Skapa runner
        if self.mock_mode:
            self.runner = MockPipelineRunner()
        else:
            self.runner = PipelineRunner(db_path=self.db_path)

        all_parquet_files = []

        # === FAS 1: EXTRACT ===
        if run_extract:
            datasets_to_extract = [ds for ds in datasets if ds.id not in existing_ids]
            skipped_datasets = [ds for ds in datasets if ds.id in existing_ids]

            # Markera skippade som klara direkt
            for ds in skipped_datasets:
                row = self.get_row(ds.id)
                if row:
                    row.set_step(STEP_EXT, StepState.DONE)
                    row.set_status("befintlig parquet")

            # Markera de som ska koras
            for ds in datasets_to_extract:
                row = self.get_row(ds.id)
                if row:
                    row.set_status("vantar...")

            ds_timers: dict[str, float] = {}

            def on_extract_event(event) -> None:
                ds_id = event.dataset
                row = self.get_row(ds_id)
                if not row:
                    return

                if event.event_type == "dataset_started":
                    row.set_step(STEP_EXT, StepState.RUNNING)
                    row.set_status("extract: startar...")
                    ds_timers[ds_id] = time.monotonic()
                    self.log_to_file(f"Extract startar: {ds_id}")
                elif event.event_type == "progress":
                    row.set_status(f"extract: {event.message or ''}")
                elif event.event_type == "dataset_completed":
                    row.set_step(STEP_EXT, StepState.DONE)
                    elapsed = time.monotonic() - ds_timers.get(ds_id, time.monotonic())
                    row.set_status(
                        f"extract klar ({event.rows_count or 0:,} rader, {elapsed:.1f}s)"
                    )
                    self.log_to_file(f"Extract klar: {ds_id} ({event.rows_count} rader)")
                elif event.event_type == "dataset_failed":
                    row.set_step(STEP_EXT, StepState.FAILED)
                    row.set_failed(f"extract: {event.message}")
                    self.log_to_file(f"Extract FEL: {ds_id} - {event.message}")
                    total_bar.failed += 1

                # Uppdatera total elapsed
                total_bar.elapsed = time.monotonic() - self._start_time

            if datasets_to_extract:
                self.log_to_file(f"Startar extract for {len(datasets_to_extract)} datasets")
                extract_result = await self.runner.run_parallel_extract(
                    dataset_configs=[ds.config for ds in datasets_to_extract],
                    output_dir="data/raw",
                    max_concurrent=4,
                    on_event=on_extract_event,
                    on_log=lambda msg: self.log_to_file(msg),
                )
            else:
                extract_result = ParallelExtractResult(success=True, parquet_files=[], failed=[])

            if not self._running:
                self.set_running_state(False)
                return

            skipped_parquet_files = [
                (ds.id, str(Path("data/raw") / f"{ds.id}.parquet")) for ds in skipped_datasets
            ]
            all_parquet_files = extract_result.parquet_files + skipped_parquet_files
        else:
            # Hoppa over extract - markera som skipped
            for ds in datasets:
                row = self.get_row(ds.id)
                if row:
                    row.set_step(STEP_EXT, StepState.SKIPPED)

        # === FAS 2: PARALLELL TRANSFORM (Staging + Mart) ===
        run_any_sql = run_staging or run_mart
        if run_any_sql and self._running and all_parquet_files:
            # Markera steg som idle/running
            for ds_id, _ in all_parquet_files:
                row = self.get_row(ds_id)
                if row and not row.error:
                    if run_staging:
                        row.set_step(STEP_STG, StepState.IDLE)
                    if run_mart:
                        row.set_step(STEP_MRT, StepState.IDLE)
                    row.set_status("vantar pa transform...")

            ds_timers_t: dict[str, float] = {}

            def on_transform_event(event) -> None:
                if not event.dataset:
                    return
                ds_id = event.dataset
                row = self.get_row(ds_id)
                if not row:
                    return

                if event.event_type == "dataset_started":
                    ds_timers_t[ds_id] = time.monotonic()
                    if run_staging:
                        row.set_step(STEP_STG, StepState.RUNNING)
                        row.set_status("staging: transformerar...")
                    elif run_mart:
                        row.set_step(STEP_MRT, StepState.RUNNING)
                        row.set_status("mart: aggregerar...")
                    self.log_to_file(f"Transform startar: {ds_id}")
                elif event.event_type == "dataset_completed":
                    if run_staging:
                        row.set_step(STEP_STG, StepState.DONE)
                    if run_mart:
                        row.set_step(STEP_MRT, StepState.DONE)
                    elapsed = time.monotonic() - ds_timers_t.get(ds_id, time.monotonic())
                    row.set_status(f"transform klar ({elapsed:.1f}s)")
                    self.log_to_file(f"Transform klar: {ds_id}")
                elif event.event_type == "dataset_failed":
                    if run_staging:
                        row.set_step(STEP_STG, StepState.FAILED)
                    row.set_failed(f"transform: {event.message or 'Fel'}")
                    self.log_to_file(f"Transform FEL: {ds_id} - {event.message}")
                    total_bar.failed += 1

                total_bar.elapsed = time.monotonic() - self._start_time

            temp_dbs = await self.runner.run_parallel_transform(
                parquet_files=all_parquet_files,
                phases=(run_staging, run_mart),
                on_log=lambda msg: self.log_to_file(msg),
                on_event=on_transform_event,
                save_sql=save_sql,
            )

            # === FAS 3: MERGE ===
            if temp_dbs and self._running:
                # Markera merge-steg som running pa alla
                for ds_id, _ in all_parquet_files:
                    row = self.get_row(ds_id)
                    if row and not row.error:
                        row.set_step(STEP_MRG, StepState.RUNNING)
                        row.set_status("merge...")

                self.log_to_file(f"Slar ihop {len(temp_dbs)} databaser...")

                await self.runner.merge_databases(
                    temp_dbs=temp_dbs,
                    on_log=lambda msg: self.log_to_file(msg),
                    keep_staging=keep_staging,
                )

                # Markera merge som klart
                for ds_id, _ in all_parquet_files:
                    row = self.get_row(ds_id)
                    if row and not row.error:
                        row.set_step(STEP_MRG, StepState.DONE)
                        row.set_status("merge klar")

                total_bar.elapsed = time.monotonic() - self._start_time

            # === FAS 4: POST-MERGE SQL ===
            if self._running:
                self.log_to_file("Kor post-merge SQL...")
                await self.runner.run_merged_sql(
                    on_log=lambda msg: self.log_to_file(msg),
                )

            # === FAS 5: EXPORT ===
            if run_export and self._running and self.runner._conn:
                for ds_id, _ in all_parquet_files:
                    row = self.get_row(ds_id)
                    if row and not row.error:
                        row.set_step(STEP_EXP, StepState.RUNNING)
                        row.set_status("exporterar...")

                self.log_to_file("Exporterar mart-tabeller till GeoParquet...")

                from g_etl.export import export_mart_tables

                output_dir = Path("data/output")

                def do_export():
                    return export_mart_tables(
                        conn=self.runner._conn,
                        output_dir=output_dir,
                        export_format="geoparquet",
                        on_log=lambda msg: self.log_to_file(msg),
                    )

                import asyncio

                loop = asyncio.get_event_loop()
                exported = await loop.run_in_executor(None, do_export)
                self.log_to_file(f"Exporterade {len(exported)} filer till {output_dir}/")

                for ds_id, _ in all_parquet_files:
                    row = self.get_row(ds_id)
                    if row and not row.error:
                        row.set_step(STEP_EXP, StepState.DONE)
            elif not run_export:
                # Markera export som skipped
                for ds_id, _ in all_parquet_files:
                    row = self.get_row(ds_id)
                    if row and not row.error:
                        row.set_step(STEP_EXP, StepState.SKIPPED)

        elif not run_any_sql:
            # Hoppa over alla SQL-steg
            for ds in datasets:
                row = self.get_row(ds.id)
                if row:
                    row.set_step(STEP_STG, StepState.SKIPPED)
                    row.set_step(STEP_MRT, StepState.SKIPPED)
                    row.set_step(STEP_MRG, StepState.SKIPPED)
                    row.set_step(STEP_EXP, StepState.SKIPPED)

        # === KLAR ===
        total_elapsed = time.monotonic() - self._start_time

        # Uppdatera den fasta warehouse.duckdb
        from g_etl.services.db_session import update_current_db

        current_db = update_current_db()
        if current_db:
            self.log_to_file(f"Uppdaterade {current_db.name}")

        # Slutrapport per rad
        total_rows = 0
        for ds in datasets:
            row = self.get_row(ds.id)
            if row and not row.error:
                # Hamta radantal fran parquet om mojligt
                rows = self._get_row_count(ds.id)
                if rows:
                    row.set_done(rows_count=rows, elapsed=total_elapsed / len(datasets))
                    total_rows += rows
                    total_bar.completed += 1
                else:
                    row.set_done(elapsed=total_elapsed / len(datasets))
                    total_bar.completed += 1

        # Uppdatera total progress-bar till klart-lage
        total_bar.total_rows = total_rows
        total_bar.elapsed = total_elapsed
        total_bar.finished = True

        self.log_to_file("Klart!")
        self.set_running_state(False)

        if self.runner:
            self.runner.close()

        if self._file_logger:
            self._file_logger.close()
            self._file_logger = None

    def _get_row_count(self, dataset_id: str) -> int | None:
        """Forsok hamta radantal for ett dataset fran parquet-fil."""
        from pathlib import Path

        parquet_path = Path("data/raw") / f"{dataset_id}.parquet"
        if not parquet_path.exists():
            return None
        try:
            import duckdb

            count = duckdb.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
            ).fetchone()
            return count[0] if count else None
        except Exception:
            return None

    # --- Actions ---

    async def action_run_selected(self) -> None:
        """Kor valda datasets."""
        datasets = self.get_selected_datasets()
        if not datasets:
            return
        self.run_datasets(datasets)

    async def action_run_type(self) -> None:
        """Kor alla datasets av aktuell typ."""
        datasets = self.get_filtered_datasets()
        self.run_datasets(datasets)

    async def action_run_all(self) -> None:
        """Kor alla aktiverade datasets."""
        datasets = self.config.get_enabled()
        self.run_datasets(datasets)

    async def action_stop(self) -> None:
        """Stoppa pagaende jobb."""
        self._running = False
        if self.runner:
            await self.runner.stop()
        self.set_running_state(False)

        if self._file_logger:
            self._file_logger.close()
            self._file_logger = None

    def action_clear_selection(self) -> None:
        """Rensa alla val."""
        for row in self.query(PipelineRow):
            if row.selected:
                row.selected = False
                row.remove_class("selected")
                row.refresh()
        self.update_title_info()

    def action_toggle_mock(self) -> None:
        """Vaxla mock-lage."""
        self.mock_mode = not self.mock_mode
        self.update_title_info()

    def action_quit(self) -> None:
        """Avsluta."""
        self.app.exit()

    async def action_clear_data(self) -> None:
        """Rensa alla datafiler."""
        stats = get_data_stats()
        self.log_to_file(
            f"Data: {stats['db_count']} DB ({stats['db_size_mb']} MB), "
            f"{stats['parquet_count']} parquet ({stats['parquet_size_mb']} MB)"
        )

        db_count, db_size = cleanup_all_databases()
        pq_count, pq_size = cleanup_all_parquet()
        log_count, log_size = cleanup_all_logs()
        sub_count, sub_size = cleanup_data_subdirs()

        total_count = db_count + pq_count + log_count + sub_count
        if total_count > 0:
            total_size = db_size + pq_size + log_size + sub_size
            self.log_to_file(f"Rensat: {total_count} filer ({total_size} MB)")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Hantera knapptryckningar."""
        if event.button.id == "btn-run":
            await self.action_run_selected()
        elif event.button.id == "btn-all":
            await self.action_run_all()
        elif event.button.id == "btn-stop":
            await self.action_stop()
