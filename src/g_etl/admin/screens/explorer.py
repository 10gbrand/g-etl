"""Data Explorer screen with ASCII map for geometry validation and export."""

from pathlib import Path

import duckdb
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Label,
    OptionList,
    Select,
    SelectionList,
    Static,
)
from textual.widgets.option_list import Option
from textual.widgets.selection_list import Selection

from g_etl.admin.widgets.ascii_map import BrailleMapWidget
from g_etl.export import export_mart_tables
from g_etl.pipeline import FileLogger
from g_etl.services.db_session import get_current_db_path
from g_etl.settings import settings


class TableInfo(Static):
    """Visar information om vald tabell."""

    DEFAULT_CSS = """
    TableInfo {
        height: auto;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = ""
        self.row_count = 0
        self.columns: list[tuple[str, str]] = []
        self.has_geometry = False

    def set_table(
        self,
        schema: str,
        table: str,
        row_count: int,
        columns: list[tuple[str, str]],
        has_geometry: bool,
    ) -> None:
        """Sätt tabell-information."""
        self.table_name = f"{schema}.{table}"
        self.row_count = row_count
        self.columns = columns
        self.has_geometry = has_geometry
        self.refresh()

    def render(self) -> str:
        """Rendera tabell-information."""
        if not self.table_name:
            return "Välj en tabell..."

        lines = [
            f"[bold]{self.table_name}[/bold]",
            f"Rader: {self.row_count:,}",
            f"Geometri: {'✓ Ja' if self.has_geometry else '✗ Nej'}",
            "",
            "Kolumner:",
        ]
        for col_name, col_type in self.columns:
            lines.append(f"  • {col_name} ({col_type})")

        return "\n".join(lines)


class ExplorerScreen(Screen):
    """Screen för att utforska data, verifiera geometrier och exportera."""

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Tillbaka"),
        Binding("r", "refresh_tables", "Uppdatera"),
        Binding("m", "show_map", "Visa karta"),
        Binding("e", "export_selected", "Exportera"),
        Binding("a", "select_all", "Markera alla"),
        Binding("q", "app.pop_screen", "Tillbaka"),
    ]

    CSS = """
    ExplorerScreen {
        layout: grid;
        grid-size: 2 3;
        grid-columns: 30 1fr;
        grid-rows: auto 1fr auto;
    }

    #header-row {
        column-span: 2;
        height: auto;
        padding: 1;
        background: $primary-darken-2;
    }

    #left-panel {
        height: 100%;
        border: solid $primary;
        padding: 0;
    }

    #mart-section {
        height: 1fr;
        border-bottom: solid $primary-darken-1;
    }

    #mart-header {
        height: auto;
        padding: 0 1;
        background: $primary-darken-1;
    }

    #mart-tables {
        height: 1fr;
    }

    #other-section {
        height: 1fr;
    }

    #other-header {
        height: auto;
        padding: 0 1;
        background: $secondary-darken-1;
    }

    #other-tables {
        height: 1fr;
    }

    #export-controls {
        height: auto;
        padding: 1;
        border-top: solid $accent;
        background: $surface-darken-1;
    }

    #export-row {
        height: auto;
        align: center middle;
    }

    #format-select {
        width: 12;
    }

    #export-btn {
        margin-left: 1;
    }

    #right-panel {
        height: 100%;
        overflow: hidden;
    }

    #top-row {
        height: 1fr;
    }

    #table-info {
        width: 1fr;
        height: 100%;
        padding: 1;
        border: solid $secondary;
        overflow-y: auto;
    }

    #map-container {
        width: 2fr;
        height: 100%;
        padding: 0;
    }

    #data-preview-container {
        height: 14;
        border: solid $accent;
        padding: 0;
        overflow: auto;
    }

    #data-table {
        height: 100%;
        width: auto;
    }

    #footer-row {
        column-span: 2;
        height: auto;
    }

    SelectionList {
        height: 100%;
    }

    OptionList {
        height: 100%;
    }

    OptionList > .option-list--option {
        padding: 0 1;
    }
    """

    def __init__(self, db_path: str | None = None) -> None:
        super().__init__()
        self.db_path = db_path or str(get_current_db_path())
        self._conn: duckdb.DuckDBPyConnection | None = None
        self.tables: list[tuple[str, str]] = []  # (schema, table)
        self.mart_tables: list[str] = []  # Bara tabellnamn i mart
        self.current_table: tuple[str, str] | None = None
        self.export_formats = [
            ("FlatGeobuf (.fgb)", "fgb"),
            ("GeoPackage (.gpkg)", "gpkg"),
            ("GeoParquet (.parquet)", "geoparquet"),
        ]

    def compose(self) -> ComposeResult:
        """Bygg screen-layouten."""
        yield Header(show_clock=True)

        with Container(id="header-row"):
            yield Label("Data Explorer - Utforska & Exportera", id="screen-title")

        with Vertical(id="left-panel"):
            with Vertical(id="mart-section"):
                yield Label("MART (välj för export)", id="mart-header")
                yield SelectionList[str](id="mart-tables")
            with Vertical(id="other-section"):
                yield Label("ÖVRIGA SCHEMAN", id="other-header")
                yield OptionList(id="other-tables")
            with Container(id="export-controls"):
                with Horizontal(id="export-row"):
                    yield Select(
                        [(label, value) for label, value in self.export_formats],
                        value="fgb",
                        id="format-select",
                    )
                    yield Button("Exportera [e]", id="export-btn", variant="primary")

        with Vertical(id="right-panel"):
            with Horizontal(id="top-row"):
                yield TableInfo(id="table-info")
                with Container(id="map-container"):
                    yield BrailleMapWidget(width=80, height=25, title="Geometri-förhandsvisning")
            with Container(id="data-preview-container"):
                yield DataTable(id="data-table")

        yield Footer()

    def on_mount(self) -> None:
        """Ladda tabeller vid mount."""
        self._load_tables()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Hämta databasanslutning."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            self._init_extensions()
        return self._conn

    def _init_extensions(self) -> None:
        """Ladda nödvändiga extensions."""
        if self._conn is None:
            return

        for ext in ["spatial", "parquet"]:
            try:
                self._conn.execute(f"LOAD {ext}")
            except Exception:
                pass

    @work(thread=True)
    def _load_tables(self) -> None:
        """Ladda lista över tabeller från databasen."""
        try:
            conn = self._get_connection()

            # Hämta alla tabeller med geometri-kolumner först, sedan övriga
            query = """
                WITH table_info AS (
                    SELECT
                        t.table_schema,
                        t.table_name,
                        MAX(CASE WHEN c.data_type LIKE '%GEOMETRY%' THEN 1 ELSE 0 END)
                            as has_geometry
                    FROM information_schema.tables t
                    LEFT JOIN information_schema.columns c
                        ON t.table_schema = c.table_schema
                        AND t.table_name = c.table_name
                    WHERE t.table_schema IN ('raw', 'staging', 'staging_2', 'mart')
                    GROUP BY t.table_schema, t.table_name
                )
                SELECT table_schema, table_name, has_geometry
                FROM table_info
                ORDER BY
                    CASE table_schema
                        WHEN 'mart' THEN 1
                        WHEN 'staging_2' THEN 2
                        WHEN 'staging' THEN 3
                        WHEN 'raw' THEN 4
                    END,
                    has_geometry DESC,
                    table_name
            """
            rows = conn.execute(query).fetchall()
            self.tables = [(row[0], row[1]) for row in rows]
            self.mart_tables = [row[1] for row in rows if row[0] == "mart"]

            # Uppdatera UI på main thread
            self.app.call_from_thread(self._populate_table_list)

        except Exception as e:
            self.app.call_from_thread(
                self.notify,
                f"Kunde inte ladda tabeller: {e}",
                severity="error",
            )

    def _populate_table_list(self) -> None:
        """Fyll i tabell-listorna."""
        # Fyll mart-tabeller (med checkboxar)
        mart_list = self.query_one("#mart-tables", SelectionList)
        mart_list.clear_options()

        for table in self.mart_tables:
            # Förvälj alla mart-tabeller
            mart_list.add_option(Selection(table, f"mart.{table}", initial_state=True))

        # Fyll övriga tabeller (read-only)
        other_list = self.query_one("#other-tables", OptionList)
        other_list.clear_options()

        current_schema = None
        for schema, table in self.tables:
            if schema == "mart":
                continue  # Dessa visas i mart-listan

            if schema != current_schema:
                other_list.add_option(Option(f"── {schema.upper()} ──", disabled=True))
                current_schema = schema

            other_list.add_option(Option(f"  {table}", id=f"{schema}.{table}"))

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Hantera val av tabell i övriga-listan."""
        if event.option.id and "." in event.option.id:
            schema, table = event.option.id.split(".", 1)
            self.current_table = (schema, table)
            self._load_table_info(schema, table)

    def on_selection_list_selection_highlighted(
        self, event: SelectionList.SelectionHighlighted
    ) -> None:
        """Hantera markering av tabell i mart-listan (visa info)."""
        if event.selection and event.selection.value:
            table_id = event.selection.value
            if "." in table_id:
                schema, table = table_id.split(".", 1)
                self.current_table = (schema, table)
                self._load_table_info(schema, table)

    @work(thread=True)
    def _load_table_info(self, schema: str, table: str) -> None:
        """Ladda information om vald tabell."""
        try:
            conn = self._get_connection()

            # Hämta radantal
            count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
            row_count = conn.execute(count_query).fetchone()[0]

            # Hämta kolumner
            columns_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
            """
            columns = conn.execute(columns_query).fetchall()

            # Kolla om det finns geometri
            geometry_col = None
            for col_name, col_type in columns:
                if "GEOMETRY" in col_type.upper():
                    geometry_col = col_name
                    break

            # Hämta sample-data (10 rader)
            # Exkludera geometri-kolumner för visning
            display_cols = [col[0] for col in columns if "GEOMETRY" not in col[1].upper()]
            if not display_cols:
                display_cols = [columns[0][0]] if columns else []

            # Visa alla kolumner (horisontell scrollning i UI)
            cols_str = ", ".join([f'"{c}"' for c in display_cols])

            sample_query = f"SELECT {cols_str} FROM {schema}.{table} LIMIT 10"
            sample_rows = conn.execute(sample_query).fetchall()

            # Uppdatera UI
            def update_ui():
                info = self.query_one("#table-info", TableInfo)
                info.set_table(schema, table, row_count, columns, geometry_col is not None)

                # Uppdatera datatabell
                self._populate_data_table(display_cols, sample_rows)

                # Ladda karta om det finns geometri
                if geometry_col:
                    self._load_map_data(schema, table, geometry_col)

            self.app.call_from_thread(update_ui)

        except Exception as e:
            self.app.call_from_thread(
                self.notify,
                f"Kunde inte ladda tabell-info: {e}",
                severity="error",
            )

    def _populate_data_table(self, columns: list[str], rows: list[tuple]) -> None:
        """Fyll datatabellen med sample-data."""
        data_table = self.query_one("#data-table", DataTable)
        data_table.clear(columns=True)

        # Lägg till kolumner
        for col in columns:
            # Trunkera långa kolumnnamn
            display_name = col[:15] + "…" if len(col) > 15 else col
            data_table.add_column(display_name, key=col)

        # Lägg till rader
        for row in rows:
            # Trunkera långa värden
            display_row = []
            for val in row:
                if val is None:
                    display_row.append("NULL")
                else:
                    str_val = str(val)
                    if len(str_val) > 30:
                        str_val = str_val[:27] + "..."
                    display_row.append(str_val)
            data_table.add_row(*display_row)

    def _find_map_widget(self) -> BrailleMapWidget:
        """Hitta kart-widgeten."""
        return self.query_one(BrailleMapWidget)

    @work(thread=True)
    def _load_map_data(self, schema: str, table: str, geometry_col: str) -> None:
        """Ladda geometri-data för kartan."""
        try:
            conn = self._get_connection()
            map_widget = self._find_map_widget()

            # Uppdatera titeln
            def set_title():
                map_widget.title = f"{schema}.{table}"
                map_widget._loaded = False
                map_widget.refresh()

            self.app.call_from_thread(set_title)

            # Ladda data
            map_widget.load_from_query(conn, schema, table, geometry_col)

            # Tvinga layout-uppdatering på main thread efter data laddats
            def refresh_layout():
                map_widget.refresh(layout=True)

            self.app.call_from_thread(refresh_layout)

        except Exception as e:
            self.app.call_from_thread(
                self.notify,
                f"Kunde inte ladda kart-data: {e}",
                severity="error",
            )

    def action_refresh_tables(self) -> None:
        """Uppdatera tabell-listan."""
        self._load_tables()
        self.notify("Uppdaterar tabeller...")

    def action_show_map(self) -> None:
        """Visa/dölj kartan."""
        # Kartan visas alltid för geometri-tabeller
        pass

    def action_select_all(self) -> None:
        """Markera/avmarkera alla mart-tabeller."""
        mart_list = self.query_one("#mart-tables", SelectionList)
        selected = mart_list.selected

        if len(selected) == len(self.mart_tables):
            # Alla är valda - avmarkera alla
            mart_list.deselect_all()
            self.notify("Avmarkerade alla tabeller")
        else:
            # Markera alla
            mart_list.select_all()
            self.notify("Markerade alla tabeller")

    def action_export_selected(self) -> None:
        """Exportera valda tabeller."""
        self._do_export()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Hantera knapp-tryck."""
        if event.button.id == "export-btn":
            self._do_export()

    def _do_export(self) -> None:
        """Starta export av valda tabeller."""
        mart_list = self.query_one("#mart-tables", SelectionList)
        selected = mart_list.selected

        if not selected:
            self.notify("Välj minst en tabell att exportera", severity="warning")
            return

        format_select = self.query_one("#format-select", Select)
        export_format = str(format_select.value)

        # Hämta output-katalog (samma som databasens katalog)
        db_dir = Path(self.db_path).parent
        output_dir = db_dir / "export"

        # selected innehåller redan värdena (t.ex. 'mart.sksbiotopskydd')
        selected_tables = list(selected)
        table_names = [t.split(".")[-1] for t in selected_tables]

        self.notify(f"Exporterar {len(table_names)} tabeller till {export_format.upper()}...")
        self._run_export(output_dir, export_format, table_names)

    @work(thread=True)
    def _run_export(self, output_dir: Path, export_format: str, table_names: list[str]) -> None:
        """Kör export i bakgrunden."""
        # Starta filloggning
        file_logger = FileLogger(logs_dir=settings.LOGS_DIR, prefix="tui_export")
        log_file = file_logger.start()

        def log_callback(msg: str) -> None:
            file_logger.log(msg)
            self.app.call_from_thread(self.notify, msg)

        try:
            log_callback(f"Loggar till: {log_file}")
            log_callback(f"Exporterar {len(table_names)} tabeller till {export_format.upper()}")
            log_callback(f"Output-katalog: {output_dir}")

            conn = self._get_connection()

            # Ladda extensions
            conn.execute("LOAD spatial")
            conn.execute("LOAD h3")

            # Använd gemensam export-funktion med tabellfilter
            exported = export_mart_tables(
                conn=conn,
                output_dir=output_dir,
                export_format=export_format,
                on_log=log_callback,
                table_names=table_names,
            )

            if exported:
                log_callback(f"✓ Exporterade {len(exported)} filer till {output_dir}")
                self.app.call_from_thread(
                    self.notify,
                    f"✓ Exporterade {len(exported)} filer till {output_dir}",
                    severity="information",
                )
            else:
                log_callback("Inga filer exporterades")
                self.app.call_from_thread(
                    self.notify,
                    "Inga filer exporterades",
                    severity="warning",
                )

        except Exception as e:
            log_callback(f"Export misslyckades: {e}")
            self.app.call_from_thread(
                self.notify,
                f"Export misslyckades: {e}",
                severity="error",
            )
        finally:
            file_logger.close()

    def on_unmount(self) -> None:
        """Stäng databas-anslutning."""
        if self._conn:
            self._conn.close()
            self._conn = None
