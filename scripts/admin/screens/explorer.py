"""Data Explorer screen med ASCII-karta för geometrivalidering."""

import duckdb
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Label,
    OptionList,
    Static,
)
from textual.widgets.option_list import Option

from config.settings import settings
from scripts.admin.services.db_session import get_current_db_path
from scripts.admin.widgets.ascii_map import BrailleMapWidget


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
    """Screen för att utforska data och verifiera geometrier."""

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Tillbaka"),
        Binding("r", "refresh_tables", "Uppdatera"),
        Binding("m", "show_map", "Visa karta"),
        Binding("q", "app.pop_screen", "Tillbaka"),
    ]

    CSS = """
    ExplorerScreen {
        layout: grid;
        grid-size: 2 3;
        grid-columns: 25 1fr;
        grid-rows: auto 1fr auto;
    }

    #header-row {
        column-span: 2;
        height: auto;
        padding: 1;
        background: $primary-darken-2;
    }

    #table-list-container {
        height: 100%;
        border: solid $primary;
        padding: 0;
    }

    #table-list {
        height: 100%;
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
        self.current_table: tuple[str, str] | None = None

    def compose(self) -> ComposeResult:
        """Bygg screen-layouten."""
        yield Header(show_clock=True)

        with Container(id="header-row"):
            yield Label("Data Explorer - Verifiera geometrier", id="screen-title")

        with Container(id="table-list-container"):
            yield OptionList(id="table-list")

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

            # Uppdatera UI på main thread
            self.app.call_from_thread(self._populate_table_list)

        except Exception as e:
            self.app.call_from_thread(
                self.notify,
                f"Kunde inte ladda tabeller: {e}",
                severity="error",
            )

    def _populate_table_list(self) -> None:
        """Fyll i tabell-listan."""
        option_list = self.query_one("#table-list", OptionList)
        option_list.clear_options()

        current_schema = None
        for schema, table in self.tables:
            if schema != current_schema:
                # Lägg till schema-header
                option_list.add_option(Option(f"── {schema.upper()} ──", disabled=True))
                current_schema = schema

            option_list.add_option(Option(f"  {table}", id=f"{schema}.{table}"))

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Hantera val av tabell."""
        if event.option.id and "." in event.option.id:
            schema, table = event.option.id.split(".", 1)
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

    @work(thread=True)
    def _load_map_data(self, schema: str, table: str, geometry_col: str) -> None:
        """Ladda geometri-data för kartan."""
        try:
            conn = self._get_connection()
            map_widget = self.query_one(BrailleMapWidget)

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

    def on_unmount(self) -> None:
        """Stäng databas-anslutning."""
        if self._conn:
            self._conn.close()
            self._conn = None
