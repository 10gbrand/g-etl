"""Detaljpanel som visar karta och data for ett valt pipeline-steg.

Visas till hoger i PipelineScreen nar man klickar pa en klarmarkerad
steg-prick (●) i en dataset-rad.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import DataTable, Static

from g_etl.admin.models.dataset import Dataset
from g_etl.admin.widgets.ascii_map import BrailleMapWidget
from g_etl.admin.widgets.pipeline_row import STEP_NAMES, STEP_SCHEMAS


class DetailHeader(Static):
    """Rubrik for detaljpanelen: 'Dataset > Steg'."""

    DEFAULT_CSS = """
    DetailHeader {
        height: auto;
        padding: 0 1;
        background: $primary-darken-1;
        text-style: bold;
    }
    """

    title: reactive[str] = reactive("")
    subtitle: reactive[str] = reactive("")

    def render(self) -> Text:
        text = Text()
        text.append(self.title, style="bold")
        if self.subtitle:
            text.append(" \u203a ", style="dim")
            text.append(self.subtitle, style="italic")
        return text


class DetailInfo(Static):
    """Visar tabell-info: schema.tabell, radantal, kolumner."""

    DEFAULT_CSS = """
    DetailInfo {
        height: auto;
        padding: 0 1;
    }
    """

    info_text: reactive[str] = reactive("")

    def render(self) -> str:
        return self.info_text or "Laddar..."


class DetailPanel(Widget):
    """Detaljpanel med karta och data-preview for ett pipeline-steg."""

    DEFAULT_CSS = """
    DetailPanel {
        width: 1fr;
        height: 100%;
        border-left: solid $primary;
        display: none;
    }

    DetailPanel.visible {
        display: block;
    }

    #detail-header {
        height: auto;
    }

    #detail-info {
        height: auto;
    }

    #detail-map-container {
        height: 1fr;
        min-height: 10;
    }

    #detail-data-container {
        height: 12;
        border-top: solid $primary-darken-2;
        overflow: auto;
    }

    #detail-data-table {
        height: 100%;
        width: auto;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._current_dataset: Dataset | None = None
        self._current_step: int | None = None

    def compose(self) -> ComposeResult:
        """Bygg panelen."""
        with Vertical():
            yield DetailHeader(id="detail-header")
            yield DetailInfo(id="detail-info")
            with Container(id="detail-map-container"):
                yield BrailleMapWidget(width=60, height=18, title="")
            with Container(id="detail-data-container"):
                yield DataTable(id="detail-data-table")

    def show_step(self, dataset: Dataset, step_index: int, db_path: str) -> None:
        """Visa data for ett specifikt steg."""
        self._current_dataset = dataset
        self._current_step = step_index

        # Uppdatera rubrik
        header = self.query_one("#detail-header", DetailHeader)
        header.title = dataset.name
        header.subtitle = STEP_NAMES[step_index]

        # Visa panelen
        self.add_class("visible")

        # Ladda data
        self._load_step_data(dataset, step_index, db_path)

    def hide(self) -> None:
        """Dölj panelen."""
        self.remove_class("visible")
        self._current_dataset = None
        self._current_step = None

    @property
    def is_visible(self) -> bool:
        """Ar panelen synlig?"""
        return self.has_class("visible")

    @work(thread=True)
    def _load_step_data(self, dataset: Dataset, step_index: int, db_path: str) -> None:
        """Ladda data for ett steg i bakgrunden."""
        schema = STEP_SCHEMAS.get(step_index)
        table_name = dataset.id

        # Export-steget: visa fil-info istallet
        if step_index == 4:
            self._load_export_info(dataset)
            return

        # Hitta rattdatabas
        actual_db_path = db_path

        # For raw-steget: lad fran parquet direkt
        if step_index == 0:
            self._load_parquet_data(dataset)
            return

        try:
            conn = duckdb.connect(actual_db_path, read_only=True)
            conn.execute("LOAD spatial")

            # Kolla om tabellen finns i schemat
            exists = conn.execute(
                f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
            """
            ).fetchone()[0]

            if not exists:
                # Forsok hitta tabellen i andra staging-scheman
                alt_schemas = conn.execute(
                    f"""
                    SELECT table_schema FROM information_schema.tables
                    WHERE table_name = '{table_name}'
                    ORDER BY table_schema
                """
                ).fetchall()
                if alt_schemas:
                    schema = alt_schemas[0][0]
                else:
                    self.app.call_from_thread(self._show_not_found, dataset, step_index)
                    conn.close()
                    return

            self._query_and_display(conn, schema, table_name)
            conn.close()

        except Exception as e:
            self.app.call_from_thread(self._show_error, str(e))

    def _load_parquet_data(self, dataset: Dataset) -> None:
        """Ladda data fran parquet-fil (Extract-steget)."""
        parquet_path = Path("data/raw") / f"{dataset.id}.parquet"
        if not parquet_path.exists():
            self.app.call_from_thread(self._show_not_found, dataset, 0)
            return

        try:
            conn = duckdb.connect()
            conn.execute("LOAD spatial")

            # Skapa en vy fran parquet-filen
            conn.execute(f"CREATE VIEW _tmp AS SELECT * FROM read_parquet('{parquet_path}')")

            self._query_and_display(conn, "main", "_tmp", source=str(parquet_path))
            conn.close()

        except Exception as e:
            self.app.call_from_thread(self._show_error, str(e))

    def _load_export_info(self, dataset: Dataset) -> None:
        """Visa info om exporterad fil."""
        output_dir = Path("data/output")
        # Sok efter exporterade filer for detta dataset
        files = []
        if output_dir.exists():
            for ext in ["*.parquet", "*.gpkg", "*.fgb"]:
                for f in output_dir.glob(ext):
                    if dataset.id in f.stem:
                        size_mb = f.stat().st_size / (1024 * 1024)
                        files.append(f"{f.name} ({size_mb:.1f} MB)")

        if files:
            info_text = f"[bold]{dataset.name} \u203a Export[/bold]\n\n"
            info_text += "Exporterade filer:\n"
            for f in files:
                info_text += f"  \u2022 {f}\n"
        else:
            info_text = (
                f"[bold]{dataset.name} \u203a Export[/bold]\n\nInga exporterade filer hittades."
            )

        def update():
            info = self.query_one("#detail-info", DetailInfo)
            info.info_text = info_text
            # Dölj karta och tabell for export
            self.query_one("#detail-map-container").display = False
            self.query_one("#detail-data-container").display = False

        self.app.call_from_thread(update)

    def _query_and_display(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
        source: str | None = None,
    ) -> None:
        """Hamta data och uppdatera UI."""
        full_name = f"{schema}.{table}" if schema != "main" else table

        # Radantal
        row_count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]

        # Kolumner
        if schema == "main":
            cols_query = f"""
                SELECT column_name, data_type
                FROM (DESCRIBE SELECT * FROM {full_name})
            """
        else:
            cols_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
            """
        columns = conn.execute(cols_query).fetchall()

        # Geometri-kolumn?
        geometry_col = None
        for col_name, col_type in columns:
            if "GEOMETRY" in col_type.upper():
                geometry_col = col_name
                break

        # Sample-data (exkludera geometri)
        display_cols = [c[0] for c in columns if "GEOMETRY" not in c[1].upper()]
        if not display_cols and columns:
            display_cols = [columns[0][0]]

        cols_str = ", ".join([f'"{c}"' for c in display_cols])
        sample_rows = conn.execute(f"SELECT {cols_str} FROM {full_name} LIMIT 10").fetchall()

        # Ladda kart-data om geometri finns
        map_data = None
        if geometry_col:
            try:
                map_widget = self.query_one(BrailleMapWidget)
                map_widget.load_from_query(conn, schema, table, geometry_col)
                map_data = True
            except Exception:
                pass

        # Uppdatera UI pa main thread
        def update():
            display_name = source or f"{schema}.{table}"
            info_text = f"[bold]{display_name}[/bold]\n"
            info_text += f"{row_count:,} rader \u00b7 {len(columns)} kolumner"
            if geometry_col:
                info_text += f" \u00b7 Geometri: {geometry_col}"

            info = self.query_one("#detail-info", DetailInfo)
            info.info_text = info_text

            # Visa/dolj karta
            map_container = self.query_one("#detail-map-container")
            map_container.display = map_data is not None

            # Visa data-container
            self.query_one("#detail-data-container").display = True

            # Fyll datatabell
            data_table = self.query_one("#detail-data-table", DataTable)
            data_table.clear(columns=True)
            for col in display_cols:
                display_name = col[:15] + "\u2026" if len(col) > 15 else col
                data_table.add_column(display_name, key=col)
            for row in sample_rows:
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

            # Uppdatera karta-layout
            if map_data:
                map_widget = self.query_one(BrailleMapWidget)
                map_widget.refresh(layout=True)

        self.app.call_from_thread(update)

    def _show_not_found(self, dataset: Dataset, step_index: int) -> None:
        """Visa meddelande att data inte hittades."""
        info = self.query_one("#detail-info", DetailInfo)
        step_name = STEP_NAMES[step_index]
        info.info_text = f"Ingen data for {dataset.name} i {step_name}-steget."
        self.query_one("#detail-map-container").display = False
        self.query_one("#detail-data-container").display = False

    def _show_error(self, error: str) -> None:
        """Visa felmeddelande."""
        info = self.query_one("#detail-info", DetailInfo)
        info.info_text = f"[red]Fel: {error}[/red]"
        self.query_one("#detail-map-container").display = False
        self.query_one("#detail-data-container").display = False
