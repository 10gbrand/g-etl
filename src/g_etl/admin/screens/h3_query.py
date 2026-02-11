"""H3 Polygon Query Screen - Rita/klistra in polygon för spatial analys."""

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, Select

from g_etl.h3_query import query_polygon


class H3QueryScreen(Screen):
    """Screen för att köra H3 polygon queries."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Tillbaka"),
        ("ctrl+t", "use_test_polygon", "Testpolygon"),
        ("ctrl+r", "run_query", "Kör analys"),
    ]

    CSS = """
    H3QueryScreen {
        align: center middle;
    }

    #query_container {
        width: 90%;
        height: 90%;
        border: solid $primary;
        background: $surface;
    }

    #input_section {
        height: auto;
        padding: 1;
        border-bottom: solid $primary;
    }

    #results_section {
        height: 1fr;
        padding: 1;
    }

    .input_row {
        height: auto;
        margin: 1 0;
    }

    Input {
        width: 1fr;
    }

    Select {
        width: 30;
    }

    Button {
        margin: 0 1;
    }

    #status {
        color: $text-muted;
        margin: 1 0;
    }

    DataTable {
        height: 1fr;
    }
    """

    # Testpolygon: Område runt Tyresta naturreservat (söder om Stockholm)
    # Garanterat att ha data från naturreservat, biotopskydd, etc.
    TEST_POLYGON_WKT = (
        "POLYGON(("
        "674000 6580000, "  # Sydväst
        "676000 6580000, "  # Sydost
        "676000 6582000, "  # Nordost
        "674000 6582000, "  # Nordväst
        "674000 6580000"  # Stäng polygon
        "))"
    )

    def __init__(self) -> None:
        super().__init__()
        self.result_data: list[dict] = []

    def compose(self) -> ComposeResult:
        """Skapa UI-komponenter."""
        yield Header()

        with Container(id="query_container"):
            with Vertical(id="input_section"):
                yield Label("🗺️  H3 Polygon Query - Spatial Analys", classes="title")
                yield Label(
                    "Klistra in WKT-polygon i SWEREF99 TM (EPSG:3006) "
                    "eller tryck Ctrl+T för testpolygon",
                    id="status",
                )

                with Horizontal(classes="input_row"):
                    yield Label("Polygon (WKT):", classes="label")
                    yield Input(
                        placeholder="POLYGON((x1 y1, x2 y2, ...))",
                        id="polygon_input",
                    )

                with Horizontal(classes="input_row"):
                    yield Label("H3 Resolution:", classes="label")
                    yield Select(
                        [
                            ("6 - ~36 km", "6"),
                            ("7 - ~15 km", "7"),
                            ("8 - ~5 km (default)", "8"),
                            ("9 - ~2 km", "9"),
                            ("10 - ~700 m", "10"),
                        ],
                        value="8",
                        id="resolution_select",
                    )

                    yield Label("Aggregering:", classes="label")
                    yield Select(
                        [
                            ("Objekt - alla objekt i området", "objects"),
                            ("Statistik - aggregerat per dataset", "stats"),
                            ("Heatmap - räkna objekt per H3-cell", "heatmap"),
                        ],
                        value="objects",
                        id="aggregation_select",
                    )

                with Horizontal(classes="input_row"):
                    yield Button("🧪 Testpolygon (Ctrl+T)", id="test_polygon_btn")
                    yield Button("▶️  Kör Analys (Ctrl+R)", id="run_btn", variant="primary")
                    yield Button("🗑️  Rensa", id="clear_btn")

            with Vertical(id="results_section"):
                yield Label("Resultat:", classes="label")
                yield DataTable(id="results_table")

        yield Footer()

    def on_mount(self) -> None:
        """Initiera tabellen när screen mountas."""
        table = self.query_one("#results_table", DataTable)
        table.cursor_type = "row"

    def action_use_test_polygon(self) -> None:
        """Fyll i testpolygon."""
        polygon_input = self.query_one("#polygon_input", Input)
        polygon_input.value = self.TEST_POLYGON_WKT
        self.query_one("#status", Label).update(
            "✓ Testpolygon inlagd (Tyresta naturreservat, ~2x2 km)"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Hantera knapptryck."""
        if event.button.id == "test_polygon_btn":
            self.action_use_test_polygon()
        elif event.button.id == "run_btn":
            self.action_run_query()
        elif event.button.id == "clear_btn":
            self.action_clear()

    def action_clear(self) -> None:
        """Rensa input och resultat."""
        self.query_one("#polygon_input", Input).value = ""
        self.query_one("#results_table", DataTable).clear()
        self.query_one("#status", Label).update("Redo för ny query")

    def action_run_query(self) -> None:
        """Kör H3 polygon query."""
        polygon_input = self.query_one("#polygon_input", Input)
        resolution_select = self.query_one("#resolution_select", Select)
        aggregation_select = self.query_one("#aggregation_select", Select)
        status_label = self.query_one("#status", Label)
        table = self.query_one("#results_table", DataTable)

        # Validera input
        polygon_wkt = polygon_input.value.strip()
        if not polygon_wkt:
            status_label.update("❌ Fel: Ingen polygon angiven")
            return

        if not polygon_wkt.upper().startswith("POLYGON"):
            status_label.update("❌ Fel: Måste vara WKT POLYGON-format")
            return

        # Hämta settings
        resolution = int(resolution_select.value)
        aggregation = aggregation_select.value

        # Visa progress
        status_label.update(f"⏳ Kör query (resolution={resolution}, mode={aggregation})...")
        table.clear()

        try:
            # Kör query med h3_query module

            df = query_polygon(
                polygon_wkt=polygon_wkt,
                resolution=resolution,
                aggregation=aggregation,
            )

            # Konvertera till dict för display
            self.result_data = df.to_dict("records")

            # Uppdatera status
            status_label.update(f"✓ Query klar! Hittade {len(self.result_data):,} resultat")

            # Visa resultat i tabell
            self._display_results(df)

        except Exception as e:
            status_label.update(f"❌ Fel: {e}")
            self.log.error(f"H3 query error: {e}", exc_info=True)

    def _display_results(self, df) -> None:
        """Visa resultat i DataTable."""
        table = self.query_one("#results_table", DataTable)
        table.clear(columns=True)

        if df.empty:
            table.add_column("Info")
            table.add_row("Inga resultat hittades i området")
            return

        # Lägg till kolumner
        for col in df.columns:
            table.add_column(str(col))

        # Lägg till rader (max 1000 för prestanda)
        max_rows = 1000
        for idx, row in df.head(max_rows).iterrows():
            table.add_row(*[str(val) for val in row])

        if len(df) > max_rows:
            self.query_one("#status", Label).update(
                f"✓ Visar {max_rows:,} av {len(df):,} resultat (använd export för fullständig data)"
            )
