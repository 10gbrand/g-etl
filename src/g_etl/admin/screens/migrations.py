"""Migrations screen för TUI.

Visar status för databasmigrationer och låter användaren
köra migrationer och rollbacks.
"""

from datetime import datetime

import duckdb
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
    Static,
)

from g_etl.migrations.migrator import Migration, MigrationStatus, Migrator


class MigrationRow(Static):
    """En rad för en migrering i listan."""

    DEFAULT_CSS = """
    MigrationRow {
        height: 2;
        padding: 0 1;
    }

    MigrationRow:hover {
        background: $primary 20%;
    }

    MigrationRow.applied {
        background: $success 20%;
    }

    MigrationRow.pending {
        background: $warning 20%;
    }

    MigrationRow.selected {
        background: $primary 40%;
    }
    """

    def __init__(self, migration: Migration) -> None:
        super().__init__()
        self.migration = migration
        self.selected = False

    def compose(self) -> ComposeResult:
        """Bygg raden."""
        status_icon = "✓" if self.migration.status == MigrationStatus.APPLIED else "○"
        down_icon = "↓" if self.migration.down_sql else " "
        yield Label(
            f"{status_icon} {self.migration.version} │ {self.migration.name} {down_icon}",
            id=f"lbl-{self.migration.version}",
        )

    def on_mount(self) -> None:
        """Sätt initial stil."""
        self._update_display()

    def on_click(self) -> None:
        """Toggle selection vid klick."""
        self.selected = not self.selected
        self._update_display()

    def _update_display(self) -> None:
        """Uppdatera visning baserat på state."""
        self.remove_class("applied", "pending", "selected")

        if self.selected:
            self.add_class("selected")
        elif self.migration.status == MigrationStatus.APPLIED:
            self.add_class("applied")
        else:
            self.add_class("pending")

    def update_status(self, status: MigrationStatus) -> None:
        """Uppdatera migreringens status."""
        self.migration.status = status
        label = self.query_one(f"#lbl-{self.migration.version}", Label)
        status_icon = "✓" if status == MigrationStatus.APPLIED else "○"
        down_icon = "↓" if self.migration.down_sql else " "
        label.update(f"{status_icon} {self.migration.version} │ {self.migration.name} {down_icon}")
        self._update_display()


class MigrationsScreen(Screen):
    """Screen för att hantera databasmigrationer."""

    BINDINGS = [
        Binding("m", "migrate", "Migrera"),
        Binding("r", "rollback", "Rollback"),
        Binding("s", "refresh", "Uppdatera"),
        Binding("p", "app.push_screen('pipeline')", "Pipeline"),
        Binding("e", "app.push_screen('explorer')", "Explorer"),
        Binding("q", "quit", "Avsluta"),
    ]

    CSS = """
    MigrationsScreen {
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

    #migrations-panel {
        height: 100%;
        border: solid $primary;
    }

    #migrations-scroll {
        height: 100%;
    }

    #info-panel {
        height: 100%;
    }

    #status-box {
        height: auto;
        padding: 1;
        border: solid $accent;
        margin-bottom: 1;
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

    .status-label {
        margin: 0 1;
    }
    """

    def __init__(self, db_path: str = "data/warehouse.duckdb") -> None:
        super().__init__()
        self.db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._migrator: Migrator | None = None

    def _get_migrator(self) -> Migrator:
        """Hämta eller skapa migrator."""
        if self._migrator is None:
            self._conn = duckdb.connect(self.db_path)
            self._migrator = Migrator(self._conn, "sql/migrations")
        return self._migrator

    def compose(self) -> ComposeResult:
        """Bygg screen-layouten."""
        yield Header(show_clock=True)

        with Container(id="header-container"):
            with Horizontal():
                yield Label("Databasmigrationer", id="app-title")

        with Horizontal(id="main-content"):
            with Container(id="migrations-panel"):
                with ScrollableContainer(id="migrations-scroll"):
                    yield Label("Laddar migrationer...", id="loading-label")

            with Vertical(id="info-panel"):
                with Container(id="status-box"):
                    yield Label("Status", classes="status-label")
                    yield Label("", id="status-applied")
                    yield Label("", id="status-pending")
                    yield Label("", id="status-db")

                with Container(id="log-panel"):
                    yield Log(id="log", highlight=True)

        with Horizontal(id="button-bar"):
            yield Button("Migrera [M]", id="btn-migrate", variant="success")
            yield Button("Rollback [R]", id="btn-rollback", variant="warning")
            yield Button("Uppdatera [S]", id="btn-refresh", variant="default")
            yield Button("Pipeline [P]", id="btn-pipeline", variant="primary")

        yield Footer()

    def on_mount(self) -> None:
        """Ladda migrationer vid mount."""
        self.refresh_migrations()

    def refresh_migrations(self) -> None:
        """Uppdatera listan med migrationer."""
        migrator = self._get_migrator()
        migrations = migrator.discover_migrations()

        # Rensa befintligt innehåll
        scroll = self.query_one("#migrations-scroll", ScrollableContainer)
        loading = self.query("#loading-label")
        if loading:
            loading.first().remove()

        # Ta bort gamla rader
        for row in self.query(MigrationRow):
            row.remove()

        # Lägg till nya rader
        if not migrations:
            scroll.mount(Label("Inga migrationer hittades i sql/migrations/"))
        else:
            for migration in migrations:
                scroll.mount(MigrationRow(migration))

        # Uppdatera status
        applied = [m for m in migrations if m.status == MigrationStatus.APPLIED]
        pending = [m for m in migrations if m.status == MigrationStatus.PENDING]

        self.query_one("#status-applied", Label).update(f"✓ Körda: {len(applied)}")
        self.query_one("#status-pending", Label).update(f"○ Väntande: {len(pending)}")
        self.query_one("#status-db", Label).update(f"DB: {self.db_path}")

    def log_message(self, message: str) -> None:
        """Lägg till meddelande i loggen."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log = self.query_one("#log", Log)
        log.write_line(f"[{timestamp}] {message}")

    async def action_migrate(self) -> None:
        """Kör väntande migrationer."""
        migrator = self._get_migrator()

        pending = migrator.get_pending()
        if not pending:
            self.log_message("Inga väntande migrationer")
            return

        self.log_message(f"Kör {len(pending)} migrering(ar)...")

        result = migrator.migrate(on_log=self.log_message)

        self.log_message(result.message)
        self.refresh_migrations()

    async def action_rollback(self) -> None:
        """Rulla tillbaka senaste migreringen."""
        migrator = self._get_migrator()

        applied = migrator.get_applied()
        if not applied:
            self.log_message("Inga migrationer att rulla tillbaka")
            return

        self.log_message("Rullar tillbaka senaste migreringen...")

        result = migrator.rollback(steps=1, on_log=self.log_message)

        self.log_message(result.message)
        self.refresh_migrations()

    def action_refresh(self) -> None:
        """Uppdatera migrationslistan."""
        self.log_message("Uppdaterar...")
        # Stäng befintlig connection för att hämta färsk data
        if self._conn:
            self._conn.close()
            self._conn = None
            self._migrator = None
        self.refresh_migrations()
        self.log_message("Uppdaterat")

    def action_quit(self) -> None:
        """Avsluta applikationen."""
        if self._conn:
            self._conn.close()
        self.app.exit()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Hantera knapptryckningar."""
        if event.button.id == "btn-migrate":
            await self.action_migrate()
        elif event.button.id == "btn-rollback":
            await self.action_rollback()
        elif event.button.id == "btn-refresh":
            self.action_refresh()
        elif event.button.id == "btn-pipeline":
            self.app.push_screen("pipeline")
