"""G-ETL Admin TUI Application.

En terminalbaserad applikation för att köra och övervaka ETL-pipelines
med Docker-stil multi-progress och geometri-validering via ASCII-kartor.
"""

from textual.app import App

from scripts.admin.models.dataset import DatasetConfig
from scripts.admin.screens.explorer import ExplorerScreen
from scripts.admin.screens.migrations import MigrationsScreen
from scripts.admin.screens.pipeline import PipelineScreen
from scripts.admin.services.db_session import cleanup_old_databases, get_session_db_path


class AdminApp(App):
    """G-ETL Admin TUI Application med stöd för flera screens."""

    TITLE = "G-ETL Admin"
    SUB_TITLE = "Pipeline & Data Explorer"

    CSS = """
    /* Global styling */
    Screen {
        background: $surface;
    }
    """

    def __init__(
        self,
        config_path: str = "config/datasets.yml",
        mock: bool = False,
    ) -> None:
        super().__init__()
        self.config_path = config_path
        self.config = DatasetConfig.load(config_path)
        self.mock_mode = mock

        # Skapa ny sessionsspecifik databas
        self.db_path = str(get_session_db_path())

        # Rensa gamla databasfiler (behåll 3 senaste)
        cleanup_old_databases(keep_count=3)

    def on_mount(self) -> None:
        """Installera screens och visa pipeline-screen."""
        # Registrera screens med factory-funktioner för lazy loading
        # Alla screens delar samma databas-sökväg
        self.install_screen(
            lambda: PipelineScreen(
                config=self.config, mock=self.mock_mode, db_path=self.db_path
            ),
            name="pipeline",
        )
        self.install_screen(
            lambda: ExplorerScreen(db_path=self.db_path),
            name="explorer",
        )
        self.install_screen(
            lambda: MigrationsScreen(db_path=self.db_path),
            name="migrations",
        )

        # Visa pipeline-screen som standard
        self.push_screen("pipeline")


def main() -> None:
    """Starta Admin TUI."""
    import argparse

    parser = argparse.ArgumentParser(description="G-ETL Admin TUI")
    parser.add_argument("--mock", action="store_true", help="Kör i mock-läge")
    parser.add_argument(
        "--config",
        default="config/datasets.yml",
        help="Sökväg till config-fil",
    )
    args = parser.parse_args()

    app = AdminApp(config_path=args.config, mock=args.mock)
    app.run()


if __name__ == "__main__":
    main()
