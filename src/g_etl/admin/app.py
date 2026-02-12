"""G-ETL Admin TUI Application.

Terminal-based application for running and monitoring ETL pipelines
with Docker-style multi-progress and geometry validation via ASCII maps.
"""

import os
import sys

# Force UTF-8 encoding for standalone binary
if not sys.stdout.encoding or sys.stdout.encoding.lower() != "utf-8":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    os.environ.setdefault("LC_ALL", "C.UTF-8")
    os.environ.setdefault("LANG", "C.UTF-8")

from textual.app import App

from g_etl.admin.models.dataset import DatasetConfig
from g_etl.admin.screens.explorer import ExplorerScreen
from g_etl.admin.screens.h3_query import H3QueryScreen
from g_etl.admin.screens.migrations import MigrationsScreen
from g_etl.admin.screens.pipeline import PipelineScreen
from g_etl.services.db_session import cleanup_old_databases, get_session_db_path


class AdminApp(App):
    """G-ETL Admin TUI Application with support for multiple screens."""

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

        # Create new session-specific database
        self.db_path = str(get_session_db_path())

        # Clean up old database files (keep 3 most recent)
        cleanup_old_databases(keep_count=3)

    def on_mount(self) -> None:
        """Install screens and show pipeline screen."""
        # Register screens with factory functions for lazy loading
        # All screens share the same database path
        self.install_screen(
            lambda: PipelineScreen(config=self.config, mock=self.mock_mode, db_path=self.db_path),
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
        self.install_screen(
            H3QueryScreen,
            name="h3_query",
        )

        # Show pipeline screen as default
        self.push_screen("pipeline")


def main() -> None:
    """Start Admin TUI."""
    import argparse

    parser = argparse.ArgumentParser(description="G-ETL Admin TUI")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode")
    parser.add_argument(
        "--config",
        default="config/datasets.yml",
        help="Path to config file",
    )
    args = parser.parse_args()

    app = AdminApp(config_path=args.config, mock=args.mock)
    app.run()


if __name__ == "__main__":
    main()
