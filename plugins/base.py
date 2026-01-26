"""Basklass för datakälla-plugins."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

import duckdb


@dataclass
class ExtractResult:
    """Resultat från en extract-operation."""

    success: bool
    rows_count: int = 0
    message: str = ""


class SourcePlugin(ABC):
    """Abstrakt basklass för datakälla-plugins.

    Varje plugin implementerar extract() som hämtar data från en källa
    och laddar den till raw-schemat i DuckDB.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returnerar pluginens namn."""
        pass

    @abstractmethod
    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data och laddar till raw-schema.

        Args:
            config: Dataset-konfiguration med plugin-specifika parametrar
            conn: DuckDB-anslutning
            on_log: Callback för loggmeddelanden

        Returns:
            ExtractResult med status och antal rader
        """
        pass

    def _log(self, message: str, on_log: Callable[[str], None] | None):
        """Hjälpmetod för att logga meddelanden."""
        if on_log:
            on_log(message)
