"""Basklass för datakälla-plugins."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass
class ExtractResult:
    """Resultat från en extract-operation."""

    success: bool
    rows_count: int = 0
    message: str = ""
    output_path: str | None = None  # Sökväg till parquet-fil vid extract_to_parquet


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
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data och laddar till raw-schema.

        Args:
            config: Dataset-konfiguration med plugin-specifika parametrar
            conn: DuckDB-anslutning
            on_log: Callback för loggmeddelanden
            on_progress: Callback för progress (0.0-1.0, meddelande)

        Returns:
            ExtractResult med status och antal rader
        """
        pass

    def extract_to_parquet(
        self,
        config: dict,
        output_dir: str | Path,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data och sparar till GeoParquet-fil.

        Standardimplementering använder en temporär DuckDB-databas för
        att läsa data och sedan exportera till parquet.

        Args:
            config: Dataset-konfiguration med plugin-specifika parametrar
            output_dir: Mapp att spara parquet-filen i
            on_log: Callback för loggmeddelanden
            on_progress: Callback för progress (0.0-1.0, meddelande)

        Returns:
            ExtractResult med status, antal rader och sökväg till fil
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        table_name = config.get("id")
        output_path = output_dir / f"{table_name}.parquet"

        # Använd temporär databas för extrahering
        temp_conn = duckdb.connect(":memory:")

        try:
            # Initiera extensions
            for ext in ["spatial", "parquet", "httpfs", "json"]:
                try:
                    temp_conn.execute(f"INSTALL {ext}")
                    temp_conn.execute(f"LOAD {ext}")
                except Exception:
                    pass

            # Skapa raw-schema
            temp_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

            # Kör extract mot temporär databas
            result = self.extract(config, temp_conn, on_log, on_progress)

            if not result.success:
                return result

            # Exportera till GeoParquet
            self._progress(0.95, "Sparar till GeoParquet...", on_progress)
            self._log(f"Exporterar till {output_path}...", on_log)

            temp_conn.execute(f"""
                COPY raw.{table_name}
                TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            self._progress(1.0, f"Sparade {result.rows_count} rader", on_progress)
            self._log(f"Sparade {result.rows_count} rader till {output_path}", on_log)

            return ExtractResult(
                success=True,
                rows_count=result.rows_count,
                message=f"Sparade {result.rows_count} rader till {output_path.name}",
                output_path=str(output_path),
            )

        except Exception as e:
            error_msg = f"Fel vid export till parquet: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)

        finally:
            temp_conn.close()

    def _log(self, message: str, on_log: Callable[[str], None] | None):
        """Hjälpmetod för att logga meddelanden."""
        if on_log:
            on_log(message)

    def _progress(
        self,
        progress: float,
        message: str,
        on_progress: Callable[[float, str], None] | None,
    ):
        """Hjälpmetod för att rapportera progress."""
        if on_progress:
            on_progress(progress, message)
