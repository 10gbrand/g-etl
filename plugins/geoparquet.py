"""Plugin för att läsa GeoParquet-filer."""

from collections.abc import Callable
from pathlib import Path

import duckdb

from plugins.base import ExtractResult, SourcePlugin


class GeoParquetPlugin(SourcePlugin):
    """Plugin för att läsa GeoParquet-filer (.parquet, .geoparquet)."""

    @property
    def name(self) -> str:
        return "geoparquet"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
    ) -> ExtractResult:
        """Läser GeoParquet-fil och laddar till raw-schema.

        Config-parametrar:
            path: Sökväg till parquet-filen (lokal eller URL)
            name: Tabellnamn i DuckDB
        """
        file_path = config.get("path")
        table_name = config.get("id")  # Använd alltid id som tabellnamn

        if not all([file_path, table_name]):
            return ExtractResult(
                success=False,
                message="Saknar path eller id i config",
            )

        # Kolla om det är en URL eller lokal fil
        is_remote = file_path.startswith(("http://", "https://", "s3://"))

        if not is_remote:
            path = Path(file_path)
            if not path.exists():
                return ExtractResult(
                    success=False,
                    message=f"Filen finns inte: {file_path}",
                )

        self._log(f"Läser {file_path}...", on_log)

        try:
            # DuckDB kan läsa parquet direkt, inklusive via httpfs
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS
                SELECT * FROM read_parquet('{file_path}')
            """)

            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Läste {rows_count} rader",
            )

        except Exception as e:
            error_msg = f"Fel vid läsning av GeoParquet: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
