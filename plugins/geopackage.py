"""Plugin för att läsa GeoPackage-filer."""

from collections.abc import Callable
from pathlib import Path

import duckdb

from plugins.base import ExtractResult, SourcePlugin


class GeoPackagePlugin(SourcePlugin):
    """Plugin för att läsa lokala GeoPackage-filer (.gpkg)."""

    @property
    def name(self) -> str:
        return "geopackage"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Läser GeoPackage-fil och laddar till raw-schema.

        Config-parametrar:
            path: Sökväg till .gpkg-filen
            name: Tabellnamn i DuckDB
            layer: Specifikt lager att läsa (optional, default: första lagret)
        """
        file_path = config.get("path")
        table_name = config.get("id")  # Använd alltid id som tabellnamn
        layer = config.get("layer")

        if not all([file_path, table_name]):
            return ExtractResult(
                success=False,
                message="Saknar path eller id i config",
            )

        path = Path(file_path)
        if not path.exists():
            return ExtractResult(
                success=False,
                message=f"Filen finns inte: {file_path}",
            )

        self._log(f"Läser {path.name}...", on_log)
        self._progress(0.1, f"Läser {path.name}...", on_progress)

        try:
            # Bygg ST_Read med layer om specificerat
            if layer:
                read_expr = f"ST_Read('{file_path}', layer='{layer}')"
            else:
                read_expr = f"ST_Read('{file_path}')"

            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS
                SELECT * FROM {read_expr}
            """)

            self._progress(0.9, "Räknar rader...", on_progress)

            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
            self._progress(1.0, f"Läste {rows_count} rader", on_progress)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Läste {rows_count} rader från {path.name}",
            )

        except Exception as e:
            error_msg = f"Fel vid läsning av GeoPackage: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
