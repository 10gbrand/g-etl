"""WFS-plugin för att hämta data från WFS-tjänster."""

from collections.abc import Callable

import duckdb

from plugins.base import ExtractResult, SourcePlugin


class WfsPlugin(SourcePlugin):
    """Plugin för att hämta geodata från WFS-tjänster."""

    @property
    def name(self) -> str:
        return "wfs"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data från WFS och laddar till raw-schema.

        Config-parametrar:
            url: WFS-tjänstens bas-URL
            layer: Lagrets namn (typename)
            name: Tabellnamn i DuckDB
            srs: Koordinatsystem (default: EPSG:3006)
            max_features: Max antal features att hämta (optional)
        """
        url = config.get("url")
        layer = config.get("layer")
        table_name = config.get("id")  # Använd alltid id som tabellnamn
        srs = config.get("srs", "EPSG:3006")
        max_features = config.get("max_features")

        if not all([url, layer, table_name]):
            return ExtractResult(
                success=False,
                message="Saknar url, layer eller id i config",
            )

        self._log(f"Hämtar {layer} från {url}...", on_log)

        # Bygg WFS-URL
        wfs_url = (
            f"{url}?service=WFS&version=2.0.0&request=GetFeature"
            f"&typename={layer}&srsName={srs}&outputFormat=application/json"
        )
        if max_features:
            wfs_url += f"&count={max_features}"

        try:
            # Använd DuckDB:s spatial extension för att läsa WFS/GeoJSON
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS
                SELECT * FROM ST_Read('{wfs_url}')
            """)

            # Hämta antal rader
            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._log(f"Hämtade {rows_count} rader till raw.{table_name}", on_log)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Hämtade {rows_count} rader",
            )

        except Exception as e:
            error_msg = f"Fel vid hämtning från WFS: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
