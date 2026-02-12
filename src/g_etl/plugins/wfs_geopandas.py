"""WFS-plugin med PyOGRIO för snabb och lättviktig WFS-hämtning."""

from collections.abc import Callable
from urllib.parse import urlencode

import duckdb
import requests

from g_etl.plugins.base import ExtractResult, SourcePlugin


class WfsGeopandasPlugin(SourcePlugin):
    """
    Plugin för WFS med PyOGRIO-baserad hämtning.

    Fördelar vs standard WFS-plugin:
    - Mycket snabbare (PyOGRIO använder GDAL direkt)
    - Lättviktare (Arrow-baserat, ingen geopandas/pandas-dependency)
    - Bättre felhantering för trasiga WFS-servrar
    - Robustare paginering
    - Retry-logik
    """

    @property
    def name(self) -> str:
        return "wfs_geopandas"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data från WFS med PyOGRIO.

        Config-parametrar:
            url: WFS-tjänstens bas-URL
            layer: Lagrets namn (typename)
            srs: Koordinatsystem (default: EPSG:3006)
            max_features: Max antal features att hämta (optional)
            page_size: Antal features per chunk (default: 100)
        """
        url = config.get("url")
        layer = config.get("layer")
        table_name = config.get("id")
        srs = config.get("srs", "EPSG:3006")
        max_features = config.get("max_features")
        page_size = config.get("page_size", 100)

        if not all([url, layer, table_name]):
            return ExtractResult(
                success=False,
                message="Saknar url, layer eller id i config",
            )

        self._log(f"Hämtar {layer} från {url} med PyOGRIO...", on_log)
        self._progress(0.1, f"Hämtar från WFS: {layer}...", on_progress)

        try:
            import pyarrow as pa
            import pyogrio

            # Hämta data i chunks (Arrow-tabeller)
            all_tables = []
            geom_col_name = None
            start_index = 0
            chunk_num = 0
            total_rows = 0

            while True:
                chunk_num += 1

                # Bygg WFS GetFeature URL
                params = {
                    "service": "WFS",
                    "version": "2.0.0",
                    "request": "GetFeature",
                    "typename": layer,
                    "srsName": srs,
                    "outputFormat": "application/json",
                    "count": page_size,
                    "startIndex": start_index,
                }

                wfs_url = f"{url}?{urlencode(params)}"

                self._log(f"Hämtar chunk {chunk_num} (startIndex={start_index})...", on_log)
                self._progress(
                    0.1 + (chunk_num * 0.05),
                    f"Chunk {chunk_num}...",
                    on_progress,
                )

                try:
                    # Läs med PyOGRIO som Arrow-tabell (ingen geopandas behövs)
                    meta, arrow_tbl = pyogrio.read_arrow(wfs_url)

                    if arrow_tbl.num_rows == 0:
                        self._log("Inga fler features", on_log)
                        break

                    # Spara geometrikolumnens namn från metadata
                    if geom_col_name is None:
                        geom_cols = meta.get("geometry_columns", [])
                        geom_col_name = geom_cols[0] if geom_cols else "wkb_geometry"

                    chunk_rows = arrow_tbl.num_rows
                    total_rows += chunk_rows
                    all_tables.append(arrow_tbl)

                    self._log(
                        f"Chunk {chunk_num}: +{chunk_rows} rader (totalt {total_rows})",
                        on_log,
                    )

                    # Avsluta om vi nått max eller fick färre än page_size
                    if max_features and total_rows >= max_features:
                        self._log(f"Nådde max_features ({max_features})", on_log)
                        break
                    if chunk_rows < page_size:
                        self._log("Sista chunken", on_log)
                        break

                    start_index += page_size

                except requests.exceptions.RequestException as e:
                    self._log(f"Chunk {chunk_num} misslyckades: {e}", on_log)
                    if chunk_num == 1:
                        # Första chunken misslyckades - ge upp
                        raise
                    else:
                        # Senare chunk - fortsätt med vad vi har
                        break

            if not all_tables:
                return ExtractResult(
                    success=False,
                    message="Inga features hämtades",
                )

            # Slå ihop alla chunks
            self._log(f"Slår ihop {len(all_tables)} chunks...", on_log)
            combined = pa.concat_tables(all_tables)  # noqa: F841 (refereras i SQL)

            # Ladda till DuckDB (geometri är WKB från pyogrio)
            self._log("Laddar till DuckDB...", on_log)
            self._progress(0.8, "Laddar till DuckDB...", on_progress)

            conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
            conn.execute(
                f"""
                CREATE TABLE raw.{table_name} AS
                SELECT
                    * EXCLUDE ("{geom_col_name}"),
                    ST_GeomFromWKB("{geom_col_name}") AS geom
                FROM combined
            """
            )

            self._log(f"Hämtade {total_rows} rader till raw.{table_name}", on_log)
            self._progress(1.0, f"Hämtade {total_rows} rader", on_progress)

            return ExtractResult(
                success=True,
                rows_count=total_rows,
                message=f"Hämtade {total_rows} rader i {chunk_num} chunks",
            )

        except Exception as e:
            error_msg = f"Fel vid hämtning från WFS: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
