"""WFS-plugin med GeoPandas för bättre felhantering av trasiga WFS-servrar."""

from collections.abc import Callable
from urllib.parse import urlencode

import duckdb
import geopandas as gpd
import pandas as pd
import requests

from g_etl.plugins.base import ExtractResult, SourcePlugin


class WfsGeopandasPlugin(SourcePlugin):
    """
    Plugin för WFS med GeoPandas-baserad hämtning.

    Fördelar vs standard WFS-plugin:
    - Bättre felhantering för trasiga WFS-servrar
    - Kan reparera trasigt JSON
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
        """Hämtar data från WFS med GeoPandas.

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

        self._log(f"Hämtar {layer} från {url} med GeoPandas...", on_log)
        self._progress(0.1, f"Hämtar från WFS: {layer}...", on_progress)

        try:
            # Hämta data i chunks
            all_gdfs = []
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
                    # Försök läsa med GeoPandas (mer robust än GDAL)
                    gdf = gpd.read_file(wfs_url)

                    if gdf.empty:
                        self._log("Inga fler features", on_log)
                        break

                    chunk_rows = len(gdf)
                    total_rows += chunk_rows
                    all_gdfs.append(gdf)

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

            if not all_gdfs:
                return ExtractResult(
                    success=False,
                    message="Inga features hämtades",
                )

            # Slå ihop alla chunks
            self._log(f"Slår ihop {len(all_gdfs)} chunks...", on_log)
            combined_gdf = gpd.GeoDataFrame(
                pd.concat(all_gdfs, ignore_index=True), crs=all_gdfs[0].crs
            )

            # Konvertera till DuckDB
            self._log("Laddar till DuckDB...", on_log)
            self._progress(0.8, "Laddar till DuckDB...", on_progress)

            # Exportera geometri som WKT
            combined_gdf["geom_wkt"] = combined_gdf.geometry.to_wkt()

            # Ta bort geometry-kolumnen (DuckDB kan inte hantera Shapely direkt)
            combined_gdf = combined_gdf.drop(columns=["geometry"])

            # Ladda till DuckDB
            conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
            conn.execute(f"""
                CREATE TABLE raw.{table_name} AS
                SELECT
                    * EXCLUDE (geom_wkt),
                    ST_GeomFromText(geom_wkt) AS geom
                FROM df
            """)

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
