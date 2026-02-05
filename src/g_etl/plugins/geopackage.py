"""Plugin for reading GeoPackage files directly (uncompressed) from URL or local file."""

import tempfile
import threading
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from g_etl.plugins.base import ExtractResult, SourcePlugin
from g_etl.utils.downloader import download_file_streaming, is_url

# Module-level cache for downloaded files
# Cache: URL -> local_path
_download_cache: dict[str, str] = {}
_url_locks: dict[str, threading.Lock] = {}
_global_lock = threading.Lock()


def _get_url_lock(url: str) -> threading.Lock:
    """Get (or create) a lock for a specific URL."""
    with _global_lock:
        if url not in _url_locks:
            _url_locks[url] = threading.Lock()
        return _url_locks[url]


def clear_download_cache() -> None:
    """Clear download cache and remove temporary files."""
    global _download_cache, _url_locks

    with _global_lock:
        for url, file_path in _download_cache.items():
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception:
                pass
        _download_cache.clear()
        _url_locks.clear()


class GeoPackagePlugin(SourcePlugin):
    """Plugin for reading GeoPackage files directly from URL or local file."""

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
        """Read GeoPackage file (from URL or disk) and load to raw schema.

        Config parameters:
            url: URL to .gpkg file OR local path
            id: Table name in DuckDB
            layer: Specific layer to read (optional)
        """
        url = config.get("url")
        table_name = config.get("id")
        layer = config.get("layer")

        if not url:
            return ExtractResult(
                success=False,
                message="Missing url in config",
            )

        is_remote = is_url(url)

        try:
            if is_remote:
                # Download from URL (with cache and lock)
                gpkg_path = self._get_or_download(url, on_log, on_progress)
            else:
                # Local file
                gpkg_path = Path(url)
                if not gpkg_path.exists():
                    return ExtractResult(
                        success=False,
                        message=f"File not found: {url}",
                    )
                self._log(f"Reading local file {gpkg_path.name}...", on_log)
                self._progress(0.5, f"Reading {gpkg_path.name}...", on_progress)

            if not gpkg_path.exists():
                return ExtractResult(
                    success=False,
                    message=f"GeoPackage file not found: {gpkg_path}",
                )

            # List available layers
            try:
                layers_result = conn.execute(
                    f"SELECT name FROM st_layers('{gpkg_path}')"
                ).fetchall()
                available_layers = [row[0] for row in layers_result]
                if len(available_layers) > 1:
                    layers_str = ", ".join(available_layers)
                    self._log(
                        f"Multiple layers in {gpkg_path.name}: {layers_str}",
                        on_log,
                    )
                    if not layer:
                        self._log(
                            f"Using first layer: {available_layers[0]} "
                            "(set 'layer' in config for specific layer)",
                            on_log,
                        )
            except Exception:
                pass

            self._log(f"Reading {gpkg_path.name}...", on_log)
            self._progress(0.7, f"Reading {gpkg_path.name}...", on_progress)

            # Read with DuckDB ST_Read
            try:
                if layer:
                    read_expr = f"ST_Read('{gpkg_path}', layer='{layer}')"
                else:
                    read_expr = f"ST_Read('{gpkg_path}')"

                conn.execute(f"""
                    CREATE OR REPLACE TABLE raw.{table_name} AS
                    SELECT * FROM {read_expr}
                """)
            except Exception as st_read_error:
                # Fallback: Use geopandas for complex geometries
                if "MULTISURFACE" in str(st_read_error) or "not supported" in str(st_read_error):
                    self._log("ST_Read failed, using geopandas...", on_log)
                    rows_count = self._read_with_geopandas(
                        conn, gpkg_path, table_name, layer, on_log
                    )
                    if rows_count is not None:
                        self._log(f"Read {rows_count} rows to raw.{table_name}", on_log)
                        self._progress(1.0, f"Read {rows_count} rows", on_progress)
                        return ExtractResult(
                            success=True,
                            rows_count=rows_count,
                            message=f"Read {rows_count} rows from {gpkg_path.name} (geopandas)",
                        )
                raise

            self._progress(0.9, "Counting rows...", on_progress)

            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._log(f"Read {rows_count} rows to raw.{table_name}", on_log)
            self._progress(1.0, f"Read {rows_count} rows", on_progress)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Read {rows_count} rows from {gpkg_path.name}",
            )

        except requests.RequestException as e:
            error_msg = f"Download error: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
        except Exception as e:
            error_msg = f"Error reading GeoPackage: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)

    def _get_or_download(
        self,
        url: str,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> Path:
        """Get cached file or download from URL.

        Returns:
            Path to the downloaded file.
        """
        url_lock = _get_url_lock(url)
        with url_lock:
            if url in _download_cache:
                cached_path = Path(_download_cache[url])
                if cached_path.exists():
                    self._log(f"Using cached file for {url}", on_log)
                    self._progress(0.6, "Using cached file...", on_progress)
                    return cached_path

            # Download med centraliserad downloader
            local_path = download_file_streaming(
                url=url,
                suffix=".gpkg",
                timeout=600,
                on_log=on_log,
                on_progress=on_progress,
                progress_weight=0.5,
            )
            _download_cache[url] = str(local_path)
            return local_path

    def _read_with_geopandas(
        self,
        conn: duckdb.DuckDBPyConnection,
        gpkg_path: Path,
        table_name: str,
        layer: str | None,
        on_log: Callable[[str], None] | None = None,
    ) -> int | None:
        """Read GeoPackage with geopandas (fallback for complex geometries).

        Returns:
            Row count or None on error.
        """
        try:
            import geopandas as gpd

            if layer:
                gdf = gpd.read_file(gpkg_path, layer=layer)
            else:
                gdf = gpd.read_file(gpkg_path)

            # Convert complex geometry types
            if "geometry" in gdf.columns:
                gdf["geometry"] = gdf["geometry"].apply(
                    lambda g: g if g is None else self._simplify_geometry(g)
                )

            # Load to DuckDB via temporary parquet
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name

            gdf.to_parquet(tmp_path)
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS
                SELECT * FROM read_parquet('{tmp_path}')
            """)
            Path(tmp_path).unlink(missing_ok=True)

            return len(gdf)

        except Exception as e:
            self._log(f"Geopandas fallback failed: {e}", on_log)
            return None

    def _simplify_geometry(self, geom):
        """Convert complex geometry types to simpler ones."""
        from shapely.geometry import MultiPolygon

        geom_type = geom.geom_type

        if geom_type in ("MultiSurface", "CurvePolygon", "CompoundCurve"):
            try:
                simplified = geom.buffer(0)
                if simplified.geom_type == "Polygon":
                    return MultiPolygon([simplified])
                return simplified
            except Exception:
                return geom

        return geom
