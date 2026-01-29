"""Plugin för att läsa zippade GeoPackage-filer från URL eller lokal fil."""

import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from plugins.base import ExtractResult, SourcePlugin


def _is_url(source: str) -> bool:
    """Kontrollera om källan är en URL eller lokal sökväg."""
    return source.startswith(("http://", "https://"))


class ZipGeoPackagePlugin(SourcePlugin):
    """Plugin för att läsa zippade GeoPackage-filer från URL eller lokal fil."""

    @property
    def name(self) -> str:
        return "zip_geopackage"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Läser zip-fil (från URL eller disk), extraherar GeoPackage och laddar till raw.

        Config-parametrar:
            url: URL till zip-filen ELLER lokal sökväg (t.ex. /Volumes/T9/data.zip)
            name: Tabellnamn i DuckDB
            layer: Specifikt lager att läsa (optional)
            gpkg_filename: Filnamn på .gpkg i zip:en (optional, hittas automatiskt)
        """
        url = config.get("url")
        table_name = config.get("id")  # Använd alltid id som tabellnamn
        layer = config.get("layer")
        gpkg_filename = config.get("gpkg_filename")

        if not url:
            return ExtractResult(
                success=False,
                message="Saknar url i config",
            )

        # Bestäm om det är URL eller lokal fil
        is_remote = _is_url(url)

        try:
            if is_remote:
                # === NEDLADDNING FRÅN URL ===
                zip_path = self._download_zip(url, on_log, on_progress)
                cleanup_zip = True  # Ta bort temp-fil efteråt
            else:
                # === LOKAL FIL ===
                local_path = Path(url)
                if not local_path.exists():
                    return ExtractResult(
                        success=False,
                        message=f"Filen finns inte: {url}",
                    )
                self._log(f"Läser lokal fil {local_path.name}...", on_log)
                self._progress(0.5, f"Läser {local_path.name}...", on_progress)
                zip_path = str(local_path)
                cleanup_zip = False  # Behåll originalfilen

            self._log("Extraherar GeoPackage...", on_log)
            self._progress(0.6, "Extraherar zip-arkiv...", on_progress)

            # Extrahera zip
            with tempfile.TemporaryDirectory() as tmp_dir:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(tmp_dir)

                # Hitta .gpkg-fil
                if gpkg_filename:
                    gpkg_path = Path(tmp_dir) / gpkg_filename
                else:
                    gpkg_files = list(Path(tmp_dir).rglob("*.gpkg"))
                    if not gpkg_files:
                        return ExtractResult(
                            success=False,
                            message="Ingen .gpkg-fil hittades i zip-arkivet",
                        )
                    gpkg_path = gpkg_files[0]
                    if len(gpkg_files) > 1:
                        self._log(f"Flera .gpkg-filer hittades, använder {gpkg_path.name}", on_log)

                if not gpkg_path.exists():
                    return ExtractResult(
                        success=False,
                        message=f"GeoPackage-filen finns inte: {gpkg_path}",
                    )

                self._log(f"Läser {gpkg_path.name}...", on_log)
                self._progress(0.7, f"Läser {gpkg_path.name}...", on_progress)

                # Försök läsa med DuckDB ST_Read först
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
                    # Fallback: Använd geopandas för komplexa geometrier (MULTISURFACE etc)
                    if "MULTISURFACE" in str(st_read_error) or "not supported" in str(st_read_error):
                        self._log("ST_Read misslyckades, använder geopandas...", on_log)
                        rows_count = self._read_with_geopandas(
                            conn, gpkg_path, table_name, layer, on_log
                        )
                        if rows_count is not None:
                            # Hoppa till rensning och returnera
                            if cleanup_zip:
                                Path(zip_path).unlink(missing_ok=True)
                            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
                            self._progress(1.0, f"Läste {rows_count} rader", on_progress)
                            return ExtractResult(
                                success=True,
                                rows_count=rows_count,
                                message=f"Läste {rows_count} rader från {gpkg_path.name} (via geopandas)",
                            )
                    raise  # Kasta vidare om det inte var geometri-problem

                self._progress(0.9, "Räknar rader...", on_progress)

                result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
                rows_count = result[0] if result else 0

            # Rensa temp-fil endast om det var en nedladdning
            if cleanup_zip:
                Path(zip_path).unlink(missing_ok=True)

            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
            self._progress(1.0, f"Läste {rows_count} rader", on_progress)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Läste {rows_count} rader från {gpkg_path.name}",
            )

        except requests.RequestException as e:
            error_msg = f"Nedladdningsfel: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
        except zipfile.BadZipFile as e:
            error_msg = f"Ogiltig zip-fil: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
        except Exception as e:
            error_msg = f"Fel vid läsning av GeoPackage: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)

    def _download_zip(
        self,
        url: str,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> str:
        """Ladda ner zip-fil från URL till temporär fil.

        Returns:
            Sökväg till den nedladdade filen.
        """
        self._log(f"Laddar ner {url}...", on_log)
        self._progress(0.0, "Ansluter...", on_progress)

        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        # Spara till temporär fil
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
            for chunk in response.iter_content(chunk_size=8192):
                tmp_zip.write(chunk)
                downloaded += len(chunk)

                # Rapportera var ~800KB (var 100:e chunk) för att inte överbelasta TUI
                if total_size > 0 and downloaded % (8192 * 100) < 8192:
                    # Nedladdning tar 0.0-0.5 av total progress
                    dl_fraction = downloaded / total_size
                    dl_progress = dl_fraction * 0.5
                    mb_done = downloaded / (1024 * 1024)
                    mb_total = total_size / (1024 * 1024)
                    self._progress(
                        dl_progress,
                        f"Laddar ner {mb_done:.1f}/{mb_total:.1f} MB...",
                        on_progress,
                    )
                elif total_size == 0 and downloaded % (8192 * 100) < 8192:
                    # Okänd storlek, visa bara nedladdat
                    mb_done = downloaded / (1024 * 1024)
                    self._progress(
                        0.25,  # Fast vid 25% för okänd storlek
                        f"Laddar ner {mb_done:.1f} MB...",
                        on_progress,
                    )

            return tmp_zip.name

    def _read_with_geopandas(
        self,
        conn: duckdb.DuckDBPyConnection,
        gpkg_path: Path,
        table_name: str,
        layer: str | None,
        on_log: Callable[[str], None] | None = None,
    ) -> int | None:
        """Läs GeoPackage med geopandas (fallback för komplexa geometrier).

        Konverterar MULTISURFACE till MULTIPOLYGON etc.

        Returns:
            Antal rader eller None vid fel.
        """
        try:
            import geopandas as gpd

            # Läs med geopandas
            if layer:
                gdf = gpd.read_file(gpkg_path, layer=layer)
            else:
                gdf = gpd.read_file(gpkg_path)

            # Konvertera geometrier om de är komplexa typer
            if "geometry" in gdf.columns:
                # Försök konvertera MULTISURFACE till MULTIPOLYGON
                gdf["geometry"] = gdf["geometry"].apply(
                    lambda g: g if g is None else self._simplify_geometry(g)
                )

            # Ladda till DuckDB via temporär parquet
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
            self._log(f"Geopandas-fallback misslyckades: {e}", on_log)
            return None

    def _simplify_geometry(self, geom):
        """Konvertera komplexa geometrityper till enklare."""
        from shapely.geometry import MultiPolygon

        geom_type = geom.geom_type

        # MULTISURFACE -> MULTIPOLYGON
        if geom_type in ("MultiSurface", "CurvePolygon", "CompoundCurve"):
            # Försök konvertera till polygon via buffer(0)
            try:
                simplified = geom.buffer(0)
                if simplified.geom_type == "Polygon":
                    return MultiPolygon([simplified])
                return simplified
            except Exception:
                return geom

        return geom
