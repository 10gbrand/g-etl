"""Plugin för att läsa zippade GeoPackage-filer från URL eller lokal fil."""

import tempfile
import threading
import zipfile
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from g_etl.plugins.base import ExtractResult, SourcePlugin
from g_etl.utils.downloader import download_file_streaming, is_url

# Modul-nivå cache för nedladdade zip-filer
# Cache: URL -> zip_path (behåller bara zip-filen, inte extraktionskatalogen)
# Varje dataset extraherar till sin egen katalog för att undvika race conditions
# Rensas via clear_download_cache()
_zip_cache: dict[str, str] = {}
_url_locks: dict[str, threading.Lock] = {}
_global_lock = threading.Lock()

# Håll koll på alla extraktionskataloger för städning
_extract_dirs: list[str] = []


def _get_url_lock(url: str) -> threading.Lock:
    """Hämta (eller skapa) ett lås för en specifik URL."""
    with _global_lock:
        if url not in _url_locks:
            _url_locks[url] = threading.Lock()
        return _url_locks[url]


def clear_download_cache() -> None:
    """Rensa nedladdningscachen och ta bort temporära filer."""
    global _zip_cache, _url_locks, _extract_dirs
    import shutil

    with _global_lock:
        # Ta bort cachade zip-filer
        for url, zip_path in _zip_cache.items():
            try:
                Path(zip_path).unlink(missing_ok=True)
            except Exception:
                pass

        # Ta bort alla extraktionskataloger
        for extract_dir in _extract_dirs:
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception:
                pass

        _zip_cache.clear()
        _extract_dirs.clear()
        _url_locks.clear()


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
        is_remote = is_url(url)

        try:
            if is_remote:
                # === NEDLADDNING FRÅN URL (med cache och lås) ===
                # Steg 1: Hämta eller ladda ner zip-fil (med lås)
                url_lock = _get_url_lock(url)
                with url_lock:
                    if url in _zip_cache:
                        # Använd cachad zip-fil
                        zip_path = _zip_cache[url]
                        self._log(f"Använder cachad nedladdning för {url}", on_log)
                        self._progress(0.5, "Använder cachad nedladdning...", on_progress)
                    else:
                        # Ladda ned zip-fil med centraliserad downloader
                        downloaded_path = download_file_streaming(
                            url=url,
                            suffix=".zip",
                            timeout=300,
                            on_log=on_log,
                            on_progress=on_progress,
                            progress_weight=0.5,
                        )
                        zip_path = str(downloaded_path)
                        _zip_cache[url] = zip_path

                # Steg 2: Extrahera till UNIK katalog per dataset
                # Använd URL-lås även för extraktion för att undvika filkonflikt
                # när flera trådar läser samma cachade zip-fil samtidigt
                self._log("Extraherar GeoPackage...", on_log)
                self._progress(0.6, "Extraherar zip-arkiv...", on_progress)

                extract_dir = tempfile.mkdtemp(prefix=f"g-etl-gpkg-{table_name}-")
                with _global_lock:
                    _extract_dirs.append(extract_dir)

                # Lås under extraktion för att undvika att flera trådar läser zip samtidigt
                with url_lock:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        zf.extractall(extract_dir)

                # Hitta .gpkg-fil i extraherad katalog
                if gpkg_filename:
                    gpkg_path = Path(extract_dir) / gpkg_filename
                else:
                    gpkg_files = list(Path(extract_dir).rglob("*.gpkg"))
                    if not gpkg_files:
                        return ExtractResult(
                            success=False,
                            message="Ingen .gpkg-fil hittades i zip-arkivet",
                        )
                    gpkg_path = gpkg_files[0]
                    if len(gpkg_files) > 1:
                        self._log(f"Flera .gpkg-filer hittades, använder {gpkg_path.name}", on_log)
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

                # Extrahera lokal fil till temp-katalog
                self._log("Extraherar GeoPackage...", on_log)
                self._progress(0.6, "Extraherar zip-arkiv...", on_progress)

                extract_dir = tempfile.mkdtemp(prefix="g-etl-gpkg-local-")
                with zipfile.ZipFile(str(local_path), "r") as zf:
                    zf.extractall(extract_dir)

                # Hitta .gpkg-fil
                if gpkg_filename:
                    gpkg_path = Path(extract_dir) / gpkg_filename
                else:
                    gpkg_files = list(Path(extract_dir).rglob("*.gpkg"))
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

            # Lista tillgängliga lager i GeoPackage
            try:
                layers_result = conn.execute(
                    f"SELECT name FROM st_layers('{gpkg_path}')"
                ).fetchall()
                available_layers = [row[0] for row in layers_result]
                if len(available_layers) > 1:
                    layers_str = ", ".join(available_layers)
                    self._log(
                        f"⚠️  {gpkg_path.name} har {len(available_layers)} lager: {layers_str}",
                        on_log,
                    )
                    if not layer:
                        self._log(
                            f"   Använder första lagret: {available_layers[0]} "
                            "(sätt 'layer' i config för specifikt lager)",
                            on_log,
                        )
            except Exception:
                pass  # st_layers fungerar inte alltid, ignorera

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
                # Fallback: Använd pyogrio för komplexa geometrier (MULTISURFACE etc)
                if "MULTISURFACE" in str(st_read_error) or "not supported" in str(st_read_error):
                    self._log("ST_Read misslyckades, använder pyogrio...", on_log)
                    rows_count = self._read_with_pyogrio(conn, gpkg_path, table_name, layer, on_log)
                    if rows_count is not None:
                        self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
                        self._progress(1.0, f"Läste {rows_count} rader", on_progress)
                        return ExtractResult(
                            success=True,
                            rows_count=rows_count,
                            message=f"Läste {rows_count} rader från {gpkg_path.name} (pyogrio)",
                        )
                raise  # Kasta vidare om det inte var geometri-problem

            self._progress(0.9, "Räknar rader...", on_progress)

            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

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

    def _read_with_pyogrio(
        self,
        conn: duckdb.DuckDBPyConnection,
        gpkg_path: Path,
        table_name: str,
        layer: str | None,
        on_log: Callable[[str], None] | None = None,
    ) -> int | None:
        """Läs GeoPackage med pyogrio (fallback för komplexa geometrier).

        Konverterar MULTISURFACE till MULTIPOLYGON etc. via shapely.

        Returns:
            Antal rader eller None vid fel.
        """
        try:
            import pyarrow as pa
            import pyogrio
            import shapely

            # Läs med pyogrio som Arrow-tabell
            kwargs = {}
            if layer:
                kwargs["layer"] = layer
            meta, table = pyogrio.read_arrow(str(gpkg_path), **kwargs)

            # Hitta geometrikolumn
            geom_cols = meta.get("geometry_columns", [])
            geom_col = geom_cols[0] if geom_cols else "wkb_geometry"

            # Konvertera komplexa geometrier via shapely
            wkb_array = table.column(geom_col).to_pylist()
            geoms = shapely.from_wkb(wkb_array)

            # Förenkla komplexa typer (MULTISURFACE → MULTIPOLYGON etc.)
            simplified = shapely.to_wkb(
                [self._simplify_geometry(g) if g is not None else None for g in geoms]
            )

            # Ersätt geometrikolumnen med förenklade WKB-geometrier
            col_idx = table.schema.get_field_index(geom_col)
            table = table.set_column(col_idx, geom_col, pa.array(simplified, type=pa.binary()))

            # Ladda till DuckDB
            conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
            conn.execute(
                f"""
                CREATE TABLE raw.{table_name} AS
                SELECT
                    * EXCLUDE ("{geom_col}"),
                    ST_GeomFromWKB("{geom_col}") AS geom
                FROM table
            """
            )

            return table.num_rows

        except Exception as e:
            self._log(f"Pyogrio-fallback misslyckades: {e}", on_log)
            return None

    def _simplify_geometry(self, geom):
        """Konvertera komplexa geometrityper till enklare."""
        from shapely.geometry import MultiPolygon

        geom_type = geom.geom_type

        # MULTISURFACE -> MULTIPOLYGON
        if geom_type in ("MultiSurface", "CurvePolygon", "CompoundCurve"):
            try:
                simplified = geom.buffer(0)
                if simplified.geom_type == "Polygon":
                    return MultiPolygon([simplified])
                return simplified
            except Exception:
                return geom

        return geom
