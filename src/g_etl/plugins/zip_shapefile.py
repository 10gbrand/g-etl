"""Plugin för att ladda ner och läsa zippade Shapefile-filer."""

import tempfile
import threading
import zipfile
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from g_etl.plugins.base import ExtractResult, SourcePlugin
from g_etl.utils.downloader import download_file_streaming, is_url

# Modul-nivå cache för nedladdade och extraherade filer
# Cache: URL -> (zip_path, extracted_dir_path)
# Rensas via clear_shapefile_cache()
_extract_cache: dict[str, tuple[str, str]] = {}
_url_locks: dict[str, threading.Lock] = {}
_global_lock = threading.Lock()


def _get_url_lock(url: str) -> threading.Lock:
    """Hämta (eller skapa) ett lås för en specifik URL."""
    with _global_lock:
        if url not in _url_locks:
            _url_locks[url] = threading.Lock()
        return _url_locks[url]


def clear_shapefile_cache() -> None:
    """Rensa nedladdningscachen och ta bort temporära filer."""
    global _extract_cache, _url_locks
    import shutil

    with _global_lock:
        for url, (zip_path, extract_dir) in _extract_cache.items():
            try:
                Path(zip_path).unlink(missing_ok=True)
            except Exception:
                pass
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception:
                pass
        _extract_cache.clear()
        _url_locks.clear()


class ZipShapefilePlugin(SourcePlugin):
    """Plugin för att ladda ner zippade Shapefile-filer från URL.

    Stödjer flera shapefiles i samma zip genom att ange shp_filename i config.
    Filer cachas så att samma URL bara laddas ner en gång.
    """

    @property
    def name(self) -> str:
        return "zip_shapefile"

    def _download_and_extract(
        self,
        url: str,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> tuple[str, str]:
        """Ladda ner och extrahera zip-fil (med cache).

        Returns:
            Tuple av (zip_path, extract_dir)
        """
        # Kolla om redan cachad
        if url in _extract_cache:
            zip_path, extract_dir = _extract_cache[url]
            if Path(extract_dir).exists():
                self._log("Använder cachad nedladdning", on_log)
                return zip_path, extract_dir

        # Skaffa lås för denna URL
        url_lock = _get_url_lock(url)

        with url_lock:
            # Dubbelkolla efter att vi fått låset
            if url in _extract_cache:
                zip_path, extract_dir = _extract_cache[url]
                if Path(extract_dir).exists():
                    self._log("Använder cachad nedladdning", on_log)
                    return zip_path, extract_dir

            # Ladda ner eller använd lokal fil
            if is_url(url):
                # Använd centraliserad downloader för URL:er
                downloaded_path = download_file_streaming(
                    url=url,
                    suffix=".zip",
                    timeout=300,
                    on_log=on_log,
                    on_progress=on_progress,
                    progress_weight=0.5,
                )
                zip_path = str(downloaded_path)
            else:
                # Lokal fil
                zip_path = url
                if not Path(zip_path).exists():
                    raise FileNotFoundError(f"Filen finns inte: {zip_path}")

            self._log("Extraherar zip-arkiv...", on_log)
            self._progress(0.5, "Extraherar...", on_progress)

            # Extrahera till permanent temp-katalog (behålls tills cache rensas)
            extract_dir = tempfile.mkdtemp(prefix="g_etl_shp_")
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)

            # Spara i cache
            _extract_cache[url] = (zip_path, extract_dir)

            return zip_path, extract_dir

    def _list_shapefiles(self, extract_dir: str) -> list[Path]:
        """Lista alla shapefiles i extraherad katalog."""
        return sorted(Path(extract_dir).rglob("*.shp"))

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Laddar ner zip, extraherar Shapefile och laddar till raw-schema.

        Config-parametrar:
            url: URL till zip-filen ELLER lokal sökväg
            id: Tabellnamn i DuckDB
            shp_filename: Specifik .shp-fil att läsa (krävs om flera finns)
            encoding: Teckenkodning för DBF-filen (default: LATIN1)
        """
        url = config.get("url")
        table_name = config.get("id")
        shp_filename = config.get("shp_filename")
        encoding = config.get("encoding", "LATIN1")

        if not url:
            return ExtractResult(success=False, message="Saknar url i config")

        try:
            # Ladda ner och extrahera (använder cache)
            zip_path, extract_dir = self._download_and_extract(url, on_log, on_progress)

            # Lista tillgängliga shapefiles
            available_shapefiles = self._list_shapefiles(extract_dir)

            if not available_shapefiles:
                return ExtractResult(
                    success=False,
                    message="Ingen .shp-fil hittades i zip-arkivet",
                )

            # Visa information om tillgängliga shapefiles
            if len(available_shapefiles) > 1:
                shp_names = [shp.name for shp in available_shapefiles]
                self._log(
                    f"⚠️  Zip-filen innehåller {len(available_shapefiles)} shapefiles: "
                    f"{', '.join(shp_names)}",
                    on_log,
                )
                if not shp_filename:
                    self._log(
                        f"   Använder första: {available_shapefiles[0].name} "
                        "(sätt 'shp_filename' i config för specifik fil)",
                        on_log,
                    )

            # Hitta rätt shapefile
            if shp_filename:
                # Sök efter specifik fil
                matching = [shp for shp in available_shapefiles if shp.name == shp_filename]
                if not matching:
                    # Prova att söka rekursivt med bara filnamnet
                    matching = [shp for shp in available_shapefiles if shp.name == shp_filename]
                if not matching:
                    available_names = [shp.name for shp in available_shapefiles]
                    return ExtractResult(
                        success=False,
                        message=f"Shapefile '{shp_filename}' finns inte. "
                        f"Tillgängliga: {', '.join(available_names)}",
                    )
                shp_path = matching[0]
            else:
                shp_path = available_shapefiles[0]

            # Kontrollera kompanjonsfiler
            required_companions = [".dbf", ".shx"]
            missing = [ext for ext in required_companions if not shp_path.with_suffix(ext).exists()]
            if missing:
                self._log(f"Varning: saknar kompanjonsfiler: {missing}", on_log)

            self._log(f"Läser {shp_path.name}...", on_log)
            self._progress(0.6, f"Läser {shp_path.name}...", on_progress)

            # Försök läsa med DuckDB ST_Read först
            try:
                conn.execute(f"""
                    CREATE OR REPLACE TABLE raw.{table_name} AS
                    SELECT * FROM ST_Read('{shp_path}')
                """)
            except Exception as st_read_error:
                # Fallback: Använd pyogrio för encoding-problem
                self._log("DuckDB ST_Read misslyckades, testar pyogrio...", on_log)
                rows_count = self._read_with_pyogrio(
                    conn, shp_path, table_name, encoding, on_log, on_progress
                )
                if rows_count is not None:
                    self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
                    self._progress(1.0, f"Läste {rows_count} rader", on_progress)
                    return ExtractResult(
                        success=True,
                        rows_count=rows_count,
                        message=f"Läste {rows_count} rader från {shp_path.name} (pyogrio)",
                    )
                raise st_read_error

            self._progress(0.9, "Räknar rader...", on_progress)

            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
            self._progress(1.0, f"Läste {rows_count} rader", on_progress)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Läste {rows_count} rader från {shp_path.name}",
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
            error_msg = f"Fel vid läsning av Shapefile: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)

    def _read_with_pyogrio(
        self,
        conn: duckdb.DuckDBPyConnection,
        shp_path: Path,
        table_name: str,
        encoding: str,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> int | None:
        """Läs Shapefile med pyogrio (fallback för encoding-problem).

        Returns:
            Antal rader eller None vid fel.
        """
        try:
            import pyogrio

            self._log(f"Läser med pyogrio (encoding={encoding})...", on_log)
            self._progress(0.7, "Läser med pyogrio...", on_progress)

            meta, table = pyogrio.read_arrow(str(shp_path), encoding=encoding)

            # Hitta geometrikolumn
            geom_cols = meta.get("geometry_columns", [])
            geom_col = geom_cols[0] if geom_cols else "wkb_geometry"

            self._log("Laddar in i DuckDB...", on_log)
            self._progress(0.9, "Laddar in i DuckDB...", on_progress)

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

        except ImportError:
            self._log("pyogrio är inte installerat - kan inte använda fallback", on_log)
            return None
        except Exception as e:
            self._log(f"Pyogrio-fallback misslyckades: {e}", on_log)
            return None
