"""Plugin för att ladda ner och läsa zippade Shapefile-filer."""

import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from plugins.base import ExtractResult, SourcePlugin


class ZipShapefilePlugin(SourcePlugin):
    """Plugin för att ladda ner zippade Shapefile-filer från URL."""

    @property
    def name(self) -> str:
        return "zip_shapefile"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Laddar ner zip, extraherar Shapefile och laddar till raw-schema.

        Config-parametrar:
            url: URL till zip-filen
            id: Tabellnamn i DuckDB
            shp_filename: Filnamn på .shp i zip:en (optional, hittas automatiskt)
        """
        url = config.get("url")
        table_name = config.get("id")  # Använd alltid id som tabellnamn
        shp_filename = config.get("shp_filename")

        if not url:
            return ExtractResult(
                success=False,
                message="Saknar url i config",
            )

        self._log(f"Laddar ner {url}...", on_log)
        self._progress(0.0, "Ansluter...", on_progress)

        try:
            # Ladda ner zip-filen med progress
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
                        # Nedladdning tar 0.0-0.6 av total progress
                        dl_fraction = downloaded / total_size
                        dl_progress = dl_fraction * 0.6
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
                            0.3,  # Fast vid 30% för okänd storlek
                            f"Laddar ner {mb_done:.1f} MB...",
                            on_progress,
                        )

                zip_path = tmp_zip.name

            self._log("Extraherar Shapefile...", on_log)
            self._progress(0.6, "Extraherar zip-arkiv...", on_progress)

            # Extrahera zip
            with tempfile.TemporaryDirectory() as tmp_dir:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(tmp_dir)

                # Hitta .shp-fil
                if shp_filename:
                    shp_path = Path(tmp_dir) / shp_filename
                else:
                    shp_files = list(Path(tmp_dir).rglob("*.shp"))
                    if not shp_files:
                        return ExtractResult(
                            success=False,
                            message="Ingen .shp-fil hittades i zip-arkivet",
                        )
                    shp_path = shp_files[0]
                    if len(shp_files) > 1:
                        self._log(f"Flera .shp-filer hittades, använder {shp_path.name}", on_log)

                if not shp_path.exists():
                    return ExtractResult(
                        success=False,
                        message=f"Shapefile-filen finns inte: {shp_path}",
                    )

                # Kontrollera att nödvändiga kompanjonsfiler finns
                required_companions = [".dbf", ".shx"]
                missing = []
                for ext in required_companions:
                    companion = shp_path.with_suffix(ext)
                    if not companion.exists():
                        missing.append(ext)

                if missing:
                    self._log(f"Varning: saknar kompanjonsfiler: {missing}", on_log)

                self._log(f"Läser {shp_path.name}...", on_log)
                self._progress(0.7, f"Läser {shp_path.name}...", on_progress)

                # Läs in till DuckDB
                conn.execute(f"""
                    CREATE OR REPLACE TABLE raw.{table_name} AS
                    SELECT * FROM ST_Read('{shp_path}')
                """)

                self._progress(0.9, "Räknar rader...", on_progress)

                result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
                rows_count = result[0] if result else 0

            # Rensa zip-fil
            Path(zip_path).unlink(missing_ok=True)

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
