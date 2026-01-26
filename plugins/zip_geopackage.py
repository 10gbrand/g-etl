"""Plugin för att ladda ner och läsa zippade GeoPackage-filer."""

import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from plugins.base import ExtractResult, SourcePlugin


class ZipGeoPackagePlugin(SourcePlugin):
    """Plugin för att ladda ner zippade GeoPackage-filer från URL."""

    @property
    def name(self) -> str:
        return "zip_geopackage"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
    ) -> ExtractResult:
        """Laddar ner zip, extraherar GeoPackage och laddar till raw-schema.

        Config-parametrar:
            url: URL till zip-filen
            name: Tabellnamn i DuckDB
            layer: Specifikt lager att läsa (optional)
            gpkg_filename: Filnamn på .gpkg i zip:en (optional, hittas automatiskt)
        """
        url = config.get("url")
        table_name = config.get("name", config.get("id"))
        layer = config.get("layer")
        gpkg_filename = config.get("gpkg_filename")

        if not url:
            return ExtractResult(
                success=False,
                message="Saknar url i config",
            )

        self._log(f"Laddar ner {url}...", on_log)

        try:
            # Ladda ner zip-filen
            response = requests.get(url, timeout=300, stream=True)
            response.raise_for_status()

            # Spara till temporär fil
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
                for chunk in response.iter_content(chunk_size=8192):
                    tmp_zip.write(chunk)
                zip_path = tmp_zip.name

            self._log("Extraherar GeoPackage...", on_log)

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

                # Läs in till DuckDB
                if layer:
                    read_expr = f"ST_Read('{gpkg_path}', layer='{layer}')"
                else:
                    read_expr = f"ST_Read('{gpkg_path}')"

                conn.execute(f"""
                    CREATE OR REPLACE TABLE raw.{table_name} AS
                    SELECT * FROM {read_expr}
                """)

                result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
                rows_count = result[0] if result else 0

            # Rensa zip-fil
            Path(zip_path).unlink(missing_ok=True)

            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)

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
