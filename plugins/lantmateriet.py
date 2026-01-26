"""Plugin för Lantmäteriets geodata-API."""

import os
from collections.abc import Callable

import duckdb
import requests

from plugins.base import ExtractResult, SourcePlugin


class LantmaterietPlugin(SourcePlugin):
    """Plugin för att hämta geodata från Lantmäteriets API.

    Kräver miljövariabler:
        LM_API_KEY: API-nyckel
        LM_API_SECRET: API-hemlighet (om OAuth används)
    """

    BASE_URL = "https://api.lantmateriet.se"

    @property
    def name(self) -> str:
        return "lantmateriet"

    def _get_auth_headers(self) -> dict:
        """Hämta autentiseringsheaders från miljövariabler."""
        api_key = os.getenv("LM_API_KEY")
        if not api_key:
            raise ValueError("LM_API_KEY saknas i miljövariabler")

        return {"Authorization": f"Bearer {api_key}"}

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data från Lantmäteriets API.

        Config-parametrar:
            endpoint: API-endpoint (t.ex. /referensdata/v1/naturreservat)
            name: Tabellnamn i DuckDB
            params: Extra query-parametrar (optional)
        """
        endpoint = config.get("endpoint")
        table_name = config.get("name")
        params = config.get("params", {})

        if not all([endpoint, table_name]):
            return ExtractResult(
                success=False,
                message="Saknar endpoint eller name i config",
            )

        self._log(f"Hämtar från Lantmäteriet: {endpoint}...", on_log)

        try:
            headers = self._get_auth_headers()
            url = f"{self.BASE_URL}{endpoint}"

            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()

            # Spara som temporär GeoJSON och läs in
            data = response.json()

            # Skapa tabell från GeoJSON via DuckDB
            import json
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
                json.dump(data, f)
                temp_path = f.name

            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS
                SELECT * FROM ST_Read('{temp_path}')
            """)

            os.unlink(temp_path)

            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            rows_count = result[0] if result else 0

            self._log(f"Hämtade {rows_count} rader till raw.{table_name}", on_log)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Hämtade {rows_count} rader",
            )

        except requests.RequestException as e:
            error_msg = f"HTTP-fel vid Lantmäteriet-anrop: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
        except Exception as e:
            error_msg = f"Fel vid hämtning från Lantmäteriet: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
