"""WFS-plugin för att hämta data från WFS-tjänster."""

from collections.abc import Callable

import duckdb

from g_etl.plugins.base import ExtractResult, SourcePlugin


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
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data från WFS och laddar till raw-schema.

        Config-parametrar:
            url: WFS-tjänstens bas-URL
            layer: Lagrets namn (typename)
            name: Tabellnamn i DuckDB
            srs: Koordinatsystem (default: EPSG:3006)
            max_features: Max antal features att hämta (optional)
            paginate: Använd paginering för stora datasets (default: False)
            page_size: Antal features per chunk vid paginering (default: 1000)
        """
        url = config.get("url")
        layer = config.get("layer")
        table_name = config.get("id")  # Använd alltid id som tabellnamn
        srs = config.get("srs", "EPSG:3006")
        max_features = config.get("max_features")
        paginate = config.get("paginate", False)
        page_size = config.get("page_size", 1000)

        if not all([url, layer, table_name]):
            return ExtractResult(
                success=False,
                message="Saknar url, layer eller id i config",
            )

        self._log(f"Hämtar {layer} från {url}...", on_log)
        self._progress(0.1, f"Hämtar från WFS: {layer}...", on_progress)

        try:
            if paginate:
                # Paginerad hämtning (för stora datasets)
                return self._extract_paginated(
                    url, layer, table_name, srs, max_features, page_size, conn, on_log, on_progress
                )
            else:
                # Enkel hämtning (för små/medelstora datasets)
                return self._extract_simple(
                    url, layer, table_name, srs, max_features, conn, on_log, on_progress
                )

        except Exception as e:
            error_msg = f"Fel vid hämtning från WFS: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)

    def _extract_simple(
        self,
        url: str,
        layer: str,
        table_name: str,
        srs: str,
        max_features: int | None,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None,
        on_progress: Callable[[float, str], None] | None,
    ) -> ExtractResult:
        """Enkel WFS-hämtning utan paginering."""
        # Bygg WFS-URL
        wfs_url = (
            f"{url}?service=WFS&version=2.0.0&request=GetFeature"
            f"&typename={layer}&srsName={srs}&outputFormat=application/json"
        )
        if max_features:
            wfs_url += f"&count={max_features}"

        # Använd DuckDB:s spatial extension för att läsa WFS/GeoJSON
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.{table_name} AS
            SELECT * FROM ST_Read('{wfs_url}')
        """)

        self._progress(0.9, "Räknar rader...", on_progress)

        # Hämta antal rader
        result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
        rows_count = result[0] if result else 0

        self._log(f"Hämtade {rows_count} rader till raw.{table_name}", on_log)
        self._progress(1.0, f"Hämtade {rows_count} rader", on_progress)

        return ExtractResult(
            success=True,
            rows_count=rows_count,
            message=f"Hämtade {rows_count} rader",
        )

    def _extract_paginated(
        self,
        url: str,
        layer: str,
        table_name: str,
        srs: str,
        max_features: int | None,
        page_size: int,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None,
        on_progress: Callable[[float, str], None] | None,
    ) -> ExtractResult:
        """Paginerad WFS-hämtning för stora datasets."""
        self._log(f"Använder paginering med {page_size} features per chunk", on_log)

        start_index = 0
        total_rows = 0
        chunk_num = 0

        # Skapa temp-tabell för första chunken
        first_chunk = True

        while True:
            chunk_num += 1

            # Bygg WFS-URL med paginering
            wfs_url = (
                f"{url}?service=WFS&version=2.0.0&request=GetFeature"
                f"&typename={layer}&srsName={srs}&outputFormat=application/json"
                f"&count={page_size}&startIndex={start_index}"
            )

            self._log(f"Hämtar chunk {chunk_num} (startIndex={start_index})...", on_log)
            self._progress(
                0.1 + (chunk_num * 0.05),  # Progressiv progress
                f"Hämtar chunk {chunk_num}...",
                on_progress,
            )

            try:
                # Läs chunk till temp-tabell
                chunk_result = conn.execute(f"SELECT * FROM ST_Read('{wfs_url}')").fetchdf()

                if chunk_result.empty or len(chunk_result) == 0:
                    # Inga fler features
                    break

                chunk_rows = len(chunk_result)
                total_rows += chunk_rows

                # Första chunken: skapa huvudtabell
                if first_chunk:
                    conn.execute(f"""
                        CREATE OR REPLACE TABLE raw.{table_name} AS
                        SELECT * FROM ST_Read('{wfs_url}')
                    """)
                    first_chunk = False
                else:
                    # Efterföljande chunks: append
                    conn.execute(f"""
                        INSERT INTO raw.{table_name}
                        SELECT * FROM ST_Read('{wfs_url}')
                    """)

                self._log(f"Chunk {chunk_num}: +{chunk_rows} rader (totalt {total_rows})", on_log)

                # Om vi nådde max_features eller fick färre än page_size, avsluta
                if max_features and total_rows >= max_features:
                    self._log(f"Nådde max_features ({max_features})", on_log)
                    break
                if chunk_rows < page_size:
                    self._log("Inga fler chunks att hämta", on_log)
                    break

                start_index += page_size

            except Exception as e:
                if first_chunk:
                    # Om första chunken misslyckas, rethrow error
                    raise
                else:
                    # Om senare chunk misslyckas, logga och avsluta
                    self._log(f"Chunk {chunk_num} misslyckades: {e}", on_log)
                    break

        self._progress(1.0, f"Hämtade {total_rows} rader i {chunk_num} chunks", on_progress)

        return ExtractResult(
            success=True,
            rows_count=total_rows,
            message=f"Hämtade {total_rows} rader i {chunk_num} chunks",
        )
