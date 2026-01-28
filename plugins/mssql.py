"""Plugin för att hämta data från Microsoft SQL Server."""

from collections.abc import Callable

import duckdb
import pyodbc

from plugins.base import ExtractResult, SourcePlugin


class MssqlPlugin(SourcePlugin):
    """Plugin för att hämta data från MSSQL-databas."""

    @property
    def name(self) -> str:
        return "mssql"

    def extract(
        self,
        config: dict,
        conn: duckdb.DuckDBPyConnection,
        on_log: Callable[[str], None] | None = None,
        on_progress: Callable[[float, str], None] | None = None,
    ) -> ExtractResult:
        """Hämtar data från MSSQL och laddar till raw-schema.

        Config-parametrar:
            id: Tabellnamn i DuckDB
            connection_string: Komplett ODBC-anslutningssträng
            ELLER individuella parametrar:
                server: Servernamn (t.ex. localhost eller server.domain.com)
                database: Databasnamn
                username: Användarnamn (optional för Windows-autentisering)
                password: Lösenord (optional för Windows-autentisering)
                driver: ODBC-drivrutin (default: ODBC Driver 18 for SQL Server)
                trust_server_certificate: Lita på servercertifikat (default: yes)
            query: SQL-fråga att köra
            geometry_column: Namn på geometrikolumn (optional, konverteras till WKT)
        """
        table_name = config.get("id")
        query = config.get("query")

        if not table_name:
            return ExtractResult(success=False, message="Saknar id i config")
        if not query:
            return ExtractResult(success=False, message="Saknar query i config")

        # Bygg anslutningssträng
        connection_string = config.get("connection_string")
        if not connection_string:
            connection_string = self._build_connection_string(config)

        if not connection_string:
            return ExtractResult(
                success=False,
                message="Saknar connection_string eller server/database i config",
            )

        self._log(f"Ansluter till MSSQL...", on_log)
        self._progress(0.1, "Ansluter till MSSQL...", on_progress)

        try:
            # Anslut till MSSQL
            mssql_conn = pyodbc.connect(connection_string)
            cursor = mssql_conn.cursor()

            self._log(f"Kör fråga...", on_log)
            self._progress(0.2, "Kör fråga...", on_progress)

            # Kör frågan
            cursor.execute(query)

            # Hämta kolumnnamn
            columns = [column[0] for column in cursor.description]
            geometry_column = config.get("geometry_column")

            self._log(f"Hämtar data...", on_log)
            self._progress(0.4, "Hämtar data...", on_progress)

            # Hämta alla rader
            rows = cursor.fetchall()
            cursor.close()
            mssql_conn.close()

            if not rows:
                self._log(f"Inga rader hittades", on_log)
                return ExtractResult(
                    success=True,
                    rows_count=0,
                    message="Inga rader hittades",
                )

            self._log(f"Bearbetar {len(rows)} rader...", on_log)
            self._progress(0.6, f"Bearbetar {len(rows)} rader...", on_progress)

            # Konvertera till lista av tupler
            processed_rows = []
            for row in rows:
                processed_row = list(row)
                # Konvertera geometri till WKT om angiven
                if geometry_column and geometry_column in columns:
                    geom_idx = columns.index(geometry_column)
                    if processed_row[geom_idx] is not None:
                        # MSSQL geometry/geography har .STAsText() metod
                        # men vi får redan bytes, så konvertera
                        geom_value = processed_row[geom_idx]
                        if hasattr(geom_value, "STAsText"):
                            processed_row[geom_idx] = geom_value.STAsText()
                        elif isinstance(geom_value, bytes):
                            # Spara som WKB för DuckDB spatial att hantera
                            processed_row[geom_idx] = geom_value
                processed_rows.append(tuple(processed_row))

            self._log(f"Laddar in i DuckDB...", on_log)
            self._progress(0.8, "Laddar in i DuckDB...", on_progress)

            # Skapa tabellen i DuckDB genom att infoga data
            # Bygg CREATE TABLE med korrekta kolumntyper
            escaped_columns = [f'"{col}"' for col in columns]
            placeholders = ", ".join(["?" for _ in columns])
            columns_str = ", ".join(escaped_columns)

            # Skapa temporär tabell först för att inferera typer
            conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")

            # Använd executemany för att infoga raderna
            # DuckDB kan inferera typer från första raderna
            if processed_rows:
                # Skapa tabell med rätt struktur baserat på första raden
                first_row = processed_rows[0]
                col_defs = []
                for i, col in enumerate(columns):
                    val = first_row[i]
                    if isinstance(val, int):
                        col_type = "BIGINT"
                    elif isinstance(val, float):
                        col_type = "DOUBLE"
                    elif isinstance(val, bool):
                        col_type = "BOOLEAN"
                    elif isinstance(val, bytes):
                        col_type = "BLOB"
                    else:
                        col_type = "VARCHAR"
                    col_defs.append(f'"{col}" {col_type}')

                create_sql = f"CREATE TABLE raw.{table_name} ({', '.join(col_defs)})"
                conn.execute(create_sql)

                # Infoga raderna i batchar
                batch_size = 1000
                for i in range(0, len(processed_rows), batch_size):
                    batch = processed_rows[i : i + batch_size]
                    insert_sql = f"INSERT INTO raw.{table_name} ({columns_str}) VALUES ({placeholders})"
                    conn.executemany(insert_sql, batch)

                    progress = 0.8 + (0.15 * min(i + batch_size, len(processed_rows)) / len(processed_rows))
                    self._progress(progress, f"Infogar rader ({i + len(batch)}/{len(processed_rows)})...", on_progress)

            rows_count = len(processed_rows)

            self._log(f"Läste {rows_count} rader till raw.{table_name}", on_log)
            self._progress(1.0, f"Läste {rows_count} rader", on_progress)

            return ExtractResult(
                success=True,
                rows_count=rows_count,
                message=f"Läste {rows_count} rader från MSSQL",
            )

        except pyodbc.Error as e:
            error_msg = f"MSSQL-fel: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)
        except Exception as e:
            error_msg = f"Fel vid läsning från MSSQL: {e}"
            self._log(error_msg, on_log)
            return ExtractResult(success=False, message=error_msg)

    def _build_connection_string(self, config: dict) -> str | None:
        """Bygg ODBC-anslutningssträng från individuella parametrar."""
        server = config.get("server")
        database = config.get("database")

        if not server or not database:
            return None

        driver = config.get("driver", "ODBC Driver 18 for SQL Server")
        trust_cert = config.get("trust_server_certificate", "yes")

        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={server}",
            f"DATABASE={database}",
            f"TrustServerCertificate={trust_cert}",
        ]

        # Lägg till autentisering
        username = config.get("username")
        password = config.get("password")

        if username and password:
            # SQL Server-autentisering
            parts.append(f"UID={username}")
            parts.append(f"PWD={password}")
        else:
            # Windows-autentisering
            parts.append("Trusted_Connection=yes")

        return ";".join(parts)
