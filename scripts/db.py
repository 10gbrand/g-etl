"""Databasverktyg för G-ETL.

Gemensamma funktioner för DuckDB-hantering som används av
både pipeline och webbgränssnitt.
"""

import os
from pathlib import Path

import duckdb

# Standardsökvägar
DEFAULT_DB_PATH = "data/warehouse.duckdb"
DEFAULT_SQL_PATH = "sql"


def init_database(conn: duckdb.DuckDBPyConnection, sql_path: str = DEFAULT_SQL_PATH) -> None:
    """Initiera databas med extensions, scheman och makron.

    Kör alla SQL-filer i sql/migrations/ i alfabetisk ordning.

    Args:
        conn: DuckDB-anslutning
        sql_path: Sökväg till sql-mappen
    """
    init_folder = Path(sql_path) / "migrations"
    if not init_folder.exists():
        return

    for sql_file in sorted(init_folder.glob("*.sql")):
        try:
            sql = sql_file.read_text()
            # Kör varje statement separat (separerade med semikolon)
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(statement)
        except Exception as e:
            # Logga men fortsätt - vissa kommandon kan redan vara körda
            print(f"[Init] {sql_file.name}: {e}")


def get_connection(
    db_path: str | None = None,
    sql_path: str = DEFAULT_SQL_PATH,
    read_only: bool = False,
    init: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Hämta en DuckDB-anslutning med initiering.

    Args:
        db_path: Sökväg till databas (default från miljövariabel eller DEFAULT_DB_PATH)
        sql_path: Sökväg till sql-mappen för initiering
        read_only: Om True, öppna i read-only läge
        init: Om True, kör initierings-SQL (ignoreras om read_only=True)

    Returns:
        DuckDB-anslutning
    """
    if db_path is None:
        db_path = os.environ.get("DB_PATH", DEFAULT_DB_PATH)

    conn = duckdb.connect(db_path, read_only=read_only)

    # Hoppa över init i read-only läge (kan inte skapa scheman/makron)
    if init and not read_only:
        init_database(conn, sql_path)

    return conn
