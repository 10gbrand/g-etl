"""Databashantering med sessionsbaserade DuckDB-filer."""

from datetime import datetime
from pathlib import Path

import duckdb

# Standardkatalog för databasfiler
DATA_DIR = Path("data")


def get_session_db_path() -> Path:
    """Generera en ny databas-sökväg med tidsstämpel.

    Returnerar sökväg som: data/warehouse_20260128_103045.duckdb
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"warehouse_{timestamp}.duckdb"


def get_latest_db_path() -> Path | None:
    """Hämta senaste databasfilen baserat på tidsstämpel.

    Returnerar None om ingen databas finns.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_files = sorted(DATA_DIR.glob("warehouse_*.duckdb"), reverse=True)

    # Filtrera bort WAL och andra temporära filer
    db_files = [f for f in db_files if not f.suffix.endswith(".wal")]

    return db_files[0] if db_files else None


def cleanup_old_databases(keep_count: int = 3) -> list[Path]:
    """Ta bort gamla databasfiler, behåll de senaste.

    Args:
        keep_count: Antal databasfiler att behålla (default: 3)

    Returns:
        Lista med borttagna filer
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Hitta alla databasfiler
    db_files = sorted(DATA_DIR.glob("warehouse_*.duckdb"), reverse=True)

    # Filtrera ut huvudfiler (inte WAL etc)
    main_files = [f for f in db_files if f.suffix == ".duckdb"]

    removed: list[Path] = []

    # Ta bort allt utom de senaste
    for db_file in main_files[keep_count:]:
        try:
            # Ta bort huvudfil
            db_file.unlink()
            removed.append(db_file)

            # Ta bort relaterade filer (WAL, etc)
            for related in DATA_DIR.glob(f"{db_file.stem}*"):
                related.unlink()
                removed.append(related)
        except OSError:
            pass  # Filen kan vara låst

    # Ta även bort den gamla warehouse.duckdb om den finns
    old_db = DATA_DIR / "warehouse.duckdb"
    if old_db.exists():
        try:
            old_db.unlink()
            removed.append(old_db)
            # Relaterade filer
            for related in DATA_DIR.glob("warehouse.duckdb*"):
                related.unlink()
                removed.append(related)
        except OSError:
            pass

    return removed


def init_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Initiera databas med extensions och scheman."""
    for ext in ["spatial", "parquet", "httpfs", "json"]:
        try:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
        except Exception:
            pass

    for schema in ["raw", "staging", "mart"]:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def list_databases() -> list[tuple[Path, str, int]]:
    """Lista alla databasfiler med info.

    Returns:
        Lista med tuples: (sökväg, tidsstämpel, storlek i MB)
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_files = sorted(DATA_DIR.glob("warehouse_*.duckdb"), reverse=True)

    result = []
    for f in db_files:
        if f.suffix == ".duckdb":
            size_mb = f.stat().st_size // (1024 * 1024)
            # Extrahera tidsstämpel från filnamn
            timestamp_str = f.stem.replace("warehouse_", "")
            result.append((f, timestamp_str, size_mb))

    return result
