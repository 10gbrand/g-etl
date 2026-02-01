"""Database management with session-based DuckDB files."""

from datetime import datetime
from pathlib import Path

import duckdb

from config.settings import settings


def get_session_db_path() -> Path:
    """Generera en ny databas-sökväg med tidsstämpel.

    Returnerar sökväg som: data/warehouse_20260128_103045.duckdb
    """
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.DATA_DIR / f"{settings.DB_PREFIX}_{timestamp}{settings.DB_EXTENSION}"


def get_latest_db_path() -> Path | None:
    """Hämta senaste databasfilen baserat på tidsstämpel.

    Returnerar None om ingen databas finns.
    """
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    pattern = f"{settings.DB_PREFIX}_*{settings.DB_EXTENSION}"
    db_files = sorted(settings.DATA_DIR.glob(pattern), reverse=True)

    # Filtrera bort WAL och andra temporära filer
    db_files = [f for f in db_files if not f.suffix.endswith(".wal")]

    return db_files[0] if db_files else None


def get_current_db_path() -> Path:
    """Hämta sökväg till den fasta databasfilen (warehouse.duckdb).

    Denna fil pekar alltid på senaste körningen.
    """
    return settings.DATA_DIR / f"{settings.DB_PREFIX}{settings.DB_EXTENSION}"


def update_current_db(source_path: Path | None = None) -> Path | None:
    """Uppdatera den fasta databasfilen med senaste körningen.

    Kopierar den senaste tidsstämplade databasen till warehouse.duckdb.

    Args:
        source_path: Sökväg till källdatabasen. Om None används senaste.

    Returns:
        Sökväg till den uppdaterade fasta databasen, eller None om ingen källa finns.
    """
    import shutil

    if source_path is None:
        source_path = get_latest_db_path()

    if source_path is None or not source_path.exists():
        return None

    current_db = get_current_db_path()

    # Ta bort befintlig fil och relaterade filer (WAL etc)
    if current_db.exists():
        try:
            current_db.unlink()
        except OSError:
            pass

    for related in settings.DATA_DIR.glob(f"{settings.DB_PREFIX}{settings.DB_EXTENSION}*"):
        try:
            related.unlink()
        except OSError:
            pass

    # Kopiera den nya databasen
    try:
        shutil.copy2(source_path, current_db)
        return current_db
    except OSError:
        return None


def cleanup_old_databases(keep_count: int | None = None) -> list[Path]:
    """Ta bort gamla databasfiler, behåll de senaste.

    Args:
        keep_count: Antal databasfiler att behålla (default från settings)

    Returns:
        Lista med borttagna filer
    """
    if keep_count is None:
        keep_count = settings.DB_KEEP_COUNT

    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Hitta alla databasfiler
    pattern = f"{settings.DB_PREFIX}_*{settings.DB_EXTENSION}"
    db_files = sorted(settings.DATA_DIR.glob(pattern), reverse=True)

    # Filtrera ut huvudfiler (inte WAL etc)
    main_files = [f for f in db_files if f.suffix == settings.DB_EXTENSION]

    removed: list[Path] = []

    # Ta bort allt utom de senaste
    for db_file in main_files[keep_count:]:
        try:
            # Ta bort huvudfil
            db_file.unlink()
            removed.append(db_file)

            # Ta bort relaterade filer (WAL, etc)
            for related in settings.DATA_DIR.glob(f"{db_file.stem}*"):
                related.unlink()
                removed.append(related)
        except OSError:
            pass  # Filen kan vara låst

    # OBS: Vi behåller warehouse.duckdb (den fasta "current" databasen)
    # Den uppdateras av update_current_db() efter varje körning

    return removed


def init_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Initiera databas med extensions och scheman."""
    for ext in settings.DUCKDB_EXTENSIONS:
        try:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
        except Exception:
            pass

    for schema in settings.DUCKDB_SCHEMAS:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def list_databases() -> list[tuple[Path, str, int]]:
    """Lista alla databasfiler med info.

    Returns:
        Lista med tuples: (sökväg, tidsstämpel, storlek i MB)
    """
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    pattern = f"{settings.DB_PREFIX}_*{settings.DB_EXTENSION}"
    db_files = sorted(settings.DATA_DIR.glob(pattern), reverse=True)

    result = []
    for f in db_files:
        if f.suffix == settings.DB_EXTENSION:
            size_mb = f.stat().st_size // (1024 * 1024)
            # Extrahera tidsstämpel från filnamn
            timestamp_str = f.stem.replace(f"{settings.DB_PREFIX}_", "")
            result.append((f, timestamp_str, size_mb))

    return result


def cleanup_all_databases() -> tuple[int, int]:
    """Ta bort ALLA databasfiler i data/.

    Returns:
        Tuple med (antal borttagna filer, total storlek i MB)
    """
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)

    removed_count = 0
    total_size = 0

    # Ta bort alla .duckdb-filer och relaterade filer
    for db_file in settings.DATA_DIR.glob(f"*{settings.DB_EXTENSION}*"):
        try:
            total_size += db_file.stat().st_size
            db_file.unlink()
            removed_count += 1
        except OSError:
            pass  # Filen kan vara låst

    return removed_count, total_size // (1024 * 1024)


def cleanup_all_parquet() -> tuple[int, int]:
    """Ta bort ALLA parquet-filer i data/raw/.

    Returns:
        Tuple med (antal borttagna filer, total storlek i MB)
    """
    settings.RAW_DIR.mkdir(parents=True, exist_ok=True)

    removed_count = 0
    total_size = 0

    for parquet_file in settings.RAW_DIR.glob("*.parquet"):
        try:
            total_size += parquet_file.stat().st_size
            parquet_file.unlink()
            removed_count += 1
        except OSError:
            pass

    return removed_count, total_size // (1024 * 1024)


def cleanup_all_logs() -> tuple[int, int]:
    """Ta bort ALLA loggfiler i logs/.

    Returns:
        Tuple med (antal borttagna filer, total storlek i MB)
    """
    settings.LOGS_DIR.mkdir(parents=True, exist_ok=True)

    removed_count = 0
    total_size = 0

    for log_file in settings.LOGS_DIR.glob("*.log"):
        try:
            total_size += log_file.stat().st_size
            log_file.unlink()
            removed_count += 1
        except OSError:
            pass

    return removed_count, total_size // (1024 * 1024)


def get_data_stats() -> dict:
    """Hämta statistik om data-filer.

    Returns:
        Dict med antal och storlek för databaser, parquet-filer och loggar
    """
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.RAW_DIR.mkdir(parents=True, exist_ok=True)
    settings.LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Databaser
    db_files = list(settings.DATA_DIR.glob(f"*{settings.DB_EXTENSION}"))
    db_count = len(db_files)
    db_size = sum(f.stat().st_size for f in db_files) // (1024 * 1024)

    # Parquet-filer
    parquet_files = list(settings.RAW_DIR.glob("*.parquet"))
    parquet_count = len(parquet_files)
    parquet_size = sum(f.stat().st_size for f in parquet_files) // (1024 * 1024)

    # Loggfiler
    log_files = list(settings.LOGS_DIR.glob("*.log"))
    log_count = len(log_files)
    log_size = sum(f.stat().st_size for f in log_files) // (1024 * 1024)

    return {
        "db_count": db_count,
        "db_size_mb": db_size,
        "parquet_count": parquet_count,
        "parquet_size_mb": parquet_size,
        "log_count": log_count,
        "log_size_mb": log_size,
    }
