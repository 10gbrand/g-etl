"""H3-index processor för staging-tabeller.

SQL-filer i sql/staging/ skapar staging-tabeller med alla kolumner utom H3.
Denna processor fyller i _h3_index-kolumnen med H3-celler beräknade i Python.
"""

import asyncio
from collections.abc import Callable

import duckdb
import h3

from config.settings import settings


def add_h3_to_staging_table(
    conn: duckdb.DuckDBPyConnection,
    dataset_id: str,
    on_log: Callable[[str], None] | None = None,
) -> bool:
    """Lägg till H3-index i en befintlig staging-tabell.

    Förutsätter att staging.{dataset_id} redan finns med kolumnerna:
    - _centroid_lat
    - _centroid_lng
    - _h3_index (NULL-värden som ska fyllas i)

    Args:
        conn: DuckDB-anslutning
        dataset_id: Dataset-ID (tabellnamn i staging)
        on_log: Callback för loggmeddelanden

    Returns:
        True om framgångsrik
    """
    table = dataset_id.lower()

    # Kolla om staging-tabellen finns
    try:
        exists = conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'staging' AND table_name = '{table}'
        """).fetchone()[0]

        if not exists:
            if on_log:
                on_log(f"  staging.{table} finns inte, hoppar över H3")
            return False
    except Exception as e:
        if on_log:
            on_log(f"  Fel vid kontroll av staging.{table}: {e}")
        return False

    # Kolla om _h3_index-kolumnen finns
    try:
        has_h3_col = conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = 'staging' AND table_name = '{table}'
              AND column_name = '_h3_index'
        """).fetchone()[0]

        if not has_h3_col:
            if on_log:
                on_log(f"  staging.{table} saknar _h3_index-kolumn, hoppar över")
            return False
    except Exception as e:
        if on_log:
            on_log(f"  Fel vid kontroll av _h3_index: {e}")
        return False

    if on_log:
        on_log(f"  Beräknar H3-index för {table}...")

    # Hämta centroids
    try:
        result = conn.execute(f"""
            SELECT
                rowid,
                _centroid_lat,
                _centroid_lng
            FROM staging.{table}
            WHERE _centroid_lat IS NOT NULL AND _centroid_lng IS NOT NULL
        """).fetchall()
    except Exception as e:
        if on_log:
            on_log(f"  Fel vid hämtning av centroids: {e}")
        return False

    if not result:
        if on_log:
            on_log(f"  Inga koordinater att beräkna H3 för i {table}")
        return True

    # Beräkna H3-celler
    h3_updates = []
    for rowid, lat, lng in result:
        try:
            cell = h3.latlng_to_cell(lat, lng, settings.H3_RESOLUTION)
            h3_updates.append((cell, rowid))
        except Exception:
            # Ogiltig koordinat, hoppa över
            pass

    if not h3_updates:
        if on_log:
            on_log(f"  Inga giltiga H3-celler för {table}")
        return True

    # Uppdatera staging-tabellen med H3-index
    try:
        # DuckDB stödjer inte UPDATE med executemany, så vi använder en temp-tabell
        conn.execute("DROP TABLE IF EXISTS _tmp_h3_update")
        conn.execute("CREATE TEMP TABLE _tmp_h3_update (h3_cell VARCHAR, row_id BIGINT)")
        conn.executemany("INSERT INTO _tmp_h3_update VALUES (?, ?)", h3_updates)

        # Skapa ny tabell med H3-värden (DuckDB har begränsat UPDATE-stöd)
        conn.execute(f"""
            CREATE OR REPLACE TABLE staging.{table} AS
            SELECT
                t.*,
            FROM (
                SELECT
                    s.* EXCLUDE (_h3_index),
                    COALESCE(h.h3_cell, s._h3_index) AS _h3_index
                FROM staging.{table} s
                LEFT JOIN _tmp_h3_update h ON s.rowid = h.row_id
            ) t
        """)

        conn.execute("DROP TABLE IF EXISTS _tmp_h3_update")

        if on_log:
            on_log(f"  Lade till {len(h3_updates)} H3-index i staging.{table}")

        return True

    except Exception as e:
        if on_log:
            on_log(f"  Fel vid uppdatering av H3 i {table}: {e}")
        return False


async def add_h3_to_all_staging(
    conn: duckdb.DuckDBPyConnection,
    dataset_ids: list[str],
    on_log: Callable[[str], None] | None = None,
) -> tuple[int, int]:
    """Lägg till H3-index i alla staging-tabeller.

    Args:
        conn: DuckDB-anslutning
        dataset_ids: Lista med dataset-ID:n
        on_log: Callback för loggmeddelanden

    Returns:
        Tuple med (antal lyckade, antal misslyckade)
    """
    success_count = 0
    fail_count = 0

    for dataset_id in dataset_ids:
        loop = asyncio.get_event_loop()

        def do_add_h3(ds_id=dataset_id):
            return add_h3_to_staging_table(conn, ds_id, on_log)

        result = await loop.run_in_executor(None, do_add_h3)

        if result:
            success_count += 1
        else:
            fail_count += 1

    return success_count, fail_count


# Bakåtkompatibilitet - alias för den gamla funktionen
async def process_all_to_staging(
    conn: duckdb.DuckDBPyConnection,
    dataset_ids: list[str],
    on_log: Callable[[str], None] | None = None,
) -> tuple[int, int]:
    """Alias för add_h3_to_all_staging (bakåtkompatibilitet)."""
    return await add_h3_to_all_staging(conn, dataset_ids, on_log)
