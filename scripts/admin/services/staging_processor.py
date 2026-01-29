"""H3-index processor för staging-tabeller.

SQL-filer i sql/staging/ skapar staging-tabeller med alla kolumner utom H3.
Denna processor fyller i _h3_index-kolumnen med H3-celler beräknade i Python.
Dessutom beräknas _h3_cells som innehåller alla H3-celler inom polygonen.
"""

import asyncio
import json
from collections.abc import Callable

import duckdb
import h3

from config.settings import settings


def _geometry_to_h3_cells(geojson_str: str, resolution: int) -> list[str]:
    """Konvertera en GeoJSON-geometri till lista av H3-celler.

    Returnerar alla H3-celler vars centrum ligger inom polygonen.

    Args:
        geojson_str: GeoJSON-sträng för geometrin
        resolution: H3-upplösning

    Returns:
        Lista med H3-cell-ID:n, tom lista om fel
    """
    try:
        geojson = json.loads(geojson_str)
        geom_type = geojson.get("type", "")

        if geom_type == "Polygon":
            # Extrahera koordinater - GeoJSON har [lng, lat] format
            coords = geojson.get("coordinates", [])
            if not coords:
                return []
            # Konvertera till (lat, lng) för h3 (h3 v4 använder LatLngPoly)
            outer_ring = [(lat, lng) for lng, lat in coords[0]]
            holes = [[(lat, lng) for lng, lat in hole] for hole in coords[1:]]
            polygon = h3.LatLngPoly(outer_ring, *holes)
            cells = h3.polygon_to_cells(polygon, resolution)
            return list(cells)

        elif geom_type == "MultiPolygon":
            all_cells = set()
            for poly_coords in geojson.get("coordinates", []):
                if not poly_coords:
                    continue
                outer_ring = [(lat, lng) for lng, lat in poly_coords[0]]
                holes = [[(lat, lng) for lng, lat in hole] for hole in poly_coords[1:]]
                polygon = h3.LatLngPoly(outer_ring, *holes)
                cells = h3.polygon_to_cells(polygon, resolution)
                all_cells.update(cells)
            return list(all_cells)

        else:
            # Punkter och linjer ger inga polyfill-celler
            return []

    except Exception:
        return []


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
    - _h3_cells (NULL-värden som ska fyllas i med polyfill)
    - geom (geometri för polyfill)

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

    # Kolla vilka H3-kolumner som finns
    try:
        h3_columns = conn.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'staging' AND table_name = '{table}'
              AND column_name IN ('_h3_index', '_h3_cells')
        """).fetchall()
        h3_columns = {row[0] for row in h3_columns}

        if not h3_columns:
            if on_log:
                on_log(f"  staging.{table} saknar H3-kolumner, hoppar över")
            return False
    except Exception as e:
        if on_log:
            on_log(f"  Fel vid kontroll av H3-kolumner: {e}")
        return False

    if on_log:
        on_log(f"  Beräknar H3-index för {table}...")

    # Hämta data för H3-beräkning (centroid + geometri)
    try:
        # Transformera geometrin till WGS84 för GeoJSON export
        result = conn.execute(f"""
            SELECT
                rowid,
                _centroid_lat,
                _centroid_lng,
                ST_AsGeoJSON(ST_Transform(geom, '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', '+proj=longlat +datum=WGS84 +no_defs')) AS geojson
            FROM staging.{table}
            WHERE geom IS NOT NULL
        """).fetchall()
    except Exception as e:
        if on_log:
            on_log(f"  Fel vid hämtning av geometrier: {e}")
        return False

    if not result:
        if on_log:
            on_log(f"  Inga geometrier att beräkna H3 för i {table}")
        return True

    # Beräkna H3-celler (centroid och polyfill)
    h3_updates = []
    total_cells = 0
    for rowid, lat, lng, geojson in result:
        # Centroid H3
        h3_cell = None
        if lat is not None and lng is not None:
            try:
                h3_cell = h3.latlng_to_cell(lat, lng, settings.H3_RESOLUTION)
            except Exception:
                pass

        # Polyfill H3 (alla celler inom polygonen)
        h3_cells_json = None
        if geojson and '_h3_cells' in h3_columns:
            cells = _geometry_to_h3_cells(geojson, settings.H3_POLYFILL_RESOLUTION)
            if cells:
                h3_cells_json = json.dumps(cells)
                total_cells += len(cells)

        h3_updates.append((h3_cell, h3_cells_json, rowid))

    if not h3_updates:
        if on_log:
            on_log(f"  Inga giltiga H3-beräkningar för {table}")
        return True

    # Uppdatera staging-tabellen med H3-värden
    try:
        # DuckDB stödjer inte UPDATE med executemany, så vi använder en temp-tabell
        conn.execute("DROP TABLE IF EXISTS _tmp_h3_update")
        conn.execute("""
            CREATE TEMP TABLE _tmp_h3_update (
                h3_cell VARCHAR,
                h3_cells VARCHAR,
                row_id BIGINT
            )
        """)
        conn.executemany(
            "INSERT INTO _tmp_h3_update VALUES (?, ?, ?)",
            h3_updates
        )

        # Bygg EXCLUDE och SELECT baserat på vilka kolumner som finns
        exclude_cols = ["_h3_index", "_h3_cells"]
        exclude_str = ", ".join(f"_{c.lstrip('_')}" for c in exclude_cols if c.lstrip('_') in [col.lstrip('_') for col in h3_columns])
        exclude_str = ", ".join(c for c in exclude_cols if c in h3_columns)

        select_parts = []
        if '_h3_index' in h3_columns:
            select_parts.append("COALESCE(h.h3_cell, s._h3_index) AS _h3_index")
        if '_h3_cells' in h3_columns:
            select_parts.append("COALESCE(h.h3_cells, s._h3_cells) AS _h3_cells")

        # Skapa ny tabell med H3-värden
        conn.execute(f"""
            CREATE OR REPLACE TABLE staging.{table} AS
            SELECT
                s.* EXCLUDE ({exclude_str}),
                {', '.join(select_parts)}
            FROM staging.{table} s
            LEFT JOIN _tmp_h3_update h ON s.rowid = h.row_id
        """)

        conn.execute("DROP TABLE IF EXISTS _tmp_h3_update")

        if on_log:
            on_log(f"  H3: {len(h3_updates)} centroids, {total_cells} polyfill-celler i staging.{table}")

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
