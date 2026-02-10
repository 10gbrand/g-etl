"""H3-baserade polygon-queries för snabb spatial analys.

OBS: Kräver att warehouse.duckdb har initierats med H3-makron från 003_db_makros.sql.
Kör 'task run' en gång för att skapa databas med alla nödvändiga makron.
"""

from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd


def query_polygon(
    polygon_wkt: str,
    resolution: int = 8,
    datasets: list[str] | None = None,
    aggregation: Literal["objects", "stats", "heatmap"] = "objects",
    db_path: str = "data/warehouse.duckdb",
) -> pd.DataFrame:
    """
    Query geodata med en polygon, returnerar överlappande objekt.

    Args:
        polygon_wkt: Polygon i WKT-format (SWEREF99 TM, EPSG:3006)
        resolution: H3-resolution (6-10, default: 8)
        datasets: Lista med dataset-IDs att inkludera (None = alla)
        aggregation: Output-typ:
            - "objects": Individuella objekt
            - "stats": Aggregerad statistik per dataset
            - "heatmap": H3-celler med counts (för visualisering)
        db_path: Sökväg till DuckDB warehouse

    Returns:
        Pandas DataFrame med resultat

    Raises:
        FileNotFoundError: Om warehouse inte finns (kör 'task run' först)
        CatalogError: Om g_h3_query_cells makro saknas (kör 'task run' först)

    Example:
        >>> from g_etl.h3_query import query_polygon
        >>>
        >>> # Rita polygon i Stockholm
        >>> polygon = '''POLYGON((
        ...     674000 6580000,
        ...     676000 6580000,
        ...     676000 6582000,
        ...     674000 6582000,
        ...     674000 6580000
        ... ))'''
        >>>
        >>> # Hitta alla naturreservat
        >>> results = query_polygon(
        ...     polygon,
        ...     datasets=['naturreservat'],
        ...     aggregation='objects'
        ... )
        >>> print(results[['dataset_id', 'source_id', 'data_1']])
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(
            f"Warehouse finns inte: {db_path}. "
            "Kör 'task run' först för att skapa databas med H3-makron."
        )

    conn = duckdb.connect(db_path, read_only=True)

    # Bygg query baserat på aggregation-typ
    if aggregation == "objects":
        query = _build_objects_query(polygon_wkt, resolution, datasets)
    elif aggregation == "stats":
        query = _build_stats_query(polygon_wkt, resolution, datasets)
    elif aggregation == "heatmap":
        query = _build_heatmap_query(polygon_wkt, resolution, datasets)
    else:
        raise ValueError(f"Okänd aggregation: {aggregation}")

    # Kör query
    try:
        result = conn.execute(query).df()
    except Exception as e:
        if "g_h3_query_cells does not exist" in str(e):
            raise RuntimeError(
                "H3-makron saknas i databasen. Kör 'task run' för att initiera warehouse.duckdb"
            ) from e
        raise
    finally:
        conn.close()

    return result


def _build_objects_query(
    polygon_wkt: str, resolution: int, datasets: list[str] | None
) -> str:
    """Bygg query för individuella objekt (använder g_h3_query_cells från 003_db_makros.sql)."""
    dataset_filter = ""
    if datasets:
        dataset_list = ", ".join(f"'{d}'" for d in datasets)
        dataset_filter = f"AND h.dataset_id IN ({dataset_list})"

    return f"""
    WITH query_h3 AS (
        SELECT UNNEST(g_h3_query_cells('{polygon_wkt}', {resolution})) AS h3_cell
    )
    SELECT DISTINCT
        h.id,
        h.dataset_id,
        h.klass,
        h.leverantor,
        h.h3_cell
    FROM mart.h3_index h
    INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
    WHERE 1=1 {dataset_filter}
    ORDER BY h.dataset_id, h.id
    """


def _build_stats_query(
    polygon_wkt: str, resolution: int, datasets: list[str] | None
) -> str:
    """Bygg query för aggregerad statistik per dataset."""
    dataset_filter = ""
    if datasets:
        dataset_list = ", ".join(f"'{d}'" for d in datasets)
        dataset_filter = f"AND h.dataset_id IN ({dataset_list})"

    return f"""
    WITH query_h3 AS (
        SELECT UNNEST(g_h3_query_cells('{polygon_wkt}', {resolution})) AS h3_cell
    )
    SELECT
        h.dataset_id,
        h.klass,
        h.leverantor,
        COUNT(DISTINCT h.id) AS object_count,
        COUNT(DISTINCT h.h3_cell) AS h3_cell_count
    FROM mart.h3_index h
    INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
    WHERE 1=1 {dataset_filter}
    GROUP BY h.dataset_id, h.klass, h.leverantor
    ORDER BY object_count DESC
    """


def _build_heatmap_query(
    polygon_wkt: str, resolution: int, datasets: list[str] | None
) -> str:
    """Bygg query för H3-heatmap."""
    dataset_filter = ""
    if datasets:
        dataset_list = ", ".join(f"'{d}'" for d in datasets)
        dataset_filter = f"WHERE dataset_id IN ({dataset_list})"

    return f"""
    WITH query_h3 AS (
        SELECT UNNEST(g_h3_query_cells('{polygon_wkt}', {resolution})) AS h3_cell
    ),
    filtered_index AS (
        SELECT * FROM mart.h3_index
        {dataset_filter}
    )
    SELECT
        h.h3_cell,
        COUNT(DISTINCT h.id) AS object_count,
        COUNT(DISTINCT h.dataset_id) AS dataset_count,
        LIST(DISTINCT h.dataset_id ORDER BY h.dataset_id) AS datasets
    FROM filtered_index h
    INNER JOIN query_h3 q ON h.h3_cell = q.h3_cell
    GROUP BY h.h3_cell
    ORDER BY object_count DESC
    """


if __name__ == "__main__":
    import sys

    # CLI för snabb testning
    if len(sys.argv) < 2:
        print("Användning: python -m g_etl.h3_query <polygon_wkt>")
        print(
            "Exempel: python -m g_etl.h3_query "
            "'POLYGON((674000 6580000, 676000 6580000, "
            "676000 6582000, 674000 6582000, 674000 6580000))'"
        )
        sys.exit(1)

    polygon = sys.argv[1]
    res = 8
    agg = "stats"

    if len(sys.argv) > 2:
        res = int(sys.argv[2])
    if len(sys.argv) > 3:
        agg = sys.argv[3]

    print(f"🗺️  H3 Polygon Query")
    print(f"Resolution: {res}, Aggregation: {agg}\n")

    df = query_polygon(polygon, resolution=res, aggregation=agg)

    print(f"Hittade {len(df)} resultat:\n")
    print(df.to_string())
