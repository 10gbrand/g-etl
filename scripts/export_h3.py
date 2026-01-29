"""Exportera H3-data för visualisering.

Användning:
    uv run python scripts/export_h3.py --format csv
    uv run python scripts/export_h3.py --format geojson --limit 10000
    uv run python scripts/export_h3.py --format html
"""

import argparse
from pathlib import Path

import duckdb


def export_csv(conn: duckdb.DuckDBPyConnection, output_path: Path, limit: int | None = None):
    """Exportera till CSV för Kepler.gl."""
    limit_clause = f"LIMIT {limit}" if limit else ""

    sql = f"""
        COPY (
            SELECT
                h3_cell,
                dataset,
                leverantor,
                klass,
                classification,
                COUNT(*) as count
            FROM mart.h3_cells
            GROUP BY h3_cell, dataset, leverantor, klass, classification
            ORDER BY count DESC
            {limit_clause}
        ) TO '{output_path}' (HEADER, DELIMITER ',')
    """
    conn.execute(sql)

    # Räkna rader
    count = conn.execute(f"SELECT COUNT(DISTINCT h3_cell) FROM mart.h3_cells").fetchone()[0]
    print(f"Exporterade {count} unika H3-celler till {output_path}")
    print(f"\nÖppna https://kepler.gl/demo och ladda upp filen.")
    print("Kepler.gl känner igen 'h3_cell'-kolumnen automatiskt som H3-index.")


def export_geojson(conn: duckdb.DuckDBPyConnection, output_path: Path, limit: int | None = None):
    """Exportera till GeoJSON med H3-polygoner."""
    limit_clause = f"LIMIT {limit}" if limit else ""

    # Skapa temporär tabell med geometri
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE h3_export AS
        SELECT
            h3_cell,
            dataset,
            leverantor,
            klass,
            classification,
            COUNT(*) as count,
            ST_GeomFromText(h3_cell_to_boundary_wkt(h3_cell)) as geom
        FROM mart.h3_cells
        GROUP BY h3_cell, dataset, leverantor, klass, classification
        ORDER BY count DESC
        {limit_clause}
    """)

    # Exportera till GeoJSON
    conn.execute(f"""
        COPY h3_export TO '{output_path}'
        WITH (FORMAT GDAL, DRIVER 'GeoJSON')
    """)

    count = conn.execute("SELECT COUNT(*) FROM h3_export").fetchone()[0]
    print(f"Exporterade {count} H3-polygoner till {output_path}")
    print(f"\nÖppna i QGIS eller på https://geojson.io")


def export_html(conn: duckdb.DuckDBPyConnection, output_path: Path, limit: int | None = None):
    """Exportera till interaktiv HTML-karta med Folium."""
    try:
        import folium
        from h3 import cell_to_boundary
    except ImportError:
        print("Kräver: uv pip install folium h3")
        return

    limit_clause = f"LIMIT {limit}" if limit else "LIMIT 5000"

    # Hämta data
    df = conn.execute(f"""
        SELECT
            h3_cell,
            dataset,
            leverantor,
            klass,
            classification,
            COUNT(*) as count
        FROM mart.h3_cells
        GROUP BY h3_cell, dataset, leverantor, klass, classification
        ORDER BY count DESC
        {limit_clause}
    """).df()

    if df.empty:
        print("Ingen data att exportera")
        return

    # Skapa karta centrerad på Sverige
    m = folium.Map(location=[63, 17], zoom_start=5, tiles="CartoDB positron")

    # Färgpalett per dataset
    colors = [
        '#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00',
        '#ffff33', '#a65628', '#f781bf', '#999999', '#66c2a5'
    ]
    datasets = df['dataset'].unique()
    color_map = {ds: colors[i % len(colors)] for i, ds in enumerate(datasets)}

    # Lägg till hexagoner
    for _, row in df.iterrows():
        try:
            # h3 v4 använder cell_to_boundary
            boundary = cell_to_boundary(row['h3_cell'])
            # Konvertera till lat/lng format för folium
            coords = [[lat, lng] for lat, lng in boundary]

            popup_html = f"""
                <b>{row['dataset']}</b><br>
                Leverantör: {row['leverantor']}<br>
                Klass: {row['klass']}<br>
                Klassificering: {row['classification']}<br>
                Antal: {row['count']}
            """

            folium.Polygon(
                locations=coords,
                popup=folium.Popup(popup_html, max_width=300),
                color=color_map[row['dataset']],
                weight=1,
                fill=True,
                fill_color=color_map[row['dataset']],
                fill_opacity=0.5
            ).add_to(m)
        except Exception as e:
            continue  # Hoppa över ogiltiga celler

    # Lägg till legend
    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid grey; font-size: 12px;">
    <b>Dataset</b><br>
    """
    for ds in datasets[:10]:  # Max 10 i legend
        legend_html += f'<i style="background:{color_map[ds]}; width:12px; height:12px; display:inline-block; margin-right:5px;"></i>{ds}<br>'
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(str(output_path))
    print(f"Exporterade {len(df)} H3-celler till {output_path}")
    print(f"\nÖppna filen i en webbläsare för att visa kartan.")


def export_parquet(conn: duckdb.DuckDBPyConnection, output_path: Path, limit: int | None = None):
    """Exportera till GeoParquet med H3-polygoner."""
    limit_clause = f"LIMIT {limit}" if limit else ""

    conn.execute(f"""
        COPY (
            SELECT
                h3_cell,
                dataset,
                leverantor,
                klass,
                classification,
                COUNT(*) as count,
                ST_GeomFromText(h3_cell_to_boundary_wkt(h3_cell)) as geometry
            FROM mart.h3_cells
            GROUP BY h3_cell, dataset, leverantor, klass, classification
            ORDER BY count DESC
            {limit_clause}
        ) TO '{output_path}' (FORMAT PARQUET)
    """)

    print(f"Exporterade till {output_path}")
    print("Kan öppnas i QGIS, GeoPandas, eller DuckDB.")


def main():
    parser = argparse.ArgumentParser(description="Exportera H3-data för visualisering")
    parser.add_argument(
        "--format", "-f",
        choices=["csv", "geojson", "html", "parquet"],
        default="csv",
        help="Exportformat (default: csv)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output-fil (default: data/h3_export.<format>)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Begränsa antal rader (default: alla)"
    )
    parser.add_argument(
        "--db", "-d",
        type=Path,
        default=Path("data/warehouse.duckdb"),
        help="DuckDB-databas (default: data/warehouse.duckdb)"
    )

    args = parser.parse_args()

    # Bestäm output-fil
    if args.output:
        output_path = args.output
    else:
        extensions = {"csv": ".csv", "geojson": ".geojson", "html": ".html", "parquet": ".parquet"}
        output_path = Path("data") / f"h3_export{extensions[args.format]}"

    # Säkerställ att output-mappen finns
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Anslut till databas
    if not args.db.exists():
        print(f"Databasen {args.db} finns inte. Kör pipelinen först.")
        return

    conn = duckdb.connect(str(args.db))

    # Ladda H3 extension
    conn.execute("LOAD h3")

    # Kolla att mart.h3_cells finns
    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'mart' AND table_name = 'h3_cells'
    """).fetchall()

    if not tables:
        print("Tabellen mart.h3_cells finns inte. Kör mart-fasen först.")
        return

    # Exportera
    if args.format == "csv":
        export_csv(conn, output_path, args.limit)
    elif args.format == "geojson":
        export_geojson(conn, output_path, args.limit)
    elif args.format == "html":
        export_html(conn, output_path, args.limit)
    elif args.format == "parquet":
        export_parquet(conn, output_path, args.limit)

    conn.close()


if __name__ == "__main__":
    main()
