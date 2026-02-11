"""Tester för heatmap-widgeten och delad laddningslogik."""

import pytest

from g_etl.admin.widgets.heatmap import (
    MatplotlibMapWidget,
    _check_deps,
    is_heatmap_available,
)


class TestDependencyCheck:
    """Tester för dependency-kontroll."""

    def test_check_deps_returns_booleans(self):
        """Dependency-kontroll returnerar tre boolean-värden."""
        has_mpl, has_ctx, has_pixels = _check_deps()
        assert isinstance(has_mpl, bool)
        assert isinstance(has_ctx, bool)
        assert isinstance(has_pixels, bool)

    def test_is_heatmap_available(self):
        """is_heatmap_available() returnerar bool."""
        result = is_heatmap_available()
        assert isinstance(result, bool)

    def test_deps_available_in_test_env(self):
        """I testmiljön bör viz-dependencies vara installerade."""
        has_mpl, has_ctx, has_pixels = _check_deps()
        assert has_mpl, "matplotlib bör vara installerat"
        assert has_ctx, "contextily bör vara installerat"
        assert has_pixels, "rich-pixels bör vara installerat"


class TestLoadCentroidsFromQuery:
    """Tester för den extraherade load_centroids_from_query."""

    def test_load_from_nonexistent_table(self, duckdb_conn):
        """Returnerar tom lista om tabellen inte finns."""
        from g_etl.admin.widgets.ascii_map import load_centroids_from_query

        points = load_centroids_from_query(duckdb_conn, "raw", "nonexistent_table")
        assert points == []

    def test_load_from_sweref_table(self, duckdb_conn):
        """Laddar punkter från tabell med SWEREF99 TM-koordinater."""
        from g_etl.admin.widgets.ascii_map import load_centroids_from_query

        # Skapa en enkel tabell med punkter i SWEREF99 TM
        duckdb_conn.execute("""
            CREATE TABLE raw.test_points AS
            SELECT ST_Point(674000 + i * 100, 6580000 + j * 100) as geometry
            FROM range(10) t(i), range(10) s(j)
        """)

        points = load_centroids_from_query(duckdb_conn, "raw", "test_points")
        assert len(points) == 100
        # Alla punkter bör vara i SWEREF99 TM (stora koordinater)
        for x, y in points:
            assert x > 100000  # SWEREF99 TM x
            assert y > 1000000  # SWEREF99 TM y

    def test_load_with_sample_size(self, duckdb_conn):
        """Respekterar sample_size-parameter."""
        from g_etl.admin.widgets.ascii_map import load_centroids_from_query

        duckdb_conn.execute("""
            CREATE TABLE raw.many_points AS
            SELECT ST_Point(674000 + i * 10, 6580000 + j * 10) as geometry
            FROM range(100) t(i), range(100) s(j)
        """)

        points = load_centroids_from_query(duckdb_conn, "raw", "many_points", sample_size=50)
        assert len(points) <= 50


@pytest.mark.skipif(
    not is_heatmap_available(),
    reason="Heatmap dependencies saknas",
)
class TestMatplotlibMapWidget:
    """Tester för MatplotlibMapWidget."""

    def test_create_widget(self):
        """Widgeten kan skapas utan data."""
        widget = MatplotlibMapWidget(width=40, height=15)
        assert widget.map_width == 40
        assert widget.map_height == 15
        assert not widget._loaded

    def test_load_points_renders(self):
        """Laddning av punkter triggrar rendering."""
        widget = MatplotlibMapWidget(width=40, height=15, show_basemap=False)
        # Stockholm-området i SWEREF99 TM
        test_points = [(674000 + i * 100, 6580000 + j * 100) for i in range(20) for j in range(20)]
        widget.load_points(test_points)

        assert widget._loaded
        assert widget._rendered_pixels is not None
        assert widget._render_error is None

    def test_load_empty_points(self):
        """Tom punktlista ger ingen rendering."""
        widget = MatplotlibMapWidget(width=40, height=15, show_basemap=False)
        widget.load_points([])

        assert widget._loaded
        assert widget._rendered_pixels is None

    def test_stats_calculated(self):
        """Statistik beräknas korrekt."""
        widget = MatplotlibMapWidget(width=40, height=15, show_basemap=False)
        test_points = [(500000, 6600000), (600000, 6700000)]
        widget.load_points(test_points)

        assert widget.stats.total_points == 2
        assert widget.stats.within_bbox == 2
        assert widget.stats.outside_bbox == 0
        assert widget.stats.coverage_percent == 100.0

    def test_custom_colormap(self):
        """Anpassad colormap fungerar."""
        widget = MatplotlibMapWidget(width=40, height=15, show_basemap=False, colormap="viridis")
        test_points = [(500000 + i * 1000, 6600000) for i in range(50)]
        widget.load_points(test_points)

        assert widget._loaded
        assert widget._rendered_pixels is not None

    def test_render_output_is_text(self):
        """render() returnerar Rich Text."""
        from rich.text import Text

        widget = MatplotlibMapWidget(width=40, height=15, show_basemap=False)
        widget.load_points([(500000, 6600000)])

        result = widget.render()
        assert isinstance(result, Text)

    def test_render_no_data(self):
        """render() utan data visar korrekt meddelande."""
        from rich.text import Text

        widget = MatplotlibMapWidget(width=40, height=15)
        widget._loaded = True

        result = widget.render()
        assert isinstance(result, Text)
        assert "Ingen data" in str(result)
