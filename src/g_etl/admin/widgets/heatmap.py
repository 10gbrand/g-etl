"""Matplotlib-baserad heatmap-widget för terminalen.

Renderar en heatmap med valfri bakgrundskarta (contextily) som en
terminalvänlig bild via rich-pixels halfblock-rendering.

Kräver optional dependencies: matplotlib, Pillow, rich-pixels
Optional: contextily (för bakgrundskarta)
"""

from __future__ import annotations

import io
import logging

import duckdb
from rich.text import Text
from textual.widgets import Static

from g_etl.admin.widgets.ascii_map import SWEDEN_BBOX, MapStats, load_centroids_from_query

logger = logging.getLogger(__name__)

# Lazy import-flaggor (cacheade efter första kontroll)
_HAS_MATPLOTLIB: bool | None = None
_HAS_CONTEXTILY: bool | None = None
_HAS_RICH_PIXELS: bool | None = None


def _check_deps() -> tuple[bool, bool, bool]:
    """Kontrollera vilka viz-dependencies som finns tillgängliga."""
    global _HAS_MATPLOTLIB, _HAS_CONTEXTILY, _HAS_RICH_PIXELS

    if _HAS_MATPLOTLIB is None:
        try:
            import matplotlib  # noqa: F401
            from PIL import Image  # noqa: F401

            _HAS_MATPLOTLIB = True
        except ImportError:
            _HAS_MATPLOTLIB = False

    if _HAS_CONTEXTILY is None:
        try:
            import contextily  # noqa: F401

            _HAS_CONTEXTILY = True
        except ImportError:
            _HAS_CONTEXTILY = False

    if _HAS_RICH_PIXELS is None:
        try:
            from rich_pixels import Pixels  # noqa: F401

            _HAS_RICH_PIXELS = True
        except ImportError:
            _HAS_RICH_PIXELS = False

    return _HAS_MATPLOTLIB, _HAS_CONTEXTILY, _HAS_RICH_PIXELS


def is_heatmap_available() -> bool:
    """Kontrollera om heatmap-rendering är tillgänglig."""
    has_mpl, _, has_pixels = _check_deps()
    return bool(has_mpl and has_pixels)


class MatplotlibMapWidget(Static):
    """Heatmap-widget som renderar matplotlib-karta i terminalen.

    Använder matplotlib för att generera en heatmap med valfri
    bakgrundskarta (contextily) och visar resultatet via rich-pixels
    halfblock-rendering.

    Fallback: Om dependencies saknas, visas ett meddelande om att
    installera dem.
    """

    DEFAULT_CSS = """
    MatplotlibMapWidget {
        height: 1fr;
        min-height: 30;
        padding: 1;
        border: round $primary;
        overflow-y: auto;
    }
    """

    def __init__(
        self,
        width: int = 80,
        height: int = 25,
        title: str = "Heatmap",
        show_basemap: bool = True,
        colormap: str = "YlOrRd",
    ) -> None:
        """Skapa en matplotlib heatmap-widget.

        Args:
            width: Bredd i terminal-celler
            height: Höjd i terminal-celler
            title: Titel för kartan
            show_basemap: Om True, visa bakgrundskarta (kräver contextily)
            colormap: Matplotlib colormap-namn
        """
        super().__init__()
        self.map_width = width
        self.map_height = height
        self.title = title
        self.show_basemap = show_basemap
        self.colormap = colormap
        self.points: list[tuple[float, float]] = []
        self.stats = MapStats()
        self._loaded = False
        self._rendered_pixels = None  # Cacheat Pixels-objekt
        self._render_error: str | None = None

    def load_from_query(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
        geometry_column: str = "geometry",
        sample_size: int = 50000,
    ) -> None:
        """Ladda geometri-centroids från en tabell.

        Args:
            conn: DuckDB-anslutning
            schema: Databasschema
            table: Tabellnamn
            geometry_column: Namn på geometrikolumnen
            sample_size: Max antal punkter att ladda
        """
        self.points = load_centroids_from_query(conn, schema, table, geometry_column, sample_size)
        self._calculate_stats()
        self._render_heatmap()
        self._loaded = True
        self.refresh()

    def load_points(self, points: list[tuple[float, float]]) -> None:
        """Ladda punkter direkt.

        Args:
            points: Lista med (x, y) koordinater i SWEREF99 TM
        """
        self.points = points
        self._calculate_stats()
        self._render_heatmap()
        self._loaded = True
        self.refresh()

    def _calculate_stats(self) -> None:
        """Beräkna statistik för punkterna."""
        if not self.points:
            self.stats = MapStats()
            return

        within = 0
        outside = 0
        xs = []
        ys = []

        for x, y in self.points:
            xs.append(x)
            ys.append(y)

            if (
                SWEDEN_BBOX["min_x"] <= x <= SWEDEN_BBOX["max_x"]
                and SWEDEN_BBOX["min_y"] <= y <= SWEDEN_BBOX["max_y"]
            ):
                within += 1
            else:
                outside += 1

        self.stats = MapStats(
            total_points=len(self.points),
            within_bbox=within,
            outside_bbox=outside,
            data_min_x=min(xs),
            data_max_x=max(xs),
            data_min_y=min(ys),
            data_max_y=max(ys),
        )

    def _render_heatmap(self) -> None:
        """Rendera heatmap med matplotlib -> PIL -> rich-pixels."""
        has_mpl, has_ctx, has_pixels = _check_deps()

        if not has_mpl or not has_pixels:
            self._render_error = (
                "Heatmap kräver: uv sync --extra viz\n  (matplotlib, rich-pixels, Pillow)"
            )
            return

        if not self.points:
            self._rendered_pixels = None
            return

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        from PIL import Image
        from rich_pixels import Pixels

        xs = np.array([p[0] for p in self.points])
        ys = np.array([p[1] for p in self.points])

        # Beräkna figure-storlek
        # rich-pixels: varje terminal-cell = 1 kolumn bred, 2 pixlar hög (halfblock)
        # Vi renderar med mer upplösning och låter rich-pixels skala ner
        pixel_width = self.map_width * 8
        pixel_height = self.map_height * 16
        dpi = 100
        fig_width = pixel_width / dpi
        fig_height = pixel_height / dpi

        fig, ax = plt.subplots(1, 1, figsize=(fig_width, fig_height), dpi=dpi)

        # Lägg till bakgrundskarta först (under heatmap)
        if self.show_basemap and has_ctx:
            try:
                import contextily as cx

                cx.add_basemap(
                    ax,
                    crs="EPSG:3006",
                    source=cx.providers.CartoDB.Positron,
                    zoom="auto",
                    alpha=0.8,
                )
            except Exception as e:
                logger.warning(f"Kunde inte ladda bakgrundskarta: {e}")

        # Heatmap via hexbin (effektiv, hanterar stora dataset)
        ax.hexbin(
            xs,
            ys,
            gridsize=50,
            cmap=self.colormap,
            alpha=0.7,
            mincnt=1,
            zorder=2,
        )

        # Sätt extent till Sveriges bbox
        ax.set_xlim(SWEDEN_BBOX["min_x"], SWEDEN_BBOX["max_x"])
        ax.set_ylim(SWEDEN_BBOX["min_y"], SWEDEN_BBOX["max_y"])
        ax.set_aspect("equal")
        ax.axis("off")

        fig.tight_layout(pad=0)

        # Spara till PIL Image via BytesIO
        buf = io.BytesIO()
        fig.savefig(
            buf,
            format="png",
            bbox_inches="tight",
            pad_inches=0,
            facecolor="white",
        )
        plt.close(fig)
        buf.seek(0)

        pil_image = Image.open(buf)

        # Konvertera till rich-pixels halfblock
        # resize: (bredd i kolumner, höjd i rader * 2 för halfblock)
        self._rendered_pixels = Pixels.from_image(
            pil_image,
            resize=(self.map_width, self.map_height * 2),
        )
        self._render_error = None

    def render(self) -> Text:
        """Rendera widgeten."""
        text = Text()

        if not self._loaded:
            text.append("Laddar heatmap...", style="dim italic")
            return text

        if self._render_error:
            text.append(f"--- {self.title} ", style="bold cyan")
            text.append("---\n\n", style="dim")
            text.append(self._render_error, style="yellow")
            return text

        if self._rendered_pixels is not None:
            # Titel
            text.append(f"--- {self.title} ", style="bold cyan")
            text.append("---\n", style="dim")

            # Rendera heatmap-pixlarna som Text
            # rich-pixels Pixels-objekt kan renderas via Rich console
            # men vi behöver det som Text. Använd __rich_console__
            from rich.console import Console

            console = Console(width=self.map_width, no_color=False, force_terminal=True)
            with console.capture() as capture:
                console.print(self._rendered_pixels, end="")
            text.append(capture.get())

            # Statistik
            text.append("\n")
            if self.stats.total_points > 0:
                coverage = self.stats.coverage_percent
                if coverage >= 99:
                    icon, icon_style = "✓", "green bold"
                elif coverage >= 90:
                    icon, icon_style = "⚠", "yellow"
                else:
                    icon, icon_style = "✗", "red bold"

                text.append(f" {icon} ", style=icon_style)
                text.append(
                    f"Inom Sverige: {self.stats.within_bbox:,}/{self.stats.total_points:,}",
                    style="bold",
                )
                text.append(f" ({coverage:.1f}%)\n", style=icon_style)

            return text

        if not self.points:
            text.append("Ingen data att visa", style="dim italic")
            return text

        text.append("Renderingsfel", style="red")
        return text
