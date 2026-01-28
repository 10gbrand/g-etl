"""ASCII-karta widget för att visualisera geometrier i terminalen."""

from dataclasses import dataclass

import duckdb
from rich.text import Text
from textual.reactive import reactive
from textual.widgets import Static

# Sveriges bounding box i SWEREF99 TM (EPSG:3006)
SWEDEN_BBOX = {
    "min_x": 266000,
    "max_x": 921000,
    "min_y": 6132000,
    "max_y": 7680000,
}


@dataclass
class MapStats:
    """Statistik för kartvisningen."""

    total_points: int = 0
    within_bbox: int = 0
    outside_bbox: int = 0
    data_min_x: float | None = None
    data_max_x: float | None = None
    data_min_y: float | None = None
    data_max_y: float | None = None

    @property
    def coverage_percent(self) -> float:
        """Andel punkter inom Sveriges bbox."""
        if self.total_points == 0:
            return 0.0
        return (self.within_bbox / self.total_points) * 100


class AsciiMapWidget(Static):
    """Widget som renderar geometrier som ASCII-karta."""

    DEFAULT_CSS = """
    AsciiMapWidget {
        height: auto;
        min-height: 20;
        padding: 1;
        border: round $primary;
    }
    """

    map_width = reactive(50)
    map_height = reactive(18)

    def __init__(
        self,
        width: int = 50,
        height: int = 18,
        title: str = "Karta",
    ) -> None:
        super().__init__()
        self.map_width = width
        self.map_height = height
        self.title = title
        self.points: list[tuple[float, float]] = []
        self.stats = MapStats()
        self._loaded = False

    def load_from_query(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
        geometry_column: str = "geometry",
        sample_size: int = 10000,
    ) -> None:
        """Ladda geometri-centroids från en tabell.

        Args:
            conn: DuckDB-anslutning
            schema: Databasschema
            table: Tabellnamn
            geometry_column: Namn på geometrikolumnen
            sample_size: Max antal punkter att ladda (för prestanda)
        """
        # Kontrollera att tabellen finns
        check_query = f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
        result = conn.execute(check_query).fetchone()
        if not result or result[0] == 0:
            self.points = []
            self._loaded = True
            self.refresh()
            return

        # Hämta centroids med sampling för stora dataset
        query = f"""
            SELECT
                ST_X(ST_Centroid({geometry_column})) as x,
                ST_Y(ST_Centroid({geometry_column})) as y
            FROM {schema}.{table}
            WHERE {geometry_column} IS NOT NULL
            USING SAMPLE {sample_size}
        """
        try:
            rows = conn.execute(query).fetchall()
            self.points = [(float(x), float(y)) for x, y in rows if x is not None and y is not None]
        except Exception:
            # Fallback utan sampling om det inte stöds
            query_simple = f"""
                SELECT
                    ST_X(ST_Centroid({geometry_column})) as x,
                    ST_Y(ST_Centroid({geometry_column})) as y
                FROM {schema}.{table}
                WHERE {geometry_column} IS NOT NULL
                LIMIT {sample_size}
            """
            rows = conn.execute(query_simple).fetchall()
            self.points = [(float(x), float(y)) for x, y in rows if x is not None and y is not None]

        self._calculate_stats()
        self._loaded = True
        self.refresh()

    def load_points(self, points: list[tuple[float, float]]) -> None:
        """Ladda punkter direkt.

        Args:
            points: Lista med (x, y) koordinater i SWEREF99 TM
        """
        self.points = points
        self._calculate_stats()
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

    def _render_map(self) -> list[str]:
        """Rendera ASCII-kartan."""
        # Skapa tom grid
        grid = [[" " for _ in range(self.map_width)] for _ in range(self.map_height)]

        # Densitetskarta - räkna antal punkter per cell
        density: dict[tuple[int, int], int] = {}

        for x, y in self.points:
            # Normalisera till grid-koordinater baserat på Sveriges bbox
            nx = int(
                (x - SWEDEN_BBOX["min_x"])
                / (SWEDEN_BBOX["max_x"] - SWEDEN_BBOX["min_x"])
                * (self.map_width - 1)
            )
            ny = int(
                (y - SWEDEN_BBOX["min_y"])
                / (SWEDEN_BBOX["max_y"] - SWEDEN_BBOX["min_y"])
                * (self.map_height - 1)
            )
            ny = self.map_height - 1 - ny  # Flip Y-axis (norr uppåt)

            # Hantera punkter utanför bbox
            if 0 <= nx < self.map_width and 0 <= ny < self.map_height:
                key = (nx, ny)
                density[key] = density.get(key, 0) + 1

        # Rita punkter med densitetsbaserade tecken
        density_chars = " ·∘●◉"  # Från låg till hög densitet
        max_density = max(density.values()) if density else 1

        for (nx, ny), count in density.items():
            # Normalisera densitet till index
            idx = int((count / max_density) * (len(density_chars) - 1))
            idx = max(1, min(idx, len(density_chars) - 1))  # Minst en punkt
            grid[ny][nx] = density_chars[idx]

        return ["".join(row) for row in grid]

    def render(self) -> Text:
        """Rendera widgeten."""
        text = Text()

        if not self._loaded:
            text.append("Laddar...", style="dim italic")
            return text

        if not self.points:
            text.append("Ingen data att visa", style="dim italic")
            return text

        # Titel
        text.append(f"─── {self.title} ", style="bold cyan")
        text.append("─" * (self.map_width - len(self.title) - 5), style="dim")
        text.append("\n")

        # Kompassriktning
        text.append(" " * (self.map_width // 2) + "N\n", style="dim")

        # Karta med ram
        map_lines = self._render_map()
        for i, line in enumerate(map_lines):
            if i == len(map_lines) // 2:
                text.append("W ", style="dim")
            else:
                text.append("  ")

            text.append("│", style="dim")
            text.append(line, style="green")
            text.append("│", style="dim")

            if i == len(map_lines) // 2:
                text.append(" E", style="dim")
            text.append("\n")

        # Botten
        text.append(" " * (self.map_width // 2 + 3) + "S\n", style="dim")

        # Statistik
        text.append("\n")

        # Status-ikon baserat på täckning
        coverage = self.stats.coverage_percent
        if coverage >= 99:
            icon = "✓"
            style = "green bold"
        elif coverage >= 90:
            icon = "⚠"
            style = "yellow"
        else:
            icon = "✗"
            style = "red bold"

        text.append(f" {icon} ", style=style)
        within = self.stats.within_bbox
        total = self.stats.total_points
        text.append(f"Inom Sverige: {within:,}/{total:,}", style="bold")
        text.append(f" ({coverage:.1f}%)\n", style=style)

        if self.stats.outside_bbox > 0:
            text.append(f"   Utanför: {self.stats.outside_bbox:,} punkter\n", style="red")

        # Data bbox
        if self.stats.data_min_x is not None:
            text.append("\n")
            text.append(" Data-extent (SWEREF99 TM):\n", style="dim")
            min_x, max_x = self.stats.data_min_x, self.stats.data_max_x
            min_y, max_y = self.stats.data_min_y, self.stats.data_max_y
            text.append(f"   X: {min_x:,.0f} - {max_x:,.0f}\n", style="dim")
            text.append(f"   Y: {min_y:,.0f} - {max_y:,.0f}\n", style="dim")

        return text


class CompactAsciiMap(Static):
    """Kompakt ASCII-karta för inlining i andra vyer."""

    DEFAULT_CSS = """
    CompactAsciiMap {
        height: auto;
        width: auto;
    }
    """

    def __init__(self, width: int = 30, height: int = 12) -> None:
        super().__init__()
        self.map_width = width
        self.map_height = height
        self.points: list[tuple[float, float]] = []
        self.stats = MapStats()

    def load_points(self, points: list[tuple[float, float]]) -> None:
        """Ladda punkter."""
        self.points = points
        self._calculate_stats()
        self.refresh()

    def _calculate_stats(self) -> None:
        """Beräkna statistik."""
        if not self.points:
            self.stats = MapStats()
            return

        within = sum(
            1
            for x, y in self.points
            if SWEDEN_BBOX["min_x"] <= x <= SWEDEN_BBOX["max_x"]
            and SWEDEN_BBOX["min_y"] <= y <= SWEDEN_BBOX["max_y"]
        )

        self.stats = MapStats(
            total_points=len(self.points),
            within_bbox=within,
            outside_bbox=len(self.points) - within,
        )

    def render(self) -> Text:
        """Rendera kompakt karta."""
        text = Text()

        # Grid
        grid = [[" " for _ in range(self.map_width)] for _ in range(self.map_height)]

        for x, y in self.points:
            nx = int(
                (x - SWEDEN_BBOX["min_x"])
                / (SWEDEN_BBOX["max_x"] - SWEDEN_BBOX["min_x"])
                * (self.map_width - 1)
            )
            ny = int(
                (y - SWEDEN_BBOX["min_y"])
                / (SWEDEN_BBOX["max_y"] - SWEDEN_BBOX["min_y"])
                * (self.map_height - 1)
            )
            ny = self.map_height - 1 - ny

            if 0 <= nx < self.map_width and 0 <= ny < self.map_height:
                grid[ny][nx] = "●"

        # Rita
        text.append("┌" + "─" * self.map_width + "┐\n", style="dim")
        for row in grid:
            text.append("│", style="dim")
            text.append("".join(row), style="green")
            text.append("│\n", style="dim")
        text.append("└" + "─" * self.map_width + "┘\n", style="dim")

        # Kort statistik
        coverage = self.stats.coverage_percent
        style = "green" if coverage >= 99 else ("yellow" if coverage >= 90 else "red")
        within = self.stats.within_bbox
        total = self.stats.total_points
        text.append(f" {within}/{total} ({coverage:.0f}%)", style=style)

        return text
