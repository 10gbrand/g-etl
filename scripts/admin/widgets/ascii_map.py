"""ASCII-karta widget för att visualisera geometrier i terminalen.

Stödjer både enkel ASCII-rendering och högupplöst braille-rendering.
Braille-tecken ger 8x högre upplösning (2x4 dots per cell).
"""

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

# Braille Unicode-block börjar vid U+2800
# Varje cell är 2x4 dots med följande bitvärden:
#   1   8
#   2  16
#   4  32
#  64 128
BRAILLE_BASE = 0x2800
BRAILLE_DOT_MAP = [
    (0, 0, 0x01),  # Rad 0, Kolumn 0
    (0, 1, 0x08),  # Rad 0, Kolumn 1
    (1, 0, 0x02),  # Rad 1, Kolumn 0
    (1, 1, 0x10),  # Rad 1, Kolumn 1
    (2, 0, 0x04),  # Rad 2, Kolumn 0
    (2, 1, 0x20),  # Rad 2, Kolumn 1
    (3, 0, 0x40),  # Rad 3, Kolumn 0
    (3, 1, 0x80),  # Rad 3, Kolumn 1
]

# Förenklad kontur av Sverige i SWEREF99 TM (EPSG:3006)
# Punkterna bildar en sluten polygon som följer kustlinjen
SWEDEN_OUTLINE = [
    # Södra Sverige (Skåne)
    (370000, 6135000),
    (420000, 6150000),
    (480000, 6170000),
    (530000, 6200000),
    (560000, 6250000),
    # Östkusten
    (590000, 6350000),
    (620000, 6450000),
    (650000, 6550000),
    (680000, 6650000),
    (700000, 6750000),
    (720000, 6850000),
    (730000, 6950000),
    (740000, 7050000),
    (760000, 7150000),
    # Norrland ostkust
    (780000, 7250000),
    (800000, 7350000),
    (820000, 7450000),
    (840000, 7550000),
    (860000, 7620000),
    # Nordligaste punkten
    (820000, 7660000),
    (750000, 7680000),
    (680000, 7660000),
    # Norska gränsen norr
    (620000, 7600000),
    (580000, 7500000),
    (540000, 7400000),
    (500000, 7300000),
    # Mellersta gränsen
    (450000, 7200000),
    (400000, 7100000),
    (380000, 7000000),
    (360000, 6900000),
    # Sydvästra gränsen
    (340000, 6800000),
    (320000, 6700000),
    (300000, 6600000),
    (290000, 6500000),
    # Västkusten
    (300000, 6400000),
    (310000, 6350000),
    (320000, 6300000),
    (330000, 6250000),
    (340000, 6200000),
    (360000, 6150000),
    # Tillbaka till start
    (370000, 6135000),
]

# Gotland (separat ö)
GOTLAND_OUTLINE = [
    (680000, 6380000),
    (710000, 6400000),
    (720000, 6450000),
    (715000, 6500000),
    (700000, 6530000),
    (675000, 6520000),
    (665000, 6470000),
    (670000, 6420000),
    (680000, 6380000),
]

# Öland
OLAND_OUTLINE = [
    (635000, 6280000),
    (645000, 6320000),
    (650000, 6370000),
    (645000, 6400000),
    (635000, 6390000),
    (630000, 6340000),
    (630000, 6290000),
    (635000, 6280000),
]


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
        # Först kolla om datan ser ut som WGS84 (små koordinater) eller SWEREF99 TM (stora)
        check_query = f"""
            SELECT ST_X(ST_Centroid({geometry_column})) as x
            FROM {schema}.{table}
            WHERE {geometry_column} IS NOT NULL
            LIMIT 1
        """
        sample_x = conn.execute(check_query).fetchone()
        needs_transform = sample_x and sample_x[0] is not None and abs(sample_x[0]) < 180

        if needs_transform:
            # Koordinater ser ut som WGS84 - transformera till SWEREF99 TM
            query = f"""
                SELECT
                    ST_X(ST_Centroid(ST_Transform({geometry_column}, 'EPSG:4326', 'EPSG:3006'))) as x,
                    ST_Y(ST_Centroid(ST_Transform({geometry_column}, 'EPSG:4326', 'EPSG:3006'))) as y
                FROM {schema}.{table}
                WHERE {geometry_column} IS NOT NULL
                USING SAMPLE {sample_size}
            """
        else:
            # Koordinater ser ut som SWEREF99 TM - använd direkt
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
            # Fallback utan sampling
            if needs_transform:
                query_simple = f"""
                    SELECT
                        ST_X(ST_Centroid(ST_Transform({geometry_column}, 'EPSG:4326', 'EPSG:3006'))) as x,
                        ST_Y(ST_Centroid(ST_Transform({geometry_column}, 'EPSG:4326', 'EPSG:3006'))) as y
                    FROM {schema}.{table}
                    WHERE {geometry_column} IS NOT NULL
                    LIMIT {sample_size}
                """
            else:
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


class BrailleMapWidget(Static):
    """Högupplöst karta med braille-tecken.

    Varje terminal-cell representerar 2x4 punkter, vilket ger 8x högre
    upplösning jämfört med vanlig ASCII-rendering.
    """

    DEFAULT_CSS = """
    BrailleMapWidget {
        height: auto;
        min-height: 20;
        padding: 1;
        border: round $primary;
    }
    """

    def __init__(
        self,
        width: int = 60,
        height: int = 30,
        title: str = "Karta",
        show_density: bool = True,
        show_outline: bool = True,
    ) -> None:
        """Skapa en braille-karta.

        Args:
            width: Bredd i terminal-celler (effektiv bredd = width * 2 dots)
            height: Höjd i terminal-celler (effektiv höjd = height * 4 dots)
            title: Titel för kartan
            show_density: Om True, visa densitet med fyllda mönster
            show_outline: Om True, visa Sveriges kontur som bakgrund
        """
        super().__init__()
        self.map_width = width
        self.map_height = height
        self.title = title
        self.show_density = show_density
        self.show_outline = show_outline
        self.points: list[tuple[float, float]] = []
        self.stats = MapStats()
        self._loaded = False

        # Effektiv dot-upplösning
        self._dot_width = width * 2
        self._dot_height = height * 4

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

        # Kolla koordinatsystem
        check_query = f"""
            SELECT ST_X(ST_Centroid({geometry_column})) as x
            FROM {schema}.{table}
            WHERE {geometry_column} IS NOT NULL
            LIMIT 1
        """
        sample_x = conn.execute(check_query).fetchone()
        needs_transform = sample_x and sample_x[0] is not None and abs(sample_x[0]) < 180

        if needs_transform:
            query = f"""
                SELECT
                    ST_X(ST_Centroid(ST_Transform({geometry_column}, 'EPSG:4326', 'EPSG:3006'))) as x,
                    ST_Y(ST_Centroid(ST_Transform({geometry_column}, 'EPSG:4326', 'EPSG:3006'))) as y
                FROM {schema}.{table}
                WHERE {geometry_column} IS NOT NULL
                USING SAMPLE {sample_size}
            """
        else:
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
            # Fallback utan SAMPLE
            query_simple = query.replace(f"USING SAMPLE {sample_size}", f"LIMIT {sample_size}")
            rows = conn.execute(query_simple).fetchall()
            self.points = [(float(x), float(y)) for x, y in rows if x is not None and y is not None]

        self._calculate_stats()
        self._loaded = True
        self.refresh()

    def load_points(self, points: list[tuple[float, float]]) -> None:
        """Ladda punkter direkt."""
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

    def _coord_to_dot(self, x: float, y: float) -> tuple[int, int]:
        """Konvertera SWEREF99 TM-koordinat till dot-koordinat."""
        dx = int(
            (x - SWEDEN_BBOX["min_x"])
            / (SWEDEN_BBOX["max_x"] - SWEDEN_BBOX["min_x"])
            * (self._dot_width - 1)
        )
        dy = int(
            (y - SWEDEN_BBOX["min_y"])
            / (SWEDEN_BBOX["max_y"] - SWEDEN_BBOX["min_y"])
            * (self._dot_height - 1)
        )
        dy = self._dot_height - 1 - dy  # Flip Y (norr uppåt)
        return dx, dy

    def _draw_line(
        self,
        outline_grid: set[tuple[int, int]],
        x1: float, y1: float,
        x2: float, y2: float,
    ) -> None:
        """Rita en linje mellan två koordinater med Bresenhams algoritm."""
        dx1, dy1 = self._coord_to_dot(x1, y1)
        dx2, dy2 = self._coord_to_dot(x2, y2)

        # Bresenham's line algorithm
        steep = abs(dy2 - dy1) > abs(dx2 - dx1)
        if steep:
            dx1, dy1 = dy1, dx1
            dx2, dy2 = dy2, dx2

        if dx1 > dx2:
            dx1, dx2 = dx2, dx1
            dy1, dy2 = dy2, dy1

        delta_x = dx2 - dx1
        delta_y = abs(dy2 - dy1)
        error = delta_x // 2
        y = dy1
        y_step = 1 if dy1 < dy2 else -1

        for x in range(dx1, dx2 + 1):
            if steep:
                if 0 <= y < self._dot_width and 0 <= x < self._dot_height:
                    outline_grid.add((y, x))
            else:
                if 0 <= x < self._dot_width and 0 <= y < self._dot_height:
                    outline_grid.add((x, y))
            error -= delta_y
            if error < 0:
                y += y_step
                error += delta_x

    def _get_outline_grid(self) -> set[tuple[int, int]]:
        """Skapa en grid med Sveriges kontur."""
        outline_grid: set[tuple[int, int]] = set()

        # Rita huvudkonturen
        for i in range(len(SWEDEN_OUTLINE) - 1):
            x1, y1 = SWEDEN_OUTLINE[i]
            x2, y2 = SWEDEN_OUTLINE[i + 1]
            self._draw_line(outline_grid, x1, y1, x2, y2)

        # Rita Gotland
        for i in range(len(GOTLAND_OUTLINE) - 1):
            x1, y1 = GOTLAND_OUTLINE[i]
            x2, y2 = GOTLAND_OUTLINE[i + 1]
            self._draw_line(outline_grid, x1, y1, x2, y2)

        # Rita Öland
        for i in range(len(OLAND_OUTLINE) - 1):
            x1, y1 = OLAND_OUTLINE[i]
            x2, y2 = OLAND_OUTLINE[i + 1]
            self._draw_line(outline_grid, x1, y1, x2, y2)

        return outline_grid

    def _render_braille_map(self) -> list[tuple[str, str]]:
        """Rendera kartan som braille-tecken.

        Returnerar lista av tupler: (tecken, stil) för varje cell.
        """
        # Skapa kontur-grid
        outline_grid = self._get_outline_grid() if self.show_outline else set()

        # Skapa data dot-grid med densitetsvärden
        dot_grid: dict[tuple[int, int], int] = {}

        for x, y in self.points:
            dx, dy = self._coord_to_dot(x, y)
            if 0 <= dx < self._dot_width and 0 <= dy < self._dot_height:
                key = (dx, dy)
                dot_grid[key] = dot_grid.get(key, 0) + 1

        # Beräkna max densitet för normalisering
        max_density = max(dot_grid.values()) if dot_grid else 1

        # Konvertera till braille-tecken med stil
        lines: list[tuple[str, str]] = []
        for cell_y in range(self.map_height):
            line_chars: list[tuple[str, str]] = []
            for cell_x in range(self.map_width):
                char_code = BRAILLE_BASE
                has_data = False
                has_outline = False

                # Kolla varje dot i cellen
                for row, col, bit in BRAILLE_DOT_MAP:
                    dx = cell_x * 2 + col
                    dy = cell_y * 4 + row

                    # Kolla om det finns data här
                    if (dx, dy) in dot_grid:
                        has_data = True
                        char_code |= bit

                    # Kolla om det finns kontur här
                    if (dx, dy) in outline_grid:
                        has_outline = True
                        char_code |= bit

                # Välj stil baserat på innehåll
                if has_data:
                    style = "bright_green"
                elif has_outline:
                    style = "dim cyan"
                else:
                    style = "dim"

                line_chars.append((chr(char_code), style))
            lines.append(line_chars)

        return lines

    def render(self) -> Text:
        """Rendera widgeten."""
        text = Text()

        if not self._loaded:
            text.append("Laddar...", style="dim italic")
            return text

        if not self.points and not self.show_outline:
            text.append("Ingen data att visa", style="dim italic")
            return text

        # Titel med info
        text.append(f"─── {self.title} ", style="bold cyan")
        resolution_info = f"({self._dot_width}×{self._dot_height} dots)"
        remaining = self.map_width - len(self.title) - len(resolution_info) - 6
        text.append("─" * max(0, remaining), style="dim")
        text.append(f" {resolution_info} ", style="dim italic")
        text.append("─\n", style="dim")

        # Nordpil
        text.append(" " * (self.map_width // 2) + "▲ N\n", style="cyan")

        # Karta med ram
        map_lines = self._render_braille_map()
        for i, line_chars in enumerate(map_lines):
            # Väst-markering på mitten
            if i == len(map_lines) // 2:
                text.append("W◀", style="cyan")
            else:
                text.append("  ")

            text.append("│", style="dim cyan")

            # Rendera varje cell med sin egen stil
            for char, style in line_chars:
                text.append(char, style=style)

            text.append("│", style="dim cyan")

            # Öst-markering på mitten
            if i == len(map_lines) // 2:
                text.append("▶E", style="cyan")
            text.append("\n")

        # Sydpil
        text.append(" " * (self.map_width // 2) + "▼ S\n", style="cyan")

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
            text.append(f"Inom Sverige: {self.stats.within_bbox:,}/{self.stats.total_points:,}", style="bold")
            text.append(f" ({coverage:.1f}%)\n", style=icon_style)

            if self.stats.outside_bbox > 0:
                text.append(f"   Utanför: {self.stats.outside_bbox:,} punkter\n", style="red")

            # Extent-info
            if self.stats.data_min_x is not None:
                text.append("\n")
                text.append(" 📍 Extent (SWEREF99 TM):\n", style="dim")
                text.append(f"   X: {self.stats.data_min_x:,.0f} – {self.stats.data_max_x:,.0f}\n", style="dim")
                text.append(f"   Y: {self.stats.data_min_y:,.0f} – {self.stats.data_max_y:,.0f}\n", style="dim")
        else:
            text.append(" ○ Ingen data laddad\n", style="dim")

        return text
