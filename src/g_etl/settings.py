"""Centrala inställningar för G-ETL.

Alla konfigurerbara värden samlas här för enkel justering.
Importera med: from g_etl.settings import settings
"""

import os
from pathlib import Path


def _cpu_count() -> int:
    """Hämta antal CPU-kärnor med fallback."""
    return os.cpu_count() or 4


class Settings:
    """Konfigurationsklass för G-ETL."""

    # === Sökvägar ===
    DATA_DIR: Path = Path("data")
    RAW_DIR: Path = Path("data/raw")
    TEMP_DIR: Path = Path("data/temp")  # Temporära per-dataset DBs
    INPUT_DATA_DIR: Path = Path("input_data")  # Lokala geodatafiler
    LOGS_DIR: Path = Path("logs")
    SQL_DIR: Path = Path("sql")
    SQL_INIT_DIR: Path = Path("sql/_init")
    CONFIG_DIR: Path = Path("config")

    # === Databas ===
    DB_PREFIX: str = "warehouse"
    DB_EXTENSION: str = ".duckdb"
    DB_KEEP_COUNT: int = 3  # Antal databaser att behålla vid cleanup

    # === H3 Spatial Index ===
    # Se: https://h3geo.org/docs/core-library/restable/
    # Resolution 13 ≈ 43.9 m² per cell (för centroid-index)
    # Resolution 11 ≈ 2149 m² per cell (för polyfill, större celler inom polygoner)
    # Resolution 12 ≈ 307 m² per cell (för linjer/punkter, högre precision)
    H3_RESOLUTION: int = 13  # För _h3_index (centroid)
    H3_POLYFILL_RESOLUTION: int = 11  # För _h3_cells (polyfill av polygoner)
    H3_LINE_RESOLUTION: int = 12  # För _h3_cells (linjer, buffrad)
    H3_POINT_RESOLUTION: int = 13  # För _h3_cells (punkter)
    H3_LINE_BUFFER_METERS: int = 10  # Buffer-radie för linjer i meter

    # === Pipeline ===
    # Auto-detekterade baserat på CPU-kärnor
    MAX_CONCURRENT_EXTRACTS: int = _cpu_count()  # I/O-bound: använd alla kärnor
    MAX_CONCURRENT_SQL: int = max(
        2, _cpu_count() // 2
    )  # CPU-bound: halva (DuckDB paralleliserar internt)
    EXTRACT_TIMEOUT_SECONDS: int = 300  # Timeout per dataset

    # === Koordinatsystem ===
    # OBS: DuckDB:s spatial extension har bugg med EPSG-koder, använd PROJ4-strängar!
    SOURCE_CRS: str = "EPSG:3006"  # SWEREF99 TM (vanligt för svenska data)
    TARGET_CRS: str = "EPSG:4326"  # WGS84 (för H3/A5)

    # PROJ4-strängar för korrekt transformation i DuckDB
    PROJ4_SWEREF99_TM: str = (
        "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    )
    PROJ4_WGS84: str = "+proj=longlat +datum=WGS84 +no_defs"

    # === DuckDB Extensions ===
    DUCKDB_EXTENSIONS: list[str] = ["spatial", "parquet", "httpfs", "json", "h3"]

    # === DuckDB Scheman ===
    # OBS: staging-scheman (staging_004, staging_005, etc.) skapas dynamiskt
    DUCKDB_SCHEMAS: list[str] = ["raw", "mart"]

    @property
    def datasets_path(self) -> Path:
        """Sökväg till datasets.yml."""
        return self.CONFIG_DIR / "datasets.yml"

    def get_db_path(self, name: str | None = None) -> Path:
        """Generera databas-sökväg.

        Args:
            name: Valfritt namn (utan extension). Om None används tidsstämpel.
        """
        from datetime import datetime

        self.DATA_DIR.mkdir(parents=True, exist_ok=True)

        if name:
            return self.DATA_DIR / f"{name}{self.DB_EXTENSION}"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.DATA_DIR / f"{self.DB_PREFIX}_{timestamp}{self.DB_EXTENSION}"

    def ensure_dirs(self) -> None:
        """Skapa alla nödvändiga kataloger."""
        for dir_path in [
            self.DATA_DIR,
            self.RAW_DIR,
            self.TEMP_DIR,
            self.INPUT_DATA_DIR,
            self.LOGS_DIR,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def get_temp_db_path(self, dataset_id: str) -> Path:
        """Generera sökväg för temporär per-dataset databas."""
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        return self.TEMP_DIR / f"{dataset_id}.duckdb"

    def cleanup_temp_dbs(self) -> None:
        """Ta bort temporära databasfiler."""
        if self.TEMP_DIR.exists():
            for db_file in self.TEMP_DIR.glob("*.duckdb"):
                try:
                    db_file.unlink()
                except OSError:
                    pass  # Ignorera om filen är låst


# Singleton-instans
settings = Settings()
