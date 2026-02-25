"""Centrala inställningar för G-ETL.

Laddar konfiguration från config/config.yml med fallback till standardvärden.
Importera med: from g_etl.settings import settings

Prioritetsordning:
1. Miljövariabler (G_ETL_DATA_DIR)
2. config/config.yml
3. Standardvärden i denna fil
"""

import os
from pathlib import Path

import yaml


def _cpu_count() -> int:
    """Hämta antal CPU-kärnor med fallback."""
    return os.cpu_count() or 4


def _load_config(config_path: Path | None = None) -> dict:
    """Ladda konfiguration från YAML-fil.

    Returnerar tom dict om filen inte finns.
    """
    path = config_path or Path("config/config.yml")
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


class Settings:
    """Konfigurationsklass för G-ETL.

    Konfigurerbara värden laddas från config/config.yml.
    Infrastrukturkonstanter definieras som klassattribut.
    """

    # === Infrastrukturkonstanter (ej konfigurerbara) ===
    DB_EXTENSION: str = ".duckdb"
    DUCKDB_EXTENSIONS: list[str] = ["spatial", "parquet", "httpfs", "json", "h3"]
    DUCKDB_SCHEMAS: list[str] = ["raw", "mart"]

    # OBS: DuckDB:s spatial extension har bugg med EPSG-koder, använd PROJ4-strängar!
    PROJ4_SWEREF99_TM: str = (
        "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    )
    PROJ4_WGS84: str = "+proj=longlat +datum=WGS84 +no_defs"

    SQL_DIR: Path = Path("sql")
    SQL_INIT_DIR: Path = Path("sql/_init")
    CONFIG_DIR: Path = Path("config")

    def __init__(self, config_path: Path | None = None):
        cfg = _load_config(config_path)
        h3 = cfg.get("h3", {}) if isinstance(cfg.get("h3"), dict) else {}

        # === Sökvägar ===
        # Prioritet: miljövariabel > config.yml > default
        env_data_dir = os.environ.get("G_ETL_DATA_DIR")
        if env_data_dir:
            self.DATA_DIR = Path(env_data_dir)
        else:
            self.DATA_DIR = Path(cfg.get("data_dir", "data"))

        self.INPUT_DATA_DIR = Path(cfg.get("input_data_dir", "input_data"))
        self.LOGS_DIR = Path(cfg.get("logs_dir", "logs"))

        # === Databas ===
        self.DB_PREFIX: str = cfg.get("db_prefix", "warehouse")
        self.DB_KEEP_COUNT: int = cfg.get("db_keep_count", 3)

        # === H3 Spatial Index ===
        # Se: https://h3geo.org/docs/core-library/restable/
        self.H3_RESOLUTION: int = h3.get("resolution", 13)
        self.H3_POLYFILL_RESOLUTION: int = h3.get("polyfill_resolution", 11)
        self.H3_LINE_RESOLUTION: int = h3.get("line_resolution", 12)
        self.H3_POINT_RESOLUTION: int = h3.get("point_resolution", 13)
        self.H3_LINE_BUFFER_METERS: int = h3.get("line_buffer_meters", 10)

        # === Pipeline ===
        self.MAX_CONCURRENT_EXTRACTS: int = cfg.get("max_concurrent_extracts", _cpu_count())
        self.MAX_CONCURRENT_SQL: int = cfg.get("max_concurrent_sql", max(2, _cpu_count() // 2))
        self.EXTRACT_TIMEOUT_SECONDS: int = cfg.get("extract_timeout_seconds", 300)

        # === Koordinatsystem ===
        self.SOURCE_CRS: str = cfg.get("source_crs", "EPSG:3006")
        self.TARGET_CRS: str = cfg.get("target_crs", "EPSG:4326")

    # === Härledda sökvägar (baserade på DATA_DIR) ===

    @property
    def RAW_DIR(self) -> Path:
        return self.DATA_DIR / "raw"

    @property
    def TEMP_DIR(self) -> Path:
        return self.DATA_DIR / "temp"

    @property
    def EXPORT_DIR(self) -> Path:
        return self.DATA_DIR / "export"

    @property
    def HEATMAPS_DIR(self) -> Path:
        return self.DATA_DIR / "heatmaps"

    @property
    def LOG_SQL_DIR(self) -> Path:
        return self.DATA_DIR / "log_sql"

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

    def cleanup_log_sql(self) -> None:
        """Rensa renderade SQL-loggfiler."""
        import shutil

        if self.LOG_SQL_DIR.exists():
            shutil.rmtree(self.LOG_SQL_DIR)
        self.LOG_SQL_DIR.mkdir(parents=True, exist_ok=True)


# Singleton-instans
settings = Settings()
