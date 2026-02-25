"""Tester för g_etl/settings.py."""

import os
from pathlib import Path

import yaml

from g_etl.settings import Settings, _load_config, settings


class TestSettings:
    """Tester för Settings-klassen."""

    def test_singleton_instance(self):
        """Kontrollera att settings är en singleton-instans."""
        assert isinstance(settings, Settings)

    def test_default_paths(self):
        """Kontrollera att default-sökvägar är korrekta."""
        s = Settings(config_path=Path("nonexistent.yml"))
        assert s.DATA_DIR == Path("data")
        assert s.RAW_DIR == Path("data/raw")
        assert s.TEMP_DIR == Path("data/temp")
        assert s.LOGS_DIR == Path("logs")
        assert s.LOG_SQL_DIR == Path("data/log_sql")
        assert s.SQL_DIR == Path("sql")

    def test_db_settings(self):
        """Kontrollera databas-inställningar."""
        assert settings.DB_PREFIX == "warehouse"
        assert settings.DB_EXTENSION == ".duckdb"
        assert settings.DB_KEEP_COUNT == 3

    def test_h3_settings(self):
        """Kontrollera H3-inställningar."""
        assert settings.H3_RESOLUTION == 13
        assert settings.H3_POLYFILL_RESOLUTION == 11
        assert settings.H3_LINE_RESOLUTION == 12
        assert settings.H3_POINT_RESOLUTION == 13
        assert settings.H3_LINE_BUFFER_METERS == 10

    def test_pipeline_settings(self):
        """Kontrollera pipeline-inställningar."""
        assert settings.MAX_CONCURRENT_EXTRACTS >= 1
        assert settings.MAX_CONCURRENT_SQL >= 2
        assert settings.EXTRACT_TIMEOUT_SECONDS == 300

    def test_crs_settings(self):
        """Kontrollera koordinatsystem-inställningar."""
        assert settings.SOURCE_CRS == "EPSG:3006"
        assert settings.TARGET_CRS == "EPSG:4326"
        assert "utm" in settings.PROJ4_SWEREF99_TM.lower()
        assert "wgs84" in settings.PROJ4_WGS84.lower()

    def test_duckdb_extensions(self):
        """Kontrollera att nödvändiga DuckDB extensions finns."""
        required = ["spatial", "parquet", "httpfs", "json", "h3"]
        for ext in required:
            assert ext in settings.DUCKDB_EXTENSIONS

    def test_duckdb_schemas(self):
        """Kontrollera att bas-scheman finns.

        OBS: Staging-scheman (staging_004, staging_005, etc.) skapas dynamiskt
        av pipeline_runner baserat på SQL-templates nummer.
        """
        required = ["raw", "mart"]
        for schema in required:
            assert schema in settings.DUCKDB_SCHEMAS

    def test_datasets_path(self):
        """Kontrollera sökväg till datasets.yml."""
        assert settings.datasets_path == Path("config/datasets.yml")

    def test_get_db_path_with_name(self, temp_dir):
        """Testa get_db_path med specifikt namn."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir

        db_path = test_settings.get_db_path("test_db")
        assert db_path == temp_dir / "test_db.duckdb"

    def test_get_db_path_without_name(self, temp_dir):
        """Testa get_db_path utan namn (genererar tidsstämpel)."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir

        db_path = test_settings.get_db_path()
        assert db_path.suffix == ".duckdb"
        assert "warehouse_" in db_path.name

    def test_get_temp_db_path(self, temp_dir):
        """Testa get_temp_db_path."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir

        db_path = test_settings.get_temp_db_path("test_dataset")
        assert db_path == temp_dir / "temp" / "test_dataset.duckdb"

    def test_ensure_dirs(self, temp_dir):
        """Testa att ensure_dirs skapar kataloger."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir / "data"
        test_settings.INPUT_DATA_DIR = temp_dir / "input_data"
        test_settings.LOGS_DIR = temp_dir / "logs"

        test_settings.ensure_dirs()

        assert (temp_dir / "data").exists()
        assert (temp_dir / "data" / "raw").exists()
        assert (temp_dir / "data" / "temp").exists()
        assert (temp_dir / "logs").exists()

    def test_cleanup_temp_dbs(self, temp_dir):
        """Testa att cleanup_temp_dbs tar bort temporära databaser."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir

        # Skapa temp-katalog med dummy-filer
        temp_subdir = temp_dir / "temp"
        temp_subdir.mkdir()
        (temp_subdir / "test1.duckdb").touch()
        (temp_subdir / "test2.duckdb").touch()
        (temp_subdir / "keep.txt").touch()

        test_settings.cleanup_temp_dbs()

        assert not (temp_subdir / "test1.duckdb").exists()
        assert not (temp_subdir / "test2.duckdb").exists()
        assert (temp_subdir / "keep.txt").exists()  # Ska inte tas bort

    def test_cleanup_log_sql(self, temp_dir):
        """Testa att cleanup_log_sql rensar och återskapar katalogen."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir

        log_sql_dir = temp_dir / "log_sql"

        # Skapa befintlig katalog med filer
        (log_sql_dir / "old_dataset").mkdir(parents=True)
        (log_sql_dir / "old_dataset" / "004_staging.sql").write_text("SELECT 1")

        test_settings.cleanup_log_sql()

        # Katalogen ska finnas men vara tom
        assert log_sql_dir.exists()
        assert not (log_sql_dir / "old_dataset").exists()

    def test_cleanup_log_sql_creates_dir(self, temp_dir):
        """Testa att cleanup_log_sql skapar katalogen om den inte finns."""
        test_settings = Settings(config_path=Path("nonexistent.yml"))
        test_settings.DATA_DIR = temp_dir

        log_sql_dir = temp_dir / "log_sql"

        assert not log_sql_dir.exists()
        test_settings.cleanup_log_sql()
        assert log_sql_dir.exists()


class TestConfigLoading:
    """Tester för YAML-konfigurationsladdning."""

    def test_missing_config_uses_defaults(self):
        """Saknad config.yml ger standardvärden."""
        s = Settings(config_path=Path("nonexistent.yml"))
        assert s.DATA_DIR == Path("data")
        assert s.H3_RESOLUTION == 13
        assert s.DB_PREFIX == "warehouse"
        assert s.SOURCE_CRS == "EPSG:3006"

    def test_loads_from_yaml(self, tmp_path):
        """Värden laddas korrekt från YAML-fil."""
        config = {
            "data_dir": "/mnt/e/g-etl",
            "db_prefix": "mydb",
            "db_keep_count": 5,
            "h3": {
                "resolution": 10,
                "polyfill_resolution": 8,
                "line_resolution": 9,
                "point_resolution": 10,
                "line_buffer_meters": 20,
            },
            "max_concurrent_extracts": 16,
            "max_concurrent_sql": 8,
            "extract_timeout_seconds": 600,
            "source_crs": "EPSG:4326",
            "target_crs": "EPSG:3006",
            "logs_dir": "/var/log/g-etl",
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config))

        s = Settings(config_path=config_file)

        assert s.DATA_DIR == Path("/mnt/e/g-etl")
        assert s.RAW_DIR == Path("/mnt/e/g-etl/raw")
        assert s.DB_PREFIX == "mydb"
        assert s.DB_KEEP_COUNT == 5
        assert s.H3_RESOLUTION == 10
        assert s.H3_POLYFILL_RESOLUTION == 8
        assert s.H3_LINE_RESOLUTION == 9
        assert s.H3_POINT_RESOLUTION == 10
        assert s.H3_LINE_BUFFER_METERS == 20
        assert s.MAX_CONCURRENT_EXTRACTS == 16
        assert s.MAX_CONCURRENT_SQL == 8
        assert s.EXTRACT_TIMEOUT_SECONDS == 600
        assert s.SOURCE_CRS == "EPSG:4326"
        assert s.TARGET_CRS == "EPSG:3006"
        assert s.LOGS_DIR == Path("/var/log/g-etl")

    def test_env_var_overrides_yaml(self, tmp_path, monkeypatch):
        """Miljövariabel G_ETL_DATA_DIR har högre prioritet än YAML."""
        config = {"data_dir": "/from/yaml"}
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config))

        monkeypatch.setenv("G_ETL_DATA_DIR", "/from/env")
        s = Settings(config_path=config_file)

        assert s.DATA_DIR == Path("/from/env")
        assert s.RAW_DIR == Path("/from/env/raw")

    def test_partial_config(self, tmp_path):
        """Partiell config - bara H3-sektion, övriga får defaults."""
        config = {"h3": {"resolution": 10}}
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config))

        s = Settings(config_path=config_file)

        # H3-resolution ändrad
        assert s.H3_RESOLUTION == 10
        # Övriga H3 behåller defaults
        assert s.H3_POLYFILL_RESOLUTION == 11
        # Andra sektioner behåller defaults
        assert s.DATA_DIR == Path("data")
        assert s.DB_PREFIX == "warehouse"

    def test_empty_config_file(self, tmp_path):
        """Tom config-fil ger standardvärden."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("")

        s = Settings(config_path=config_file)
        assert s.DATA_DIR == Path("data")
        assert s.H3_RESOLUTION == 13

    def test_load_config_returns_empty_for_missing_file(self):
        """_load_config returnerar tom dict för saknad fil."""
        result = _load_config(Path("nonexistent.yml"))
        assert result == {}

    def test_infrastructure_constants_unchanged(self):
        """Infrastrukturkonstanter påverkas inte av config."""
        s = Settings(config_path=Path("nonexistent.yml"))
        assert s.DB_EXTENSION == ".duckdb"
        assert "spatial" in s.DUCKDB_EXTENSIONS
        assert "raw" in s.DUCKDB_SCHEMAS
        assert s.SQL_DIR == Path("sql")
        assert s.CONFIG_DIR == Path("config")


class TestCpuCount:
    """Tester för CPU-räkning."""

    def test_cpu_count_returns_positive(self):
        """Kontrollera att cpu_count returnerar positivt värde."""
        from g_etl.settings import _cpu_count

        count = _cpu_count()
        assert count >= 1

    def test_cpu_count_fallback(self, monkeypatch):
        """Testa fallback när os.cpu_count() returnerar None."""

        # Definiera en ny funktion som returnerar None
        def mock_cpu_count():
            return None

        monkeypatch.setattr(os, "cpu_count", mock_cpu_count)

        # Importera _cpu_count-funktionen direkt
        from g_etl.settings import _cpu_count

        result = _cpu_count()
        assert result == 4
