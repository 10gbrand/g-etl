"""Tester för g_etl/settings.py."""

import os
from pathlib import Path

from g_etl.settings import Settings, settings


class TestSettings:
    """Tester för Settings-klassen."""

    def test_singleton_instance(self):
        """Kontrollera att settings är en singleton-instans."""
        assert isinstance(settings, Settings)

    def test_default_paths(self):
        """Kontrollera att default-sökvägar är korrekta."""
        assert settings.DATA_DIR == Path("data")
        assert settings.RAW_DIR == Path("data/raw")
        assert settings.TEMP_DIR == Path("data/temp")
        assert settings.LOGS_DIR == Path("logs")
        assert settings.LOG_SQL_DIR == Path("data/log_sql")
        assert settings.SQL_DIR == Path("sql")

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

    def test_get_db_path_with_name(self, temp_dir, monkeypatch):
        """Testa get_db_path med specifikt namn."""
        test_settings = Settings()
        monkeypatch.setattr(test_settings, "DATA_DIR", temp_dir)

        db_path = test_settings.get_db_path("test_db")
        assert db_path == temp_dir / "test_db.duckdb"

    def test_get_db_path_without_name(self, temp_dir, monkeypatch):
        """Testa get_db_path utan namn (genererar tidsstämpel)."""
        test_settings = Settings()
        monkeypatch.setattr(test_settings, "DATA_DIR", temp_dir)

        db_path = test_settings.get_db_path()
        assert db_path.suffix == ".duckdb"
        assert "warehouse_" in db_path.name

    def test_get_temp_db_path(self, temp_dir, monkeypatch):
        """Testa get_temp_db_path."""
        test_settings = Settings()
        monkeypatch.setattr(test_settings, "TEMP_DIR", temp_dir)

        db_path = test_settings.get_temp_db_path("test_dataset")
        assert db_path == temp_dir / "test_dataset.duckdb"

    def test_ensure_dirs(self, temp_dir, monkeypatch):
        """Testa att ensure_dirs skapar kataloger."""
        test_settings = Settings()
        data_dir = temp_dir / "data"
        raw_dir = temp_dir / "data/raw"
        temp_subdir = temp_dir / "data/temp"
        logs_dir = temp_dir / "logs"

        monkeypatch.setattr(test_settings, "DATA_DIR", data_dir)
        monkeypatch.setattr(test_settings, "RAW_DIR", raw_dir)
        monkeypatch.setattr(test_settings, "TEMP_DIR", temp_subdir)
        monkeypatch.setattr(test_settings, "LOGS_DIR", logs_dir)

        test_settings.ensure_dirs()

        assert data_dir.exists()
        assert raw_dir.exists()
        assert temp_subdir.exists()
        assert logs_dir.exists()

    def test_cleanup_temp_dbs(self, temp_dir, monkeypatch):
        """Testa att cleanup_temp_dbs tar bort temporära databaser."""
        test_settings = Settings()
        monkeypatch.setattr(test_settings, "TEMP_DIR", temp_dir)

        # Skapa några dummy-filer
        (temp_dir / "test1.duckdb").touch()
        (temp_dir / "test2.duckdb").touch()
        (temp_dir / "keep.txt").touch()

        test_settings.cleanup_temp_dbs()

        assert not (temp_dir / "test1.duckdb").exists()
        assert not (temp_dir / "test2.duckdb").exists()
        assert (temp_dir / "keep.txt").exists()  # Ska inte tas bort

    def test_cleanup_log_sql(self, temp_dir, monkeypatch):
        """Testa att cleanup_log_sql rensar och återskapar katalogen."""
        test_settings = Settings()
        log_sql_dir = temp_dir / "log_sql"
        monkeypatch.setattr(test_settings, "LOG_SQL_DIR", log_sql_dir)

        # Skapa befintlig katalog med filer
        (log_sql_dir / "old_dataset").mkdir(parents=True)
        (log_sql_dir / "old_dataset" / "004_staging.sql").write_text("SELECT 1")

        test_settings.cleanup_log_sql()

        # Katalogen ska finnas men vara tom
        assert log_sql_dir.exists()
        assert not (log_sql_dir / "old_dataset").exists()

    def test_cleanup_log_sql_creates_dir(self, temp_dir, monkeypatch):
        """Testa att cleanup_log_sql skapar katalogen om den inte finns."""
        test_settings = Settings()
        log_sql_dir = temp_dir / "log_sql"
        monkeypatch.setattr(test_settings, "LOG_SQL_DIR", log_sql_dir)

        assert not log_sql_dir.exists()
        test_settings.cleanup_log_sql()
        assert log_sql_dir.exists()


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
