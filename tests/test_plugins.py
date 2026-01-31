"""Tester för plugins."""

from unittest.mock import MagicMock

import duckdb
import pytest

from plugins import PLUGINS, get_plugin
from plugins.base import ExtractResult, SourcePlugin


class TestExtractResult:
    """Tester för ExtractResult dataclass."""

    def test_default_values(self):
        """Kontrollera default-värden."""
        result = ExtractResult(success=True)
        assert result.success is True
        assert result.rows_count == 0
        assert result.message == ""
        assert result.output_path is None

    def test_with_all_values(self):
        """Testa med alla värden satta."""
        result = ExtractResult(
            success=True,
            rows_count=1000,
            message="Läste 1000 rader",
            output_path="/path/to/file.parquet",
        )
        assert result.success is True
        assert result.rows_count == 1000
        assert result.message == "Läste 1000 rader"
        assert result.output_path == "/path/to/file.parquet"

    def test_failure_result(self):
        """Testa failure-resultat."""
        result = ExtractResult(success=False, message="Fel vid nedladdning")
        assert result.success is False
        assert result.message == "Fel vid nedladdning"


class ConcretePlugin(SourcePlugin):
    """Konkret implementation för testning."""

    @property
    def name(self) -> str:
        return "test_plugin"

    def extract(self, config, conn, on_log=None, on_progress=None):
        table_name = config.get("id", "test")

        # Skapa en enkel testtabell
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.{table_name} AS
            SELECT 1 as id, 'test' as name
        """)

        self._log("Extraherade data", on_log)
        self._progress(1.0, "Klar", on_progress)

        return ExtractResult(success=True, rows_count=1, message="OK")


class TestSourcePluginBase:
    """Tester för SourcePlugin-basklassen."""

    @pytest.fixture
    def plugin(self):
        """Skapa konkret plugin-instans."""
        return ConcretePlugin()

    def test_name_property(self, plugin):
        """Testa name-property."""
        assert plugin.name == "test_plugin"

    def test_extract_basic(self, plugin, duckdb_conn):
        """Testa grundläggande extract."""
        config = {"id": "test_table"}
        result = plugin.extract(config, duckdb_conn)

        assert result.success is True
        assert result.rows_count == 1

        # Verifiera att tabellen skapades
        count = duckdb_conn.execute("SELECT COUNT(*) FROM raw.test_table").fetchone()[0]
        assert count == 1

    def test_log_callback(self, plugin, duckdb_conn):
        """Testa logg-callback."""
        logs = []
        on_log = lambda msg: logs.append(msg)

        config = {"id": "test"}
        plugin.extract(config, duckdb_conn, on_log=on_log)

        assert len(logs) > 0
        assert "Extraherade data" in logs[0]

    def test_progress_callback(self, plugin, duckdb_conn):
        """Testa progress-callback."""
        progress_updates = []
        on_progress = lambda p, m: progress_updates.append((p, m))

        config = {"id": "test"}
        plugin.extract(config, duckdb_conn, on_progress=on_progress)

        assert len(progress_updates) > 0
        assert progress_updates[-1][0] == 1.0  # Sista uppdateringen ska vara 100%

    def test_log_with_none_callback(self, plugin):
        """Testa att _log hanterar None-callback."""
        # Ska inte krascha
        plugin._log("Test message", None)

    def test_progress_with_none_callback(self, plugin):
        """Testa att _progress hanterar None-callback."""
        # Ska inte krascha
        plugin._progress(0.5, "Halvvägs", None)

    def test_extract_to_parquet(self, plugin, temp_dir):
        """Testa extract_to_parquet."""
        config = {"id": "parquet_test"}
        result = plugin.extract_to_parquet(config, temp_dir)

        assert result.success is True
        assert result.output_path is not None
        assert result.output_path.endswith(".parquet")

        # Verifiera att filen skapades
        from pathlib import Path

        assert Path(result.output_path).exists()

    def test_extract_to_parquet_creates_dir(self, plugin, temp_dir):
        """Testa att extract_to_parquet skapar katalog."""
        output_dir = temp_dir / "subdir" / "output"
        config = {"id": "test"}
        result = plugin.extract_to_parquet(config, output_dir)

        assert result.success is True
        assert output_dir.exists()


class TestPluginRegistry:
    """Tester för plugin-registret."""

    def test_plugins_dict_exists(self):
        """Kontrollera att PLUGINS finns."""
        assert isinstance(PLUGINS, dict)

    def test_required_plugins_registered(self):
        """Kontrollera att nödvändiga plugins är registrerade."""
        required = ["wfs", "lantmateriet", "geoparquet", "zip_geopackage", "zip_shapefile"]
        for plugin_name in required:
            assert plugin_name in PLUGINS, f"Plugin '{plugin_name}' saknas"

    def test_get_plugin_valid(self):
        """Testa get_plugin med giltig plugin."""
        plugin = get_plugin("wfs")
        assert isinstance(plugin, SourcePlugin)
        assert plugin.name == "wfs"

    def test_get_plugin_invalid(self):
        """Testa get_plugin med ogiltig plugin."""
        with pytest.raises(ValueError) as excinfo:
            get_plugin("nonexistent_plugin")
        assert "Okänd plugin" in str(excinfo.value)

    def test_all_plugins_have_name(self):
        """Kontrollera att alla plugins har name-property."""
        for plugin_name, plugin_class in PLUGINS.items():
            plugin = plugin_class()
            assert plugin.name == plugin_name

    def test_all_plugins_implement_extract(self):
        """Kontrollera att alla plugins implementerar extract."""
        for plugin_name, plugin_class in PLUGINS.items():
            plugin = plugin_class()
            assert hasattr(plugin, "extract")
            assert callable(plugin.extract)


class FailingPlugin(SourcePlugin):
    """Plugin som alltid misslyckas - för testning."""

    @property
    def name(self) -> str:
        return "failing_plugin"

    def extract(self, config, conn, on_log=None, on_progress=None):
        return ExtractResult(success=False, message="Simulerat fel")


class TestPluginFailure:
    """Tester för plugin-fel."""

    def test_extract_to_parquet_failure(self, temp_dir):
        """Testa att extract_to_parquet hanterar fel."""
        plugin = FailingPlugin()
        config = {"id": "test"}
        result = plugin.extract_to_parquet(config, temp_dir)

        assert result.success is False
        assert "Simulerat fel" in result.message


class TestZipGeoPackagePlugin:
    """Tester specifika för ZipGeoPackagePlugin."""

    def test_clear_download_cache_import(self):
        """Testa att clear_download_cache kan importeras."""
        from plugins.zip_geopackage import clear_download_cache

        # Ska inte krascha
        clear_download_cache()

    def test_is_url_function(self):
        """Testa URL-detektering."""
        from plugins.zip_geopackage import _is_url

        assert _is_url("https://example.com/file.zip") is True
        assert _is_url("http://example.com/file.zip") is True
        assert _is_url("/path/to/local/file.zip") is False
        assert _is_url("relative/path.zip") is False

    def test_get_url_lock(self):
        """Testa URL-lås."""
        from plugins.zip_geopackage import _get_url_lock

        lock1 = _get_url_lock("https://example.com/1")
        lock2 = _get_url_lock("https://example.com/1")
        lock3 = _get_url_lock("https://example.com/2")

        # Samma URL ska ge samma lås
        assert lock1 is lock2
        # Olika URL ska ge olika lås
        assert lock1 is not lock3
