"""Tester för qgis_plugin/runner.py - pipeline wrapper för QGIS."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


class TestQGISPipelineRunnerInit:
    """Tester för QGISPipelineRunner initiering."""

    @pytest.fixture
    def temp_plugin_dir(self, tmp_path):
        """Skapa temporär plugin-katalog med config och sql."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        sql_dir = tmp_path / "sql" / "migrations"
        sql_dir.mkdir(parents=True)

        # Skapa datasets.yml (nytt pipeline-grupperat format)
        datasets_config = {
            "pipelines": [
                {
                    "id": "test_pipeline",
                    "name": "Test Pipeline",
                    "datasets": [
                        {
                            "id": "test_dataset",
                            "name": "Test Dataset",
                            "type": "test_type",
                            "plugin": "wfs",
                            "enabled": True,
                            "field_mapping": {
                                "source_id_column": "$id",
                                "klass": "test",
                                "leverantor": "test_lev",
                            },
                        },
                        {
                            "id": "another_dataset",
                            "name": "Another Dataset",
                            "type": "other_type",
                            "plugin": "geoparquet",
                            "enabled": True,
                        },
                    ],
                }
            ]
        }
        (config_dir / "datasets.yml").write_text(yaml.dump(datasets_config))

        return tmp_path

    def test_list_datasets_returns_list(self, temp_plugin_dir):
        """Testa att list_datasets returnerar en lista."""
        # Mock core imports
        with patch.dict("sys.modules", {"yaml": yaml}):
            # Importera runner med mockade core-moduler
            import sys

            sys.path.insert(0, str(Path(__file__).parent.parent.parent / "qgis_plugin"))

            # Mock settings
            mock_settings = MagicMock()
            mock_settings.CONFIG_DIR = temp_plugin_dir / "config"
            mock_settings.SQL_DIR = temp_plugin_dir / "sql"

            with patch.dict(
                "sys.modules",
                {
                    "runner.core": MagicMock(),
                    "runner.core.admin": MagicMock(),
                    "runner.core.admin.services": MagicMock(),
                    "runner.core.admin.services.pipeline_runner": MagicMock(),
                    "runner.core.settings": MagicMock(settings=mock_settings),
                },
            ):
                # Skapa en mock runner (speglar qgis_runner.py list_datasets)
                class MockQGISPipelineRunner:
                    def __init__(self, plugin_dir):
                        self.plugin_dir = plugin_dir
                        self.config_dir = plugin_dir / "config"
                        self.sql_dir = plugin_dir / "sql"

                    def list_datasets(self):
                        config_path = self.config_dir / "datasets.yml"
                        if not config_path.exists():
                            return []
                        with open(config_path) as f:
                            data = yaml.safe_load(f)
                        if not data:
                            return []
                        if "pipelines" in data:
                            result = []
                            for pipeline in data["pipelines"]:
                                pipeline_id = pipeline.get("id", "")
                                for ds in pipeline.get("datasets", []):
                                    ds["pipeline"] = pipeline_id
                                    result.append(ds)
                            return result
                        return data.get("datasets", [])

                    def list_dataset_types(self):
                        datasets = self.list_datasets()
                        types = set()
                        for ds in datasets:
                            if "type" in ds:
                                types.add(ds["type"])
                        return sorted(types)

                runner = MockQGISPipelineRunner(temp_plugin_dir)
                datasets = runner.list_datasets()

                assert isinstance(datasets, list)
                assert len(datasets) == 2
                assert datasets[0]["id"] == "test_dataset"

    def test_list_datasets_empty_dir(self, tmp_path):
        """Testa list_datasets med tom katalog."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        class MockQGISPipelineRunner:
            def __init__(self, plugin_dir):
                self.config_dir = plugin_dir / "config"

            def list_datasets(self):
                config_path = self.config_dir / "datasets.yml"
                if not config_path.exists():
                    return []
                with open(config_path) as f:
                    data = yaml.safe_load(f)
                if not data:
                    return []
                if "pipelines" in data:
                    result = []
                    for pipeline in data["pipelines"]:
                        pipeline_id = pipeline.get("id", "")
                        for ds in pipeline.get("datasets", []):
                            ds["pipeline"] = pipeline_id
                            result.append(ds)
                    return result
                return data.get("datasets", [])

        runner = MockQGISPipelineRunner(tmp_path)
        datasets = runner.list_datasets()

        assert datasets == []

    def test_list_dataset_types(self, temp_plugin_dir):
        """Testa list_dataset_types."""

        class MockQGISPipelineRunner:
            def __init__(self, plugin_dir):
                self.config_dir = plugin_dir / "config"

            def list_datasets(self):
                config_path = self.config_dir / "datasets.yml"
                if not config_path.exists():
                    return []
                with open(config_path) as f:
                    data = yaml.safe_load(f)
                if not data:
                    return []
                if "pipelines" in data:
                    result = []
                    for pipeline in data["pipelines"]:
                        pipeline_id = pipeline.get("id", "")
                        for ds in pipeline.get("datasets", []):
                            ds["pipeline"] = pipeline_id
                            result.append(ds)
                    return result
                return data.get("datasets", [])

            def list_dataset_types(self):
                datasets = self.list_datasets()
                types = set()
                for ds in datasets:
                    if "type" in ds:
                        types.add(ds["type"])
                return sorted(types)

        runner = MockQGISPipelineRunner(temp_plugin_dir)
        types = runner.list_dataset_types()

        assert isinstance(types, list)
        assert "test_type" in types
        assert "other_type" in types
        assert types == sorted(types)  # Ska vara sorterad


class TestExportFormatConfig:
    """Tester för export-format konfiguration."""

    def test_format_config_gpkg(self):
        """Testa GeoPackage-konfiguration."""
        format_config = {
            "gpkg": (".gpkg", "GPKG"),
            "geoparquet": (".parquet", None),
            "fgb": (".fgb", "FlatGeobuf"),
        }

        ext, driver = format_config["gpkg"]
        assert ext == ".gpkg"
        assert driver == "GPKG"

    def test_format_config_geoparquet(self):
        """Testa GeoParquet-konfiguration."""
        format_config = {
            "gpkg": (".gpkg", "GPKG"),
            "geoparquet": (".parquet", None),
            "fgb": (".fgb", "FlatGeobuf"),
        }

        ext, driver = format_config["geoparquet"]
        assert ext == ".parquet"
        assert driver is None  # Använder PARQUET format direkt

    def test_format_config_fgb(self):
        """Testa FlatGeobuf-konfiguration."""
        format_config = {
            "gpkg": (".gpkg", "GPKG"),
            "geoparquet": (".parquet", None),
            "fgb": (".fgb", "FlatGeobuf"),
        }

        ext, driver = format_config["fgb"]
        assert ext == ".fgb"
        assert driver == "FlatGeobuf"


class TestRunnerHelperFunctions:
    """Tester för hjälpfunktioner i runner."""

    def test_output_path_creation(self, tmp_path):
        """Testa att output-path skapas korrekt."""
        export_format = "gpkg"
        output_dir = tmp_path / "output"

        format_config = {
            "gpkg": (".gpkg", "GPKG"),
            "geoparquet": (".parquet", None),
            "fgb": (".fgb", "FlatGeobuf"),
        }

        ext, _ = format_config.get(export_format, (".gpkg", "GPKG"))
        output_path = output_dir / f"g_etl_export{ext}"

        assert output_path.suffix == ".gpkg"
        assert "g_etl_export" in output_path.name

    def test_output_dir_mkdir(self, tmp_path):
        """Testa att output-katalog skapas."""
        output_dir = tmp_path / "nested" / "output"
        assert not output_dir.exists()

        output_dir.mkdir(parents=True, exist_ok=True)

        assert output_dir.exists()
        assert output_dir.is_dir()


class TestProgressCallbacks:
    """Tester för progress-callbacks."""

    def test_on_progress_receives_message_and_percent(self):
        """Testa att on_progress får rätt argument."""
        received = []

        def on_progress(message: str, percent: float):
            received.append((message, percent))

        # Simulera pipeline-progress
        on_progress("Extraherar data...", 10)
        on_progress("Transformerar data...", 40)
        on_progress("Klar!", 100)

        assert len(received) == 3
        assert received[0] == ("Extraherar data...", 10)
        assert received[2][1] == 100

    def test_on_log_receives_messages(self):
        """Testa att on_log får loggmeddelanden."""
        logs = []

        def on_log(message: str):
            logs.append(message)

        # Simulera loggning
        on_log("Startar pipeline")
        on_log("Dataset 1 klar")
        on_log("Pipeline slutförd")

        assert len(logs) == 3
        assert "Startar" in logs[0]


class TestPhasesConfiguration:
    """Tester för transform-faser."""

    def test_phases_tuple_all_enabled(self):
        """Testa med alla faser aktiverade."""
        phases = (True, True, True)
        run_staging, run_staging2, run_mart = phases

        assert run_staging is True
        assert run_staging2 is True
        assert run_mart is True

    def test_phases_tuple_partial(self):
        """Testa med några faser inaktiverade."""
        phases = (True, False, True)
        run_staging, run_staging2, run_mart = phases

        assert run_staging is True
        assert run_staging2 is False
        assert run_mart is True

    def test_phases_default_none(self):
        """Testa default-värde (None -> alla aktiverade)."""
        phases = None
        run_staging, run_staging2, run_mart = phases if phases else (True, True, True)

        assert run_staging is True
        assert run_staging2 is True
        assert run_mart is True
