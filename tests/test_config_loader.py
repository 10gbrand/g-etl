"""Tester för config_loader.py."""

import yaml

from g_etl.config_loader import _flatten_pipelines, load_datasets_config, load_pipelines_config


class TestFlattenPipelines:
    """Tester för _flatten_pipelines."""

    def test_basic_flatten(self):
        """Plattar ut en pipeline till dataset-lista med pipeline injicerat."""
        pipelines = [
            {
                "id": "ext_restr",
                "name": "Test",
                "datasets": [
                    {"id": "ds1", "name": "DS1", "plugin": "wfs"},
                    {"id": "ds2", "name": "DS2", "plugin": "wfs"},
                ],
            },
        ]
        result = _flatten_pipelines(pipelines)
        assert len(result) == 2
        assert result[0]["pipeline"] == "ext_restr"
        assert result[0]["id"] == "ds1"
        assert result[1]["pipeline"] == "ext_restr"
        assert result[1]["id"] == "ds2"

    def test_multiple_pipelines(self):
        """Datasets från flera pipelines plattas ut i ordning."""
        pipelines = [
            {"id": "p1", "datasets": [{"id": "ds1"}]},
            {"id": "p2", "datasets": [{"id": "ds2"}, {"id": "ds3"}]},
        ]
        result = _flatten_pipelines(pipelines)
        assert len(result) == 3
        assert result[0]["pipeline"] == "p1"
        assert result[1]["pipeline"] == "p2"
        assert result[2]["pipeline"] == "p2"

    def test_empty_pipeline(self):
        """Tom pipeline ger inga datasets."""
        pipelines = [{"id": "empty", "datasets": []}]
        result = _flatten_pipelines(pipelines)
        assert result == []

    def test_pipeline_without_datasets_key(self):
        """Pipeline utan datasets-nyckel ger inga datasets."""
        pipelines = [{"id": "broken"}]
        result = _flatten_pipelines(pipelines)
        assert result == []


class TestLoadDatasetsConfig:
    """Tester för load_datasets_config."""

    def test_new_format(self, tmp_path):
        """Nytt format (pipelines) laddas och plattas ut."""
        config = {
            "pipelines": [
                {
                    "id": "ext_restr",
                    "name": "Externa restriktioner",
                    "datasets": [
                        {"id": "ds1", "name": "DS1", "plugin": "wfs"},
                    ],
                },
            ]
        }
        path = tmp_path / "datasets.yml"
        path.write_text(yaml.dump(config))
        result = load_datasets_config(path)
        assert len(result) == 1
        assert result[0]["pipeline"] == "ext_restr"
        assert result[0]["id"] == "ds1"

    def test_old_format_backward_compat(self, tmp_path):
        """Gammalt format (datasets) fungerar oförändrat."""
        config = {
            "datasets": [
                {"id": "ds1", "name": "DS1", "pipeline": "ext_restr"},
            ]
        }
        path = tmp_path / "datasets.yml"
        path.write_text(yaml.dump(config))
        result = load_datasets_config(path)
        assert len(result) == 1
        assert result[0]["pipeline"] == "ext_restr"

    def test_missing_file(self, tmp_path):
        """Saknad fil returnerar tom lista."""
        result = load_datasets_config(tmp_path / "nonexistent.yml")
        assert result == []

    def test_empty_file(self, tmp_path):
        """Tom YAML-fil returnerar tom lista."""
        path = tmp_path / "datasets.yml"
        path.write_text("")
        result = load_datasets_config(path)
        assert result == []


class TestLoadPipelinesConfig:
    """Tester för load_pipelines_config."""

    def test_pipelines_metadata(self, tmp_path):
        """Laddar pipeline-metadata korrekt."""
        config = {
            "pipelines": [
                {
                    "id": "ext_restr",
                    "name": "Externa restriktioner",
                    "datasets": [
                        {"id": "ds1", "enabled": True},
                        {"id": "ds2", "enabled": False},
                        {"id": "ds3"},  # enabled default = True
                    ],
                },
            ]
        }
        path = tmp_path / "datasets.yml"
        path.write_text(yaml.dump(config))
        result = load_pipelines_config(path)
        assert len(result) == 1
        assert result[0]["id"] == "ext_restr"
        assert result[0]["name"] == "Externa restriktioner"
        assert result[0]["dataset_count"] == 3
        assert result[0]["enabled_count"] == 2

    def test_old_format_returns_empty(self, tmp_path):
        """Gammalt format returnerar tom lista."""
        config = {"datasets": [{"id": "ds1"}]}
        path = tmp_path / "datasets.yml"
        path.write_text(yaml.dump(config))
        result = load_pipelines_config(path)
        assert result == []
