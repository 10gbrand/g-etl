"""Laddning och normalisering av datasets.yml.

Stödjer både nytt format (pipelines: [...]) och gammalt format (datasets: [...]).
Returnerar alltid en platt lista av dataset-dicts med 'pipeline' injicerat.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from g_etl.settings import settings


def load_datasets_config(config_path: Path | str | None = None) -> list[dict]:
    """Ladda datasets.yml och returnera platt lista av dataset-dicts.

    Stödjer:
      - Nytt format: pipelines: [{id: ..., datasets: [...]}]
      - Gammalt format: datasets: [{id: ..., pipeline: ..., ...}]

    I nya formatet injiceras 'pipeline' i varje dataset-dict
    från förälder-pipelinen.

    Args:
        config_path: Sökväg till datasets.yml. Default: settings.datasets_path

    Returns:
        Platt lista av dataset-dicts, varje dict har 'pipeline' key.
    """
    path = Path(config_path) if config_path else settings.datasets_path
    if not path.exists():
        return []

    with open(path) as f:
        data = yaml.safe_load(f)

    if not data:
        return []

    # Nytt format: pipelines-grupperat
    if "pipelines" in data:
        return _flatten_pipelines(data["pipelines"])

    # Gammalt format: platt lista (bakåtkompatibilitet)
    return data.get("datasets", [])


def load_pipelines_config(config_path: Path | str | None = None) -> list[dict]:
    """Ladda pipeline-metadata (id, name) från datasets.yml.

    Returnerar lista av pipeline-dicts med 'id', 'name', och 'dataset_count'.
    Stödjer bara nytt format. Gammalt format returnerar tom lista.
    """
    path = Path(config_path) if config_path else settings.datasets_path
    if not path.exists():
        return []

    with open(path) as f:
        data = yaml.safe_load(f)

    if not data or "pipelines" not in data:
        return []

    result = []
    for pipeline in data["pipelines"]:
        datasets = pipeline.get("datasets", [])
        result.append(
            {
                "id": pipeline["id"],
                "name": pipeline.get("name", pipeline["id"]),
                "dataset_count": len(datasets),
                "enabled_count": sum(1 for ds in datasets if ds.get("enabled", True)),
            }
        )
    return result


def _flatten_pipelines(pipelines: list[dict]) -> list[dict]:
    """Platta ut pipelines-struktur till flat dataset-lista.

    Injicerar 'pipeline' key i varje dataset-dict.
    """
    result = []
    for pipeline in pipelines:
        pipeline_id = pipeline.get("id", "")
        for ds in pipeline.get("datasets", []):
            ds["pipeline"] = pipeline_id
            result.append(ds)
    return result
