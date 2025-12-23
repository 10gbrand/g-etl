from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import yaml


class DatasetStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Dataset:
    id: str
    name: str
    description: str
    dbt_model: str
    source_type: str
    enabled: bool = True
    status: DatasetStatus = DatasetStatus.PENDING
    progress: int = 0
    total_steps: int = 0
    error_message: str = ""
    elapsed_seconds: float = 0.0

    @property
    def status_icon(self) -> str:
        icons = {
            DatasetStatus.PENDING: "○",
            DatasetStatus.RUNNING: "⟳",
            DatasetStatus.COMPLETED: "✓",
            DatasetStatus.FAILED: "✗",
        }
        return icons.get(self.status, "?")

    @property
    def progress_percent(self) -> int:
        if self.total_steps == 0:
            return 0
        return int((self.progress / self.total_steps) * 100)


@dataclass
class DatasetConfig:
    datasets: list[Dataset] = field(default_factory=list)

    @classmethod
    def load(cls, config_path: Path | str = "config/datasets.yml") -> "DatasetConfig":
        path = Path(config_path)
        if not path.exists():
            return cls()

        with open(path) as f:
            data = yaml.safe_load(f)

        datasets = [
            Dataset(
                id=d["id"],
                name=d["name"],
                description=d.get("description", ""),
                dbt_model=d["dbt_model"],
                source_type=d.get("source_type", "unknown"),
                enabled=d.get("enabled", True),
            )
            for d in data.get("datasets", [])
        ]

        return cls(datasets=datasets)

    def get_enabled(self) -> list[Dataset]:
        return [d for d in self.datasets if d.enabled]

    def get_by_id(self, dataset_id: str) -> Dataset | None:
        for d in self.datasets:
            if d.id == dataset_id:
                return d
        return None
