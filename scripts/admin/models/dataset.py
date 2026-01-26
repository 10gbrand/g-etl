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
    plugin: str
    typ: str = ""  # Typ för gruppering (t.ex. naturvardsverket_wfs)
    enabled: bool = True
    status: DatasetStatus = DatasetStatus.PENDING
    progress: int = 0
    total_steps: int = 0
    error_message: str = ""
    elapsed_seconds: float = 0.0
    # Plugin-specifika config-värden
    config: dict = field(default_factory=dict)

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

        datasets = []
        for d in data.get("datasets", []):
            # Extrahera känd config, resten går till config-dict
            dataset = Dataset(
                id=d["id"],
                name=d["name"],
                description=d.get("description", ""),
                plugin=d.get("plugin", "unknown"),
                typ=d.get("typ", ""),
                enabled=d.get("enabled", True),
                config=d,  # Hela dict:en för plugin-användning
            )
            datasets.append(dataset)

        return cls(datasets=datasets)

    def get_enabled(self) -> list[Dataset]:
        return [d for d in self.datasets if d.enabled]

    def get_by_type(self, typ: str) -> list[Dataset]:
        """Filtrera datasets efter typ."""
        return [d for d in self.datasets if d.typ == typ and d.enabled]

    def get_types(self) -> list[str]:
        """Hämta alla unika typer."""
        return list(set(d.typ for d in self.datasets if d.typ))

    def get_by_id(self, dataset_id: str) -> Dataset | None:
        for d in self.datasets:
            if d.id == dataset_id:
                return d
        return None
