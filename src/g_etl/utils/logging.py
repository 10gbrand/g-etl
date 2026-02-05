"""Centraliserad loggning för G-ETL.

Används av både CLI/TUI och QGIS-plugin för konsekvent loggning till fil.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

# Default: behåll de senaste 20 loggfilerna
DEFAULT_MAX_LOG_FILES = 20


class FileLogger:
    """Hanterar loggning till fil med automatisk rotation.

    Används av:
    - CLI pipeline (pipeline.py)
    - TUI admin (admin/screens/)
    - QGIS plugin (qgis_runner.py)

    Example:
        logger = FileLogger(logs_dir=Path("logs"), prefix="pipeline")
        log_file = logger.start()
        logger.log("Startar process...")
        logger.close()
    """

    def __init__(
        self,
        logs_dir: Path,
        prefix: str = "pipeline",
        title: str = "G-ETL Pipeline Log",
        max_log_files: int | None = DEFAULT_MAX_LOG_FILES,
    ):
        """Initiera FileLogger.

        Args:
            logs_dir: Katalog för loggfiler.
            prefix: Prefix för loggfilnamn (t.ex. "pipeline" -> "pipeline_2024-01-15_123456.log").
            title: Titel som skrivs i loggfilens header.
            max_log_files: Max antal loggfiler att behålla. None = ingen rotation.
        """
        self.logs_dir = logs_dir
        self.prefix = prefix
        self.title = title
        self.max_log_files = max_log_files
        self.log_file: Path | None = None
        self._file_handle = None

    def start(self) -> Path:
        """Startar en ny loggfil och returnerar sökvägen."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Skapa filnamn med timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.log_file = self.logs_dir / f"{self.prefix}_{timestamp}.log"

        # Öppna filen
        self._file_handle = open(self.log_file, "w", encoding="utf-8")

        # Skriv header
        self._file_handle.write(f"# {self.title}\n")
        self._file_handle.write(f"# Startad: {datetime.now().isoformat()}\n")
        self._file_handle.write(f"# {'=' * 58}\n\n")
        self._file_handle.flush()

        # Rensa gamla loggfiler
        if self.max_log_files is not None:
            self._cleanup_old_logs()

        return self.log_file

    def log(self, message: str) -> None:
        """Skriv ett meddelande till loggfilen."""
        if self._file_handle:
            timestamp = datetime.now().strftime("%H:%M:%S")
            self._file_handle.write(f"[{timestamp}] {message}\n")
            self._file_handle.flush()

    def close(self) -> None:
        """Stänger loggfilen."""
        if self._file_handle:
            self._file_handle.write(f"\n# {'=' * 58}\n")
            self._file_handle.write(f"# Avslutad: {datetime.now().isoformat()}\n")
            self._file_handle.close()
            self._file_handle = None

    def _cleanup_old_logs(self) -> None:
        """Ta bort gamla loggfiler, behåll de senaste max_log_files."""
        if self.max_log_files is None:
            return

        log_files = sorted(
            self.logs_dir.glob(f"{self.prefix}_*.log"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )

        # Ta bort filer utöver max_log_files
        for old_file in log_files[self.max_log_files :]:
            try:
                old_file.unlink()
            except OSError:
                pass  # Ignorera om filen inte kan tas bort

    def __enter__(self) -> FileLogger:
        """Context manager support."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager support."""
        self.close()
