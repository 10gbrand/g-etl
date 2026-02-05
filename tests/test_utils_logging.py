"""Tester för centraliserad FileLogger."""

import re
from pathlib import Path

from g_etl.utils.logging import DEFAULT_MAX_LOG_FILES, FileLogger


class TestFileLogger:
    """Tester för FileLogger-klassen."""

    def test_creates_log_file(self, tmp_path: Path):
        """FileLogger skapar loggfil med korrekt namn."""
        logger = FileLogger(logs_dir=tmp_path, prefix="test")
        log_file = logger.start()

        assert log_file.exists()
        assert log_file.parent == tmp_path
        assert log_file.name.startswith("test_")
        assert log_file.suffix == ".log"

        logger.close()

    def test_log_file_has_header(self, tmp_path: Path):
        """Loggfilen har korrekt header med titel och timestamp."""
        logger = FileLogger(logs_dir=tmp_path, prefix="test", title="Min Test Logg")
        log_file = logger.start()
        logger.close()

        content = log_file.read_text()
        assert "# Min Test Logg" in content
        assert "# Startad:" in content
        assert "# =" in content

    def test_log_writes_timestamped_messages(self, tmp_path: Path):
        """Log-meddelanden får timestamp i format [HH:MM:SS]."""
        logger = FileLogger(logs_dir=tmp_path, prefix="test")
        logger.start()
        logger.log("Test meddelande 1")
        logger.log("Test meddelande 2")
        logger.close()

        content = logger.log_file.read_text()
        # Kolla att meddelanden finns med timestamp
        assert re.search(r"\[\d{2}:\d{2}:\d{2}\] Test meddelande 1", content)
        assert re.search(r"\[\d{2}:\d{2}:\d{2}\] Test meddelande 2", content)

    def test_close_writes_footer(self, tmp_path: Path):
        """Close skriver footer med avslutnings-timestamp."""
        logger = FileLogger(logs_dir=tmp_path, prefix="test")
        logger.start()
        logger.close()

        content = logger.log_file.read_text()
        assert "# Avslutad:" in content

    def test_context_manager(self, tmp_path: Path):
        """FileLogger fungerar som context manager."""
        with FileLogger(logs_dir=tmp_path, prefix="test") as logger:
            logger.log("Via context manager")

        assert logger.log_file.exists()
        content = logger.log_file.read_text()
        assert "Via context manager" in content
        assert "# Avslutad:" in content

    def test_creates_logs_directory(self, tmp_path: Path):
        """FileLogger skapar logs-katalogen om den inte finns."""
        nested_dir = tmp_path / "nested" / "logs"
        assert not nested_dir.exists()

        logger = FileLogger(logs_dir=nested_dir, prefix="test")
        logger.start()
        logger.close()

        assert nested_dir.exists()
        assert logger.log_file.exists()

    def test_cleanup_old_logs(self, tmp_path: Path):
        """FileLogger rensar gamla loggfiler baserat på max_log_files."""
        # Skapa 5 "gamla" loggfiler
        for i in range(5):
            (tmp_path / f"test_2024-01-0{i}_120000.log").write_text(f"log {i}")

        # Skapa en ny med max_log_files=3
        logger = FileLogger(logs_dir=tmp_path, prefix="test", max_log_files=3)
        logger.start()
        logger.close()

        # Det ska nu finnas max 3 loggfiler
        log_files = list(tmp_path.glob("test_*.log"))
        assert len(log_files) == 3

    def test_no_cleanup_when_disabled(self, tmp_path: Path):
        """Ingen cleanup när max_log_files=None."""
        # Skapa 5 "gamla" loggfiler
        for i in range(5):
            (tmp_path / f"test_2024-01-0{i}_120000.log").write_text(f"log {i}")

        # Skapa en ny med max_log_files=None
        logger = FileLogger(logs_dir=tmp_path, prefix="test", max_log_files=None)
        logger.start()
        logger.close()

        # Alla 6 loggfiler ska finnas kvar
        log_files = list(tmp_path.glob("test_*.log"))
        assert len(log_files) == 6

    def test_default_max_log_files(self):
        """DEFAULT_MAX_LOG_FILES har rimligt värde."""
        assert DEFAULT_MAX_LOG_FILES == 20

    def test_different_prefixes_dont_interfere(self, tmp_path: Path):
        """Cleanup påverkar bara filer med samma prefix."""
        # Skapa filer med olika prefix
        for i in range(3):
            (tmp_path / f"alpha_2024-01-0{i}_120000.log").write_text(f"alpha {i}")
            (tmp_path / f"beta_2024-01-0{i}_120000.log").write_text(f"beta {i}")

        # Skapa ny alpha-logg med max_log_files=2
        logger = FileLogger(logs_dir=tmp_path, prefix="alpha", max_log_files=2)
        logger.start()
        logger.close()

        # Alpha ska ha 2 filer, beta ska fortfarande ha 3
        alpha_files = list(tmp_path.glob("alpha_*.log"))
        beta_files = list(tmp_path.glob("beta_*.log"))
        assert len(alpha_files) == 2
        assert len(beta_files) == 3

    def test_log_before_start_does_nothing(self, tmp_path: Path):
        """Log före start() gör inget (ingen exception)."""
        logger = FileLogger(logs_dir=tmp_path, prefix="test")
        # Ska inte krascha
        logger.log("Detta ska inte skrivas")
        assert logger.log_file is None

    def test_close_before_start_does_nothing(self, tmp_path: Path):
        """Close före start() gör inget (ingen exception)."""
        logger = FileLogger(logs_dir=tmp_path, prefix="test")
        # Ska inte krascha
        logger.close()


class TestFileLoggerBackwardsCompatibility:
    """Tester för bakåtkompatibilitet."""

    def test_import_from_pipeline(self):
        """FileLogger kan importeras från g_etl.pipeline (bakåtkompatibilitet)."""
        from g_etl.pipeline import FileLogger as PipelineFileLogger
        from g_etl.utils.logging import FileLogger as UtilsFileLogger

        # Ska vara samma klass
        assert PipelineFileLogger is UtilsFileLogger

    def test_import_from_utils(self):
        """FileLogger kan importeras från g_etl.utils."""
        from g_etl.utils import FileLogger

        assert FileLogger is not None
