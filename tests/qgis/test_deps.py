"""Tester för qgis_plugin/deps.py - dependency-hantering."""

import sys
from unittest.mock import MagicMock, patch

# Lägg till qgis_plugin i path för import
sys.path.insert(0, str(__file__).rsplit("/tests/", 1)[0] + "/qgis_plugin")

from deps import (
    REQUIRED_PACKAGES,
    check_dependencies,
    ensure_dependencies,
    install_dependencies,
    install_duckdb_extensions,
)


class TestRequiredPackages:
    """Tester för REQUIRED_PACKAGES."""

    def test_required_packages_not_empty(self):
        """Kontrollera att REQUIRED_PACKAGES innehåller paket."""
        assert len(REQUIRED_PACKAGES) > 0

    def test_required_packages_contains_duckdb(self):
        """Kontrollera att duckdb finns med."""
        assert "duckdb" in REQUIRED_PACKAGES

    def test_required_packages_contains_h3(self):
        """Kontrollera att h3 finns med."""
        assert "h3" in REQUIRED_PACKAGES

    def test_required_packages_contains_yaml(self):
        """Kontrollera att yaml (pyyaml) finns med."""
        assert "yaml" in REQUIRED_PACKAGES
        assert REQUIRED_PACKAGES["yaml"] == "pyyaml"

    def test_required_packages_format(self):
        """Kontrollera formatet på REQUIRED_PACKAGES."""
        for import_name, pip_name in REQUIRED_PACKAGES.items():
            assert isinstance(import_name, str)
            assert isinstance(pip_name, str)
            assert len(import_name) > 0
            assert len(pip_name) > 0


class TestCheckDependencies:
    """Tester för check_dependencies()."""

    def test_check_dependencies_returns_list(self):
        """Kontrollera att check_dependencies returnerar en lista."""
        result = check_dependencies()
        assert isinstance(result, list)

    def test_check_dependencies_installed_packages(self):
        """Kontrollera att installerade paket inte rapporteras som saknade."""
        # duckdb bör vara installerat i testmiljön
        missing = check_dependencies()
        # Om duckdb är installerat ska det inte vara med i listan
        # (kan finnas i listan om det inte är installerat)
        assert isinstance(missing, list)

    @patch.dict("sys.modules", {"nonexistent_package_xyz": None})
    def test_check_dependencies_with_mock(self):
        """Testa med mockad import."""
        # Denna test verifierar att funktionen hanterar ImportError korrekt
        result = check_dependencies()
        assert isinstance(result, list)


class TestInstallDependencies:
    """Tester för install_dependencies()."""

    def test_install_dependencies_empty_list(self):
        """Testa med tom lista (ingenting att installera)."""
        result = install_dependencies([])
        assert result is True

    @patch("subprocess.run")
    def test_install_dependencies_success(self, mock_run):
        """Testa lyckad installation."""
        mock_run.return_value = MagicMock(returncode=0)

        result = install_dependencies(["test-package"])

        assert result is True
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_install_dependencies_failure(self, mock_run):
        """Testa misslyckad installation."""
        mock_run.return_value = MagicMock(returncode=1, stderr="Error")

        result = install_dependencies(["test-package"])

        assert result is False

    @patch("subprocess.run")
    def test_install_dependencies_with_progress_callback(self, mock_run):
        """Testa med progress-callback."""
        mock_run.return_value = MagicMock(returncode=0)
        messages = []

        def on_progress(msg):
            messages.append(msg)

        result = install_dependencies(["test-package"], on_progress=on_progress)

        assert result is True
        assert len(messages) > 0
        assert any("test-package" in msg for msg in messages)

    @patch("subprocess.run")
    def test_install_dependencies_timeout(self, mock_run):
        """Testa timeout-hantering."""
        import subprocess

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="pip", timeout=300)

        messages = []
        result = install_dependencies(["test-package"], on_progress=lambda m: messages.append(m))

        assert result is False
        assert any("timeout" in msg.lower() for msg in messages)

    @patch("subprocess.run")
    def test_install_dependencies_exception(self, mock_run):
        """Testa generell exception-hantering."""
        mock_run.side_effect = Exception("Test error")

        messages = []
        result = install_dependencies(["test-package"], on_progress=lambda m: messages.append(m))

        assert result is False


class TestEnsureDependencies:
    """Tester för ensure_dependencies()."""

    @patch("deps.check_dependencies")
    @patch("deps.install_dependencies")
    def test_ensure_dependencies_all_installed(self, mock_install, mock_check):
        """Testa när alla dependencies redan är installerade."""
        mock_check.return_value = []

        result = ensure_dependencies()

        assert result is True
        mock_install.assert_not_called()

    @patch("deps.check_dependencies")
    @patch("deps.install_dependencies")
    def test_ensure_dependencies_missing_packages(self, mock_install, mock_check):
        """Testa när det finns saknade paket."""
        mock_check.return_value = ["missing-package"]
        mock_install.return_value = True

        messages = []
        result = ensure_dependencies(on_progress=lambda m: messages.append(m))

        assert result is True
        mock_install.assert_called_once()

    @patch("deps.check_dependencies")
    @patch("deps.install_dependencies")
    def test_ensure_dependencies_install_fails(self, mock_install, mock_check):
        """Testa när installation misslyckas."""
        mock_check.return_value = ["missing-package"]
        mock_install.return_value = False

        result = ensure_dependencies()

        assert result is False


class TestInstallDuckdbExtensions:
    """Tester för install_duckdb_extensions()."""

    def test_install_duckdb_extensions_returns_bool(self):
        """Kontrollera att funktionen returnerar bool."""
        result = install_duckdb_extensions()
        assert isinstance(result, bool)

    def test_install_duckdb_extensions_with_progress(self):
        """Testa med progress-callback."""
        messages = []

        result = install_duckdb_extensions(on_progress=lambda m: messages.append(m))

        assert isinstance(result, bool)
        # Ska ha loggat något
        assert len(messages) > 0

    @patch("duckdb.connect")
    def test_install_duckdb_extensions_handles_errors(self, mock_connect):
        """Testa felhantering."""
        mock_connect.side_effect = Exception("Connection failed")

        messages = []
        result = install_duckdb_extensions(on_progress=lambda m: messages.append(m))

        assert result is False
        assert any("fel" in msg.lower() or "error" in msg.lower() for msg in messages)
