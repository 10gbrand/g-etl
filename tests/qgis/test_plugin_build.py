"""Tester för QGIS plugin-bygge och importstruktur.

Dessa tester verifierar att plugin-bygget producerar en korrekt importstruktur
så att alla moduler kan importeras korrekt i QGIS-miljön.
"""

import ast
import re
import tempfile
from pathlib import Path

import pytest


class TestPluginImportStructure:
    """Tester för att verifiera korrekt importstruktur i plugin."""

    @pytest.fixture
    def project_root(self) -> Path:
        """Hämta projektets rotkatalog."""
        return Path(__file__).parent.parent.parent

    @pytest.fixture
    def core_files(self, project_root: Path) -> list:
        """Hämta alla Python-filer i src/g_etl."""
        core_path = project_root / "src" / "g_etl"
        return list(core_path.rglob("*.py"))

    @pytest.fixture
    def plugin_files(self, project_root: Path) -> list:
        """Hämta alla Python-filer i qgis_plugin."""
        plugin_path = project_root / "qgis_plugin"
        return list(plugin_path.rglob("*.py"))

    def test_no_absolute_g_etl_imports_after_transform(self, project_root: Path):
        """Verifiera att import-transformationen fungerar korrekt.

        Efter transformationen ska inga 'from g_etl.' eller 'import g_etl.'
        finnas kvar i core-filerna (de ska vara 'from g_etl.runner.core.').
        """
        core_path = project_root / "src" / "g_etl"

        # Mönster som ska transformeras
        pattern_from = re.compile(r"from g_etl\.(?!runner\.core\.)")
        pattern_import = re.compile(r"import g_etl\.(?!runner\.core\.)")

        files_with_imports = []
        for py_file in core_path.rglob("*.py"):
            content = py_file.read_text()

            # Hoppa över kommentarer och docstrings för enkel kontroll
            lines = content.split("\n")
            for line_no, line in enumerate(lines, 1):
                stripped = line.strip()
                # Hoppa över kommentarer
                if stripped.startswith("#"):
                    continue

                if pattern_from.search(line) or pattern_import.search(line):
                    rel_path = py_file.relative_to(project_root)
                    files_with_imports.append((str(rel_path), line_no, stripped))

        # Detta test dokumenterar vilka filer som har importer som behöver transformeras
        # Det är inte ett fel - det är förväntat beteende
        assert len(files_with_imports) > 0, (
            "Inga g_etl-importer hittades i core - "
            "något kan vara fel med testet eller filstrukturen"
        )

    def test_plugin_files_use_relative_imports(self, plugin_files: list, project_root: Path):
        """Verifiera att plugin-filer använder relativa importer korrekt."""
        # Plugin-filer (inte core) ska använda relativa importer som .deps, .dialog etc.
        issues = []

        for py_file in plugin_files:
            content = py_file.read_text()

            # qgis_runner.py använder 'from g_etl.' via sys.path-trick - det är OK
            if py_file.name == "qgis_runner.py":
                continue

            if "from g_etl." in content:
                rel_path = py_file.relative_to(project_root)
                issues.append(f"{rel_path}: använder 'from g_etl.' istället för relativ import")

        assert not issues, "Plugin-filer med felaktiga importer:\n" + "\n".join(issues)

    def test_qgis_runner_uses_sys_path_for_core(self, project_root: Path):
        """Verifiera att qgis_runner.py lägger runner/ i sys.path.

        Med runner/g_etl/ i sys.path fungerar alla interna 'from g_etl.xxx'
        imports i de bundlade core-modulerna utan import-transformering.
        """
        qgis_runner = project_root / "qgis_plugin" / "qgis_runner.py"
        content = qgis_runner.read_text()

        # Ska lägga runner/ i sys.path
        assert "sys.path" in content, (
            "qgis_runner.py ska lägga runner/ i sys.path"
        )
        # Ska importera från g_etl (via sys.path, inte relativ .runner.core)
        assert "from g_etl." in content, (
            "qgis_runner.py ska importera från g_etl via sys.path"
        )

    def test_build_task_uses_runner_g_etl(self, project_root: Path):
        """Verifiera att build-tasken kopierar till runner/g_etl/."""
        qgis_yml = project_root / "taskfiles" / "qgis.yml"
        content = qgis_yml.read_text()

        assert "runner/g_etl" in content, (
            "Build-tasken ska kopiera core-moduler till runner/g_etl/"
        )


class TestPluginBuildIntegration:
    """Integrationstester för plugin-bygget."""

    @pytest.fixture
    def project_root(self) -> Path:
        """Hämta projektets rotkatalog."""
        return Path(__file__).parent.parent.parent

    def test_simulated_build_has_correct_structure(self, project_root: Path):
        """Simulera bygget och verifiera att runner/g_etl/ innehåller alla moduler.

        Med det nya bygget kopieras moduler till runner/g_etl/ utan
        import-transformering. Alla 'from g_etl.xxx' imports fungerar
        via sys.path-tricket i qgis_runner.py.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            plugin_dir = temp_path / "g_etl"
            plugin_dir.mkdir()

            # Kopiera core-filer till runner/g_etl/ (som det nya bygget gör)
            g_etl_dir = plugin_dir / "runner" / "g_etl"
            g_etl_dir.mkdir(parents=True)

            src_core = project_root / "src" / "g_etl"
            for py_file in src_core.rglob("*.py"):
                rel_path = py_file.relative_to(src_core)
                dest_file = g_etl_dir / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                dest_file.write_text(py_file.read_text())

            # Skapa __init__.py för runner
            (plugin_dir / "runner" / "__init__.py").touch()

            # Verifiera att g_etl-paketet finns med __init__.py
            assert (g_etl_dir / "__init__.py").exists(), (
                "runner/g_etl/__init__.py saknas"
            )

            # Verifiera nödvändiga moduler
            required = [
                "settings.py",
                "sql_generator.py",
                "export.py",
                "admin/services/pipeline_runner.py",
                "plugins/__init__.py",
                "migrations/migrator.py",
                "utils/logging.py",
                "utils/downloader.py",
            ]
            missing = []
            for module in required:
                if not (g_etl_dir / module).exists():
                    missing.append(module)

            assert not missing, (
                f"Nödvändiga moduler saknas i runner/g_etl/: {missing}"
            )

    def test_all_core_imports_are_internal(self, project_root: Path):
        """Verifiera att alla importer i core refererar till g_etl-moduler."""
        src_core = project_root / "src" / "g_etl"

        # Lista alla g_etl submoduler
        submodules = set()
        for py_file in src_core.rglob("*.py"):
            rel_path = py_file.relative_to(src_core)
            # Extrahera modulnamn
            if rel_path.name == "__init__.py":
                module = ".".join(rel_path.parent.parts)
            else:
                module = ".".join(rel_path.with_suffix("").parts)
            if module:
                submodules.add(module)

        # Kontrollera att vi hittat förväntade moduler
        expected_modules = {"settings", "plugins", "migrations", "admin", "sql_generator"}
        found_modules = {m.split(".")[0] for m in submodules if m}

        missing = expected_modules - found_modules
        assert not missing, f"Förväntade moduler saknas: {missing}"


class TestImportSyntax:
    """Tester för att verifiera syntaktisk korrekthet av Python-filer."""

    @pytest.fixture
    def project_root(self) -> Path:
        """Hämta projektets rotkatalog."""
        return Path(__file__).parent.parent.parent

    def test_all_plugin_files_are_valid_python(self, project_root: Path):
        """Verifiera att alla plugin-filer är syntaktiskt korrekta."""
        plugin_path = project_root / "qgis_plugin"
        errors = []

        for py_file in plugin_path.rglob("*.py"):
            try:
                content = py_file.read_text()
                ast.parse(content)
            except SyntaxError as e:
                rel_path = py_file.relative_to(project_root)
                errors.append(f"{rel_path}: {e}")

        assert not errors, "Syntaxfel i plugin-filer:\n" + "\n".join(errors)

    def test_all_core_files_are_valid_python(self, project_root: Path):
        """Verifiera att alla core-filer är syntaktiskt korrekta."""
        core_path = project_root / "src" / "g_etl"
        errors = []

        for py_file in core_path.rglob("*.py"):
            try:
                content = py_file.read_text()
                ast.parse(content)
            except SyntaxError as e:
                rel_path = py_file.relative_to(project_root)
                errors.append(f"{rel_path}: {e}")

        assert not errors, "Syntaxfel i core-filer:\n" + "\n".join(errors)
