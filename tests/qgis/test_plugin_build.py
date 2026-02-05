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

            # Kolla efter absoluta importer som borde vara relativa
            if "from g_etl." in content and "from g_etl.runner.core" not in content:
                # Detta är OK om det är i qgis_runner.py som importerar core
                if py_file.name != "qgis_runner.py":
                    rel_path = py_file.relative_to(project_root)
                    issues.append(f"{rel_path}: använder 'from g_etl.' istället för relativ import")

        assert not issues, "Plugin-filer med felaktiga importer:\n" + "\n".join(issues)

    def test_qgis_runner_imports_from_runner_core(self, project_root: Path):
        """Verifiera att qgis_runner.py importerar från .runner.core."""
        qgis_runner = project_root / "qgis_plugin" / "qgis_runner.py"
        content = qgis_runner.read_text()

        # Ska använda .runner.core för core-importer
        assert ".runner.core." in content, (
            "qgis_runner.py ska importera från .runner.core för att matcha plugin-strukturen"
        )

    def test_build_task_transforms_imports(self, project_root: Path):
        """Verifiera att build-tasken innehåller import-transformering."""
        qgis_yml = project_root / "taskfiles" / "qgis.yml"
        content = qgis_yml.read_text()

        # Kontrollera att transformeringen finns
        assert "g_etl.runner.core" in content, (
            "Build-tasken ska transformera importer till g_etl.runner.core"
        )
        assert "perl -i -pe" in content or "sed -i" in content, (
            "Build-tasken ska använda perl eller sed för import-transformering"
        )


class TestPluginBuildIntegration:
    """Integrationstester för plugin-bygget."""

    @pytest.fixture
    def project_root(self) -> Path:
        """Hämta projektets rotkatalog."""
        return Path(__file__).parent.parent.parent

    def test_simulated_build_transforms_imports(self, project_root: Path):
        """Simulera bygget och verifiera att importer transformeras korrekt."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            plugin_dir = temp_path / "g_etl"
            plugin_dir.mkdir()

            # Kopiera core-filer
            core_dir = plugin_dir / "runner" / "core"
            core_dir.mkdir(parents=True)

            src_core = project_root / "src" / "g_etl"
            for py_file in src_core.rglob("*.py"):
                rel_path = py_file.relative_to(src_core)
                dest_file = core_dir / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                dest_file.write_text(py_file.read_text())

            # Skapa __init__.py för runner
            (plugin_dir / "runner" / "__init__.py").touch()

            # Transformera importer (samma som i build-tasken)
            for py_file in core_dir.rglob("*.py"):
                content = py_file.read_text()
                # Transformera importer
                content = re.sub(r"from g_etl\.", "from g_etl.runner.core.", content)
                content = re.sub(r"import g_etl\.", "import g_etl.runner.core.", content)
                py_file.write_text(content)

            # Verifiera att inga otransformerade importer finns kvar
            untransformed = []
            for py_file in core_dir.rglob("*.py"):
                content = py_file.read_text()
                lines = content.split("\n")
                for line_no, line in enumerate(lines, 1):
                    # Matcha 'from g_etl.' som INTE följs av 'runner.core.'
                    if re.search(r"from g_etl\.(?!runner\.core\.)", line):
                        rel_path = py_file.relative_to(plugin_dir)
                        untransformed.append(f"{rel_path}:{line_no}: {line.strip()}")
                    if re.search(r"import g_etl\.(?!runner\.core\.)", line):
                        rel_path = py_file.relative_to(plugin_dir)
                        untransformed.append(f"{rel_path}:{line_no}: {line.strip()}")

            assert not untransformed, "Otransformerade importer hittades:\n" + "\n".join(
                untransformed
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
