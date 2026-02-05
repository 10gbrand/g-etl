"""Hantering av Python-dependencies för G-ETL QGIS Plugin.

Installerar nödvändiga paket vid första körning om de saknas.
"""

import subprocess
import sys
from collections.abc import Callable

# Paket som krävs för G-ETL core
REQUIRED_PACKAGES = {
    "duckdb": "duckdb",
    "h3": "h3",
    "yaml": "pyyaml",
    "jinja2": "jinja2",
    "requests": "requests",
    "dotenv": "python-dotenv",
}


def check_dependencies() -> list[str]:
    """Kontrollera vilka dependencies som saknas.

    Returns:
        Lista med paketnamn (pip) som behöver installeras.
    """
    missing = []
    for import_name, pip_name in REQUIRED_PACKAGES.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_name)
    return missing


def install_dependencies(
    packages: list[str],
    on_progress: Callable[[str], None] | None = None,
) -> bool:
    """Installera saknade dependencies.

    Args:
        packages: Lista med pip-paketnamn att installera.
        on_progress: Callback för statusmeddelanden.

    Returns:
        True om installationen lyckades.
    """
    if not packages:
        return True

    if on_progress:
        on_progress(f"Installerar {len(packages)} paket: {', '.join(packages)}")

    try:
        cmd = [sys.executable, "-m", "pip", "install", "--quiet", *packages]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode != 0:
            if on_progress:
                on_progress(f"Fel vid installation: {result.stderr}")
            return False

        if on_progress:
            on_progress("Installation klar!")
        return True

    except subprocess.TimeoutExpired:
        if on_progress:
            on_progress("Installation tog för lång tid (timeout)")
        return False
    except Exception as e:
        if on_progress:
            on_progress(f"Fel: {e}")
        return False


def ensure_dependencies(on_progress: Callable[[str], None] | None = None) -> bool:
    """Säkerställ att alla dependencies finns installerade.

    Args:
        on_progress: Callback för statusmeddelanden.

    Returns:
        True om alla dependencies finns (eller installerades).
    """
    missing = check_dependencies()

    if not missing:
        return True

    if on_progress:
        on_progress(f"Saknade paket: {', '.join(missing)}")

    return install_dependencies(missing, on_progress)


def install_duckdb_extensions(on_progress: Callable[[str], None] | None = None) -> bool:
    """Installera DuckDB extensions (spatial, h3).

    Dessa installeras runtime i DuckDB, inte via pip.

    Args:
        on_progress: Callback för statusmeddelanden.

    Returns:
        True om installationen lyckades.
    """
    try:
        import duckdb

        conn = duckdb.connect(":memory:")

        extensions = ["spatial", "h3", "parquet", "httpfs", "json"]
        for ext in extensions:
            if on_progress:
                on_progress(f"Installerar DuckDB extension: {ext}")
            try:
                conn.execute(f"INSTALL {ext}")
                conn.execute(f"LOAD {ext}")
            except Exception as e:
                if on_progress:
                    on_progress(f"Varning: Kunde inte installera {ext}: {e}")

        conn.close()
        return True

    except Exception as e:
        if on_progress:
            on_progress(f"Fel vid installation av DuckDB extensions: {e}")
        return False
