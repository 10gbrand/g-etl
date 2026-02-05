"""Hantering av Python-dependencies för G-ETL QGIS Plugin.

Installerar nödvändiga paket vid första körning om de saknas.

Kompatibel med Python 3.9+ (QGIS LTR).
"""

import os
import subprocess
import sys
from typing import Callable, List, Optional

# Paket som krävs för G-ETL core
REQUIRED_PACKAGES = {
    "duckdb": "duckdb",
    "h3": "h3",
    "yaml": "pyyaml",
    "jinja2": "jinja2",
    "requests": "requests",
    "dotenv": "python-dotenv",
}


def get_qgis_pip_path() -> Optional[str]:
    """Hitta pip i QGIS-installationen (macOS).

    Returns:
        Sökväg till pip3 eller None om ej hittad.
    """
    # macOS QGIS LTR
    qgis_pip_paths = [
        "/Applications/QGIS-LTR.app/Contents/MacOS/bin/pip3",
        "/Applications/QGIS.app/Contents/MacOS/bin/pip3",
    ]

    for pip_path in qgis_pip_paths:
        if os.path.exists(pip_path):
            return pip_path

    return None


def get_install_command(packages: List[str]) -> str:
    """Generera manuellt installationskommando för användaren.

    Args:
        packages: Lista med paketnamn.

    Returns:
        Kommandosträng för manuell installation.
    """
    pkg_str = " ".join(packages)

    # Kolla om vi är på macOS med QGIS
    qgis_pip = get_qgis_pip_path()
    if qgis_pip:
        return f"{qgis_pip} install {pkg_str}"

    # Fallback till generiskt kommando
    return f"pip3 install {pkg_str}"


def check_dependencies() -> List[str]:
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


def _try_pip_install(
    cmd: List[str],
    description: str,
    on_progress: Optional[Callable[[str], None]] = None,
) -> bool:
    """Försök köra ett pip-kommando.

    Args:
        cmd: Kommando att köra.
        description: Beskrivning för loggning.
        on_progress: Callback för statusmeddelanden.

    Returns:
        True om installationen lyckades.
    """
    if on_progress:
        on_progress(f"Försöker: {description}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0:
            if on_progress:
                on_progress("Installation klar!")
            return True
        else:
            if on_progress:
                stderr = result.stderr.strip()
                if stderr:
                    on_progress(f"Misslyckades: {stderr[:200]}")
    except subprocess.TimeoutExpired:
        if on_progress:
            on_progress("Timeout efter 5 minuter")
    except Exception as e:
        if on_progress:
            on_progress(f"Fel: {e}")

    return False


def install_dependencies(
    packages: List[str],
    on_progress: Optional[Callable[[str], None]] = None,
) -> bool:
    """Installera saknade dependencies.

    Försöker flera metoder i ordning:
    1. QGIS-bundlad pip (macOS)
    2. pip med --user flagga
    3. pip med --break-system-packages (Python 3.11+)
    4. Standard pip

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

    # Försök 1: QGIS-specifik pip (macOS)
    qgis_pip = get_qgis_pip_path()
    if qgis_pip:
        # Prova med --user först
        if _try_pip_install(
            [qgis_pip, "install", "--user", "--quiet", *packages],
            "QGIS pip --user",
            on_progress,
        ):
            return True

        # Prova utan --user
        if _try_pip_install(
            [qgis_pip, "install", "--quiet", *packages],
            "QGIS pip",
            on_progress,
        ):
            return True

    # Försök 2: python -m pip med --user
    if _try_pip_install(
        [sys.executable, "-m", "pip", "install", "--user", "--quiet", *packages],
        "pip --user",
        on_progress,
    ):
        return True

    # Försök 3: pip med --break-system-packages (Python 3.11+ PEP 668)
    if _try_pip_install(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--user",
            "--break-system-packages",
            "--quiet",
            *packages,
        ],
        "pip --break-system-packages",
        on_progress,
    ):
        return True

    # Försök 4: Standard pip (kan kräva admin-rättigheter)
    if _try_pip_install(
        [sys.executable, "-m", "pip", "install", "--quiet", *packages],
        "pip standard",
        on_progress,
    ):
        return True

    return False


def ensure_dependencies(on_progress: Optional[Callable[[str], None]] = None) -> bool:
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


def install_duckdb_extensions(on_progress: Optional[Callable[[str], None]] = None) -> bool:
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
