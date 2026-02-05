"""Centraliserad nedladdningslogik för G-ETL plugins.

Används av plugins som behöver ladda ner filer från URL:er med
progress-rapportering och streaming.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path

import requests

# Chunk-storlek för streaming (8KB)
CHUNK_SIZE = 8192

# Rapportera progress var N:te chunk (~800KB)
PROGRESS_REPORT_INTERVAL = 100


def download_file_streaming(
    url: str,
    suffix: str = ".bin",
    timeout: int = 300,
    on_log: Callable[[str], None] | None = None,
    on_progress: Callable[[float, str], None] | None = None,
    progress_weight: float = 0.5,
) -> Path:
    """Ladda ner fil från URL med streaming och progress-rapportering.

    Laddar ner filen i chunks för att hantera stora filer utan att
    förbruka för mycket minne. Rapporterar progress via callback.

    Args:
        url: URL att ladda ner från.
        suffix: Filsuffix för temporärfilen (t.ex. ".zip", ".gpkg").
        timeout: Timeout i sekunder för HTTP-anropet.
        on_log: Callback för loggmeddelanden.
        on_progress: Callback för progress (0.0-1.0, meddelande).
        progress_weight: Hur stor del av total progress nedladdningen utgör (0.0-1.0).
            Används när nedladdning är en del av en större operation.

    Returns:
        Path till den nedladdade temporärfilen.

    Raises:
        requests.RequestException: Vid HTTP-fel.

    Example:
        path = download_file_streaming(
            url="https://example.com/data.zip",
            suffix=".zip",
            on_log=lambda msg: print(msg),
            on_progress=lambda p, m: print(f"{p:.0%}: {m}")
        )
    """
    if on_log:
        on_log(f"Laddar ner {url}...")
    if on_progress:
        on_progress(0.0, "Ansluter...")

    response = requests.get(url, timeout=timeout, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0
    chunk_count = 0

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            tmp.write(chunk)
            downloaded += len(chunk)
            chunk_count += 1

            # Rapportera progress var PROGRESS_REPORT_INTERVAL:e chunk
            if on_progress and chunk_count % PROGRESS_REPORT_INTERVAL == 0:
                mb_done = downloaded / (1024 * 1024)

                if total_size > 0:
                    fraction = downloaded / total_size
                    progress = fraction * progress_weight
                    mb_total = total_size / (1024 * 1024)
                    on_progress(progress, f"Laddar ner {mb_done:.1f}/{mb_total:.1f} MB...")
                else:
                    # Okänd storlek, visa bara nedladdat
                    on_progress(progress_weight / 2, f"Laddar ner {mb_done:.1f} MB...")

        return Path(tmp.name)


def is_url(path: str) -> bool:
    """Kontrollera om en sträng är en URL.

    Args:
        path: Sträng att kontrollera.

    Returns:
        True om strängen ser ut som en URL, annars False.
    """
    return path.startswith(("http://", "https://"))
