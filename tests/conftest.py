"""Pytest fixtures för G-ETL tester."""

import tempfile
from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def temp_dir():
    """Skapar temporär katalog för tester."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def duckdb_conn():
    """Skapar en in-memory DuckDB-anslutning med extensions."""
    conn = duckdb.connect(":memory:")

    # Installera och ladda extensions
    for ext in ["spatial", "parquet", "json"]:
        try:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
        except Exception:
            pass

    # Skapa scheman
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging_004")
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart")

    yield conn
    conn.close()


@pytest.fixture
def sample_dataset_config():
    """Exempel på dataset-konfiguration från datasets.yml."""
    return {
        "id": "test_dataset",
        "name": "Test Dataset",
        "description": "Ett test-dataset",
        "typ": "test",
        "plugin": "wfs",
        "enabled": True,
        "field_mapping": {
            "source_id_column": "$objekt_id",
            "klass": "test_klass",
            "grupp": "$kategori",
            "typ": "test_typ",
            "leverantor": "test_leverantor",
        },
    }


@pytest.fixture
def sample_polygon_wkt():
    """Enkel polygon i WKT-format (SWEREF99 TM)."""
    return (
        "POLYGON((400000 6500000, 400100 6500000, 400100 6500100, 400000 6500100, 400000 6500000))"
    )


@pytest.fixture
def sample_point_wkt():
    """Enkel punkt i WKT-format (SWEREF99 TM)."""
    return "POINT(400050 6500050)"


@pytest.fixture
def sample_linestring_wkt():
    """Enkel linje i WKT-format (SWEREF99 TM)."""
    return "LINESTRING(400000 6500000, 400100 6500100, 400200 6500050)"
