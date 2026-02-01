"""G-ETL Database Migrations.

Minimal migreringsverktyg för DuckDB och PostgreSQL.
"""

from g_etl.migrations.migrator import (
    Migration,
    MigrationResult,
    MigrationStatus,
    Migrator,
)

__all__ = ["Migration", "MigrationResult", "Migrator", "MigrationStatus"]
