"""G-ETL Database Migrations.

Minimal migreringsverktyg för DuckDB och PostgreSQL.
"""

from scripts.migrations.migrator import (
    Migration,
    MigrationResult,
    Migrator,
    MigrationStatus,
)

__all__ = ["Migration", "MigrationResult", "Migrator", "MigrationStatus"]
