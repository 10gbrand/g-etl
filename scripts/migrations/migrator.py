"""Minimal SQL-baserad migreringsmotor.

Stödjer både DuckDB och PostgreSQL med samma API.
Migreringsfiler använder rena SQL-kommentarer för up/down-sektioner.

Filformat:
    sql/migrations/001_create_schemas.sql
    sql/migrations/002_create_tables.sql

SQL-filformat:
    -- migrate:up
    CREATE SCHEMA raw;
    CREATE SCHEMA staging;

    -- migrate:down
    DROP SCHEMA staging CASCADE;
    DROP SCHEMA raw CASCADE;
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Protocol


class MigrationStatus(str, Enum):
    """Status för en migrering."""

    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"


@dataclass
class Migration:
    """Representerar en migreringsfil."""

    version: str
    name: str
    path: Path
    up_sql: str
    down_sql: str
    status: MigrationStatus = MigrationStatus.PENDING
    applied_at: datetime | None = None
    error: str | None = None

    @property
    def full_name(self) -> str:
        """Fullständigt namn (version_name)."""
        return f"{self.version}_{self.name}"


@dataclass
class MigrationResult:
    """Resultat från en migreringsoperation."""

    success: bool
    applied: list[str]
    failed: list[tuple[str, str]]  # (version, error)
    message: str


class DatabaseConnection(Protocol):
    """Protokoll för databasanslutningar (DuckDB/PostgreSQL)."""

    def execute(self, sql: str) -> object: ...
    def fetchone(self) -> tuple | None: ...
    def fetchall(self) -> list[tuple]: ...


class Migrator:
    """Minimal SQL-baserad migreringsmotor.

    Fungerar med både DuckDB och PostgreSQL genom att använda
    standard SQL för spårningstabellen.
    """

    MIGRATIONS_TABLE = "_migrations"

    def __init__(
        self,
        conn: DatabaseConnection,
        migrations_dir: Path | str = "sql/migrations",
    ):
        """Initiera migratorn.

        Args:
            conn: Databasanslutning (DuckDB eller PostgreSQL)
            migrations_dir: Sökväg till migreringsfiler
        """
        self.conn = conn
        self.migrations_dir = Path(migrations_dir)
        self._ensure_migrations_table()

    def _ensure_migrations_table(self) -> None:
        """Skapa migrations-tabellen om den inte finns."""
        # Använd standard SQL som fungerar i både DuckDB och PostgreSQL
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.MIGRATIONS_TABLE} (
                version VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                checksum VARCHAR
            )
        """)

    def _parse_migration_file(self, path: Path) -> tuple[str, str]:
        """Parsa en migreringsfil och extrahera up/down SQL.

        Args:
            path: Sökväg till .sql-fil

        Returns:
            Tuple av (up_sql, down_sql)
        """
        content = path.read_text(encoding="utf-8")

        # Hitta up och down sektioner
        up_match = re.search(
            r"--\s*migrate:up\s*\n(.*?)(?=--\s*migrate:down|\Z)",
            content,
            re.DOTALL | re.IGNORECASE,
        )
        down_match = re.search(
            r"--\s*migrate:down\s*\n(.*?)(?=--\s*migrate:up|\Z)",
            content,
            re.DOTALL | re.IGNORECASE,
        )

        up_sql = up_match.group(1).strip() if up_match else content.strip()
        down_sql = down_match.group(1).strip() if down_match else ""

        return up_sql, down_sql

    def _parse_filename(self, path: Path) -> tuple[str, str]:
        """Extrahera version och namn från filnamn.

        Format: 001_create_schemas.sql -> ("001", "create_schemas")
        """
        stem = path.stem
        match = re.match(r"^(\d+)_(.+)$", stem)
        if match:
            return match.group(1), match.group(2)
        return stem, stem

    def _get_applied_versions(self) -> set[str]:
        """Hämta versioner som redan är körda."""
        try:
            result = self.conn.execute(
                f"SELECT version FROM {self.MIGRATIONS_TABLE}"
            )
            rows = result.fetchall()
            return {row[0] for row in rows}
        except Exception:
            return set()

    def _calculate_checksum(self, sql: str) -> str:
        """Beräkna enkel checksum för SQL."""
        import hashlib

        return hashlib.md5(sql.encode()).hexdigest()[:16]

    def discover_migrations(self) -> list[Migration]:
        """Hitta alla migreringsfiler och deras status.

        Returns:
            Lista av Migration-objekt sorterade efter version
        """
        if not self.migrations_dir.exists():
            return []

        applied = self._get_applied_versions()
        migrations = []

        for path in sorted(self.migrations_dir.glob("*.sql")):
            version, name = self._parse_filename(path)
            up_sql, down_sql = self._parse_migration_file(path)

            status = (
                MigrationStatus.APPLIED
                if version in applied
                else MigrationStatus.PENDING
            )

            migrations.append(
                Migration(
                    version=version,
                    name=name,
                    path=path,
                    up_sql=up_sql,
                    down_sql=down_sql,
                    status=status,
                )
            )

        return migrations

    def get_pending(self) -> list[Migration]:
        """Hämta migrationer som inte är körda."""
        return [m for m in self.discover_migrations() if m.status == MigrationStatus.PENDING]

    def get_applied(self) -> list[Migration]:
        """Hämta migrationer som är körda."""
        return [m for m in self.discover_migrations() if m.status == MigrationStatus.APPLIED]

    def migrate(
        self,
        target_version: str | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> MigrationResult:
        """Kör väntande migrationer (up).

        Args:
            target_version: Kör upp till och med denna version (None = alla)
            on_log: Callback för loggmeddelanden

        Returns:
            MigrationResult med status
        """
        pending = self.get_pending()
        applied = []
        failed = []

        if not pending:
            return MigrationResult(
                success=True,
                applied=[],
                failed=[],
                message="Inga väntande migrationer",
            )

        for migration in pending:
            if target_version and migration.version > target_version:
                break

            if on_log:
                on_log(f"Kör {migration.full_name}...")

            try:
                # Kör up-SQL
                self.conn.execute(migration.up_sql)

                # Registrera som körd
                checksum = self._calculate_checksum(migration.up_sql)
                self.conn.execute(f"""
                    INSERT INTO {self.MIGRATIONS_TABLE} (version, name, checksum)
                    VALUES ('{migration.version}', '{migration.name}', '{checksum}')
                """)

                applied.append(migration.version)
                if on_log:
                    on_log(f"  ✓ {migration.full_name} klar")

            except Exception as e:
                error_msg = str(e)
                failed.append((migration.version, error_msg))
                if on_log:
                    on_log(f"  ✗ {migration.full_name}: {error_msg}")
                # Avbryt vid första felet
                break

        success = len(failed) == 0
        message = f"Körde {len(applied)} migrering(ar)"
        if failed:
            message += f", {len(failed)} misslyckades"

        return MigrationResult(
            success=success,
            applied=applied,
            failed=failed,
            message=message,
        )

    def rollback(
        self,
        steps: int = 1,
        on_log: Callable[[str], None] | None = None,
    ) -> MigrationResult:
        """Rulla tillbaka migrationer (down).

        Args:
            steps: Antal migrationer att rulla tillbaka
            on_log: Callback för loggmeddelanden

        Returns:
            MigrationResult med status
        """
        applied = list(reversed(self.get_applied()))
        rolled_back = []
        failed = []

        if not applied:
            return MigrationResult(
                success=True,
                applied=[],
                failed=[],
                message="Inga migrationer att rulla tillbaka",
            )

        for i, migration in enumerate(applied):
            if i >= steps:
                break

            if not migration.down_sql:
                if on_log:
                    on_log(f"  ⚠ {migration.full_name} saknar down-SQL, hoppar över")
                continue

            if on_log:
                on_log(f"Rullar tillbaka {migration.full_name}...")

            try:
                # Kör down-SQL
                self.conn.execute(migration.down_sql)

                # Ta bort från migrations-tabellen
                self.conn.execute(f"""
                    DELETE FROM {self.MIGRATIONS_TABLE}
                    WHERE version = '{migration.version}'
                """)

                rolled_back.append(migration.version)
                if on_log:
                    on_log(f"  ✓ {migration.full_name} återställd")

            except Exception as e:
                error_msg = str(e)
                failed.append((migration.version, error_msg))
                if on_log:
                    on_log(f"  ✗ {migration.full_name}: {error_msg}")
                break

        success = len(failed) == 0
        message = f"Rullade tillbaka {len(rolled_back)} migrering(ar)"
        if failed:
            message += f", {len(failed)} misslyckades"

        return MigrationResult(
            success=success,
            applied=rolled_back,
            failed=failed,
            message=message,
        )

    def status(self) -> list[dict]:
        """Hämta status för alla migrationer.

        Returns:
            Lista med status-dictionaries
        """
        migrations = self.discover_migrations()
        return [
            {
                "version": m.version,
                "name": m.name,
                "status": m.status.value,
                "has_down": bool(m.down_sql),
            }
            for m in migrations
        ]

    def create(
        self,
        name: str,
        on_log: Callable[[str], None] | None = None,
    ) -> Path:
        """Skapa en ny tom migreringsfil.

        Args:
            name: Namn på migreringen (t.ex. "create_users_table")
            on_log: Callback för loggmeddelanden

        Returns:
            Sökväg till den skapade filen
        """
        self.migrations_dir.mkdir(parents=True, exist_ok=True)

        # Hitta nästa versionsnummer
        existing = list(self.migrations_dir.glob("*.sql"))
        if existing:
            versions = []
            for f in existing:
                match = re.match(r"^(\d+)", f.stem)
                if match:
                    versions.append(int(match.group(1)))
            next_version = max(versions) + 1 if versions else 1
        else:
            next_version = 1

        # Skapa filnamn
        version_str = f"{next_version:03d}"
        safe_name = re.sub(r"[^\w]+", "_", name.lower()).strip("_")
        filename = f"{version_str}_{safe_name}.sql"
        filepath = self.migrations_dir / filename

        # Skapa fil med mall
        template = f"""-- Migration: {name}
-- Version: {version_str}
-- Skapad: {datetime.now().isoformat()}

-- migrate:up
-- Skriv din SQL här


-- migrate:down
-- Skriv rollback-SQL här (valfritt)

"""
        filepath.write_text(template, encoding="utf-8")

        if on_log:
            on_log(f"Skapade {filepath}")

        return filepath
