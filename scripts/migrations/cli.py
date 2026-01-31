"""CLI för databasmigrationer.

Användning:
    python -m scripts.migrations.cli status
    python -m scripts.migrations.cli migrate
    python -m scripts.migrations.cli rollback
    python -m scripts.migrations.cli create "add users table"
"""

import argparse
import sys
from pathlib import Path

import duckdb

from config.settings import settings
from scripts.migrations.migrator import Migrator, MigrationStatus


def get_connection(db_path: str | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Skapa databasanslutning."""
    path = db_path or str(settings.DATA_DIR / f"{settings.DB_PREFIX}{settings.DB_EXTENSION}")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(path, read_only=read_only)


def cmd_status(args: argparse.Namespace) -> int:
    """Visa status för alla migrationer."""
    conn = get_connection(args.db, read_only=True)
    migrator = Migrator(conn, args.migrations_dir, read_only=True)

    migrations = migrator.discover_migrations()

    if not migrations:
        print("Inga migrationer hittades")
        print(f"Sökväg: {args.migrations_dir}")
        return 0

    # Separera statiska och template-migrationer
    static_migrations = [m for m in migrations if not migrator.is_template_migration(m)]
    template_migrations = [m for m in migrations if migrator.is_template_migration(m)]

    # Visa statiska migrationer
    print("=== Statiska migrationer ===")
    print(f"{'Version':<10} {'Namn':<40} {'Status':<10} {'Down'}")
    print("-" * 70)

    for m in static_migrations:
        status_icon = "✓" if m.status == MigrationStatus.APPLIED else "○"
        down_icon = "✓" if m.down_sql else "-"
        print(f"{m.version:<10} {m.name:<40} {status_icon:<10} {down_icon}")

    # Visa template-migrationer
    if template_migrations:
        print()
        print("=== Template-migrationer (körs per dataset) ===")
        print(f"{'Version':<10} {'Namn':<40} {'Datasets körda'}")
        print("-" * 70)

        for m in template_migrations:
            # Hämta antal körda datasets för denna template
            try:
                result = conn.execute(f"""
                    SELECT COUNT(*) FROM {migrator.MIGRATIONS_TABLE}
                    WHERE version LIKE '{m.version}:%'
                """)
                count = result.fetchone()[0]
            except Exception:
                count = 0

            print(f"{m.version:<10} {m.name:<40} {count}")

    # Sammanfattning
    pending_static = [m for m in static_migrations if m.status == MigrationStatus.PENDING]
    applied_static = [m for m in static_migrations if m.status == MigrationStatus.APPLIED]

    print()
    print(f"Statiska: {len(static_migrations)} ({len(applied_static)} körda, {len(pending_static)} väntande)")
    print(f"Templates: {len(template_migrations)} (körs per dataset)")

    conn.close()
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    """Kör väntande migrationer."""
    conn = get_connection(args.db)
    migrator = Migrator(conn, args.migrations_dir)

    def log(msg: str) -> None:
        print(msg)

    print("=== Kör migrationer ===")
    result = migrator.migrate(target_version=args.target, on_log=log)

    print()
    print(result.message)

    conn.close()
    return 0 if result.success else 1


def cmd_rollback(args: argparse.Namespace) -> int:
    """Rulla tillbaka migrationer."""
    conn = get_connection(args.db)
    migrator = Migrator(conn, args.migrations_dir)

    def log(msg: str) -> None:
        print(msg)

    print(f"=== Rullar tillbaka {args.steps} migrering(ar) ===")
    result = migrator.rollback(steps=args.steps, on_log=log)

    print()
    print(result.message)

    conn.close()
    return 0 if result.success else 1


def cmd_create(args: argparse.Namespace) -> int:
    """Skapa en ny migreringsfil."""
    conn = get_connection(args.db)
    migrator = Migrator(conn, args.migrations_dir)

    def log(msg: str) -> None:
        print(msg)

    filepath = migrator.create(args.name, on_log=log)
    print(f"\nRedigera filen och lägg till din SQL:")
    print(f"  {filepath}")

    conn.close()
    return 0


def main() -> int:
    """Huvudfunktion för CLI."""
    parser = argparse.ArgumentParser(
        description="G-ETL Database Migrations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--db",
        help="Sökväg till databas (default: data/warehouse.duckdb)",
    )
    parser.add_argument(
        "--migrations-dir",
        default="sql/migrations",
        help="Sökväg till migreringsfiler (default: sql/migrations)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # status
    status_parser = subparsers.add_parser("status", help="Visa status för migrationer")
    status_parser.set_defaults(func=cmd_status)

    # migrate
    migrate_parser = subparsers.add_parser("migrate", help="Kör väntande migrationer")
    migrate_parser.add_argument(
        "--target",
        help="Kör upp till denna version",
    )
    migrate_parser.set_defaults(func=cmd_migrate)

    # rollback
    rollback_parser = subparsers.add_parser("rollback", help="Rulla tillbaka migrationer")
    rollback_parser.add_argument(
        "--steps",
        type=int,
        default=1,
        help="Antal migrationer att rulla tillbaka (default: 1)",
    )
    rollback_parser.set_defaults(func=cmd_rollback)

    # create
    create_parser = subparsers.add_parser("create", help="Skapa ny migrering")
    create_parser.add_argument(
        "name",
        help="Namn på migreringen (t.ex. 'add users table')",
    )
    create_parser.set_defaults(func=cmd_create)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
