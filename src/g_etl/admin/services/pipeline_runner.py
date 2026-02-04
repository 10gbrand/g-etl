"""Pipeline runner service för Admin TUI."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import yaml

from g_etl.migrations.migrator import Migrator
from g_etl.plugins import clear_download_cache, get_plugin
from g_etl.settings import settings
from g_etl.sql_generator import SQLGenerator


@dataclass
class PipelineEvent:
    """Event för pipeline-statusuppdateringar."""

    event_type: str  # "started", "progress", "completed", "failed"
    message: str
    dataset: str | None = None
    status: str | None = None
    rows_count: int | None = None
    progress: float | None = None  # 0.0 - 1.0
    current_step: int | None = None
    total_steps: int | None = None


@dataclass
class ParallelExtractResult:
    """Resultat från parallell extraktion."""

    success: bool
    parquet_files: list[tuple[str, str]] = field(default_factory=list)  # (dataset_id, path)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (dataset_id, error)


class PipelineRunner:
    """Asynkron pipeline-runner för TUI."""

    def __init__(
        self,
        db_path: str | None = None,
        sql_path: str | Path | None = None,
    ):
        self.db_path = db_path or str(
            settings.DATA_DIR / f"{settings.DB_PREFIX}{settings.DB_EXTENSION}"
        )
        self.sql_path = Path(sql_path) if sql_path else settings.SQL_DIR
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Hämta eller skapa databasanslutning."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            self._init_database()
        return self._conn

    def _init_database(self):
        """Initiera databas med extensions, scheman och makron.

        Använder Migrator.run_init_migrations() - gemensam logik med CLI.
        """
        conn = self._conn
        if conn is None:
            return

        init_folder = self.sql_path / "migrations"
        if init_folder.exists():
            # Använd gemensam Migrator-metod
            migrator = Migrator(conn, init_folder)
            migrator.run_init_migrations()
        else:
            # Fallback om migrations-mappen inte finns
            for ext in ["spatial", "parquet", "httpfs", "json"]:
                try:
                    conn.execute(f"INSTALL {ext}")
                    conn.execute(f"LOAD {ext}")
                except Exception:
                    pass

            for schema in ["raw", "staging", "staging_2", "mart"]:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def _split_sql_statements(self, sql_content: str) -> list[str]:
        """Dela upp SQL-innehåll i enskilda statements.

        Hanterar:
        - Semikolon som statement-avslutare
        - Kommentarer (-- och /* */)
        - Strängar med enkla citattecken
        - migrate:down sektioner (ignoreras)

        Args:
            sql_content: SQL-fil innehåll

        Returns:
            Lista med enskilda SQL-statements
        """
        # Stoppa vid migrate:down (kör bara "up" migrationer)
        if "-- migrate:down" in sql_content:
            sql_content = sql_content.split("-- migrate:down")[0]

        statements = []
        current_stmt = []
        in_string = False
        in_block_comment = False
        i = 0

        while i < len(sql_content):
            char = sql_content[i]

            # Hantera block-kommentarer /* */
            if not in_string and sql_content[i : i + 2] == "/*":
                in_block_comment = True
                current_stmt.append(char)
                i += 1
            elif in_block_comment and sql_content[i : i + 2] == "*/":
                in_block_comment = False
                current_stmt.append(char)
                current_stmt.append(sql_content[i + 1])
                i += 2
                continue
            elif in_block_comment:
                current_stmt.append(char)
                i += 1
                continue

            # Hantera rad-kommentarer --
            if not in_string and sql_content[i : i + 2] == "--":
                # Läs till slutet av raden
                while i < len(sql_content) and sql_content[i] != "\n":
                    current_stmt.append(sql_content[i])
                    i += 1
                if i < len(sql_content):
                    current_stmt.append(sql_content[i])
                    i += 1
                continue

            # Hantera strängar
            if char == "'" and not in_block_comment:
                in_string = not in_string
                current_stmt.append(char)
                i += 1
                continue

            # Statement-avslutare
            if char == ";" and not in_string and not in_block_comment:
                current_stmt.append(char)
                stmt = "".join(current_stmt).strip()
                if stmt and stmt != ";":
                    statements.append(stmt)
                current_stmt = []
                i += 1
                continue

            current_stmt.append(char)
            i += 1

        # Hantera eventuellt kvarvarande statement utan semikolon
        remaining = "".join(current_stmt).strip()
        if remaining and remaining != ";":
            statements.append(remaining)

        return statements

    async def run_dataset(
        self,
        dataset_config: dict,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör extract för ett dataset."""
        self._running = True
        dataset_name = dataset_config.get("name", dataset_config.get("id"))
        plugin_name = dataset_config.get("plugin")

        if not plugin_name:
            if on_log:
                on_log(f"Saknar plugin för {dataset_name}")
            return False

        try:
            plugin = get_plugin(plugin_name)
        except ValueError as e:
            if on_log:
                on_log(str(e))
            return False

        if on_event:
            on_event(
                PipelineEvent(
                    event_type="dataset_started",
                    message=f"Startar {dataset_name}",
                    dataset=dataset_name,
                )
            )

        # Skapa on_progress callback som bryggar till on_event
        def on_progress(progress: float, message: str) -> None:
            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="progress",
                        message=message,
                        dataset=dataset_name,
                        progress=progress,
                    )
                )

        # Kör extract i executor för att inte blocka event loop
        conn = self._get_connection()

        def do_extract():
            return plugin.extract(dataset_config, conn, on_log, on_progress=on_progress)

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, do_extract)

        if result.success:
            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="dataset_completed",
                        message=f"Klar: {dataset_name} ({result.rows_count} rader)",
                        dataset=dataset_name,
                        status="success",
                        rows_count=result.rows_count,
                    )
                )
            return True
        else:
            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="dataset_failed",
                        message=f"Fel: {result.message}",
                        dataset=dataset_name,
                        status="error",
                    )
                )
            return False

    async def run_transforms(
        self,
        dataset_ids: list[str] | None = None,
        folders: list[str] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör SQL-transformationer för angivna datasets.

        Args:
            dataset_ids: Lista med dataset-ID:n att köra transformationer för.
                         Om None körs alla. Mappar som börjar med _ (t.ex. _common)
                         eller zzz_ (t.ex. zzz_end) körs alltid.
            folders: Lista med mappar att köra (t.ex. ["staging"] eller ["mart"]).
                     Om None körs både staging och mart.
            on_log: Callback för loggmeddelanden.
        """
        conn = self._get_connection()
        success = True

        # Bestäm vilka mappar som ska köras
        target_folders = folders if folders else ["staging", "mart"]

        for folder in target_folders:
            sql_folder = self.sql_path / folder
            if not sql_folder.exists():
                continue

            # Rekursiv sökning efter SQL-filer i undermappar
            for sql_file in sorted(sql_folder.glob("**/*.sql")):
                # Hämta första undermappens namn (dataset-ID eller _common)
                rel_to_folder = sql_file.relative_to(sql_folder)
                subfolder = rel_to_folder.parts[0] if rel_to_folder.parts else ""

                # Hoppa över om:
                # - dataset_ids är specificerat OCH
                # - mappen inte börjar med _ (t.ex. _common) OCH
                # - mappen inte börjar med zzz_ (t.ex. zzz_end) OCH
                # - mappen inte matchar något dataset-ID
                if dataset_ids is not None:
                    is_special_folder = subfolder.startswith("_") or subfolder.startswith("zzz_")
                    if not is_special_folder and subfolder not in dataset_ids:
                        continue

                # Visa relativ sökväg från sql-mappen
                rel_path = sql_file.relative_to(self.sql_path)
                if on_log:
                    on_log(f"Kör {rel_path}...")

                try:
                    sql = sql_file.read_text()

                    def do_sql():
                        conn.execute(sql)

                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, do_sql)

                except Exception as e:
                    if on_log:
                        on_log(f"Fel i {sql_file.name}: {e}")
                    success = False

        return success

    async def run_parallel_extract(
        self,
        dataset_configs: list[dict],
        output_dir: str | Path | None = None,
        max_concurrent: int = 4,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> ParallelExtractResult:
        """Kör extract för flera dataset parallellt till GeoParquet.

        Args:
            dataset_configs: Lista av dataset-konfigurationer
            output_dir: Mapp för parquet-filer
            max_concurrent: Max antal parallella extractions
            on_event: Callback för progress-events
            on_log: Callback för loggmeddelanden

        Returns:
            ParallelExtractResult med lyckade och misslyckade datasets
        """
        self._running = True
        output_path = Path(output_dir) if output_dir else settings.RAW_DIR
        output_path.mkdir(parents=True, exist_ok=True)
        concurrent = max_concurrent or settings.MAX_CONCURRENT_EXTRACTS

        semaphore = asyncio.Semaphore(concurrent)
        results: list[tuple[str, str | None, str | None]] = []  # (id, path, error)

        async def extract_one(config: dict) -> tuple[str, str | None, str | None]:
            """Extrahera ett dataset."""
            dataset_id = config.get("id", config.get("name"))
            plugin_name = config.get("plugin")

            if not plugin_name:
                return (dataset_id, None, f"Saknar plugin för {dataset_id}")

            try:
                plugin = get_plugin(plugin_name)
            except ValueError as e:
                return (dataset_id, None, str(e))

            async with semaphore:
                if not self._running:
                    return (dataset_id, None, "Avbruten")

                if on_event:
                    on_event(
                        PipelineEvent(
                            event_type="dataset_started",
                            message=f"Startar {dataset_id}",
                            dataset=dataset_id,
                        )
                    )

                def on_progress(progress: float, message: str) -> None:
                    if on_event:
                        on_event(
                            PipelineEvent(
                                event_type="progress",
                                message=message,
                                dataset=dataset_id,
                                progress=progress,
                            )
                        )

                def do_extract():
                    return plugin.extract_to_parquet(config, output_path, on_log, on_progress)

                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, do_extract)

                if result.success:
                    if on_event:
                        on_event(
                            PipelineEvent(
                                event_type="dataset_completed",
                                message=f"Klar: {dataset_id} ({result.rows_count} rader)",
                                dataset=dataset_id,
                                status="success",
                                rows_count=result.rows_count,
                            )
                        )
                    return (dataset_id, result.output_path, None)
                else:
                    if on_event:
                        on_event(
                            PipelineEvent(
                                event_type="dataset_failed",
                                message=f"Fel: {result.message}",
                                dataset=dataset_id,
                                status="error",
                            )
                        )
                    return (dataset_id, None, result.message)

        # Kör alla extraktioner parallellt (med spårade tasks)
        self._tasks = [asyncio.create_task(extract_one(config)) for config in dataset_configs]

        try:
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            # Pipeline stoppades
            results = []
            for task in self._tasks:
                if task.done() and not task.cancelled():
                    try:
                        results.append(task.result())
                    except Exception:
                        pass
        finally:
            self._tasks = []

        # Filtrera bort exceptions/cancelled
        valid_results = [r for r in results if isinstance(r, tuple) and len(r) == 3]

        # Rensa nedladdningscache (friggör temp-filer)
        clear_download_cache()

        # Sammanställ resultat
        parquet_files = [(r[0], r[1]) for r in valid_results if r[1] is not None]
        failed = [(r[0], r[2]) for r in valid_results if r[2] is not None]

        # Lägg till avbrutna som failed
        if not self._running:
            cancelled_count = len(dataset_configs) - len(valid_results)
            if cancelled_count > 0:
                failed.append(("", f"{cancelled_count} avbrutna"))

        return ParallelExtractResult(
            success=len(failed) == 0 and self._running,
            parquet_files=parquet_files,
            failed=failed,
        )

    def _normalize_geometry_column(
        self,
        table_name: str,
        on_log: Callable[[str], None] | None = None,
    ) -> None:
        """Normalisera geometrikolumnens namn till 'geom'.

        Söker efter geometrikolumner med namn: geometry, shape, geometri
        och döper om dem till 'geom'.
        """
        conn = self._get_connection()

        # Alternativa geometrikolumnnamn (i prioritetsordning)
        alt_geom_names = ["geometry", "shape", "geometri"]

        try:
            # Kolla om 'geom' redan finns
            result = conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'raw'
                AND table_name = '{table_name}'
                AND LOWER(column_name) = 'geom'
            """).fetchone()

            if result:
                # 'geom' finns redan, inget att göra
                return

            # Sök efter alternativa geometrikolumnnamn
            for alt_name in alt_geom_names:
                result = conn.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'raw'
                    AND table_name = '{table_name}'
                    AND LOWER(column_name) = '{alt_name}'
                """).fetchone()

                if result:
                    actual_col_name = result[0]
                    if on_log:
                        on_log(f"  Döper om '{actual_col_name}' till 'geom' i raw.{table_name}")
                    conn.execute(f"""
                        ALTER TABLE raw.{table_name}
                        RENAME COLUMN "{actual_col_name}" TO geom
                    """)
                    return

        except Exception as e:
            if on_log:
                on_log(f"  Varning: Kunde inte normalisera geometrikolumn i {table_name}: {e}")

    async def load_parquet_to_db(
        self,
        parquet_files: list[tuple[str, str]],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Ladda parquet-filer till DuckDB raw-schema.

        Args:
            parquet_files: Lista av (dataset_id, parquet_path)
            on_log: Callback för loggmeddelanden
            on_event: Callback för progress-events

        Returns:
            True om alla laddades framgångsrikt
        """
        conn = self._get_connection()
        success = True

        for i, (dataset_id, parquet_path) in enumerate(parquet_files):
            if on_log:
                on_log(f"Laddar {dataset_id} till databas...")

            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="progress",
                        message=f"Laddar {dataset_id}...",
                        dataset=dataset_id,
                        progress=(i + 1) / len(parquet_files),
                    )
                )

            try:

                def do_load():
                    conn.execute(f"""
                        CREATE OR REPLACE TABLE raw.{dataset_id} AS
                        SELECT * FROM read_parquet('{parquet_path}')
                    """)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, do_load)

                # Normalisera geometrikolumnens namn till 'geom'
                self._normalize_geometry_column(dataset_id, on_log)

            except Exception as e:
                if on_log:
                    on_log(f"Fel vid laddning av {dataset_id}: {e}")
                success = False

        return success

    def _load_datasets_config(self) -> dict[str, dict]:
        """Ladda datasets.yml och returnera som dict keyed på dataset id."""
        config_path = settings.CONFIG_DIR / "datasets.yml"
        if not config_path.exists():
            return {}

        with open(config_path) as f:
            config = yaml.safe_load(f)

        datasets = config.get("datasets", [])
        return {ds["id"]: ds for ds in datasets if "id" in ds}

    async def run_templates(
        self,
        dataset_ids: list[str],
        template_filter: str | None = None,
        phase_name: str | None = None,
        max_concurrent: int = 4,
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
        force: bool = False,
    ) -> bool:
        """Kör templates för alla datasets parallellt.

        Hittar automatiskt alla *_template.sql filer i sql/migrations/
        och kör dem i nummerordning. Inom varje template körs datasets
        parallellt för snabbare exekvering.

        Använder Migrator för att spåra körda template-migrationer per dataset.

        Args:
            dataset_ids: Lista med dataset-ID:n att processa
            template_filter: Filtrera templates som innehåller denna sträng
                            (t.ex. "_staging_", "_staging2_", "_mart_")
            phase_name: Namn på fasen för loggning (t.ex. "Staging")
            max_concurrent: Max antal parallella dataset-körningar
            on_log: Callback för loggmeddelanden
            on_event: Callback för progress-events
            force: Om True, kör även redan körda migrationer

        Returns:
            True om alla processades framgångsrikt
        """
        generator = SQLGenerator()
        datasets_config = self._load_datasets_config()
        conn = self._get_connection()
        migrator = Migrator(conn, self.sql_path / "migrations")

        # Hämta och filtrera templates
        all_templates = generator.list_templates()
        if template_filter:
            templates = [t for t in all_templates if template_filter in t]
        else:
            templates = all_templates

        if not templates:
            if on_log:
                filter_msg = f" (filter: {template_filter})" if template_filter else ""
                on_log(f"Inga templates hittades{filter_msg}")
            return True

        # Bygg mapping från template-namn till Migration-objekt
        template_migrations = {}
        for migration in migrator.discover_migrations():
            if migrator.is_template_migration(migration):
                template_migrations[migration.name] = migration

        phase_display = phase_name or "Templates"
        if on_log:
            n = len(templates)
            on_log(f"=== Kör {phase_display} med {n} template(s), {max_concurrent} parallella ===")

        semaphore = asyncio.Semaphore(max_concurrent)
        completed_count = 0
        total_tasks = len(templates) * len(dataset_ids)
        results: list[tuple[str, str, bool, str | None]] = []  # (template, dataset, success, error)

        async def process_dataset(
            template_name: str, dataset_id: str
        ) -> tuple[str, str, bool, str | None]:
            """Processa ett dataset med en template."""
            nonlocal completed_count

            async with semaphore:
                # Hämta config från datasets.yml
                ds_config = datasets_config.get(dataset_id, {})

                # Hämta Migration-objekt för denna template
                migration = template_migrations.get(template_name)

                # Kolla om redan körd (om inte force)
                if migration and not force:
                    if migrator.is_template_applied(migration.version, dataset_id):
                        completed_count += 1
                        if on_log:
                            on_log(f"  ○ {dataset_id}: {template_name} redan körd")
                        return (template_name, dataset_id, True, None)

                if on_event:
                    on_event(
                        PipelineEvent(
                            event_type="progress",
                            message=f"{phase_display} {dataset_id}...",
                            dataset=dataset_id,
                            progress=completed_count / total_tasks,
                        )
                    )

                try:
                    # Generera SQL
                    sql = generator.render_template(template_name, dataset_id, ds_config)
                    if not sql:
                        completed_count += 1
                        return (template_name, dataset_id, True, None)

                    # Kör SQL med egen connection och spåra med Migrator
                    def do_sql():
                        # Varje parallell task får egen connection
                        task_conn = duckdb.connect(self.db_path)
                        try:
                            task_conn.execute(sql)

                            # Registrera som körd i migrations-tabellen
                            if migration:
                                task_migrator = Migrator(task_conn, self.sql_path / "migrations")
                                template_version = task_migrator.get_template_version(
                                    migration.version, dataset_id
                                )
                                checksum = task_migrator._calculate_checksum(sql)
                                template_name_with_ds = f"{migration.name}:{dataset_id}"
                                try:
                                    tbl = task_migrator.MIGRATIONS_TABLE
                                    ver = template_version
                                    nm = template_name_with_ds
                                    task_conn.execute(f"""
                                        INSERT INTO {tbl} (version, name, checksum)
                                        VALUES ('{ver}', '{nm}', '{checksum}')
                                    """)
                                except Exception:
                                    pass  # Redan registrerad
                        finally:
                            task_conn.close()

                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, do_sql)

                    completed_count += 1
                    if on_log:
                        on_log(f"  ✓ {dataset_id}: {template_name} OK")

                    return (template_name, dataset_id, True, None)

                except Exception as e:
                    completed_count += 1
                    if on_log:
                        on_log(f"  ✗ {dataset_id}: FEL - {e}")
                    return (template_name, dataset_id, False, str(e))

        # Kör templates sekventiellt (de kan ha dependencies)
        # men datasets parallellt inom varje template
        for template_name in templates:
            tasks = [process_dataset(template_name, dataset_id) for dataset_id in dataset_ids]
            template_results = await asyncio.gather(*tasks)
            results.extend(template_results)

        success_count = sum(1 for r in results if r[2])
        fail_count = sum(1 for r in results if not r[2])

        if on_log:
            on_log(f"{phase_display} klar: {success_count} lyckade, {fail_count} misslyckade")

        return fail_count == 0

    async def run_parallel_transform(
        self,
        parquet_files: list[tuple[str, str]],
        phases: tuple[bool, bool, bool] | None = None,
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> list[tuple[str, str]]:
        """Kör valda templates parallellt med separata temp-DBs per dataset.

        Varje dataset får sin egen DuckDB-fil för äkta parallelism utan
        fillåsningscontention. Returnerar lista med (dataset_id, temp_db_path)
        för efterföljande merge.

        Args:
            parquet_files: Lista av (dataset_id, parquet_path)
            phases: Tuple med (staging, staging2, mart) - vilka faser som ska köras
                    Om None körs alla faser.
            on_log: Callback för loggmeddelanden
            on_event: Callback för progress-events

        Returns:
            Lista av (dataset_id, temp_db_path) för merge
        """
        generator = SQLGenerator()
        datasets_config = self._load_datasets_config()
        all_templates = generator.list_templates()

        # Filtrera templates baserat på valda faser
        run_staging, run_staging2, run_mart = phases if phases else (True, True, True)
        templates = []
        for t in sorted(all_templates):
            t_lower = t.lower()
            # staging2 måste checkas först (annars matchar staging)
            if "_staging2_" in t_lower or "_staging_2_" in t_lower:
                if run_staging2:
                    templates.append(t)
            elif "_staging_" in t_lower:
                if run_staging:
                    templates.append(t)
            elif "_mart_" in t_lower:
                if run_mart:
                    templates.append(t)
            else:
                # Övriga templates körs alltid
                templates.append(t)

        # Bygg mapping från template-namn till Migration-objekt för spårning
        conn = self._get_connection()
        migrator = Migrator(conn, self.sql_path / "migrations")
        template_migrations = {}
        for migration in migrator.discover_migrations():
            if migrator.is_template_migration(migration):
                template_migrations[migration.name] = migration

        max_concurrent = settings.MAX_CONCURRENT_SQL
        semaphore = asyncio.Semaphore(max_concurrent)
        results: list[tuple[str, str, bool]] = []  # (dataset_id, temp_path, success)

        if on_log:
            cpu_count = settings.MAX_CONCURRENT_SQL
            phases_str = ", ".join(
                [
                    p
                    for p, enabled in [
                        ("Staging", run_staging),
                        ("Staging_2", run_staging2),
                        ("Mart", run_mart),
                    ]
                    if enabled
                ]
            )
            num_datasets = len(parquet_files)
            on_log(f"=== Parallell transform: {num_datasets} datasets, {cpu_count} parallella ===")
            on_log(f"    Faser: {phases_str} ({len(templates)} templates)")

        async def process_dataset(dataset_id: str, parquet_path: str) -> tuple[str, str, bool]:
            """Processa ett dataset i egen temp-DB."""
            async with semaphore:
                temp_db_path = str(settings.get_temp_db_path(dataset_id))
                ds_config = datasets_config.get(dataset_id, {})

                if on_event:
                    on_event(
                        PipelineEvent(
                            event_type="dataset_started",
                            message=f"Transform {dataset_id}",
                            dataset=dataset_id,
                        )
                    )

                try:

                    def do_transform():
                        # Skapa och initiera temp-DB
                        temp_conn = duckdb.connect(temp_db_path)
                        try:
                            # Ladda extensions
                            for ext in settings.DUCKDB_EXTENSIONS:
                                try:
                                    temp_conn.execute(f"INSTALL {ext}")
                                    temp_conn.execute(f"LOAD {ext}")
                                except Exception:
                                    pass

                            # Skapa scheman
                            for schema in settings.DUCKDB_SCHEMAS:
                                temp_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

                            # Skapa migrations-tabell för spårning
                            temp_migrator = Migrator(temp_conn, self.sql_path / "migrations")

                            # Kör init-migrationer (001-003) för att ladda makron
                            temp_migrator.run_init_migrations()

                            # Ladda parquet till raw
                            temp_conn.execute(f"""
                                CREATE OR REPLACE TABLE raw.{dataset_id} AS
                                SELECT * FROM read_parquet('{parquet_path}')
                            """)

                            # Normalisera geometrikolumn
                            self._normalize_geometry_column_in_conn(temp_conn, dataset_id)

                            # Kör alla templates i ordning och spåra
                            for template_name in templates:
                                migration = template_migrations.get(template_name)
                                sql = generator.render_template(
                                    template_name, dataset_id, ds_config
                                )
                                if sql:
                                    temp_conn.execute(sql)

                                    # Registrera som körd
                                    if migration:
                                        template_version = temp_migrator.get_template_version(
                                            migration.version, dataset_id
                                        )
                                        checksum = temp_migrator._calculate_checksum(sql)
                                        template_name_with_ds = f"{migration.name}:{dataset_id}"
                                        try:
                                            tbl = temp_migrator.MIGRATIONS_TABLE
                                            ver = template_version
                                            nm = template_name_with_ds
                                            temp_conn.execute(f"""
                                                INSERT INTO {tbl} (version, name, checksum)
                                                VALUES ('{ver}', '{nm}', '{checksum}')
                                            """)
                                        except Exception:
                                            pass

                        finally:
                            temp_conn.close()

                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, do_transform)

                    if on_event:
                        on_event(
                            PipelineEvent(
                                event_type="dataset_completed",
                                message=f"Klar: {dataset_id}",
                                dataset=dataset_id,
                                status="success",
                            )
                        )

                    if on_log:
                        on_log(f"  {dataset_id}: Transform OK")

                    return (dataset_id, temp_db_path, True)

                except Exception as e:
                    if on_event:
                        on_event(
                            PipelineEvent(
                                event_type="dataset_failed",
                                message=f"Fel: {e}",
                                dataset=dataset_id,
                                status="error",
                            )
                        )
                    if on_log:
                        on_log(f"  {dataset_id}: FEL - {e}")
                    return (dataset_id, temp_db_path, False)

        # Kör alla datasets parallellt
        tasks = [
            process_dataset(dataset_id, parquet_path) for dataset_id, parquet_path in parquet_files
        ]
        results = await asyncio.gather(*tasks)

        success_count = sum(1 for r in results if r[2])
        fail_count = sum(1 for r in results if not r[2])

        if on_log:
            on_log(f"Transform klar: {success_count} lyckade, {fail_count} misslyckade")

        # Returnera lyckade temp-DBs för merge
        return [(r[0], r[1]) for r in results if r[2]]

    def _normalize_geometry_column_in_conn(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
    ) -> None:
        """Normalisera geometrikolumnens namn till 'geom' i given connection."""
        alt_geom_names = ["geometry", "shape", "geometri"]

        try:
            result = conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'raw'
                AND table_name = '{table_name}'
                AND LOWER(column_name) = 'geom'
            """).fetchone()

            if result:
                return

            for alt_name in alt_geom_names:
                result = conn.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'raw'
                    AND table_name = '{table_name}'
                    AND LOWER(column_name) = '{alt_name}'
                """).fetchone()

                if result:
                    actual_col_name = result[0]
                    conn.execute(f"""
                        ALTER TABLE raw.{table_name}
                        RENAME COLUMN "{actual_col_name}" TO geom
                    """)
                    return

        except Exception:
            pass

    async def merge_databases(
        self,
        temp_dbs: list[tuple[str, str]],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Slå ihop temporära databaser till huvuddatabasen.

        Args:
            temp_dbs: Lista av (dataset_id, temp_db_path)
            on_log: Callback för loggmeddelanden
            on_event: Callback för progress-events

        Returns:
            True om alla mergades framgångsrikt
        """
        if not temp_dbs:
            return True

        conn = self._get_connection()
        success = True

        if on_log:
            on_log(f"=== Merge: {len(temp_dbs)} databaser → warehouse ===")

        for i, (dataset_id, temp_db_path) in enumerate(temp_dbs):
            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="progress",
                        message=f"Merge {dataset_id}...",
                        dataset=dataset_id,
                        progress=(i + 1) / len(temp_dbs),
                    )
                )

            try:

                def do_merge():
                    # Attach temp-DB
                    conn.execute(f"ATTACH '{temp_db_path}' AS temp_db (READ_ONLY)")

                    try:
                        # Kopiera alla tabeller från alla scheman
                        # Använd duckdb_tables() istället för information_schema
                        # (information_schema fungerar inte för attached DBs)
                        for schema in settings.DUCKDB_SCHEMAS:
                            tables = conn.execute(f"""
                                SELECT table_name
                                FROM duckdb_tables()
                                WHERE database_name = 'temp_db'
                                AND schema_name = '{schema}'
                            """).fetchall()

                            for (table_name,) in tables:
                                conn.execute(f"""
                                    CREATE OR REPLACE TABLE {schema}.{table_name} AS
                                    SELECT * FROM temp_db.{schema}.{table_name}
                                """)
                    finally:
                        conn.execute("DETACH temp_db")

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, do_merge)

                if on_log:
                    on_log(f"  {dataset_id}: Merge OK")

            except Exception as e:
                if on_log:
                    on_log(f"  {dataset_id}: Merge FEL - {e}")
                success = False

        # Rensa temp-filer
        if on_log:
            on_log("Rensar temporära databaser...")
        settings.cleanup_temp_dbs()

        return success

    async def run_merged_sql(
        self,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör *_merged.sql filer efter att data slagits ihop.

        Dessa filer kan innehålla aggregeringar över alla datasets,
        t.ex. en samlad mart.h3_cells tabell.

        Returns:
            True om alla kördes framgångsrikt
        """
        conn = self._get_connection()
        migrations_dir = self.sql_path / "migrations"

        if not migrations_dir.exists():
            return True

        # Hitta alla *_merged.sql filer
        merged_files = sorted(migrations_dir.glob("*_merged.sql"))

        if not merged_files:
            return True

        if on_log:
            on_log(f"=== Kör {len(merged_files)} merged SQL-filer ===")

        success = True
        for sql_file in merged_files:
            if on_log:
                on_log(f"  Kör {sql_file.name}...")

            try:
                sql = sql_file.read_text()

                def do_sql(sql_to_run=sql):
                    conn.execute(sql_to_run)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, do_sql)

            except Exception as e:
                if on_log:
                    on_log(f"    FEL: {e}")
                success = False

        return success

    async def run_static_sql(
        self,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Kör icke-template SQL-filer i sql/migrations/.

        Hittar och kör alla SQL-filer som INTE är templates
        och INTE är infrastruktur (001-003).

        Returns:
            True om alla kördes framgångsrikt
        """
        conn = self._get_connection()
        migrations_dir = self.sql_path / "migrations"

        if not migrations_dir.exists():
            return True

        success = True
        for sql_file in sorted(migrations_dir.glob("*.sql")):
            # Hoppa över templates
            if "_template.sql" in sql_file.name:
                continue

            # Hoppa över infrastruktur (001-003)
            if sql_file.name[:3] in ("001", "002", "003"):
                continue

            if on_log:
                on_log(f"Kör {sql_file.name}...")

            try:
                sql = sql_file.read_text()

                def do_sql(sql_to_run=sql):
                    conn.execute(sql_to_run)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, do_sql)

            except Exception as e:
                if on_log:
                    on_log(f"  Fel i {sql_file.name}: {e}")
                success = False

        return success

    async def stop(self):
        """Stoppa pågående körning."""
        self._running = False
        # Avbryt alla pågående tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()

    def close(self):
        """Stäng databasanslutning."""
        if self._conn:
            self._conn.close()
            self._conn = None


class MockPipelineRunner(PipelineRunner):
    """Mock runner för att testa TUI utan riktig data."""

    async def run_dataset(
        self,
        dataset_config: dict,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        self._running = True
        dataset_name = dataset_config.get("name", dataset_config.get("id"))
        total_steps = 5

        if on_log:
            on_log(f"[MOCK] Startar {dataset_name}...")

        if on_event:
            on_event(
                PipelineEvent(
                    event_type="dataset_started",
                    message=f"Startar {dataset_name}",
                    dataset=dataset_name,
                    progress=0.0,
                    current_step=0,
                    total_steps=total_steps,
                )
            )

        # Simulera arbete med progress-uppdateringar
        steps = [
            "Ansluter till källa...",
            "Hämtar metadata...",
            "Laddar data...",
            "Validerar geometri...",
            "Sparar till databas...",
        ]

        for i, step_msg in enumerate(steps):
            if not self._running:
                return False

            if on_log:
                on_log(f"[MOCK] {step_msg}")

            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="progress",
                        message=step_msg,
                        dataset=dataset_name,
                        progress=(i + 1) / total_steps,
                        current_step=i + 1,
                        total_steps=total_steps,
                    )
                )

            await asyncio.sleep(0.4)  # 0.4s per steg = 2s totalt

        rows = 1000 + hash(dataset_name) % 9000  # Varierande antal rader

        if on_log:
            on_log(f"[MOCK] Klar: {dataset_name} ({rows} rader)")

        if on_event:
            on_event(
                PipelineEvent(
                    event_type="dataset_completed",
                    message=f"Klar: {dataset_name}",
                    dataset=dataset_name,
                    status="success",
                    rows_count=rows,
                    progress=1.0,
                )
            )

        self._running = False
        return True

    async def run_transforms(
        self,
        dataset_ids: list[str] | None = None,
        folders: list[str] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        target_folders = folders if folders else ["staging", "mart"]
        transform_steps = []

        # Lägg till common-steg för valda mappar
        if "staging" in target_folders:
            transform_steps.append("staging/_common/00_staging_procedure.sql")
        if "mart" in target_folders:
            transform_steps.append("mart/_common/01_output_functions.sql")

        # Lägg till dataset-specifika steg
        if dataset_ids:
            for ds_id in dataset_ids:
                if "staging" in target_folders:
                    transform_steps.append(f"staging/{ds_id}/01_staging.sql")
                if "mart" in target_folders:
                    transform_steps.append(f"mart/{ds_id}/01_final.sql")

        for step in transform_steps:
            if on_log:
                on_log(f"[MOCK] Kör {step}...")
            await asyncio.sleep(0.2)

        folder_str = ", ".join(target_folders)
        if on_log:
            on_log(f"[MOCK] {folder_str} transformationer klara")
        return True

    async def run_parallel_extract(
        self,
        dataset_configs: list[dict],
        output_dir: str | Path | None = None,
        max_concurrent: int | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> ParallelExtractResult:
        """Mock parallell extraktion."""
        self._running = True
        output_path = Path(output_dir) if output_dir else settings.RAW_DIR
        concurrent = max_concurrent or settings.MAX_CONCURRENT_EXTRACTS
        semaphore = asyncio.Semaphore(concurrent)
        parquet_files: list[tuple[str, str]] = []

        async def extract_one(config: dict) -> tuple[str, str]:
            dataset_id = config.get("id", config.get("name"))

            async with semaphore:
                if not self._running:
                    return (dataset_id, "")

                if on_event:
                    on_event(
                        PipelineEvent(
                            event_type="dataset_started",
                            message=f"Startar {dataset_id}",
                            dataset=dataset_id,
                        )
                    )

                steps = ["Hämtar...", "Bearbetar...", "Sparar parquet..."]
                for i, step in enumerate(steps):
                    if on_log:
                        on_log(f"[MOCK] {dataset_id}: {step}")
                    if on_event:
                        on_event(
                            PipelineEvent(
                                event_type="progress",
                                message=step,
                                dataset=dataset_id,
                                progress=(i + 1) / len(steps),
                            )
                        )
                    await asyncio.sleep(0.3)

                rows = 1000 + hash(dataset_id) % 9000

                if on_event:
                    on_event(
                        PipelineEvent(
                            event_type="dataset_completed",
                            message=f"Klar: {dataset_id} ({rows} rader)",
                            dataset=dataset_id,
                            status="success",
                            rows_count=rows,
                        )
                    )

                return (dataset_id, str(output_path / f"{dataset_id}.parquet"))

        tasks = [extract_one(config) for config in dataset_configs]
        results = await asyncio.gather(*tasks)
        parquet_files = [(r[0], r[1]) for r in results if r[1]]

        return ParallelExtractResult(
            success=True,
            parquet_files=parquet_files,
            failed=[],
        )

    async def load_parquet_to_db(
        self,
        parquet_files: list[tuple[str, str]],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Mock laddning av parquet till databas."""
        for i, (dataset_id, _) in enumerate(parquet_files):
            if on_log:
                on_log(f"[MOCK] Laddar {dataset_id} till databas...")
            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="progress",
                        message=f"Laddar {dataset_id}...",
                        dataset=dataset_id,
                        progress=(i + 1) / len(parquet_files),
                    )
                )
            await asyncio.sleep(0.1)

        if on_log:
            on_log("[MOCK] Alla parquet-filer laddade")
        return True

    async def run_templates(
        self,
        dataset_ids: list[str],
        template_filter: str | None = None,
        phase_name: str | None = None,
        max_concurrent: int = 4,
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Mock template-körning (parallell)."""
        phase = phase_name or "Templates"
        if on_log:
            on_log(f"[MOCK] === Kör {phase} ({max_concurrent} parallella) ===")

        semaphore = asyncio.Semaphore(max_concurrent)
        completed = 0

        async def process_one(dataset_id: str, index: int) -> None:
            nonlocal completed
            async with semaphore:
                if on_log:
                    on_log(f"[MOCK]   {dataset_id}: {phase} OK")
                if on_event:
                    on_event(
                        PipelineEvent(
                            event_type="progress",
                            message=f"{phase} {dataset_id}...",
                            dataset=dataset_id,
                            progress=(index + 1) / len(dataset_ids),
                        )
                    )
                await asyncio.sleep(0.05)
                completed += 1

        tasks = [process_one(ds_id, i) for i, ds_id in enumerate(dataset_ids)]
        await asyncio.gather(*tasks)

        if on_log:
            on_log(f"[MOCK] {phase} klar: {len(dataset_ids)} lyckade, 0 misslyckade")
        return True

    async def run_parallel_transform(
        self,
        parquet_files: list[tuple[str, str]],
        phases: tuple[bool, bool, bool] | None = None,
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> list[tuple[str, str]]:
        """Mock parallell transform."""
        run_staging, run_staging2, run_mart = phases if phases else (True, True, True)
        phases_str = ", ".join(
            [
                p
                for p, enabled in [
                    ("Staging", run_staging),
                    ("Staging_2", run_staging2),
                    ("Mart", run_mart),
                ]
                if enabled
            ]
        )
        if on_log:
            on_log(f"[MOCK] === Parallell transform: {len(parquet_files)} datasets ===")
            on_log(f"[MOCK]     Faser: {phases_str}")

        results = []
        for dataset_id, parquet_path in parquet_files:
            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="dataset_started",
                        message=f"Transform {dataset_id}",
                        dataset=dataset_id,
                    )
                )

            await asyncio.sleep(0.1)

            if on_event:
                on_event(
                    PipelineEvent(
                        event_type="dataset_completed",
                        message=f"Klar: {dataset_id}",
                        dataset=dataset_id,
                        status="success",
                    )
                )

            if on_log:
                on_log(f"[MOCK]   {dataset_id}: Transform OK")

            results.append((dataset_id, f"data/temp/{dataset_id}.duckdb"))

        return results

    async def merge_databases(
        self,
        temp_dbs: list[tuple[str, str]],
        on_log: Callable[[str], None] | None = None,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> bool:
        """Mock merge."""
        if on_log:
            on_log(f"[MOCK] === Merge: {len(temp_dbs)} databaser ===")

        for dataset_id, _ in temp_dbs:
            if on_log:
                on_log(f"[MOCK]   {dataset_id}: Merge OK")
            await asyncio.sleep(0.05)

        return True

    async def run_merged_sql(
        self,
        on_log: Callable[[str], None] | None = None,
    ) -> bool:
        """Mock merged SQL."""
        if on_log:
            on_log("[MOCK] === Kör merged SQL ===")
        await asyncio.sleep(0.1)
        return True
