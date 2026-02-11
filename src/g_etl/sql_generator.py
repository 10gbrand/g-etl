"""SQL Generator för template-baserade transformationer.

Generisk SQL-generator som:
1. Hittar alla *_template.sql filer i sql/migrations/ (root + pipeline-underkataloger)
2. Ersätter {{ variabel }} med värden från datasets.yml
3. Kör templates i nummerordning

Stödjer multi-pipeline:
- Root-templates (004_*) körs för alla datasets
- Pipeline-underkataloger (aab_ext_restr/) körs per pipeline
- Varje dataset tillhör exakt en pipeline via datasets.yml

Användning:
    from g_etl.sql_generator import SQLGenerator

    generator = SQLGenerator()

    # Lista templates för en pipeline (root + pipeline-specifika)
    templates = generator.list_templates(pipeline="ext_restr")

    # Rendera en template med pipeline-kontext
    sql = generator.render_template(
        "aab_ext_restr/001_staging_normalisering_template.sql",
        "naturreservat", config, pipeline="ext_restr"
    )
"""

from dataclasses import dataclass, field
from pathlib import Path

from g_etl.settings import settings


@dataclass
class TemplateInfo:
    """Metadata för en template-fil."""

    filename: str  # "001_staging_normalisering_template.sql"
    relative_path: str  # "aab_ext_restr/001_staging_normalisering_template.sql"
    pipeline: str | None  # "ext_restr" (kort pipeline-namn) eller None för root
    pipeline_dir: str | None  # "aab_ext_restr" (katalognamn) eller None
    number: str  # "001"


@dataclass
class DatasetConfig:
    """Konfiguration för ett dataset från field_mapping i datasets.yml."""

    # Identitet
    dataset_id: str = ""
    pipeline: str = ""  # Pipeline-namn (t.ex. "ext_restr")

    # Fältmappning (från field_mapping)
    source_id_column: str = ""
    geometry_column: str = "geom"
    h3_center_resolution: int = 13
    h3_polyfill_resolution: int = 11
    h3_line_resolution: int = 12
    h3_point_resolution: int = 13
    h3_line_buffer_meters: int = 10
    klass: str = ""
    grupp: str = ""
    typ: str = ""
    leverantor: str = ""

    # Extra data-mappningar
    data_mappings: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dataset_yml(cls, dataset_id: str, config: dict) -> "DatasetConfig":
        """Skapa DatasetConfig från datasets.yml-entry."""
        fm = config.get("field_mapping", {})

        return cls(
            dataset_id=dataset_id,
            pipeline=config.get("pipeline", ""),
            source_id_column=fm.get("source_id_column", ""),
            geometry_column=fm.get("geometry_column", "geom"),
            h3_center_resolution=fm.get("h3_center_resolution", 13),
            h3_polyfill_resolution=fm.get("h3_polyfill_resolution", 11),
            h3_line_resolution=fm.get("h3_line_resolution", 12),
            h3_point_resolution=fm.get("h3_point_resolution", 13),
            h3_line_buffer_meters=fm.get("h3_line_buffer_meters", 10),
            klass=fm.get("klass", ""),
            grupp=fm.get("grupp", ""),
            typ=fm.get("typ", ""),
            leverantor=fm.get("leverantor", ""),
            data_mappings=fm.get("data_mappings", {}),
        )


class SQLGenerator:
    """Generisk SQL-generator för template-baserade transformationer.

    Stödjer multi-pipeline med underkataloger i sql/migrations/:
    - Root-templates (004_*_template.sql) körs för alla datasets
    - Pipeline-underkataloger (aab_ext_restr/) körs per pipeline
    """

    def __init__(self, sql_path: Path | None = None):
        self.sql_path = sql_path or settings.SQL_DIR
        self._template_cache: dict[str, str] = {}

    def _load_template(self, template_path: str) -> str:
        """Läs mall från fil (cachad).

        Args:
            template_path: Relativ sökväg från migrations/ (t.ex.
                "004_staging_transform_template.sql" eller
                "aab_ext_restr/001_staging_normalisering_template.sql")

        Extraherar endast 'migrate:up' sektionen om den finns.
        """
        if template_path not in self._template_cache:
            full_path = self.sql_path / "migrations" / template_path
            if full_path.exists():
                content = full_path.read_text()
                # Extrahera endast up-sektionen (stoppa vid migrate:down)
                if "-- migrate:down" in content:
                    content = content.split("-- migrate:down")[0]
                # Ta bort migrate:up markören
                if "-- migrate:up" in content:
                    content = content.split("-- migrate:up", 1)[1]
                self._template_cache[template_path] = content.strip()
            else:
                self._template_cache[template_path] = ""
        return self._template_cache[template_path]

    # === Pipeline-hantering ===

    def _dir_to_pipeline_name(self, dirname: str) -> str:
        """Extrahera pipeline-namn från katalognamn (ta bort ordningsprefix).

        Konvention: katalognamn = {prefix}_{pipeline_namn}
        där prefix är 3 bokstäver (t.ex. aaa, aab).

        Exempel:
            aab_ext_restr → ext_restr
            aaa_avdelning → avdelning
            ext_restr → ext_restr (ingen prefix)
        """
        parts = dirname.split("_", 1)
        if len(parts) == 2 and len(parts[0]) == 3 and parts[0].isalpha():
            return parts[1]
        return dirname

    def _pipeline_name_to_dir(self, pipeline: str) -> str | None:
        """Hitta katalognamn för ett pipeline-namn.

        Söker igenom alla underkataloger i sql/migrations/ och matchar
        pipeline-namn efter att ordningsprefix strippats.

        Returns:
            Katalognamn (t.ex. "aab_ext_restr") eller None om inte hittad.
        """
        migrations_dir = self.sql_path / "migrations"
        if not migrations_dir.exists():
            return None

        for subdir in sorted(migrations_dir.iterdir()):
            if subdir.is_dir() and self._dir_to_pipeline_name(subdir.name) == pipeline:
                return subdir.name
        return None

    def list_pipeline_dirs(self) -> list[tuple[str, str]]:
        """Lista alla pipeline-underkataloger.

        Returns:
            Lista av (katalognamn, pipeline-namn) sorterat på katalognamn.
        """
        migrations_dir = self.sql_path / "migrations"
        if not migrations_dir.exists():
            return []

        result = []
        for subdir in sorted(migrations_dir.iterdir()):
            if subdir.is_dir() and not subdir.name.startswith((".", "_")):
                pipeline_name = self._dir_to_pipeline_name(subdir.name)
                result.append((subdir.name, pipeline_name))
        return result

    def list_templates(self, pipeline: str | None = None) -> list[TemplateInfo]:
        """Lista templates för en specifik pipeline.

        Returnerar alltid delade root-templates plus pipeline-specifika
        templates om pipeline anges.

        Args:
            pipeline: Pipeline-namn (t.ex. "ext_restr").
                Om None returneras bara root-templates.

        Returns:
            Lista av TemplateInfo sorterade i körningsordning.
        """
        migrations_dir = self.sql_path / "migrations"
        if not migrations_dir.exists():
            return []

        result = []

        # 1. Delade root-templates (alltid inkluderade)
        for f in sorted(migrations_dir.glob("*_template.sql")):
            num = self._extract_template_number(f.name)
            result.append(
                TemplateInfo(
                    filename=f.name,
                    relative_path=f.name,
                    pipeline=None,
                    pipeline_dir=None,
                    number=num,
                )
            )

        # 2. Pipeline-specifika templates
        if pipeline:
            pipeline_dir = self._pipeline_name_to_dir(pipeline)
            if pipeline_dir:
                subdir = migrations_dir / pipeline_dir
                for f in sorted(subdir.glob("*_template.sql")):
                    num = self._extract_template_number(f.name)
                    result.append(
                        TemplateInfo(
                            filename=f.name,
                            relative_path=f"{pipeline_dir}/{f.name}",
                            pipeline=pipeline,
                            pipeline_dir=pipeline_dir,
                            number=num,
                        )
                    )

        return result

    # === Template-nummer och scheman ===

    def _is_column_ref(self, value: str | None) -> bool:
        """Kolla om ett värde är en kolumnreferens (börjar med $)."""
        if value is None or value == "":
            return False
        return value.startswith("$")

    def _get_column_name(self, value: str) -> str:
        """Extrahera kolumnnamn från $-prefixat värde."""
        if value.startswith("$"):
            return value[1:]
        return value

    def _extract_template_number(self, template_name: str) -> str:
        """Extrahera numret (NNN) från template-filnamn.

        Exempel: '004_staging_transform_template.sql' -> '004'
        """
        parts = template_name.split("_")
        if parts and parts[0].isdigit():
            return parts[0]
        return ""

    def _get_schema_name(self, template_name: str, pipeline: str | None = None) -> str:
        """Generera schemanamn baserat på template-typ, nummer och pipeline.

        Root-staging: staging_004
        Pipeline-staging: staging_ext_restr_001
        Mart: mart (alltid)
        """
        num = self._extract_template_number(template_name)
        if "_staging_" in template_name.lower():
            if pipeline:
                return f"staging_{pipeline}_{num}" if num else f"staging_{pipeline}"
            return f"staging_{num}" if num else "staging"
        elif "_mart_" in template_name.lower():
            return "mart"
        return "staging"

    def _find_last_staging_schema(self, pipeline: str, templates: list[TemplateInfo] | None) -> str:
        """Hitta senaste staging-schemat i en pipeline.

        Söker igenom templates bakifrån och returnerar schemat
        för den sista staging-template:n.
        """
        if not templates:
            return "staging_004"

        # Filtrera pipeline-templates med staging
        staging_templates = [
            t for t in templates if t.pipeline == pipeline and "_staging_" in t.filename.lower()
        ]
        if staging_templates:
            last = staging_templates[-1]
            return f"staging_{pipeline}_{last.number}"

        # Inga staging i pipeline → referera till sista delade
        return "staging_004"

    def _get_prev_schema_name(
        self,
        template_name: str,
        pipeline: str | None = None,
        pipeline_templates: list[TemplateInfo] | None = None,
    ) -> str:
        """Hämta föregående schema för referens.

        Root-templates:
            004_staging_* → raw
            005_staging_* → staging_004

        Pipeline-templates (t.ex. ext_restr):
            001_staging_* → staging_004 (sista delade schemat)
            002_staging_* → staging_ext_restr_001
            001_mart_*    → staging_004 (om inga staging i pipeline)
            003_mart_*    → staging_ext_restr_002 (sista staging)
        """
        num = self._extract_template_number(template_name)
        if not num:
            return "raw"

        num_int = int(num)

        if pipeline:
            # Pipeline-kontext
            if "_staging_" in template_name.lower():
                if num_int <= 1:
                    # Första staging i pipeline → sista delade schemat
                    return "staging_004"
                # Kedjad staging inom pipeline
                return f"staging_{pipeline}_{num_int - 1:03d}"
            elif "_mart_" in template_name.lower():
                # Mart refererar till sista staging i pipelinen
                return self._find_last_staging_schema(pipeline, pipeline_templates)
            return "staging_004"

        # Root-kontext (utan pipeline)
        if "_staging_" in template_name.lower():
            if num_int <= 4:
                return "raw"
            return f"staging_{num_int - 1:03d}"
        elif "_mart_" in template_name.lower():
            # Mart i root refererar till staging_004 (eller senaste)
            return "staging_004"

        return "raw"

    # === Variabelsubstitution ===

    def _build_variables(
        self,
        config: DatasetConfig,
        template_name: str = "",
        pipeline: str | None = None,
        pipeline_templates: list[TemplateInfo] | None = None,
    ) -> dict[str, str]:
        """Bygg variabel-dict för substitution."""
        # Schema-variabler baserade på template-nummer och pipeline
        schema = self._get_schema_name(template_name, pipeline) if template_name else "staging"
        prev_schema = (
            self._get_prev_schema_name(template_name, pipeline, pipeline_templates)
            if template_name
            else "raw"
        )

        # Grundläggande variabler
        variables = {
            "dataset_id": config.dataset_id,
            "schema": schema,
            "prev_schema": prev_schema,
            "source_id_column": self._get_column_name(config.source_id_column),
            "geometry_column": config.geometry_column,
            "h3_center_resolution": str(config.h3_center_resolution),
            "h3_polyfill_resolution": str(config.h3_polyfill_resolution),
            "h3_line_resolution": str(config.h3_line_resolution),
            "h3_point_resolution": str(config.h3_point_resolution),
            "h3_line_buffer_meters": str(config.h3_line_buffer_meters),
            "klass": config.klass,
            "leverantor": config.leverantor,
        }

        # source_id_expr - kolumnreferens eller tom sträng
        src_col = self._get_column_name(config.source_id_column)
        if src_col and src_col.strip():
            variables["source_id_expr"] = f"s.{src_col}::VARCHAR"
        else:
            variables["source_id_expr"] = "''"

        # grupp_expr - kolumnreferens ($prefix) eller literal
        if self._is_column_ref(config.grupp):
            col_name = self._get_column_name(config.grupp)
            variables["grupp_expr"] = f"COALESCE(s.{col_name}::VARCHAR, '')"
        else:
            variables["grupp_expr"] = f"'{config.grupp}'" if config.grupp else "''"

        # typ_expr - kolumnreferens ($prefix) eller literal
        if config.typ is None or config.typ == "":
            variables["typ_expr"] = "''"
        elif self._is_column_ref(config.typ):
            col_name = self._get_column_name(config.typ)
            variables["typ_expr"] = f"COALESCE(s.{col_name}::VARCHAR, '')"
        else:
            variables["typ_expr"] = f"'{config.typ}'"

        # data_N_expr för extra kolumner (alltid kolumnreferenser)
        for i in range(1, 6):
            target = f"data_{i}"
            if target in config.data_mappings:
                source = self._get_column_name(config.data_mappings[target])
                variables[f"data_{i}_expr"] = f"COALESCE(s.{source}::VARCHAR, '')"
            else:
                variables[f"data_{i}_expr"] = "''"

        return variables

    def _substitute(self, template: str, variables: dict[str, str]) -> str:
        """Ersätt {{ variabel }} med värden."""
        result = template
        for key, value in variables.items():
            result = result.replace("{{ " + key + " }}", value)
            result = result.replace("{{" + key + "}}", value)
        return result

    # === Rendering ===

    def render_template(
        self,
        template_path: str,
        dataset_id: str,
        config: dict | DatasetConfig | None = None,
        pipeline: str | None = None,
        pipeline_templates: list[TemplateInfo] | None = None,
    ) -> str:
        """Rendera en template med variabelsubstitution.

        Args:
            template_path: Relativ sökväg (t.ex. "004_staging_transform_template.sql"
                eller "aab_ext_restr/001_staging_normalisering_template.sql")
            dataset_id: Dataset-ID
            config: Dict från datasets.yml eller DatasetConfig
            pipeline: Pipeline-namn (t.ex. "ext_restr") för schema-generering
            pipeline_templates: Lista av TemplateInfo för prev_schema-beräkning

        Returns:
            SQL-sträng med substituerade variabler
        """
        template = self._load_template(template_path)
        if not template:
            return ""

        # Konvertera config till DatasetConfig
        if config is None:
            cfg = DatasetConfig(dataset_id=dataset_id)
        elif isinstance(config, dict):
            cfg = DatasetConfig.from_dataset_yml(dataset_id, config)
        else:
            cfg = config
            cfg.dataset_id = dataset_id

        # Extrahera filnamn för schema-logik (utan katalogprefix)
        filename = Path(template_path).name

        variables = self._build_variables(cfg, filename, pipeline, pipeline_templates)
        return self._substitute(template, variables)

    def get_schema_create_sql(self, template_name: str, pipeline: str | None = None) -> str:
        """Generera SQL för att skapa schemat som template använder.

        Args:
            template_name: Template-filnamn (utan katalogprefix)
            pipeline: Pipeline-namn för pipeline-specifika scheman

        Returns:
            SQL-sats för CREATE SCHEMA IF NOT EXISTS.
        """
        schema = self._get_schema_name(template_name, pipeline)
        return f"CREATE SCHEMA IF NOT EXISTS {schema};"

    def render_all_templates(
        self,
        dataset_id: str,
        config: dict | DatasetConfig | None = None,
        pipeline: str | None = None,
    ) -> list[tuple[str, str]]:
        """Rendera alla templates för ett dataset.

        Args:
            dataset_id: Dataset-ID
            config: Dict från datasets.yml eller DatasetConfig
            pipeline: Pipeline-namn (t.ex. "ext_restr")

        Returns:
            Lista av (template_path, rendered_sql) tuples i nummerordning
        """
        templates = self.list_templates(pipeline=pipeline)
        results = []
        for tmpl in templates:
            sql = self.render_template(tmpl.relative_path, dataset_id, config, pipeline, templates)
            if sql:
                results.append((tmpl.relative_path, sql))
        return results

    # === Bakåtkompatibla metoder ===

    def staging_sql(self, dataset_id: str, config: dict | None = None) -> str:
        """Generera staging SQL (bakåtkompatibel)."""
        full_config = {"staging": config or {}}
        return self.render_template(
            "004_staging_transform_template.sql",
            dataset_id,
            full_config,
        )

    def staging2_sql(self, dataset_id: str, config: dict | None = None) -> str:
        """Generera staging normalisering SQL (bakåtkompatibel)."""
        full_config = {"staging": config or {}}
        return self.render_template(
            "aab_ext_restr/001_staging_normalisering_template.sql",
            dataset_id,
            full_config,
            pipeline="ext_restr",
        )

    def mart_h3_sql(self) -> str:
        """Läs mart.h3_cells SQL."""
        return self._load_template("aab_ext_restr/002_mart_h3_cells_template.sql")


# Singleton-instans
_generator = None


def get_generator() -> SQLGenerator:
    """Hämta singleton-instans av SQLGenerator."""
    global _generator
    if _generator is None:
        _generator = SQLGenerator()
    return _generator


def staging_sql(dataset_id: str, config: dict | None = None) -> str:
    """Generera staging SQL för ett dataset."""
    return get_generator().staging_sql(dataset_id, config)


def staging2_sql(dataset_id: str, config: dict | None = None) -> str:
    """Generera staging normalisering SQL för ett dataset."""
    return get_generator().staging2_sql(dataset_id, config)


def render_template(template_name: str, dataset_id: str, config: dict | None = None) -> str:
    """Rendera en template med config."""
    return get_generator().render_template(template_name, dataset_id, config)


def list_templates() -> list[TemplateInfo]:
    """Lista alla root-templates."""
    return get_generator().list_templates()
