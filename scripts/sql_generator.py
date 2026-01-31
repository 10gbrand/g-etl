"""SQL Generator för template-baserade transformationer.

Generisk SQL-generator som:
1. Hittar alla *_template.sql filer i sql/migrations/
2. Ersätter {{ variabel }} med värden från datasets.yml
3. Kör templates i nummerordning

Användning:
    from scripts.sql_generator import SQLGenerator

    generator = SQLGenerator()

    # Lista alla templates
    templates = generator.list_templates()

    # Generera SQL för ett dataset med en specifik template
    sql = generator.render_template("004_staging_transform_template.sql", "naturreservat", config)

    # Generera SQL för alla templates för ett dataset
    for template, sql in generator.render_all_templates("naturreservat", config):
        conn.execute(sql)
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

from config.settings import settings


@dataclass
class DatasetConfig:
    """Konfiguration för ett dataset från field_mapping i datasets.yml."""

    # Identitet
    dataset_id: str = ""

    # Fältmappning (från field_mapping)
    source_id_column: str = ""
    geometry_column: str = "geom"
    h3_center_resolution: int = 13
    h3_polyfill_resolution: int = 11
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
            source_id_column=fm.get("source_id_column", ""),
            geometry_column=fm.get("geometry_column", "geom"),
            h3_center_resolution=fm.get("h3_center_resolution", 13),
            h3_polyfill_resolution=fm.get("h3_polyfill_resolution", 11),
            klass=fm.get("klass", ""),
            grupp=fm.get("grupp", ""),
            typ=fm.get("typ", ""),
            leverantor=fm.get("leverantor", ""),
            data_mappings=fm.get("data_mappings", {}),
        )


class SQLGenerator:
    """Generisk SQL-generator för template-baserade transformationer."""

    def __init__(self, sql_path: Path | None = None):
        self.sql_path = sql_path or settings.SQL_DIR
        self._template_cache: dict[str, str] = {}

    def _load_template(self, template_name: str) -> str:
        """Läs mall från fil (cachad).

        Extraherar endast 'migrate:up' sektionen om den finns.
        """
        if template_name not in self._template_cache:
            template_path = self.sql_path / "migrations" / template_name
            if template_path.exists():
                content = template_path.read_text()
                # Extrahera endast up-sektionen (stoppa vid migrate:down)
                if "-- migrate:down" in content:
                    content = content.split("-- migrate:down")[0]
                # Ta bort migrate:up markören
                if "-- migrate:up" in content:
                    content = content.split("-- migrate:up", 1)[1]
                self._template_cache[template_name] = content.strip()
            else:
                self._template_cache[template_name] = ""
        return self._template_cache[template_name]

    def list_templates(self) -> list[str]:
        """Lista alla template-filer i nummerordning."""
        migrations_dir = self.sql_path / "migrations"
        if not migrations_dir.exists():
            return []

        templates = sorted([
            f.name for f in migrations_dir.glob("*_template.sql")
        ])
        return templates

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

    def _build_variables(self, config: DatasetConfig) -> dict[str, str]:
        """Bygg variabel-dict för substitution."""
        # Grundläggande variabler
        variables = {
            "dataset_id": config.dataset_id,
            "source_id_column": self._get_column_name(config.source_id_column),
            "geometry_column": config.geometry_column,
            "h3_center_resolution": str(config.h3_center_resolution),
            "h3_polyfill_resolution": str(config.h3_polyfill_resolution),
            "klass": config.klass,
            "leverantor": config.leverantor,
        }

        # source_id_expr - kolumnreferens eller tom sträng
        # source_id_column är alltid en kolumnreferens (kan ha $ eller inte)
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

    def render_template(
        self,
        template_name: str,
        dataset_id: str,
        config: dict | DatasetConfig | None = None,
    ) -> str:
        """Rendera en template med variabelsubstitution.

        Args:
            template_name: Filnamn på template (t.ex. "004_staging_transform_template.sql")
            dataset_id: Dataset-ID
            config: Dict från datasets.yml eller DatasetConfig

        Returns:
            SQL-sträng med substituerade variabler
        """
        template = self._load_template(template_name)
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

        variables = self._build_variables(cfg)
        return self._substitute(template, variables)

    def render_all_templates(
        self,
        dataset_id: str,
        config: dict | DatasetConfig | None = None,
    ) -> list[tuple[str, str]]:
        """Rendera alla templates för ett dataset.

        Args:
            dataset_id: Dataset-ID
            config: Dict från datasets.yml eller DatasetConfig

        Returns:
            Lista av (template_name, rendered_sql) tuples i nummerordning
        """
        results = []
        for template_name in self.list_templates():
            sql = self.render_template(template_name, dataset_id, config)
            if sql:
                results.append((template_name, sql))
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
        """Generera staging_2 SQL (bakåtkompatibel)."""
        full_config = {"staging_2": config or {}}
        return self.render_template(
            "005_staging2_normalisering_template.sql",
            dataset_id,
            full_config,
        )

    def mart_h3_sql(self) -> str:
        """Läs mart.h3_cells SQL."""
        return self._load_template("006_mart_h3_cells.sql")


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
    """Generera staging_2 SQL för ett dataset."""
    return get_generator().staging2_sql(dataset_id, config)


def render_template(template_name: str, dataset_id: str, config: dict | None = None) -> str:
    """Rendera en template med config."""
    return get_generator().render_template(template_name, dataset_id, config)


def list_templates() -> list[str]:
    """Lista alla templates."""
    return get_generator().list_templates()
