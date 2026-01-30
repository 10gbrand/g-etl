"""SQL Generator för staging och mart-tabeller.

Genererar SQL baserat på konfiguration i datasets.yml och använder
makron definierade i migrations.

Användning:
    from scripts.sql_generator import SQLGenerator

    generator = SQLGenerator()
    sql = generator.staging_sql("avverkningsanmalningar", {"source_id_column": "beteckn"})
    conn.execute(sql)
"""

from dataclasses import dataclass, field


@dataclass
class StagingConfig:
    """Konfiguration för staging-transformation."""

    source_id_column: str = "id"
    geometry_column: str = "geom"
    h3_resolution: int = 13
    h3_polyfill_resolution: int = 11
    where_clause: str | None = None


@dataclass
class Staging2Config:
    """Konfiguration för staging_2-normalisering."""

    source_id_column: str = "id"
    klass: str = ""
    grupp: str = ""
    typ: str | None = None  # None = ingen typ-kolumn, str = kolumnnamn
    leverantor: str = ""
    data_mappings: dict[str, str] = field(default_factory=dict)  # {source_col: target_col}


class SQLGenerator:
    """Genererar SQL för staging och mart-transformationer."""

    def staging_sql(self, dataset_id: str, config: dict | StagingConfig | None = None) -> str:
        """Generera SQL för staging-transformation.

        Använder makron från 004_staging_procedure.sql:
        - validate_geom()
        - wgs84_centroid_lat/lng()
        - h3_centroid()
        - h3_polyfill()
        - json_without_geom()

        Args:
            dataset_id: ID för datasetet (t.ex. 'avverkningsanmalningar')
            config: StagingConfig eller dict med konfiguration

        Returns:
            SQL-sträng för CREATE OR REPLACE TABLE
        """
        if config is None:
            cfg = StagingConfig()
        elif isinstance(config, dict):
            cfg = StagingConfig(**{k: v for k, v in config.items() if hasattr(StagingConfig, k)})
        else:
            cfg = config

        where_clause = f"WHERE {cfg.where_clause}" if cfg.where_clause else "WHERE geom IS NOT NULL"

        return f"""
-- Auto-genererad staging för {dataset_id}
-- Källa: raw.{dataset_id}
-- Konfiguration: source_id={cfg.source_id_column}

CREATE OR REPLACE TABLE staging.{dataset_id} AS
WITH source_data AS (
    SELECT * FROM raw.{dataset_id}
    {where_clause}
)
SELECT
    -- Alla originalkolumner exklusive geometri
    s.* EXCLUDE (geom),

    -- === VALIDERAD GEOMETRI ===
    validate_geom(s.geom) AS geom,

    -- === STAGING METADATA ===
    CURRENT_TIMESTAMP AS _imported_at,
    MD5(ST_AsText(s.geom)) AS _geom_md5,
    MD5(to_json(s)::VARCHAR) AS _attr_md5,
    json_without_geom(to_json(s)) AS _json_data,
    MD5(CAST(s.{cfg.source_id_column} AS VARCHAR)) AS _source_id_md5,

    -- === CENTROID (WGS84) ===
    wgs84_centroid_lat(s.geom) AS _centroid_lat,
    wgs84_centroid_lng(s.geom) AS _centroid_lng,

    -- === H3 INDEX ===
    h3_centroid(s.geom) AS _h3_index,
    h3_polyfill(s.geom) AS _h3_cells,

    -- === RESERVERAD ===
    NULL::VARCHAR AS _a5_index

FROM source_data s;
"""

    def _is_column_ref(self, value: str | None) -> bool:
        """Kolla om ett värde är en kolumnreferens (inte en literal sträng)."""
        if value is None or value == "":
            return False
        # Om det börjar med ' är det en literal
        if value.startswith("'"):
            return False
        # Om det är en giltig identifierare (börjar med bokstav/underscore)
        # och inte innehåller mellanslag, anta att det är en kolumnreferens
        if value[0].isalpha() or value[0] == "_":
            return True
        return False

    def staging2_sql(self, dataset_id: str, config: dict | Staging2Config | None = None) -> str:
        """Generera SQL för staging_2-normalisering.

        Args:
            dataset_id: ID för datasetet
            config: Staging2Config eller dict med konfiguration

        Returns:
            SQL-sträng för CREATE OR REPLACE TABLE
        """
        if config is None:
            cfg = Staging2Config()
        elif isinstance(config, dict):
            cfg = Staging2Config(**{k: v for k, v in config.items() if hasattr(Staging2Config, k)})
        else:
            cfg = config

        # Hantera source_id_column - om tom, använd '' som literal
        if cfg.source_id_column and cfg.source_id_column.strip():
            source_id_expr = f"s.{cfg.source_id_column}::VARCHAR"
        else:
            source_id_expr = "''"

        # Hantera grupp - kan vara kolumnreferens eller literal
        if self._is_column_ref(cfg.grupp):
            grupp_expr = f"COALESCE(s.{cfg.grupp}::VARCHAR, '')"
        else:
            grupp_expr = f"'{cfg.grupp}'" if cfg.grupp else "''"

        # Hantera typ - kan vara kolumnreferens eller literal
        if cfg.typ is None or cfg.typ == "":
            typ_expr = "''"
        elif self._is_column_ref(cfg.typ):
            typ_expr = f"COALESCE(s.{cfg.typ}::VARCHAR, '')"
        else:
            typ_expr = f"'{cfg.typ}'"

        # Bygg data-kolumner
        data_cols = []
        for i in range(1, 6):
            target = f"data_{i}"
            if target in cfg.data_mappings:
                source = cfg.data_mappings[target]
                data_cols.append(f"COALESCE(s.{source}::VARCHAR, '') AS {target}")
            else:
                # Kolla om det finns en mapping med source-namn
                source_match = [k for k, v in cfg.data_mappings.items() if v == target]
                if source_match:
                    data_cols.append(f"COALESCE(s.{source_match[0]}::VARCHAR, '') AS {target}")
                else:
                    data_cols.append(f"'' AS {target}")

        data_cols_sql = ",\n    ".join(data_cols)

        return f"""
-- Auto-genererad staging_2 för {dataset_id}
-- Källa: staging.{dataset_id}
-- Konfiguration: klass={cfg.klass}, leverantor={cfg.leverantor}

CREATE OR REPLACE TABLE staging_2.{dataset_id} AS
SELECT
    s._source_id_md5 AS id,
    {source_id_expr} AS source_id,
    '{cfg.klass}' AS klass,
    {grupp_expr} AS grupp,
    {typ_expr} AS typ,
    '{cfg.leverantor}' AS leverantor,
    s._h3_index AS h3_center,
    s._h3_cells AS h3_cells,
    s._json_data AS json_data,
    {data_cols_sql},
    s.geom

FROM staging.{dataset_id} s;
"""

    def mart_h3_sql(self) -> str:
        """Generera SQL för mart.h3_cells aggregering.

        Skapar en tabell som aggregerar alla H3-celler från staging_2.

        Returns:
            SQL-sträng för CREATE OR REPLACE TABLE
        """
        return """
-- Auto-genererad mart.h3_cells
-- Aggregerar H3-celler från alla staging_2-tabeller
-- Körs efter att alla staging_2-tabeller är skapade

CREATE OR REPLACE TABLE mart.h3_cells (
    h3_cell VARCHAR NOT NULL,
    dataset VARCHAR NOT NULL,
    leverantor VARCHAR,
    klass VARCHAR,
    classification VARCHAR
);

-- Tabellen populeras dynamiskt av pipeline_runner.populate_h3_cells()
"""


# Singleton-instans för enkel användning
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
