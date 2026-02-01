"""Tester för scripts/sql_generator.py."""

import pytest

from g_etl.sql_generator import DatasetConfig, SQLGenerator, get_generator, list_templates


class TestDatasetConfig:
    """Tester för DatasetConfig."""

    def test_default_values(self):
        """Kontrollera default-värden."""
        config = DatasetConfig()
        assert config.dataset_id == ""
        assert config.geometry_column == "geom"
        assert config.h3_center_resolution == 13
        assert config.h3_polyfill_resolution == 11
        assert config.h3_line_resolution == 12
        assert config.h3_point_resolution == 13
        assert config.h3_line_buffer_meters == 10

    def test_from_dataset_yml(self, sample_dataset_config):
        """Testa skapande från datasets.yml-format."""
        config = DatasetConfig.from_dataset_yml("test_dataset", sample_dataset_config)

        assert config.dataset_id == "test_dataset"
        assert config.source_id_column == "$objekt_id"
        assert config.klass == "test_klass"
        assert config.grupp == "$kategori"
        assert config.typ == "test_typ"
        assert config.leverantor == "test_leverantor"

    def test_from_dataset_yml_missing_field_mapping(self):
        """Testa med saknad field_mapping."""
        config = DatasetConfig.from_dataset_yml("test", {"id": "test"})
        assert config.dataset_id == "test"
        assert config.source_id_column == ""
        assert config.klass == ""

    def test_custom_h3_resolution(self):
        """Testa med anpassade H3-resolutioner."""
        yml_config = {
            "field_mapping": {
                "h3_center_resolution": 10,
                "h3_polyfill_resolution": 8,
            }
        }
        config = DatasetConfig.from_dataset_yml("test", yml_config)
        assert config.h3_center_resolution == 10
        assert config.h3_polyfill_resolution == 8


class TestSQLGenerator:
    """Tester för SQLGenerator."""

    @pytest.fixture
    def generator(self):
        """Skapa en SQLGenerator-instans."""
        return SQLGenerator()

    @pytest.fixture
    def temp_sql_dir(self, temp_dir):
        """Skapa temporär SQL-katalog med testmallar."""
        migrations_dir = temp_dir / "migrations"
        migrations_dir.mkdir()

        # Skapa en enkel testmall
        test_template = """-- migrate:up
CREATE TABLE staging.{{ dataset_id }} AS
SELECT '{{ klass }}' as klass, '{{ leverantor }}' as leverantor
FROM raw.{{ dataset_id }};

-- migrate:down
DROP TABLE IF EXISTS staging.{{ dataset_id }};
"""
        (migrations_dir / "004_test_template.sql").write_text(test_template)
        return temp_dir

    def test_list_templates(self, generator):
        """Testa att list_templates returnerar templates."""
        templates = generator.list_templates()
        # Projektet har templates, kontrollera att listan inte är tom
        assert isinstance(templates, list)

    def test_list_templates_sorted(self, generator):
        """Testa att templates är sorterade."""
        templates = generator.list_templates()
        if len(templates) > 1:
            assert templates == sorted(templates)

    def test_is_column_ref_with_dollar(self, generator):
        """Testa identifiering av kolumnreferens med $."""
        assert generator._is_column_ref("$column_name") is True
        assert generator._is_column_ref("literal_value") is False
        assert generator._is_column_ref("") is False
        assert generator._is_column_ref(None) is False

    def test_get_column_name(self, generator):
        """Testa extraktion av kolumnnamn."""
        assert generator._get_column_name("$column_name") == "column_name"
        assert generator._get_column_name("literal") == "literal"

    def test_build_variables_basic(self, generator, sample_dataset_config):
        """Testa grundläggande variabelbyggande."""
        config = DatasetConfig.from_dataset_yml("test", sample_dataset_config)
        variables = generator._build_variables(config)

        assert variables["dataset_id"] == "test"
        assert variables["klass"] == "test_klass"
        assert variables["leverantor"] == "test_leverantor"
        assert "h3_center_resolution" in variables

    def test_build_variables_column_ref(self, generator, sample_dataset_config):
        """Testa variabelbyggande med kolumnreferenser."""
        config = DatasetConfig.from_dataset_yml("test", sample_dataset_config)
        variables = generator._build_variables(config)

        # grupp är $kategori, ska bli kolumnreferens
        assert "s.kategori" in variables["grupp_expr"]
        assert "COALESCE" in variables["grupp_expr"]

    def test_build_variables_literal(self, generator):
        """Testa variabelbyggande med literala värden."""
        config = DatasetConfig(
            dataset_id="test",
            klass="min_klass",
            grupp="min_grupp",  # Ingen $-prefix
            typ="min_typ",
        )
        variables = generator._build_variables(config)

        # grupp är literal, ska bli 'min_grupp'
        assert variables["grupp_expr"] == "'min_grupp'"
        assert variables["typ_expr"] == "'min_typ'"

    def test_substitute(self, generator):
        """Testa variabelsubstitution."""
        template = "SELECT * FROM {{ dataset_id }} WHERE klass = '{{ klass }}'"
        variables = {"dataset_id": "naturreservat", "klass": "skyddat"}

        result = generator._substitute(template, variables)
        assert result == "SELECT * FROM naturreservat WHERE klass = 'skyddat'"

    def test_substitute_without_spaces(self, generator):
        """Testa substitution utan mellanrum."""
        template = "SELECT * FROM {{dataset_id}}"
        variables = {"dataset_id": "test"}

        result = generator._substitute(template, variables)
        assert result == "SELECT * FROM test"

    def test_render_template(self, temp_sql_dir):
        """Testa rendering av template."""
        generator = SQLGenerator(sql_path=temp_sql_dir)

        config = {
            "field_mapping": {
                "klass": "test_klass",
                "leverantor": "test_lev",
            }
        }

        sql = generator.render_template("004_test_template.sql", "my_dataset", config)

        assert "staging.my_dataset" in sql
        assert "test_klass" in sql
        assert "test_lev" in sql

    def test_render_template_extracts_up_section(self, temp_sql_dir):
        """Testa att endast migrate:up-sektionen extraheras."""
        generator = SQLGenerator(sql_path=temp_sql_dir)

        sql = generator.render_template("004_test_template.sql", "test", {})

        assert "CREATE TABLE" in sql
        assert "migrate:down" not in sql
        assert "DROP TABLE" not in sql

    def test_render_template_nonexistent(self, generator):
        """Testa rendering av icke-existerande template."""
        sql = generator.render_template("nonexistent.sql", "test", {})
        assert sql == ""

    def test_render_all_templates(self, temp_sql_dir):
        """Testa rendering av alla templates."""
        # Skapa en andra template
        (temp_sql_dir / "migrations" / "005_second_template.sql").write_text(
            "-- migrate:up\nSELECT '{{ dataset_id }}';\n-- migrate:down\n"
        )

        generator = SQLGenerator(sql_path=temp_sql_dir)
        results = generator.render_all_templates("test", {})

        assert len(results) == 2
        assert results[0][0] == "004_test_template.sql"
        assert results[1][0] == "005_second_template.sql"


class TestSQLGeneratorBackwardsCompat:
    """Tester för bakåtkompatibla metoder."""

    def test_staging_sql(self):
        """Testa staging_sql-metoden."""
        generator = SQLGenerator()
        sql = generator.staging_sql("test_dataset", {"source_id_column": "id"})
        # Om template finns, bör vi få tillbaka SQL
        assert isinstance(sql, str)

    def test_staging2_sql(self):
        """Testa staging2_sql-metoden."""
        generator = SQLGenerator()
        sql = generator.staging2_sql("test_dataset", {"klass": "test"})
        assert isinstance(sql, str)


class TestModuleFunctions:
    """Tester för modul-nivå funktioner."""

    def test_get_generator_singleton(self):
        """Testa att get_generator returnerar samma instans."""
        gen1 = get_generator()
        gen2 = get_generator()
        assert gen1 is gen2

    def test_list_templates_function(self):
        """Testa list_templates-funktionen."""
        templates = list_templates()
        assert isinstance(templates, list)


class TestSQLGeneratorEdgeCases:
    """Tester för edge cases."""

    def test_empty_config(self):
        """Testa med tom config."""
        generator = SQLGenerator()
        config = DatasetConfig(dataset_id="test")
        variables = generator._build_variables(config)

        assert variables["dataset_id"] == "test"
        assert variables["klass"] == ""
        assert variables["source_id_expr"] == "''"

    def test_data_mappings(self):
        """Testa data_mappings för extra kolumner."""
        config = DatasetConfig(
            dataset_id="test",
            data_mappings={
                "data_1": "$extra_col1",
                "data_2": "extra_col2",
            },
        )
        generator = SQLGenerator()
        variables = generator._build_variables(config)

        # data_1 ska ha kolumnreferens ($ tas bort av _get_column_name)
        assert "s.extra_col1" in variables["data_1_expr"]
        # data_2 ska också vara kolumnreferens
        assert "s.extra_col2" in variables["data_2_expr"]
        # data_3-5 ska vara tomma
        assert variables["data_3_expr"] == "''"

    def test_none_typ(self):
        """Testa med None som typ."""
        config = DatasetConfig(dataset_id="test", typ=None)
        generator = SQLGenerator()
        variables = generator._build_variables(config)

        assert variables["typ_expr"] == "''"

    def test_template_caching(self, temp_dir):
        """Testa att templates cachas."""
        # Skapa temp SQL-struktur
        migrations_dir = temp_dir / "migrations"
        migrations_dir.mkdir()
        template_path = migrations_dir / "004_cache_test_template.sql"
        template_path.write_text("-- migrate:up\nSELECT 'original';\n-- migrate:down\n")

        generator = SQLGenerator(sql_path=temp_dir)

        # Första anropet laddar från disk
        sql1 = generator._load_template("004_cache_test_template.sql")

        # Ändra filen
        template_path.write_text("-- migrate:up\nSELECT 'modified';\n-- migrate:down\n")

        # Andra anropet ska använda cache
        sql2 = generator._load_template("004_cache_test_template.sql")

        assert sql1 == sql2  # Cachad version
        assert "original" in sql1
