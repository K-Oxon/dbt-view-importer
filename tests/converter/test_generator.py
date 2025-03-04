"""ModelGeneratorのテスト"""

from bq2dbt.converter.generator import ModelGenerator
from bq2dbt.utils.naming import NamingPreset


def test_model_generator_init(temp_output_dir, sql_template_path, yml_template_path):
    """ModelGeneratorの初期化をテスト"""
    generator = ModelGenerator(temp_output_dir, sql_template_path, yml_template_path)

    assert generator.output_dir == temp_output_dir
    assert generator.sql_template_path == sql_template_path
    assert generator.yml_template_path == yml_template_path


def test_load_template(temp_output_dir, sql_template_path):
    """テンプレートの読み込みをテスト"""
    generator = ModelGenerator(temp_output_dir, sql_template_path)

    template = generator._load_template(sql_template_path)
    assert template is not None


def test_generate_sql_model(temp_output_dir, sql_template_path):
    """SQLモデルの生成をテスト"""
    generator = ModelGenerator(temp_output_dir, sql_template_path)

    fully_qualified_name = "test-project.test_dataset.test_view"
    sql_definition = "SELECT * FROM test_table"

    content, path = generator.generate_sql_model(
        fully_qualified_name,
        sql_definition,
        naming_preset=NamingPreset.TABLE_ONLY,
        dry_run=True,
    )

    assert "test_view" in path.name
    assert ".sql" in path.name
    assert "SELECT * FROM test_table" in content
    assert "test-project.test_dataset.test_view" in content


def test_generate_yaml_model(temp_output_dir, yml_template_path):
    """YAMLモデルの生成をテスト"""
    generator = ModelGenerator(temp_output_dir, yml_template_path=yml_template_path)

    fully_qualified_name = "test-project.test_dataset.test_view"
    schema_fields = [
        {"name": "id", "description": "ID field"},
        {"name": "name", "description": "Name field"},
    ]

    content, path = generator.generate_yaml_model(
        fully_qualified_name,
        schema_fields,
        description="Test description",
        naming_preset=NamingPreset.TABLE_ONLY,
        dry_run=True,
    )

    assert "test_view" in path.name
    assert ".yml" in path.name
    assert "id" in content
    assert "name" in content
    assert "ID field" in content
    assert "Name field" in content
    assert "Test description" in content
