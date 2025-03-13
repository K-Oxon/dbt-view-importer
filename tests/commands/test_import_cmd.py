"""import_cmdのテスト"""
from unittest.mock import MagicMock, patch

from bq2dbt.commands.import_views import import_views
from bq2dbt.commands.importer import import_cmd
from click.testing import CliRunner


def test_import_cmd_group():
    """インポートコマンドグループのテスト"""
    runner = CliRunner()
    result = runner.invoke(import_cmd, ["--help"])
    assert result.exit_code == 0
    assert "インポートするコマンド" in result.output


@patch("bq2dbt.converter.importer.BigQueryClient")
@patch("bq2dbt.converter.importer.ModelGenerator")
@patch("bq2dbt.converter.importer.DependencyResolver")
def test_import_views_command(mock_resolver, mock_generator, mock_bq_client):
    """インポートビューコマンドのテスト（基本機能と主要オプション）"""
    # モックの設定
    mock_bq_instance = MagicMock()
    mock_bq_client.return_value = mock_bq_instance
    mock_bq_instance.get_table_type.return_value = "VIEW"

    mock_resolver_instance = MagicMock()
    mock_resolver.return_value = mock_resolver_instance

    mock_generator_instance = MagicMock()
    mock_generator.return_value = mock_generator_instance
    mock_generator_instance.generate_sql_model.return_value = (
        "sql content",
        "path/to/model.sql",
    )
    mock_generator_instance.generate_yaml_model.return_value = (
        "yml content",
        "path/to/model.yml",
    )

    # ケース1: 基本的なインポート（依存関係解析あり）
    mock_bq_instance.list_views.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]
    mock_resolver_instance.get_topological_order.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]
    mock_resolver_instance.analyze_dependencies.return_value = (
        ["test-project.test_dataset.view1", "test-project.test_dataset.view2"],
        {"dependency_graph": "mock"},
    )

    with patch("os.path.exists", return_value=True), patch(
        "bq2dbt.converter.importer.Confirm.ask", return_value=True
    ), patch("bq2dbt.converter.importer.Path.mkdir"), patch(
        "bq2dbt.converter.importer.Path.exists", return_value=False
    ):
        runner = CliRunner()
        result = runner.invoke(
            import_views,
            [
                "--project-id",
                "test-project",
                "--dataset",
                "test_dataset",
                "--output-dir",
                "output",
                "--include-dependencies",
            ],
            obj={"VERBOSE": False},
        )

    # 結果の検証
    assert result.exit_code == 0
    mock_bq_client.assert_called_with("test-project", location="asia-northeast1")
    mock_bq_instance.list_views.assert_called_with(
        "test_dataset", include_patterns=None, exclude_patterns=None
    )
    mock_resolver_instance.analyze_dependencies.assert_called_once()
    assert mock_bq_instance.get_table_type.call_count >= 2
    assert mock_generator_instance.generate_sql_model.call_count == 2
    assert mock_generator_instance.generate_yaml_model.call_count == 2

    # ケース2: フィルター付きインポート
    mock_bq_client.reset_mock()
    mock_bq_instance.reset_mock()
    mock_resolver.reset_mock()
    mock_resolver_instance.reset_mock()
    mock_generator.reset_mock()
    mock_generator_instance.reset_mock()

    mock_bq_instance.list_views.return_value = [
        "test-project.test_dataset.view1",
    ]
    mock_resolver_instance.get_topological_order.return_value = [
        "test-project.test_dataset.view1",
    ]
    mock_resolver_instance.analyze_dependencies.return_value = (
        ["test-project.test_dataset.view1"],
        {"dependency_graph": "mock"},
    )

    with patch("os.path.exists", return_value=True), patch(
        "bq2dbt.converter.importer.Confirm.ask", return_value=True
    ), patch("bq2dbt.converter.importer.Path.mkdir"), patch(
        "bq2dbt.converter.importer.Path.exists", return_value=False
    ):
        runner = CliRunner()
        result = runner.invoke(
            import_views,
            [
                "--project-id",
                "test-project",
                "--dataset",
                "test_dataset",
                "--output-dir",
                "output",
                "--include-views",
                "view1",
                "--exclude-views",
                "view2",
            ],
            obj={"VERBOSE": False},
        )

    # 結果の検証
    assert result.exit_code == 0
    mock_bq_instance.list_views.assert_called_with(
        "test_dataset", include_patterns=["view1"], exclude_patterns=["view2"]
    )
    assert mock_generator_instance.generate_sql_model.call_count == 1

    # ケース3: 依存関係解析なしのインポート
    mock_bq_client.reset_mock()
    mock_bq_instance.reset_mock()
    mock_resolver.reset_mock()
    mock_resolver_instance.reset_mock()
    mock_generator.reset_mock()
    mock_generator_instance.reset_mock()

    mock_bq_instance.list_views.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]

    with patch("os.path.exists", return_value=True), patch(
        "bq2dbt.converter.importer.Confirm.ask", return_value=True
    ), patch("bq2dbt.converter.importer.Path.mkdir"), patch(
        "bq2dbt.converter.importer.Path.exists", return_value=False
    ):
        runner = CliRunner()
        result = runner.invoke(
            import_views,
            [
                "--project-id",
                "test-project",
                "--dataset",
                "test_dataset",
                "--output-dir",
                "output",
            ],
            obj={"VERBOSE": False},
        )

    # 結果の検証
    assert result.exit_code == 0
    mock_resolver_instance.analyze_dependencies.assert_not_called()
    assert mock_generator_instance.generate_sql_model.call_count == 2
