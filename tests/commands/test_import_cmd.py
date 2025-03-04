"""import_cmdのテスト"""
import os
from unittest.mock import MagicMock, patch

from bq2dbt.commands.import_cmd import import_cmd, import_views
from click.testing import CliRunner


def test_import_cmd_group():
    """インポートコマンドグループのテスト"""
    runner = CliRunner()
    result = runner.invoke(import_cmd, ["--help"])
    assert result.exit_code == 0
    assert "インポートするコマンド" in result.output


@patch("bq2dbt.commands.import_cmd.BigQueryClient")
@patch("bq2dbt.commands.import_cmd.ModelGenerator")
@patch("bq2dbt.commands.import_cmd.DependencyResolver")
def test_import_views_command_basic(mock_resolver, mock_generator, mock_bq_client):
    """インポートビューコマンドの基本的なテスト"""
    # モックの設定
    mock_bq_instance = MagicMock()
    mock_bq_client.return_value = mock_bq_instance
    mock_bq_instance.list_views.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]

    mock_resolver_instance = MagicMock()
    mock_resolver.return_value = mock_resolver_instance
    mock_resolver_instance.get_topological_order.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]

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

    # CLIランナーの設定
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("output")

        # コマンド実行
        result = runner.invoke(
            import_views,
            [
                "--project-id",
                "test-project",
                "--dataset",
                "test_dataset",
                "--output-dir",
                "output",
                "--non-interactive",
            ],
            obj={"VERBOSE": False},
        )

        # 結果確認
        assert result.exit_code == 0

        # BigQueryClientの呼び出し確認
        mock_bq_client.assert_called_once_with("test-project")
        mock_bq_instance.list_views.assert_called_once_with(
            "test_dataset",
            include_patterns=None,
            exclude_patterns=None,
        )

        # DependencyResolverの呼び出し確認
        mock_resolver.assert_called_once_with(mock_bq_instance)
        mock_resolver_instance.build_dependency_graph.assert_called_once_with(
            [
                "test-project.test_dataset.view1",
                "test-project.test_dataset.view2",
            ]
        )
        mock_resolver_instance.get_topological_order.assert_called_once()

        # ModelGeneratorの呼び出し確認
        mock_generator.assert_called_once()
        assert mock_generator_instance.generate_sql_model.call_count == 2
        assert mock_generator_instance.generate_yaml_model.call_count == 2


@patch("bq2dbt.commands.import_cmd.BigQueryClient")
def test_import_views_command_with_filters(mock_bq_client):
    """フィルタオプション付きのインポートビューコマンドテスト"""
    # モックの設定
    mock_bq_instance = MagicMock()
    mock_bq_client.return_value = mock_bq_instance
    mock_bq_instance.list_views.return_value = []

    # CLIランナーの設定
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("output")

        # コマンド実行（フィルタ付き）
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
                "include_*,another_*",
                "--exclude-views",
                "exclude_*,temp_*",
                "--non-interactive",
            ],
            obj={"VERBOSE": False},
        )

        # 結果確認
        assert result.exit_code == 0

        # BigQueryClientの呼び出し確認（フィルタが適用されていることを確認）
        mock_bq_instance.list_views.assert_called_once_with(
            "test_dataset",
            include_patterns=["include_*", "another_*"],
            exclude_patterns=["exclude_*", "temp_*"],
        )
