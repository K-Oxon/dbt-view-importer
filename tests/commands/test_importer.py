"""importerモジュールのテスト"""
import os
from unittest.mock import MagicMock, patch

from bq2dbt.commands.importer import import_cmd, import_views
from click.testing import CliRunner


def test_import_cmd_group():
    """インポートコマンドグループのテスト"""
    runner = CliRunner()
    result = runner.invoke(import_cmd, ["--help"])
    assert result.exit_code == 0
    assert "インポートするコマンド" in result.output


@patch("bq2dbt.commands.importer.BigQueryClient")
@patch("bq2dbt.commands.importer.ModelGenerator")
@patch("bq2dbt.commands.importer.DependencyResolver")
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
    # analyze_dependenciesの戻り値を設定
    mock_resolver_instance.analyze_dependencies.return_value = (
        ["test-project.test_dataset.view1", "test-project.test_dataset.view2"],
        {"dependency_graph": "mock"},
    )

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
                "--include-dependencies",
            ],
            obj={"VERBOSE": False},
        )

        # 結果確認
        assert result.exit_code == 0

        # BigQueryClientの呼び出し確認
        mock_bq_client.assert_called_once_with(
            "test-project", location="asia-northeast1"
        )
        mock_bq_instance.list_views.assert_called_once_with(
            "test_dataset",
            include_patterns=None,
            exclude_patterns=None,
        )

        # DependencyResolverの呼び出し確認
        mock_resolver.assert_called_once_with(mock_bq_instance)

        # analyze_dependenciesが正しいパラメータで呼ばれることを確認（status_callbackは中身は問わずに存在だけを確認）
        assert mock_resolver_instance.analyze_dependencies.call_count == 1
        call_args, call_kwargs = mock_resolver_instance.analyze_dependencies.call_args
        assert call_args[0] == [
            "test-project.test_dataset.view1",
            "test-project.test_dataset.view2",
        ]
        assert call_args[1] == "test_dataset"
        assert "status_callback" in call_kwargs

        mock_resolver_instance.get_topological_order.assert_called_once()

        # ModelGeneratorの呼び出し確認
        mock_generator.assert_called_once()
        assert mock_generator_instance.generate_sql_model.call_count == 2
        assert mock_generator_instance.generate_yaml_model.call_count == 2


@patch("bq2dbt.commands.importer.BigQueryClient")
@patch("bq2dbt.commands.importer.ModelGenerator")
@patch("bq2dbt.commands.importer.DependencyResolver")
def test_import_views_command_with_filters(
    mock_resolver, mock_generator, mock_bq_client
):
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
        mock_bq_client.assert_called_once_with(
            "test-project", location="asia-northeast1"
        )
        mock_bq_instance.list_views.assert_called_once_with(
            "test_dataset",
            include_patterns=["include_*", "another_*"],
            exclude_patterns=["exclude_*", "temp_*"],
        )


@patch("bq2dbt.commands.importer.BigQueryClient")
@patch("bq2dbt.commands.importer.ModelGenerator")
@patch("bq2dbt.commands.importer.DependencyResolver")
def test_import_views_command_without_dependencies(
    mock_resolver, mock_generator, mock_bq_client
):
    """依存関係分析を無効にしたモードのテスト"""
    # モックの設定
    mock_bq_instance = MagicMock()
    mock_bq_client.return_value = mock_bq_instance
    mock_bq_instance.list_views.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]

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

    # CLIランナーの設定
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("output")

        # コマンド実行 - include_dependenciesは指定しない（デフォルトはFalse）
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
        mock_bq_client.assert_called_once_with(
            "test-project", location="asia-northeast1"
        )
        mock_bq_instance.list_views.assert_called_once_with(
            "test_dataset",
            include_patterns=None,
            exclude_patterns=None,
        )

        # DependencyResolver - 依存関係分析を行わないモードではリゾルバーが使われないことを確認
        mock_resolver.assert_not_called()

        # ModelGeneratorは呼び出される
        mock_generator.assert_called_once()
        assert mock_generator_instance.generate_sql_model.call_count == 2
        assert mock_generator_instance.generate_yaml_model.call_count == 2
