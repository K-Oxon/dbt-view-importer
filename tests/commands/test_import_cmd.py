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

    # テンポラリディレクトリを使用
    with patch("os.path.exists", return_value=True), patch(
        "bq2dbt.converter.importer.Confirm.ask", return_value=True
    ), patch("bq2dbt.converter.importer.Path.mkdir"), patch(
        "bq2dbt.converter.importer.Path.exists", return_value=True
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
    # BigQueryClientが正しく初期化されたことを確認
    mock_bq_client.assert_called_once_with("test-project", location="asia-northeast1")
    # ビュー一覧の取得が呼ばれたことを確認
    mock_bq_instance.list_views.assert_called_once_with(
        "test_dataset", include_patterns=None, exclude_patterns=None
    )
    # 依存関係の解析が呼ばれたことを確認
    mock_resolver_instance.analyze_dependencies.assert_called_once()
    # モデル生成が呼ばれたことを確認
    assert mock_generator_instance.generate_sql_model.call_count == 2
    assert mock_generator_instance.generate_yaml_model.call_count == 2


@patch("bq2dbt.converter.importer.BigQueryClient")
@patch("bq2dbt.converter.importer.ModelGenerator")
@patch("bq2dbt.converter.importer.DependencyResolver")
def test_import_views_command_with_filters(
    mock_resolver, mock_generator, mock_bq_client
):
    """フィルター付きのインポートビューコマンドのテスト"""
    # モックの設定
    mock_bq_instance = MagicMock()
    mock_bq_client.return_value = mock_bq_instance
    mock_bq_instance.list_views.return_value = [
        "test-project.test_dataset.view1",
    ]

    mock_resolver_instance = MagicMock()
    mock_resolver.return_value = mock_resolver_instance
    mock_resolver_instance.get_topological_order.return_value = [
        "test-project.test_dataset.view1",
    ]
    # analyze_dependenciesの戻り値を設定
    mock_resolver_instance.analyze_dependencies.return_value = (
        ["test-project.test_dataset.view1"],
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

    # テンポラリディレクトリを使用
    with patch("os.path.exists", return_value=True), patch(
        "bq2dbt.converter.importer.Confirm.ask", return_value=True
    ), patch("bq2dbt.converter.importer.Path.mkdir"), patch(
        "bq2dbt.converter.importer.Path.exists", return_value=True
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
                "--include-dependencies",
            ],
            obj={"VERBOSE": False},
        )

    # 結果の検証
    assert result.exit_code == 0
    # BigQueryClientが正しく初期化されたことを確認
    mock_bq_client.assert_called_once_with("test-project", location="asia-northeast1")
    # ビュー一覧の取得が呼ばれたことを確認（フィルター付き）
    mock_bq_instance.list_views.assert_called_once_with(
        "test_dataset", include_patterns=["view1"], exclude_patterns=["view2"]
    )
    # 依存関係の解析が呼ばれたことを確認
    mock_resolver_instance.analyze_dependencies.assert_called_once()
    # モデル生成が呼ばれたことを確認
    assert mock_generator_instance.generate_sql_model.call_count == 1
    assert mock_generator_instance.generate_yaml_model.call_count == 1


@patch("bq2dbt.converter.importer.BigQueryClient")
@patch("bq2dbt.converter.importer.ModelGenerator")
@patch("bq2dbt.converter.importer.DependencyResolver")
def test_import_views_command_without_dependencies(
    mock_resolver, mock_generator, mock_bq_client
):
    """依存関係解析なしのインポートビューコマンドのテスト"""
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

    # テンポラリディレクトリを使用
    with patch("os.path.exists", return_value=True), patch(
        "bq2dbt.converter.importer.Confirm.ask", return_value=True
    ), patch("bq2dbt.converter.importer.Path.mkdir"), patch(
        "bq2dbt.converter.importer.Path.exists", return_value=True
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
    # BigQueryClientが正しく初期化されたことを確認
    mock_bq_client.assert_called_once_with("test-project", location="asia-northeast1")
    # ビュー一覧の取得が呼ばれたことを確認
    mock_bq_instance.list_views.assert_called_once_with(
        "test_dataset", include_patterns=None, exclude_patterns=None
    )
    # 依存関係の解析が呼ばれないことを確認
    mock_resolver_instance.analyze_dependencies.assert_not_called()
    # モデル生成が呼ばれたことを確認
    assert mock_generator_instance.generate_sql_model.call_count == 2
    assert mock_generator_instance.generate_yaml_model.call_count == 2
