"""converter.importerモジュールのテスト"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from bq2dbt.converter.importer import (
    analyze_dependencies,
    check_file_exists,
    confirm_view_import,
    convert_view,
    fetch_views,
    initialize_bigquery_client,
)
from bq2dbt.utils.naming import NamingPreset
from rich.console import Console


def test_initialize_bigquery_client():
    """BigQueryクライアント初期化のテスト"""
    console = Console()
    mock_bq_instance = MagicMock()

    with patch(
        "bq2dbt.converter.importer.BigQueryClient", return_value=mock_bq_instance
    ) as mock_bq_client:
        result = initialize_bigquery_client("test-project", "asia-northeast1", console)

        assert result is mock_bq_instance
        mock_bq_client.assert_called_once_with(
            "test-project", location="asia-northeast1"
        )


def test_fetch_views():
    """ビュー一覧取得のテスト"""
    console = Console()
    mock_bq_client = MagicMock()
    mock_bq_client.list_views.return_value = [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]

    result = fetch_views(
        mock_bq_client, "test_dataset", None, None, console, "test-project"
    )

    assert result == [
        "test-project.test_dataset.view1",
        "test-project.test_dataset.view2",
    ]
    mock_bq_client.list_views.assert_called_once_with(
        "test_dataset", include_patterns=None, exclude_patterns=None
    )


def test_fetch_views_with_filters():
    """フィルター付きのビュー一覧取得のテスト"""
    console = Console()
    mock_bq_client = MagicMock()
    mock_bq_client.list_views.return_value = [
        "test-project.test_dataset.view1",
    ]

    result = fetch_views(
        mock_bq_client,
        "test_dataset",
        ["view1"],
        ["view2"],
        console,
        "test-project",
    )

    assert result == ["test-project.test_dataset.view1"]
    mock_bq_client.list_views.assert_called_once_with(
        "test_dataset", include_patterns=["view1"], exclude_patterns=["view2"]
    )


def test_fetch_views_empty_result():
    """空の結果を返すビュー一覧取得のテスト"""
    console = Console()
    mock_bq_client = MagicMock()
    mock_bq_client.list_views.return_value = []

    result = fetch_views(
        mock_bq_client, "test_dataset", None, None, console, "test-project"
    )

    assert result is None
    mock_bq_client.list_views.assert_called_once_with(
        "test_dataset", include_patterns=None, exclude_patterns=None
    )


def test_analyze_dependencies_with_dependencies():
    """依存関係分析ありのテスト"""
    console = Console()
    logger = MagicMock()
    mock_bq_client = MagicMock()

    mock_resolver = MagicMock()
    with patch(
        "bq2dbt.converter.importer.DependencyResolver", return_value=mock_resolver
    ):
        mock_resolver.analyze_dependencies.return_value = (
            ["test-project.test_dataset.view1", "test-project.test_dataset.view2"],
            {"dependency_graph": "mock"},
        )
        mock_resolver.get_topological_order.return_value = [
            "test-project.test_dataset.view1",
            "test-project.test_dataset.view2",
        ]

        views = ["test-project.test_dataset.view1"]
        all_views, ordered_views = analyze_dependencies(
            views, "test_dataset", True, mock_bq_client, console, logger, max_depth=3
        )

        assert all_views == [
            "test-project.test_dataset.view1",
            "test-project.test_dataset.view2",
        ]
        assert ordered_views == [
            "test-project.test_dataset.view1",
            "test-project.test_dataset.view2",
        ]
        mock_resolver.analyze_dependencies.assert_called_once()
        mock_resolver.get_topological_order.assert_called_once()


def test_analyze_dependencies_without_dependencies():
    """依存関係分析なしのテスト"""
    console = Console()
    logger = MagicMock()
    mock_bq_client = MagicMock()

    with patch("bq2dbt.converter.importer.DependencyResolver") as mock_resolver_class:
        views = ["test-project.test_dataset.view1", "test-project.test_dataset.view2"]
        all_views, ordered_views = analyze_dependencies(
            views, "test_dataset", False, mock_bq_client, console, logger
        )

        assert all_views == views
        assert ordered_views == views
        mock_resolver_class.assert_not_called()


def test_check_file_exists():
    """ファイル存在確認のテスト"""
    view = "test-project.test_dataset.view1"
    naming_preset = NamingPreset.DATASET_PREFIX
    output_path = Path("/tmp/output")

    with patch(
        "bq2dbt.converter.importer.Path.exists", side_effect=[True, False]
    ), patch(
        "bq2dbt.converter.importer.generate_model_name",
        return_value="test_dataset__view1",
    ):
        sql_exists, yml_exists, sql_path, yml_path = check_file_exists(
            view, naming_preset, output_path
        )

        assert sql_exists is True
        assert yml_exists is False
        assert str(sql_path).endswith("test_dataset__view1.sql")
        assert str(yml_path).endswith("test_dataset__view1.yml")


def test_confirm_view_import_non_interactive():
    """非インタラクティブモードでのインポート確認テスト"""
    view = "test-project.test_dataset.view1"
    files_exist = True
    existing_files = ["SQL: test_dataset__view1.sql"]
    non_interactive = True

    import_this_view, overwrite = confirm_view_import(
        view, files_exist, existing_files, non_interactive
    )

    assert import_this_view is True
    assert overwrite is True


def test_convert_view():
    """ビュー変換のテスト"""
    view = "test-project.test_dataset.view1"
    mock_bq_client = MagicMock()
    # テーブルタイプをVIEWに設定
    mock_bq_client.get_table_type.return_value = "VIEW"
    mock_bq_client.get_view_definition.return_value = "SELECT * FROM table"
    mock_bq_client.get_view_schema.return_value = [
        {"name": "column1", "type": "STRING"}
    ]

    mock_generator = MagicMock()
    mock_generator.generate_sql_model.return_value = (
        "sql content",
        Path("/tmp/model.sql"),
    )
    mock_generator.generate_yaml_model.return_value = (
        "yml content",
        Path("/tmp/model.yml"),
    )

    naming_preset_enum = NamingPreset.DATASET_PREFIX
    dry_run = False
    debug = False
    logger = MagicMock()

    result = convert_view(
        view, mock_bq_client, mock_generator, naming_preset_enum, dry_run, debug, logger
    )

    assert result == (view, Path("/tmp/model.sql"), Path("/tmp/model.yml"))
    mock_bq_client.get_table_type.assert_called_once_with(view)
    mock_bq_client.get_view_definition.assert_called_once_with(view)
    mock_bq_client.get_view_schema.assert_called_once_with(view)
    mock_generator.generate_sql_model.assert_called_once()
    mock_generator.generate_yaml_model.assert_called_once()


def test_convert_view_not_a_view():
    """ビューでないオブジェクトの変換テスト"""
    view = "test-project.test_dataset.table1"
    mock_bq_client = MagicMock()
    # テーブルタイプをTABLEに設定
    mock_bq_client.get_table_type.return_value = "TABLE"

    # モックジェネレーターを作成
    mock_generator = MagicMock()

    naming_preset_enum = NamingPreset.DATASET_PREFIX
    dry_run = False
    debug = False
    logger = MagicMock()

    # ValueErrorが発生することを確認
    with pytest.raises(ValueError) as excinfo:
        convert_view(
            view,
            mock_bq_client,
            mock_generator,
            naming_preset_enum,
            dry_run,
            debug,
            logger,
        )

    assert "オブジェクトはビューではありません" in str(excinfo.value)
    mock_bq_client.get_table_type.assert_called_once_with(view)
    mock_bq_client.get_view_definition.assert_not_called()
