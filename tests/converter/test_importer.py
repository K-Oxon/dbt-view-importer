"""converter.importerモジュールのテスト"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from bq2dbt.converter.importer import (
    _match_pattern,
    analyze_dependencies,
    check_file_exists,
    confirm_view_import,
    convert_view,
    fetch_views,
    filter_views,
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

    # 通常のケース
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
    mock_bq_client.list_views.assert_called_with(
        "test_dataset", include_patterns=None, exclude_patterns=None
    )

    # フィルター付きのケース
    mock_bq_client.reset_mock()
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
    mock_bq_client.list_views.assert_called_with(
        "test_dataset", include_patterns=["view1"], exclude_patterns=["view2"]
    )

    # 空の結果を返すケース
    mock_bq_client.reset_mock()
    mock_bq_client.list_views.return_value = []

    result = fetch_views(
        mock_bq_client, "test_dataset", None, None, console, "test-project"
    )

    assert result is None
    mock_bq_client.list_views.assert_called_with(
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


def test_match_pattern():
    """_match_pattern関数のテスト"""
    # 完全一致
    assert _match_pattern("view1", "view1") is True

    # 前方一致
    assert _match_pattern("view1", "view*") is True
    assert _match_pattern("view_suffix", "view*") is True

    # 後方一致
    assert _match_pattern("prefix_view", "*view") is True

    # 部分一致
    assert _match_pattern("prefix_middle_suffix", "*middle*") is True

    # 複数の*
    assert _match_pattern("abc_def_ghi", "abc*def*") is True

    # マッチしないケース
    assert _match_pattern("view1", "view2") is False
    assert _match_pattern("view1", "*view2") is False
    assert _match_pattern("abc_def", "abc*xyz") is False


def test_filter_views_no_patterns():
    """フィルターパターンが指定されていない場合のテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset2.view3",
    ]

    # パターンが指定されていない場合は全てのビューを返す
    result = filter_views(views)
    assert result == views

    # 空のパターンリストの場合も全てのビューを返す
    result = filter_views(views, include_patterns=[], exclude_patterns=[])
    assert result == views


def test_filter_views_include_patterns():
    """includeパターンによるフィルタリングのテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_sales",
        "project1.dataset2.report_users",
    ]

    # 特定のviewのみを含める
    result = filter_views(views, include_patterns=["view1"])
    assert result == ["project1.dataset1.view1"]

    # 前方一致パターン
    result = filter_views(views, include_patterns=["view*"])
    assert result == ["project1.dataset1.view1", "project1.dataset1.view2"]

    # 複数のincludeパターン
    result = filter_views(views, include_patterns=["view1", "report_*"])
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.report_sales",
        "project1.dataset2.report_users",
    ]


def test_filter_views_exclude_patterns():
    """excludeパターンによるフィルタリングのテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.tmp_view",
        "project1.dataset2.test_view",
    ]

    # 特定のviewを除外
    result = filter_views(views, exclude_patterns=["view1"])
    assert result == [
        "project1.dataset1.view2",
        "project1.dataset1.tmp_view",
        "project1.dataset2.test_view",
    ]

    # 前方一致パターンで除外
    result = filter_views(views, exclude_patterns=["tmp_*", "test_*"])
    assert result == ["project1.dataset1.view1", "project1.dataset1.view2"]

    # 複数のexcludeパターン
    result = filter_views(views, exclude_patterns=["view1", "tmp_*"])
    assert result == ["project1.dataset1.view2", "project1.dataset2.test_view"]


def test_filter_views_include_and_exclude():
    """includeとexcludeの両方を指定した場合のテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_daily",
        "project1.dataset1.report_weekly",
        "project1.dataset2.tmp_report",
        "project1.dataset2.test_view",
    ]

    # includeで絞り込んだ後、excludeで除外
    result = filter_views(
        views,
        include_patterns=["report_*", "view*"],
        exclude_patterns=["*weekly"],
    )
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_daily",
    ]


def test_filter_views_invalid_view_names():
    """無効なビュー名のフィルタリングテスト"""
    views = [
        "project1.dataset1.view1",
        "invalid_format",  # 形式が不正
        "project1.dataset1",  # 部分が足りない
    ]
    logger = MagicMock()

    result = filter_views(views, include_patterns=["view*"], logger=logger)
    assert result == ["project1.dataset1.view1"]

    # loggerがwarningを呼び出したことを確認
    logger.warning.assert_any_call("無効なビュー名形式: invalid_format")
    logger.warning.assert_any_call("無効なビュー名形式: project1.dataset1")


def test_filter_views_empty_list():
    """空のビューリストのテスト"""
    # 空のリストを渡した場合は空のリストが返る
    result = filter_views([])
    assert result == []

    # includeとexcludeを指定しても空のリストが返る
    result = filter_views([], include_patterns=["view*"], exclude_patterns=["tmp_*"])
    assert result == []


def test_filter_views_fqn_include_patterns():
    """FQN全体に対するincludeパターンによるフィルタリングのテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_sales",
        "project1.dataset2.report_users",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # 特定のviewのみを含める（FQN全体）
    result = filter_views(views, include_patterns=["*.*.view1"])
    assert result == ["project1.dataset1.view1"]

    # 前方一致パターン（FQN全体）
    result = filter_views(views, include_patterns=["*.*.view*"])
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # データセット部分を含むパターンマッチング
    result = filter_views(views, include_patterns=["*.dataset1.*"])
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_sales",
    ]

    # FQN全体に対するパターンマッチング
    result = filter_views(views, include_patterns=["project1.dataset*.view*"])
    assert result == ["project1.dataset1.view1", "project1.dataset1.view2"]

    # 複数のincludeパターン（混合）
    result = filter_views(views, include_patterns=["*.*.view1", "project1.*.report_*"])
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.report_sales",
        "project1.dataset2.report_users",
    ]

    # sample_datasetを含むパターン
    result = filter_views(views, include_patterns=["*.sample_dataset*.*"])
    assert result == ["project2.sample_dataset_foo.sample_view_01"]


def test_filter_views_fqn_exclude_patterns():
    """FQN全体に対するexcludeパターンによるフィルタリングのテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.tmp_view",
        "project1.dataset2.test_view",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # 特定のviewを除外（FQN全体）
    result = filter_views(views, exclude_patterns=["*.*.view1"])
    assert result == [
        "project1.dataset1.view2",
        "project1.dataset1.tmp_view",
        "project1.dataset2.test_view",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # 前方一致パターンで除外（FQN全体）
    result = filter_views(views, exclude_patterns=["*.*.tmp_*", "*.*.test_*"])
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # データセット部分を含むパターンマッチングで除外
    result = filter_views(views, exclude_patterns=["*.dataset1.*"])
    assert result == [
        "project1.dataset2.test_view",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # FQN全体に対するパターンマッチングで除外
    result = filter_views(views, exclude_patterns=["project1.*.*"])
    assert result == ["project2.sample_dataset_foo.sample_view_01"]

    # 複数のexcludeパターン（混合）
    result = filter_views(views, exclude_patterns=["*.*.view1", "*.dataset2.*"])
    assert result == [
        "project1.dataset1.view2",
        "project1.dataset1.tmp_view",
        "project2.sample_dataset_foo.sample_view_01",
    ]


def test_filter_views_fqn_include_and_exclude():
    """FQN全体に対するincludeとexcludeの両方を指定した場合のテスト"""
    views = [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_daily",
        "project1.dataset1.report_weekly",
        "project1.dataset2.tmp_report",
        "project1.dataset2.test_view",
        "project2.sample_dataset_foo.sample_view_01",
    ]

    # includeで絞り込んだ後、excludeで除外（FQN全体パターン）
    result = filter_views(
        views,
        include_patterns=["project1.*.*"],
        exclude_patterns=["*.dataset2.*"],
    )
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project1.dataset1.report_daily",
        "project1.dataset1.report_weekly",
    ]

    # データセット部分を含むパターンマッチング
    result = filter_views(
        views,
        include_patterns=["*.dataset*.view*", "*.sample_dataset*.*"],
        exclude_patterns=["*.*.test_*"],
    )
    assert result == [
        "project1.dataset1.view1",
        "project1.dataset1.view2",
        "project2.sample_dataset_foo.sample_view_01",
    ]
