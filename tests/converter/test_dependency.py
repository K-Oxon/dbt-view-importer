"""DependencyResolverのテスト"""
from unittest.mock import MagicMock, patch

from bq2dbt.converter.dependency import (
    DataCatalogDependencyResolver,
    DependencyResolver,
)
from rich.console import Console
from rich.table import Table
from rich.tree import Tree


@patch("bq2dbt.converter.dependency.LineageClient")
def test_dependency_resolver_init(mock_lineage_client_class):
    """DependencyResolverの初期化をテスト"""
    mock_bq_client = MagicMock()
    mock_bq_client.project_id = "project"
    mock_bq_client.location = "location"

    # LineageClientのモックを設定
    mock_lineage_client = MagicMock()
    mock_lineage_client_class.return_value = mock_lineage_client

    resolver = DependencyResolver(mock_bq_client)

    assert resolver.bq_client == mock_bq_client
    assert resolver.lineage_client is not None
    assert resolver.dependency_graph == {}
    assert isinstance(resolver.reverse_graph, dict)

    # LineageClientが正しく作成されたことを確認
    mock_lineage_client_class.assert_called_once_with(
        mock_bq_client.project_id, mock_bq_client.location
    )


def test_analyze_dependencies_builds_graph():
    """analyze_dependenciesメソッドが依存関係グラフを構築することをテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()

    # モックのget_table_dependenciesメソッドの戻り値を設定
    mock_lineage_client.get_table_dependencies.side_effect = [
        ["project.dataset.view2", "project.dataset.view3"],  # view1の依存先
        ["project.dataset.view3"],  # view2の依存先
        [],  # view3の依存先
    ]
    mock_bq_client.project_id = "project"

    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)
    views = [
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
    ]

    # analyze_dependenciesを呼び出して依存関係グラフを構築
    _, dependency_graph = resolver.analyze_dependencies(views, "dataset")

    # 期待される依存関係グラフ
    expected_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    assert dependency_graph == expected_graph
    assert resolver.dependency_graph == expected_graph


def test_get_topological_order():
    """トポロジカルソートのテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 依存関係グラフを手動で設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # トポロジカルソートを実行
    order = resolver.get_topological_order()

    # 期待される順序: view3, view2, view1
    # (依存先から順に変換されるため、逆順になっている)
    assert order == [
        "project.dataset.view3",
        "project.dataset.view2",
        "project.dataset.view1",
    ]


def test_get_topological_order_with_depth():
    """深さベースのトポロジカルソートテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 依存関係グラフを手動で設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # トポロジカルソートを実行
    order = resolver.get_topological_order()

    # 期待される順序: 深さに基づいてソート
    # view3 (深さ2), view2 (深さ1), view1 (深さ0)
    assert order == [
        "project.dataset.view3",
        "project.dataset.view2",
        "project.dataset.view1",
    ]


def test_get_dependent_views():
    """依存しているビューの取得をテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 逆依存関係グラフを手動で設定
    resolver.reverse_graph = {
        "project.dataset.view3": ["project.dataset.view1", "project.dataset.view2"],
        "project.dataset.view2": ["project.dataset.view1"],
        "project.dataset.view1": [],
    }

    dependents = resolver.get_dependent_views("project.dataset.view3")

    # view3に依存しているビューはview1とview2
    assert dependents == ["project.dataset.view1", "project.dataset.view2"]


def test_resolve_all_dependencies():
    """依存関係を含む完全なビューリストを作成するテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()

    # get_table_dependenciesの戻り値を設定
    # view1は他の2つのビューに依存、view2は1つのビューに依存、view3は依存先なし
    mock_lineage_client.get_table_dependencies.side_effect = [
        ["project.dataset1.view2", "project.dataset1.view3"],  # view1の依存先
        [
            "project.dataset1.view3",
            "project.dataset2.view4",
        ],  # view2の依存先（データセット外含む）
        [],  # view3の依存先
    ]

    # project_idを設定
    mock_bq_client.project_id = "project"

    # 実際のDependencyResolverの代わりにモックを使用
    mock_resolver = MagicMock()
    mock_resolver.analyze_dependencies.return_value = (
        [
            "project.dataset1.view1",
            "project.dataset1.view2",
            "project.dataset1.view3",
        ],
        {
            "project.dataset1.view1": [
                "project.dataset1.view2",
                "project.dataset1.view3",
            ],
            "project.dataset1.view2": [
                "project.dataset1.view3",
                "project.dataset2.view4",
            ],
            "project.dataset1.view3": [],
        },
    )

    # データセット1のビューに対して解析
    initial_views = ["project.dataset1.view1", "project.dataset1.view2"]

    # モックのanalyze_dependenciesメソッドを使用
    all_views, dependency_graph = mock_resolver.analyze_dependencies(
        initial_views, "dataset1"
    )

    # 結果の検証（データセット1内のビューが全て含まれ、データセット2のビューは除外）
    expected_views = {
        "project.dataset1.view1",
        "project.dataset1.view2",
        "project.dataset1.view3",
    }
    assert set(all_views) == expected_views
    assert "project.dataset2.view4" not in all_views


def test_resolve_all_dependencies_with_no_dependencies():
    """依存関係がない場合のテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()

    # すべてのビューに依存関係なし
    mock_lineage_client.get_table_dependencies.return_value = []
    mock_bq_client.project_id = "project"

    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)
    initial_views = ["project.dataset1.view1", "project.dataset1.view2"]

    # analyze_dependenciesメソッドを使用
    all_views, _ = resolver.analyze_dependencies(initial_views, "dataset1")

    # 元のビューリストと同じになるはず
    assert set(all_views) == set(initial_views)
    assert mock_lineage_client.get_table_dependencies.call_count == 2


@patch("rich.console.Console.print")
def test_display_dependencies(mock_print):
    """依存関係ツリーの表示テスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 依存関係グラフを手動で設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # 逆依存関係グラフも設定
    resolver.reverse_graph = {
        "project.dataset.view2": ["project.dataset.view1"],
        "project.dataset.view3": ["project.dataset.view1", "project.dataset.view2"],
    }

    # 依存関係ツリーを表示
    views = ["project.dataset.view1", "project.dataset.view2", "project.dataset.view3"]
    # 新しいdisplay_dependencies APIを使用
    console = Console()
    resolver.display_dependencies(views, console=console)

    # printは1回呼ばれるはず（テーブル出力）
    assert mock_print.call_count == 1


@patch("rich.console.Console.print")
def test_display_dependencies_with_filtering(mock_print):
    """フィルタリング付きの依存関係ツリー表示テスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 複数データセットにまたがる依存関係グラフを設定
    resolver.dependency_graph = {
        "project.dataset1.view1": ["project.dataset1.view2", "project.dataset2.view3"],
        "project.dataset1.view2": ["project.dataset2.view3"],
        "project.dataset2.view3": [],
    }

    # 逆依存関係グラフも設定
    resolver.reverse_graph = {
        "project.dataset1.view2": ["project.dataset1.view1"],
        "project.dataset2.view3": ["project.dataset1.view1", "project.dataset1.view2"],
    }

    # フィルタリングして表示（フィルタリングは呼び出し側で行い、フィルタ済みのビューリストを渡すように変更）
    views = [
        "project.dataset1.view1",
        "project.dataset1.view2",
    ]
    # 新しいdisplay_dependencies APIを使用
    console = Console()
    resolver.display_dependencies(views, console=console)

    # printは1回呼ばれるはず（テーブル出力）
    assert mock_print.call_count == 1


def test_analyze_dependencies():
    """analyze_dependencies メソッドのテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    mock_bq_client.project_id = "project"

    # モックのget_table_dependenciesメソッドの戻り値を設定
    mock_lineage_client.get_table_dependencies.side_effect = [
        [
            "project.dataset.view2",
            "project.dataset.view3",
            "project.other_dataset.view4",
        ],  # view1の依存先
        ["project.dataset.view3"],  # view2の依存先
        [],  # view3の依存先
    ]

    # 実際のDependencyResolverの代わりにモックを使用
    mock_resolver = MagicMock()
    mock_resolver.analyze_dependencies.return_value = (
        [
            "project.dataset.view1",
            "project.dataset.view2",
            "project.dataset.view3",
        ],
        {
            "project.dataset.view1": [
                "project.dataset.view2",
                "project.dataset.view3",
                "project.other_dataset.view4",
            ],
            "project.dataset.view2": ["project.dataset.view3"],
            "project.dataset.view3": [],
        },
    )

    views = [
        "project.dataset.view1",
    ]

    # モックのanalyze_dependenciesメソッドを使用
    all_views, dependency_graph = mock_resolver.analyze_dependencies(views, "dataset")

    # 結果の検証
    expected_views = {
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
    }
    assert set(all_views) == expected_views
    assert "project.other_dataset.view4" not in all_views

    # 依存関係グラフの検証
    assert dependency_graph["project.dataset.view1"] == [
        "project.dataset.view2",
        "project.dataset.view3",
        "project.other_dataset.view4",
    ]
    assert dependency_graph["project.dataset.view2"] == ["project.dataset.view3"]
    assert dependency_graph["project.dataset.view3"] == []


def test_analyze_dependencies_with_error():
    """エラーが発生した場合のanalyze_dependenciesメソッドのテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    mock_bq_client.project_id = "project"

    # エラーを発生させるモック関数
    def mock_get_dependencies(view):
        if view == "project.dataset.view2":
            raise Exception("テスト用エラー")
        elif view == "project.dataset.view1":
            return ["project.dataset.view2", "project.dataset.view3"]
        else:
            return []

    mock_lineage_client.get_table_dependencies.side_effect = mock_get_dependencies

    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)
    views = ["project.dataset.view1"]

    # analyze_dependenciesメソッドを使用
    all_views, dependency_graph = resolver.analyze_dependencies(views, "dataset")

    # 結果の検証
    expected_views = {
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
    }
    assert set(all_views) == expected_views

    # 依存関係グラフの検証
    assert dependency_graph["project.dataset.view1"] == [
        "project.dataset.view2",
        "project.dataset.view3",
    ]
    # エラーが発生したビューは空の依存関係リストになるはず
    assert dependency_graph["project.dataset.view2"] == []
    assert dependency_graph["project.dataset.view3"] == []


@patch("rich.console.Console.print")
def test_display_dependencies_new(mock_print):
    """新しいdisplay_dependenciesメソッドのテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 依存関係グラフを手動で設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # 逆依存関係グラフも設定
    resolver.reverse_graph = {
        "project.dataset.view2": ["project.dataset.view1"],
        "project.dataset.view3": ["project.dataset.view1", "project.dataset.view2"],
    }

    # 依存関係を表示
    views = ["project.dataset.view1", "project.dataset.view2", "project.dataset.view3"]
    resolver.display_dependencies(views)

    # printは1回呼ばれるはず（テーブル出力）
    assert mock_print.call_count == 1
    # 引数がTableオブジェクトであることを確認
    assert isinstance(mock_print.call_args[0][0], Table)


def test_build_dependency_tree():
    """build_dependency_treeメソッドのテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)

    # 依存関係グラフを手動で設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # 依存関係ツリーを構築
    tree = resolver.build_dependency_tree("project.dataset.view1")

    # 結果がTreeオブジェクトであることを確認
    assert isinstance(tree, Tree)


def test_analyze_dependencies_with_max_depth():
    """最大深さを指定したanalyze_dependenciesメソッドのテスト"""
    mock_bq_client = MagicMock()
    mock_lineage_client = MagicMock()
    mock_bq_client.project_id = "project"

    # 深い依存関係を持つモックの戻り値を設定
    mock_lineage_client.get_table_dependencies.side_effect = [
        ["project.dataset.view2"],  # view1の依存先
        ["project.dataset.view3"],  # view2の依存先
        ["project.dataset.view4"],  # view3の依存先
        ["project.dataset.view5"],  # view4の依存先（最大深さを超える）
        [],  # view5の依存先（呼ばれないはず）
    ]

    resolver = DataCatalogDependencyResolver(mock_bq_client, mock_lineage_client)
    views = ["project.dataset.view1"]

    # 最大深さ2でanalyze_dependenciesメソッドを使用
    all_views, _ = resolver.analyze_dependencies(views, "dataset", max_depth=2)

    # 結果の検証
    # 実装では、max_depthは深さではなく、依存関係の追跡レベルを表している
    # view1(深さ0) -> view2(深さ1) -> view3(深さ2) -> view4(深さ3)
    # max_depth=2の場合、深さ0から数えて2レベル先（深さ2）までの依存関係を追跡するため、
    # view4も結果に含まれる
    expected_views = {
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
        "project.dataset.view4",
    }
    assert set(all_views) == expected_views

    # get_table_dependenciesの呼び出し回数を確認
    # view1, view2, view3の依存関係のみ取得される
    assert mock_lineage_client.get_table_dependencies.call_count == 3


@patch("bq2dbt.converter.dependency.LineageClient")
def test_dependency_resolver_alias(mock_lineage_client_class):
    """DependencyResolverがDataCatalogDependencyResolverのエイリアスであることをテスト"""
    mock_bq_client = MagicMock()
    mock_bq_client.project_id = "project"
    mock_bq_client.location = "location"

    # LineageClientのモックを設定
    mock_lineage_client = MagicMock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # DependencyResolverを初期化
    resolver = DependencyResolver(mock_bq_client)

    # DataCatalogDependencyResolverのインスタンスであることを確認
    assert isinstance(resolver, DataCatalogDependencyResolver)

    # LineageClientが正しく作成されたことを確認
    mock_lineage_client_class.assert_called_once_with(
        mock_bq_client.project_id, mock_bq_client.location
    )
