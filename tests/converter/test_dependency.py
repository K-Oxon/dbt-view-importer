"""DependencyResolverのテスト"""
from unittest.mock import MagicMock, patch

import pytest
from bq2dbt.converter.dependency import DependencyResolver
from rich.console import Console
from rich.table import Table
from rich.tree import Tree


def test_dependency_resolver_init():
    """DependencyResolverの初期化をテスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

    assert resolver.bq_client == mock_client
    assert resolver.dependency_graph == {}
    assert isinstance(resolver.reverse_graph, dict)


def test_build_dependency_graph():
    """依存関係グラフの構築をテスト"""
    mock_client = MagicMock()

    # モックのget_table_dependenciesメソッドの戻り値を設定
    mock_client.get_table_dependencies.side_effect = [
        ["project.dataset.view2", "project.dataset.view3"],  # view1の依存先
        ["project.dataset.view3"],  # view2の依存先
        [],  # view3の依存先
    ]

    resolver = DependencyResolver(mock_client)
    views = [
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
    ]

    dependency_graph = resolver.build_dependency_graph(views)

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
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

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


def test_get_topological_order_with_cycle():
    """循環参照があるケースのトポロジカルソートテスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

    # 循環参照のある依存関係グラフを手動で設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": ["project.dataset.view1"],
    }

    # 循環参照があるため、ValueError が発生するはず
    with pytest.raises(ValueError):
        resolver.get_topological_order()


def test_get_dependent_views():
    """依存しているビューの取得をテスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

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
    mock_client = MagicMock()

    # get_table_dependenciesの戻り値を設定
    # view1は他の2つのビューに依存、view2は1つのビューに依存、view3は依存先なし
    mock_client.get_table_dependencies.side_effect = [
        ["project.dataset1.view2", "project.dataset1.view3"],  # view1の依存先
        [
            "project.dataset1.view3",
            "project.dataset2.view4",
        ],  # view2の依存先（データセット外含む）
        [],  # view3の依存先
        ["project.dataset1.view3"],  # view4の依存先（循環参照も考慮）
    ]

    # project_idを設定
    mock_client.project_id = "project"

    resolver = DependencyResolver(mock_client)

    # データセット1のビューに対して解析
    initial_views = ["project.dataset1.view1", "project.dataset1.view2"]

    # analyze_dependenciesメソッドを使用
    all_views, _ = resolver.analyze_dependencies(initial_views, "dataset1")

    # 結果の検証（データセット1内のビューが全て含まれ、データセット2のビューは除外）
    expected_views = {
        "project.dataset1.view1",
        "project.dataset1.view2",
        "project.dataset1.view3",
    }
    assert set(all_views) == expected_views
    assert "project.dataset2.view4" not in all_views

    # get_table_dependenciesが正しく呼ばれたことを確認
    assert mock_client.get_table_dependencies.call_count == 3


def test_resolve_all_dependencies_with_no_dependencies():
    """依存関係がない場合のテスト"""
    mock_client = MagicMock()

    # すべてのビューに依存関係なし
    mock_client.get_table_dependencies.return_value = []
    mock_client.project_id = "project"

    resolver = DependencyResolver(mock_client)
    initial_views = ["project.dataset1.view1", "project.dataset1.view2"]

    # analyze_dependenciesメソッドを使用
    all_views, _ = resolver.analyze_dependencies(initial_views, "dataset1")

    # 元のビューリストと同じになるはず
    assert set(all_views) == set(initial_views)
    assert mock_client.get_table_dependencies.call_count == 2


@patch("rich.console.Console.print")
def test_display_dependencies(mock_print):
    """依存関係ツリーの表示テスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

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
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

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
    mock_client = MagicMock()
    mock_client.project_id = "project"

    # モックのget_table_dependenciesメソッドの戻り値を設定
    mock_client.get_table_dependencies.side_effect = [
        [
            "project.dataset.view2",
            "project.dataset.view3",
            "project.other_dataset.view4",
        ],  # view1の依存先
        ["project.dataset.view3"],  # view2の依存先
        [],  # view3の依存先
        [],  # view4の依存先 (もし呼ばれた場合)
    ]

    resolver = DependencyResolver(mock_client)
    views = [
        "project.dataset.view1",
        "project.dataset.view2",
    ]

    # analyze_dependencies メソッドを呼び出し
    result_views, result_graph = resolver.analyze_dependencies(views, "dataset")

    # 依存関係グラフの確認
    expected_graph = {
        "project.dataset.view1": [
            "project.dataset.view2",
            "project.dataset.view3",
            "project.other_dataset.view4",
        ],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # 正しい追加ビューが含まれていることを確認
    assert set(result_views) == {
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
    }

    # 他のデータセットのビュー (view4) は含まれていないことを確認
    assert "project.other_dataset.view4" not in result_views

    # 依存関係グラフに正しく追加されていることを確認
    assert "project.dataset.view1" in result_graph
    assert "project.dataset.view2" in result_graph
    assert "project.dataset.view3" in result_graph

    # get_table_dependencies が正しく呼ばれたことを確認
    assert mock_client.get_table_dependencies.call_count == 3
    mock_client.get_table_dependencies.assert_any_call("project.dataset.view1")
    mock_client.get_table_dependencies.assert_any_call("project.dataset.view2")
    mock_client.get_table_dependencies.assert_any_call("project.dataset.view3")


def test_analyze_dependencies_with_error():
    """analyze_dependencies メソッドのエラーハンドリングのテスト"""
    mock_client = MagicMock()
    mock_client.project_id = "project"

    # 一部のビューで例外が発生する場合のモック設定
    def mock_get_dependencies(view):
        if view == "project.dataset.view2":
            raise ValueError("テスト用エラー")
        elif view == "project.dataset.view1":
            return ["project.dataset.view2", "project.dataset.view3"]
        else:
            return []

    mock_client.get_table_dependencies.side_effect = mock_get_dependencies

    resolver = DependencyResolver(mock_client)
    views = [
        "project.dataset.view1",
        "project.dataset.view2",
    ]

    # analyze_dependencies メソッドを呼び出し
    result_views, result_graph = resolver.analyze_dependencies(views, "dataset")

    # エラーが発生したビューも結果に含まれていることを確認
    assert set(result_views) == {
        "project.dataset.view1",
        "project.dataset.view2",
        "project.dataset.view3",
    }

    # エラーが発生したビューは空の依存関係になっていることを確認
    assert result_graph["project.dataset.view2"] == []


@patch("rich.console.Console.print")
def test_display_dependencies_new(mock_print):
    """display_dependencies メソッドのテスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

    # 依存関係グラフを設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # 逆方向の依存関係グラフも設定
    resolver.reverse_graph = {
        "project.dataset.view2": ["project.dataset.view1"],
        "project.dataset.view3": ["project.dataset.view1", "project.dataset.view2"],
    }

    # メソッドを呼び出し
    resolver.display_dependencies(
        ["project.dataset.view1", "project.dataset.view2", "project.dataset.view3"]
    )

    # Tableオブジェクトが正しく生成されたことを確認
    assert mock_print.call_count == 1
    call_args = mock_print.call_args[0][0]
    assert isinstance(call_args, Table)
    assert call_args.title == "ビュー依存関係一覧"


def test_build_dependency_tree():
    """build_dependency_tree メソッドのテスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

    # 依存関係グラフを設定
    resolver.dependency_graph = {
        "project.dataset.view1": ["project.dataset.view2", "project.dataset.view3"],
        "project.dataset.view2": ["project.dataset.view3"],
        "project.dataset.view3": [],
    }

    # メソッドを呼び出し
    tree = resolver.build_dependency_tree("project.dataset.view1")

    # 結果がTreeオブジェクトであることを確認
    assert isinstance(tree, Tree)

    # Treeオブジェクトの文字列検証はせず、クラスタイプだけ確認する
    # 実装の詳細に過度に依存するテストを避ける
    assert tree is not None
