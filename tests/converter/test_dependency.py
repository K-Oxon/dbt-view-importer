"""DependencyResolverのテスト"""
from unittest.mock import MagicMock

import pytest
from bq2dbt.converter.dependency import DependencyResolver


def test_dependency_resolver_init():
    """DependencyResolverの初期化をテスト"""
    mock_client = MagicMock()
    resolver = DependencyResolver(mock_client)

    assert resolver.bq_client == mock_client
    assert resolver.dependency_graph == {}
    assert resolver.reverse_graph == {}


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
