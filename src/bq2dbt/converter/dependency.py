"""依存関係解析モジュール。

BigQueryビュー間の依存関係を解析し、変換順序を決定します。
"""

import logging
from collections import defaultdict, deque
from typing import Dict, List

from bq2dbt.converter.bigquery import BigQueryClient

logger = logging.getLogger(__name__)


class DependencyResolver:
    """ビュー間の依存関係を解析するクラス。"""

    def __init__(self, bq_client: BigQueryClient):
        """依存関係リゾルバーを初期化します。

        Args:
            bq_client: BigQueryクライアント
        """
        self.bq_client = bq_client
        self.dependency_graph: Dict[str, List[str]] = {}  # ビュー名 -> 依存先リスト
        self.reverse_graph: Dict[str, List[str]] = defaultdict(
            list
        )  # 依存先 -> 依存元リスト

        logger.debug("依存関係リゾルバーを初期化しました")

    def build_dependency_graph(self, views: List[str]) -> Dict[str, List[str]]:
        """依存関係グラフを構築します。

        Args:
            views: 分析対象のビューのリスト

        Returns:
            依存関係グラフ（ビュー名 -> 依存先リスト）
        """
        logger.info(f"{len(views)}個のビューの依存関係を分析します")

        # 依存関係グラフを初期化
        self.dependency_graph = {}
        self.reverse_graph = defaultdict(list)

        # 各ビューの依存関係を取得
        for view in views:
            # BigQueryクライアントを使用して依存先を取得
            dependencies = self.bq_client.get_table_dependencies(view)

            # 依存先のうち、対象ビューリストに含まれるもののみを保持
            filtered_deps = [dep for dep in dependencies if dep in views]

            # 依存関係グラフに追加
            self.dependency_graph[view] = filtered_deps

            # 逆方向の依存関係も記録
            for dep in filtered_deps:
                self.reverse_graph[dep].append(view)

        logger.debug(
            f"依存関係グラフを構築しました: {len(self.dependency_graph)}個のノード"
        )
        return self.dependency_graph

    def get_topological_order(self) -> List[str]:
        """トポロジカルソートによる変換順序を取得します。

        Returns:
            ビューの変換順序（依存関係の順にソート）

        Raises:
            ValueError: 循環参照がある場合
        """
        if not self.dependency_graph:
            raise ValueError("依存関係グラフが構築されていません")

        # 入力次数（各ノードが依存しているノードの数）を計算
        in_degree = {node: 0 for node in self.dependency_graph}
        for node, deps in self.dependency_graph.items():
            for dep in deps:
                in_degree[dep] = in_degree.get(dep, 0) + 1

        # 入力次数が0のノードをキューに追加
        queue = deque([node for node, degree in in_degree.items() if degree == 0])

        # トポロジカルソート
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)

            # 依存先の入力次数を減らす
            for dep in self.dependency_graph.get(node, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)

        # すべてのノードが結果に含まれていない場合は循環参照がある
        if len(result) != len(self.dependency_graph):
            raise ValueError("循環参照が検出されました")

        # 結果を逆順にして返す（依存先から順に変換するため）
        return result[::-1]

    def get_dependent_views(self, view: str) -> List[str]:
        """指定したビューに依存しているビューのリストを取得します。

        Args:
            view: ビュー名

        Returns:
            依存しているビューのリスト
        """
        return self.reverse_graph.get(view, [])
