"""依存関係解析モジュール。

BigQueryビュー間の依存関係を解析し、変換順序を決定します。
"""

import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.table import Table
from rich.tree import Tree

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

    def analyze_dependencies(
        self, views: List[str], target_dataset_id: str
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """指定されたビューリストの依存関係を解析し、変換に必要なビューリストを作成します。

        Args:
            views: 分析対象のビューのリスト（project.dataset.table形式）
            target_dataset_id: 変換対象のデータセットID

        Returns:
            (拡張されたビューリスト, 依存関係グラフ)のタプル
            拡張されたビューリストには、指定されたビューと、それらが依存する他のビューが含まれます
        """
        logger.info(f"{len(views)}個のビューの依存関係を分析します")

        # 対象データセットの完全修飾パターン
        project_id = self.bq_client.project_id
        target_pattern = f"{project_id}.{target_dataset_id}."

        # 必要なビューを追跡するセット（重複を避けるため）
        # 最初に指定されたビューをすべて追加
        required_views: Set[str] = set(views)
        processed_views: Set[str] = set()

        # 依存関係グラフを初期化
        self.dependency_graph = {}
        self.reverse_graph = defaultdict(list)

        # 処理が必要なビューがなくなるまで繰り返し
        while required_views - processed_views:
            # 未処理のビューを1つ取得
            view = next(iter(required_views - processed_views))

            try:
                # ビューの依存関係を取得
                dependencies = self.bq_client.get_table_dependencies(view)

                # 依存関係グラフに追加
                self.dependency_graph[view] = dependencies

                # 逆方向の依存関係も記録
                for dep in dependencies:
                    self.reverse_graph[dep].append(view)

                    # 対象データセットに含まれる依存ビューのみを追加処理対象に含める
                    if dep.startswith(target_pattern):
                        required_views.add(dep)

                # 処理済みとしてマーク
                processed_views.add(view)

            except Exception as e:
                logger.error(f"ビュー {view} の依存関係解析に失敗しました: {e}")
                # エラーが発生したビューは依存関係がないものとして処理
                self.dependency_graph[view] = []
                processed_views.add(view)

        # セットをリストに変換
        result_views = list(required_views)

        logger.info(f"依存関係解析が完了しました。対象ビュー数: {len(result_views)}")
        return result_views, self.dependency_graph

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
            try:
                # BigQueryクライアントを使用して依存先を取得
                dependencies = self.bq_client.get_table_dependencies(view)

                # 依存先のうち、対象ビューリストに含まれるもののみを保持
                filtered_deps = [dep for dep in dependencies if dep in views]

                # 依存関係グラフに追加
                self.dependency_graph[view] = filtered_deps

                # 逆方向の依存関係も記録
                for dep in filtered_deps:
                    self.reverse_graph[dep].append(view)
            except Exception as e:
                logger.error(f"ビュー {view} の依存関係解析に失敗しました: {e}")
                # エラーが発生したビューは依存関係がないものとして処理
                self.dependency_graph[view] = []

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

    def display_dependencies(
        self, views: List[str], console: Optional[Console] = None
    ) -> None:
        """ビューの依存関係を表示します。

        Args:
            views: 表示するビューのリスト
            console: 表示に使用するRichコンソール（省略可）
        """
        if not console:
            console = Console()

        if not self.dependency_graph:
            console.print(
                "[yellow]依存関係グラフが構築されていません。先にanalyze_dependencies()を呼び出してください。[/]"
            )
            return

        # 依存関係テーブルを作成
        table = Table(title="ビュー依存関係一覧")
        table.add_column("ビュー名", style="cyan")
        table.add_column("依存先", style="green")
        table.add_column("依存元", style="magenta")

        for view in views:
            # 依存先と依存元を取得
            dependencies = self.dependency_graph.get(view, [])
            dependents = self.reverse_graph.get(view, [])

            # テーブルに行を追加
            table.add_row(
                view,
                "\n".join(dependencies) if dependencies else "-",
                "\n".join(dependents) if dependents else "-",
            )

        console.print(table)

    def build_dependency_tree(self, root_view: str) -> Tree:
        """指定されたビューを起点とする依存関係ツリーを構築します。

        Args:
            root_view: ルートとなるビュー名

        Returns:
            依存関係を表すRichツリー
        """
        tree = Tree(f"[bold cyan]{root_view}[/]")
        visited = set()

        def add_dependencies(node: str, parent_tree: Tree) -> None:
            if node in visited:
                parent_tree.add(f"[dim]{node} (循環参照)[/]")
                return

            visited.add(node)
            dependencies = self.dependency_graph.get(node, [])

            for dep in dependencies:
                dep_node = parent_tree.add(f"[green]{dep}[/]")
                add_dependencies(dep, dep_node)

        add_dependencies(root_view, tree)
        return tree
