"""依存関係解析モジュール。

BigQueryビュー間の依存関係を解析し、変換順序を決定します。
"""

import abc
import logging
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.lineage import LineageClient

logger = logging.getLogger(__name__)


class DependencyResolverBase(abc.ABC):
    """依存関係解析の基底クラス。

    このクラスは依存関係解析の共通インターフェースを定義します。
    具体的な実装は派生クラスで行います。
    """

    def __init__(self) -> None:
        """依存関係リゾルバーの基底クラスを初期化します。"""
        self.dependency_graph: Dict[str, List[str]] = {}  # ビュー名 -> 依存先リスト
        self.reverse_graph: Dict[str, List[str]] = defaultdict(
            list
        )  # 依存先 -> 依存元リスト
        logger.debug("依存関係リゾルバー基底クラスを初期化しました")

    @abc.abstractmethod
    def analyze_dependencies(
        self,
        views: List[str],
        target_dataset_id: str,
        max_depth: int = 3,
        status_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """指定されたビューリストの依存関係を解析し、変換に必要なビューリストを作成します。

        Args:
            views: 分析対象のビューのリスト（project.dataset.table形式）
            target_dataset_id: 変換対象のデータセットID
            max_depth: 依存関係を追跡する最大深さ
            status_callback: 処理状況を通知するコールバック関数

        Returns:
            (拡張されたビューリスト, 依存関係グラフ)のタプル
        """
        pass

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

    def get_topological_order(self) -> List[str]:
        """依存関係に基づいた変換順序を取得します。

        Returns:
            ビューの変換順序（依存関係の順にソート）
        """
        if not self.dependency_graph:
            raise ValueError("依存関係グラフが構築されていません")

        # 依存関係の逆順で返す（依存先から順に変換するため）
        # 単純に依存関係の深さでソート
        view_depth: Dict[str, int] = {}

        # 初期化：すべてのビューの深さを0に設定
        for view in self.dependency_graph:
            view_depth[view] = 0

        # 各ビューの深さを計算
        for view, deps in self.dependency_graph.items():
            for dep in deps:
                view_depth[dep] = max(view_depth.get(dep, 0), view_depth[view] + 1)

        # 深さでソート
        return sorted(
            self.dependency_graph.keys(),
            key=lambda v: view_depth.get(v, 0),
            reverse=True,
        )


class DataCatalogDependencyResolver(DependencyResolverBase):
    """Google Cloud Data Catalog Lineage APIを使用した依存関係解析クラス。"""

    def __init__(
        self, bq_client: BigQueryClient, lineage_client: LineageClient
    ) -> None:
        """Data Catalog Lineage APIを使用した依存関係リゾルバーを初期化します。

        Args:
            bq_client: BigQueryクライアント
            lineage_client: Lineageクライアント
        """
        super().__init__()
        self.bq_client = bq_client
        self.lineage_client = lineage_client
        logger.debug("Data Catalog依存関係リゾルバーを初期化しました")

    def analyze_dependencies(
        self,
        views: List[str],
        target_dataset_id: str,
        max_depth: int = 3,
        status_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """指定されたビューリストの依存関係を解析し、変換に必要なビューリストを作成します。

        Args:
            views: 分析対象のビューのリスト（project.dataset.table形式）
            target_dataset_id: 変換対象のデータセットID
            max_depth: 依存関係を追跡する最大深さ
            status_callback: 処理状況を通知するコールバック関数

        Returns:
            (拡張されたビューリスト, 依存関係グラフ)のタプル
        """
        logger.info(
            f"{len(views)}個のビューの依存関係を分析します（最大深さ: {max_depth}）"
        )

        # 対象データセットの完全修飾パターン
        project_id = self.bq_client.project_id
        target_pattern = f"{project_id}.{target_dataset_id}."

        # 必要なビューを追跡するセット（重複を避けるため）
        required_views: Set[str] = set(views)
        processed_views: Set[str] = set()

        # 依存関係グラフを初期化
        self.dependency_graph = {}
        self.reverse_graph = defaultdict(list)

        # 各ビューの深さを追跡
        view_depth: Dict[str, int] = {view: 0 for view in views}

        # 処理キュー
        queue = [(view, 0) for view in views]  # (ビュー名, 現在の深さ)

        # 処理済みビュー数
        processed_count = 0
        total_count = len(queue)

        # キューが空になるまで処理
        while queue:
            view, depth = queue.pop(0)

            # 最大深さに達した場合はスキップ
            if depth > max_depth:
                continue

            # 既に処理済みの場合はスキップ
            if view in processed_views:
                continue

            # 処理状況をコールバックで通知
            if status_callback:
                status_callback(view, processed_count, total_count)

            try:
                # ビューの依存関係を取得
                dependencies = self.lineage_client.get_table_dependencies(view)

                # 依存関係グラフに追加
                self.dependency_graph[view] = dependencies

                # 逆方向の依存関係も記録
                for dep in dependencies:
                    self.reverse_graph[dep].append(view)

                    # 対象データセットに含まれる依存ビューのみを追加処理対象に含める
                    if dep.startswith(target_pattern):
                        required_views.add(dep)

                        # まだ処理していないビューで、最大深さに達していない場合はキューに追加
                        if dep not in processed_views and depth + 1 <= max_depth:
                            queue.append((dep, depth + 1))
                            view_depth[dep] = depth + 1
                            total_count += 1

                # 処理済みとしてマーク
                processed_views.add(view)
                processed_count += 1

            except Exception as e:
                logger.error(f"ビュー {view} の依存関係解析に失敗しました: {e}")
                # エラーが発生したビューは依存関係がないものとして処理
                self.dependency_graph[view] = []
                processed_views.add(view)
                processed_count += 1

        # 最終状態をコールバックで通知
        if status_callback:
            status_callback("完了", processed_count, total_count)

        # セットをリストに変換
        result_views = list(required_views)

        logger.info(f"依存関係解析が完了しました。対象ビュー数: {len(result_views)}")
        return result_views, self.dependency_graph


# 後方互換性のために元のクラス名を維持
class DependencyResolver(DataCatalogDependencyResolver):
    """後方互換性のためのエイリアスクラス。

    このクラスはDataCatalogDependencyResolverの単なるエイリアスです。
    """

    def __init__(self, bq_client: BigQueryClient) -> None:
        """後方互換性のためのコンストラクタ。

        Args:
            bq_client: BigQueryクライアント
        """
        # 同じプロジェクトとロケーションでLineageClientを作成
        lineage_client = LineageClient(bq_client.project_id, bq_client.location)
        super().__init__(bq_client, lineage_client)
        logger.debug("後方互換性のためのDependencyResolverを初期化しました")
