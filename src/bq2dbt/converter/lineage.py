"""Google Cloud Data Catalog Lineage APIクライアントモジュール。

BigQueryビュー間の依存関係を取得するためのLineage APIクライアントを提供します。
"""

import logging
from typing import List

from google.cloud import datacatalog_lineage_v1

logger = logging.getLogger(__name__)


class LineageClient:
    """Google Cloud Data Catalog Lineage APIとの通信を行うクライアントクラス。"""

    def __init__(self, project_id: str, location: str = "asia-northeast1"):
        """Lineageクライアントを初期化します。

        Args:
            project_id: Google Cloudプロジェクト
            location: Google Cloudのロケーション（デフォルト: asia-northeast1）
        """
        self.project_id = project_id
        self.location = location
        self.lineage_client = datacatalog_lineage_v1.LineageClient()
        logger.debug(
            f"Lineageクライアントを初期化しました: プロジェクト={project_id}, ロケーション={location}"
        )

    def get_table_dependencies(self, fully_qualified_name: str) -> List[str]:
        """ビューが参照しているテーブル/ビューの一覧を取得します。

        Google Cloud Datacatalog Lineage APIを使用して、ビューの依存関係を取得します。

        Args:
            fully_qualified_name: ビューの完全修飾名 (例: "project.dataset.view")

        Returns:
            参照先の完全修飾名のリスト
        """
        try:
            # fully_qualified_nameからプロジェクト、データセット、テーブル名を抽出
            parts = fully_qualified_name.split(".")
            if len(parts) != 3:
                raise ValueError(f"無効なテーブル名形式です: {fully_qualified_name}")

            project_id, dataset_id, table_name = parts

            # BigQuery用のFQNフォーマットを作成（Lineage API用）
            bq_fqn = f"bigquery:{fully_qualified_name}"

            logger.debug(f"Lineage APIを使用して依存関係を取得: {bq_fqn}")

            # 検索リクエストを作成 (このビューをターゲットとするリンクを検索)
            target = datacatalog_lineage_v1.EntityReference()
            target.fully_qualified_name = bq_fqn

            # APIの呼び出し
            # GCPのロケーションはBigQueryと一致させる必要があります
            location = self.location

            request = datacatalog_lineage_v1.SearchLinksRequest(
                target=target,
                parent=f"projects/{project_id}/locations/{location}",
            )

            # APIを呼び出し、結果を取得
            page_result = self.lineage_client.search_links(request=request)

            # 結果を処理
            dependencies = []
            for link in page_result:
                # ソースからBigQueryの完全修飾名を抽出
                source_fqn = link.source.fully_qualified_name
                if source_fqn.startswith("bigquery:"):
                    # bigquery:project.dataset.tableから project.dataset.table 形式に変換
                    bq_name = source_fqn[len("bigquery:") :]
                    dependencies.append(bq_name)

            logger.info(
                f"依存関係を{len(dependencies)}件取得しました: {fully_qualified_name}"
            )
            return dependencies

        except Exception as e:
            logger.error(f"依存関係の取得に失敗しました: {fully_qualified_name} - {e}")
            # エラーが発生した場合は空のリストを返す
            return []
