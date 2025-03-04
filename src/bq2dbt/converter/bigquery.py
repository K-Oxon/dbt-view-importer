"""BigQuery APIクライアントモジュール。

BigQueryのビュー取得と定義取得を行うクライアントを提供します。
"""

import logging
from typing import Dict, List, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryClient:
    """BigQuery APIとの通信を行うクライアントクラス。"""

    def __init__(self, project_id: str):
        """BigQueryクライアントを初期化します。

        Args:
            project_id: BigQueryプロジェクトID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        logger.debug(f"BigQueryクライアントを初期化しました: プロジェクト={project_id}")

    def list_views(
        self,
        dataset_id: str,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
    ) -> List[str]:
        """データセット内のビュー一覧を取得します。

        Args:
            dataset_id: データセットID
            include_patterns: 含めるビューのパターン（省略可）
            exclude_patterns: 除外するビューのパターン（省略可）

        Returns:
            ビューの完全修飾名のリスト (例: ["project.dataset.view1", "project.dataset.view2"])
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        logger.info(f"データセット {dataset_ref} のビュー一覧を取得します")

        # Information Schemaからビュー一覧を取得
        query = f"""
            SELECT
              table_name
            FROM
              `{dataset_ref}.INFORMATION_SCHEMA.VIEWS`
            ORDER BY
              table_name
        """

        query_job = self.client.query(query)
        rows = list(query_job)

        # パターンフィルタリングを適用
        filtered_views = []
        for row in rows:
            view_name = row.table_name
            fully_qualified_name = f"{self.project_id}.{dataset_id}.{view_name}"

            # 含めるパターンによるフィルタリング
            if include_patterns:
                include_match = False
                for pattern in include_patterns:
                    # 簡易的なパターンマッチング（*を使用）
                    if self._match_pattern(view_name, pattern):
                        include_match = True
                        break
                if not include_match:
                    continue  # マッチしない場合はスキップ

            # 除外パターンによるフィルタリング
            if exclude_patterns:
                exclude_match = False
                for pattern in exclude_patterns:
                    if self._match_pattern(view_name, pattern):
                        exclude_match = True
                        break
                if exclude_match:
                    continue  # マッチする場合はスキップ

            filtered_views.append(fully_qualified_name)

        logger.debug(f"{len(filtered_views)}個のビューが見つかりました")
        return filtered_views

    def _match_pattern(self, text: str, pattern: str) -> bool:
        """簡易的なパターンマッチングを行います。

        Args:
            text: マッチング対象のテキスト
            pattern: パターン（*をワイルドカードとして使用可能）

        Returns:
            マッチする場合はTrue、しない場合はFalse
        """
        import re

        # *をワイルドカードとして扱い、正規表現に変換
        regex_pattern = pattern.replace("*", ".*")
        return bool(re.match(f"^{regex_pattern}$", text))

    def get_view_definition(self, fully_qualified_name: str) -> str:
        """ビューのSQL定義を取得します。

        Args:
            fully_qualified_name: ビューの完全修飾名 (例: "project.dataset.view")

        Returns:
            ビューのSQL定義

        Raises:
            ValueError: ビューが存在しない場合
        """
        parts = fully_qualified_name.split(".")
        if len(parts) != 3:
            raise ValueError(f"無効なビュー名: {fully_qualified_name}")

        project_id, dataset_id, view_name = parts
        logger.debug(f"ビュー定義を取得します: {fully_qualified_name}")

        # Information Schemaからビュー定義を取得
        query = f"""
            SELECT
              view_definition
            FROM
              `{project_id}.{dataset_id}.INFORMATION_SCHEMA.VIEWS`
            WHERE
              table_name = '{view_name}'
        """

        query_job = self.client.query(query)
        rows = list(query_job)

        if not rows:
            raise ValueError(f"ビューが存在しません: {fully_qualified_name}")

        view_definition = rows[0].view_definition
        logger.debug(f"ビュー定義を取得しました: {len(view_definition)}文字")

        return view_definition

    def get_view_schema(self, fully_qualified_name: str) -> List[Dict[str, str]]:
        """ビューのスキーマ情報を取得します。

        Args:
            fully_qualified_name: ビューの完全修飾名 (例: "project.dataset.view")

        Returns:
            スキーマ情報の辞書のリスト（各辞書には name, type, description などのキーが含まれる）

        Raises:
            ValueError: ビューが存在しない場合
        """
        parts = fully_qualified_name.split(".")
        if len(parts) != 3:
            raise ValueError(f"無効なビュー名: {fully_qualified_name}")

        project_id, dataset_id, view_name = parts
        logger.debug(f"スキーマ情報を取得します: {fully_qualified_name}")

        try:
            # テーブル参照を取得
            table_ref = self.client.dataset(dataset_id, project=project_id).table(
                view_name
            )
            # テーブル（ビュー）のメタデータを取得
            table = self.client.get_table(table_ref)

            # スキーマ情報をリストに変換
            schema_fields = []
            for field in table.schema:
                field_info = {
                    "name": field.name,
                    "type": field.field_type,
                    "description": field.description or "",
                    "mode": field.mode or "NULLABLE",
                }
                schema_fields.append(field_info)

            logger.debug(f"{len(schema_fields)}個のフィールドが見つかりました")
            return schema_fields

        except Exception as e:
            logger.error(f"スキーマ取得中にエラーが発生しました: {e}")
            raise ValueError(f"スキーマを取得できません: {fully_qualified_name} - {e}")

    def get_table_dependencies(self, fully_qualified_name: str) -> List[str]:
        """ビューが参照しているテーブル/ビューの一覧を取得します。

        注: このメソッドは現在、Information Schemaのみを使用した簡易実装です。
        将来的にはData Lineage APIを使用した正確な実装に置き換える予定です。

        Args:
            fully_qualified_name: ビューの完全修飾名 (例: "project.dataset.view")

        Returns:
            参照先の完全修飾名のリスト
        """
        # 現時点では簡易的な実装
        # SQL定義からテーブル参照を抽出する処理は複雑なため、まずは実装を省略
        # 将来的にはLineage APIを使用した正確な実装に置き換える予定
        logger.warning("依存関係の分析はまだ実装されていません")
        return []
