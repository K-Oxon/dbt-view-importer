"""BigQueryClientのテスト"""
import os
from unittest.mock import MagicMock, patch

import pytest
from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.lineage import LineageClient


# モックを使用したテスト
def test_bigquery_client_init():
    """BigQueryClientの初期化をテスト"""
    with patch("bq2dbt.converter.bigquery.bigquery.Client") as mock_client:
        client = BigQueryClient("test-project")
        mock_client.assert_called_once_with(project="test-project")
        assert client.project_id == "test-project"


def test_list_views():
    """ビュー一覧の取得をテスト（モック使用）"""
    with patch("bq2dbt.converter.bigquery.bigquery.Client") as mock_bq_client:
        # モックのクエリ結果を設定
        mock_instance = mock_bq_client.return_value

        # 正しいRowオブジェクトをシミュレート
        class MockRow:
            def __init__(self, table_name):
                self.table_name = table_name

            def __getitem__(self, key):
                # 辞書アクセスをサポート
                return getattr(self, key)

        # リストでiterableにする
        mock_query_result = [MockRow("view1"), MockRow("view2")]

        # 直接query_jobをリスト変換できるようにする
        mock_query_job = mock_query_result
        mock_instance.query.return_value = mock_query_job

        client = BigQueryClient("test-project")
        views = client.list_views("test_dataset")

        # 期待される結果: プロジェクト.データセット.ビュー名
        expected_views = [
            "test-project.test_dataset.view1",
            "test-project.test_dataset.view2",
        ]

        assert views == expected_views

        # queryメソッドが適切なSQLで呼び出されたか確認
        mock_instance.query.assert_called_once()
        call_args = mock_instance.query.call_args[0][0]
        assert "SELECT" in call_args
        assert "table_name" in call_args
        assert "FROM" in call_args
        assert "INFORMATION_SCHEMA.VIEWS" in call_args


def test_get_view_definition():
    """ビュー定義の取得をテスト（モック使用）"""
    with patch("bq2dbt.converter.bigquery.bigquery.Client") as mock_bq_client:
        # モックのクエリ結果を設定
        mock_instance = mock_bq_client.return_value

        # 正しいRowオブジェクトをシミュレート
        class MockRow:
            def __init__(self, view_definition):
                self.view_definition = view_definition

            def __getitem__(self, key):
                # 辞書アクセスをサポート
                return getattr(self, key)

        # リストでiterableにする
        mock_query_result = [MockRow("SELECT * FROM test_table")]

        # 直接query_jobをリスト変換できるようにする
        mock_query_job = mock_query_result
        mock_instance.query.return_value = mock_query_job

        client = BigQueryClient("test-project")
        definition = client.get_view_definition("test-project.test_dataset.test_view")

        assert definition == "SELECT * FROM test_table"

        # queryメソッドが適切なSQLで呼び出されたか確認
        mock_instance.query.assert_called_once()
        call_args = mock_instance.query.call_args[0][0]
        assert "view_definition" in call_args
        assert "FROM" in call_args
        assert "INFORMATION_SCHEMA.VIEWS" in call_args
        assert "table_name" in call_args
        assert "test_view" in call_args


def test_get_view_schema():
    """ビューのスキーマ情報取得をテスト（モック使用）"""
    with patch("bq2dbt.converter.bigquery.bigquery.Client") as mock_bq_client:
        # モックの設定
        mock_instance = mock_bq_client.return_value

        # get_tableの返り値を設定
        mock_table = MagicMock()
        mock_schema_field1 = MagicMock()
        mock_schema_field1.name = "id"
        mock_schema_field1.field_type = "INT64"
        mock_schema_field1.description = "ID column"
        mock_schema_field1.mode = "NULLABLE"

        mock_schema_field2 = MagicMock()
        mock_schema_field2.name = "name"
        mock_schema_field2.field_type = "STRING"
        mock_schema_field2.description = "Name column"
        mock_schema_field2.mode = "NULLABLE"

        mock_table.schema = [mock_schema_field1, mock_schema_field2]
        mock_instance.get_table.return_value = mock_table

        # dataset関数のモック
        mock_dataset = MagicMock()
        mock_table_ref = MagicMock()
        mock_dataset.table.return_value = mock_table_ref
        mock_instance.dataset.return_value = mock_dataset

        client = BigQueryClient("test-project")
        schema = client.get_view_schema("test-project.test_dataset.test_view")

        # 期待されるスキーマ情報
        expected_schema = [
            {
                "name": "id",
                "type": "INT64",
                "description": "ID column",
                "mode": "NULLABLE",
            },
            {
                "name": "name",
                "type": "STRING",
                "description": "Name column",
                "mode": "NULLABLE",
            },
        ]

        assert schema == expected_schema

        # 適切なメソッドが呼び出されたか確認
        mock_instance.dataset.assert_called_once_with(
            "test_dataset", project="test-project"
        )
        mock_dataset.table.assert_called_once_with("test_view")
        mock_instance.get_table.assert_called_once_with(mock_table_ref)


@patch("google.cloud.datacatalog_lineage_v1.LineageClient")
def test_get_table_dependencies(mock_lineage_client_class):
    """テーブル依存関係の取得をテスト"""
    # LineageClientのモックを設定
    mock_lineage_client = MagicMock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # SearchLinksの結果をモック
    mock_link1 = MagicMock()
    mock_link1.source.fully_qualified_name = "bigquery:project.dataset.source_view1"

    mock_link2 = MagicMock()
    mock_link2.source.fully_qualified_name = "bigquery:project.dataset.source_view2"

    # PagedIteratorのような動作をするモックを作成
    mock_page_result = MagicMock()
    mock_page_result.__iter__.return_value = [mock_link1, mock_link2]
    mock_lineage_client.search_links.return_value = mock_page_result

    # LineageClientを初期化
    client = LineageClient("project")

    # 依存関係を取得
    dependencies = client.get_table_dependencies("project.dataset.view")

    # 結果を検証
    assert dependencies == [
        "project.dataset.source_view1",
        "project.dataset.source_view2",
    ]

    # search_linksが正しい引数で呼ばれたことを確認
    mock_lineage_client.search_links.assert_called_once()
    call_args = mock_lineage_client.search_links.call_args[1]

    # リクエストオブジェクトを検証
    request = call_args["request"]
    assert request.parent == "projects/project/locations/asia-northeast1"
    assert request.target.fully_qualified_name == "bigquery:project.dataset.view"


def test_get_table_dependencies_error_handling():
    """テーブル依存関係の取得時のエラーハンドリングをテスト"""
    with patch(
        "google.cloud.datacatalog_lineage_v1.LineageClient"
    ) as mock_lineage_client:
        # LineageClientのモック - 例外をスロー
        mock_lineage_instance = mock_lineage_client.return_value
        mock_lineage_instance.search_links.side_effect = Exception("API error")

        # テスト対象インスタンスの作成
        client = LineageClient("test-project")

        # テスト実行 - エラーが発生しても空リストが返されることを確認
        result = client.get_table_dependencies("test-project.test_dataset.test_view")
        assert result == []

        # APIが呼び出されたことを確認
        mock_lineage_instance.search_links.assert_called_once()


# 実際のBigQueryに接続するテスト (オプション)
# 環境変数 ENABLE_BQ_TESTS=1 が設定されている場合のみ実行
@pytest.mark.skipif(
    os.environ.get("ENABLE_BQ_TESTS") != "1",
    reason="Skipping tests that require BigQuery connection",
)
class TestBigQueryIntegration:
    """BigQuery実連携テスト"""

    def test_real_connection(self, bq_project_id):
        """実際のBigQueryへの接続テスト"""
        client = BigQueryClient(bq_project_id)
        assert client.project_id == bq_project_id

    def test_real_list_views(self, bq_project_id, bq_dataset_id):
        """実際のBigQueryからビュー一覧を取得するテスト"""
        client = BigQueryClient(bq_project_id)
        views = client.list_views(bq_dataset_id)
        # 結果の検証は環境によって異なるため、基本的な型チェックのみ
        assert isinstance(views, list)
        # ビューが存在する場合は形式を確認
        if views:
            assert all(f"{bq_project_id}.{bq_dataset_id}." in view for view in views)
