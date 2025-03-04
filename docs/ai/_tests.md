# テスト実装状況と戦略

## 実装済みテスト概要

現在のプロジェクトには以下のテストが実装されています。

### BigQueryClient テスト (`tests/converter/test_bigquery.py`)

- `test_bigquery_client_init`: BigQueryClientの初期化をテスト
- `test_list_views`: ビュー一覧の取得処理をテスト（モック使用）
- `test_get_view_definition`: ビュー定義の取得をテスト（モック使用）
- `test_get_view_schema`: ビュースキーマの取得をテスト（モック使用）
- `test_get_table_dependencies`: テーブル依存関係の取得をテスト（モック使用）
- `TestBigQueryIntegration`: 実際のBigQuery接続が必要な統合テスト（デフォルトではスキップ）
  - `test_real_connection`: 実際のBigQuery接続をテスト
  - `test_real_list_views`: 実際のBigQueryからビュー一覧を取得するテスト

### コマンドテスト (`tests/commands/test_import_cmd.py`)

- `test_import_cmd_group`: インポートコマンドグループの動作をテスト
- `test_import_views_command_basic`: 基本的なビューインポートコマンドの動作をテスト（モック使用）
- `test_import_views_command_with_filters`: フィルター付きビューインポートコマンドの動作をテスト（モック使用）

### 依存関係テスト (`tests/converter/test_dependency.py`)

- 依存関係の解析と解決に関するテスト

### ジェネレーターテスト (`tests/converter/test_generator.py`)

- dbtモデル生成に関するテスト

## 最近修正したテスト

### 1. `test_get_view_definition` の修正

問題点:
- モックの設定がBigQueryClientの実装と合致していなかった
- テストではモックに対して`result()`メソッドを返す設定をしていたが、実際の実装ではqueryの結果を直接リスト変換していた

修正内容:
```python
# 修正前
mock_query_job = MagicMock()
mock_query_result = [MockRow("SELECT * FROM test_table")]
mock_query_job.result.return_value = mock_query_result
mock_instance.query.return_value = mock_query_job

# 修正後
mock_query_result = [MockRow("SELECT * FROM test_table")]
mock_query_job = mock_query_result
mock_instance.query.return_value = mock_query_job
```

### 2. `test_list_views` の修正

- 同様の問題を修正
- モックの設定を実際の実装に合わせて調整

### 3. `test_import_views_command_basic` の修正

問題点:
- テストでは`list_views`メソッドが単にデータセット名のみで呼び出されることを期待していたが、実際の実装では追加パラメータ(`include_patterns`, `exclude_patterns`)も渡していた

修正内容:
```python
# 修正前
mock_bq_instance.list_views.assert_called_once_with("test_dataset")

# 修正後
mock_bq_instance.list_views.assert_called_once_with(
    "test_dataset",
    include_patterns=None,
    exclude_patterns=None,
)
```

## テスト戦略

### 1. 単体テスト強化

- **型アノテーションの追加**: 現在のlinterエラーを解消するため、すべてのテスト関数に適切な型アノテーションを追加
- **エッジケースカバレッジ**: 以下のエッジケースを追加でテスト
  - 空の結果セット
  - エラーケース（無効なビュー名、アクセス権限エラーなど）
  - フィルターの複雑なパターン
- **モック設計の改善**: BigQueryクライアントのモック作成を再利用可能な関数やフィクスチャに抽出

### 2. コマンドテスト強化

- **コマンドラインオプション網羅**: すべてのコマンドラインオプションの組み合わせをテスト
- **対話モードのテスト**: 対話モード（`--non-interactive`フラグなし）での動作確認
- **エラーハンドリングテスト**: コマンド実行中に発生する可能性のあるエラーの処理をテスト

### 3. 統合テスト

- **テスト用データセット**: テスト用のBigQueryデータセットとビューを作成する自動化スクリプト
- **エンドツーエンドテスト**: 実際のBigQueryからdbtモデルを生成するエンドツーエンドテスト
- **CI対応**: CI環境でも統合テストが実行できるよう、テスト用の認証情報管理方法を整備

### 4. テストカバレッジ目標

- コード全体で80%以上のカバレッジを目指す
- 特に以下の重要コンポーネントは90%以上のカバレッジを確保
  - `BigQueryClient`クラス
  - `DependencyResolver`クラス
  - `ModelGenerator`クラス
  - コマンドライン処理部分

### 5. テスト自動化

- **pre-commit**: コミット前の自動テスト実行を設定
- **CI/CD**: GitHub ActionsなどでのCI/CDパイプラインにテスト実行を組み込み
- **テストレポート**: テスト結果とカバレッジレポートの自動生成と保存

## 今後のテスト実装計画

1. **短期目標**
   - 型アノテーションの追加によるlinterエラーの解消
   - エラーケースのテスト追加
   - テストカバレッジレポート生成の設定

2. **中期目標**
   - 未カバーのエッジケースに対するテスト追加
   - モック設計の最適化
   - 統合テスト環境の整備

3. **長期目標**
   - 自動テスト実行基盤の整備
   - エンドツーエンドテストの実装
   - パフォーマンステストの追加（大量データ処理時の動作確認）

## テスト実装における注意点

1. **テストの独立性**: 各テストは他のテストに依存せず、独立して実行できるようにする
2. **モックの適切な使用**: 外部依存を持つコンポーネントはモックを使用し、テストの再現性を確保
3. **テスト速度**: テストスイート全体が高速に実行できるよう、不必要に重いテストは避ける
4. **メンテナンス性**: テストコードも本番コードと同様にクリーンに保ち、メンテナンス性を確保
5. **デバッグ可能性**: テスト失敗時に原因が特定しやすいよう、明確なアサーションメッセージを使用 