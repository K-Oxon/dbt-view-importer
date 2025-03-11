# テスト実装状況と戦略

## 実装済みテスト概要 (2024/03/11 更新)

現在のプロジェクトには以下のテストが実装されています。

### BigQueryClient テスト (`tests/converter/test_bigquery.py`)

- `test_bigquery_client_init`: BigQueryClientの初期化をテスト
- `test_list_views`: ビュー一覧の取得処理をテスト（モック使用）
- `test_get_view_definition`: ビュー定義の取得をテスト（モック使用）
- `test_get_view_schema`: ビュースキーマの取得をテスト（モック使用）
- `test_get_table_dependencies`: テーブル依存関係の取得をテスト（Lineage APIのモック使用）
- `test_get_table_dependencies_error_handling`: 依存関係取得時のエラー処理をテスト
- `TestBigQueryIntegration`: 実際のBigQuery接続が必要な統合テスト（デフォルトではスキップ）
  - `test_real_connection`: 実際のBigQuery接続をテスト
  - `test_real_list_views`: 実際のBigQueryからビュー一覧を取得するテスト

### コマンドテスト (`tests/commands/test_import_cmd.py`)

- `test_import_cmd_group`: インポートコマンドグループの動作をテスト
- `test_import_views_command_basic`: 基本的なビューインポートコマンドの動作をテスト（モック使用）
- `test_import_views_command_with_filters`: フィルター付きビューインポートコマンドの動作をテスト（モック使用）

### 依存関係テスト (`tests/converter/test_dependency.py`)

- `test_dependency_resolver_init`: DependencyResolverの初期化をテスト
- `test_build_dependency_graph`: 依存関係グラフ構築をテスト
- `test_get_topological_order`: トポロジカルソートをテスト
- `test_get_topological_order_with_cycle`: 循環参照があるケースをテスト
- `test_get_dependent_views`: 依存ビュー取得をテスト
- `test_analyze_dependencies`: 依存関係解析機能をテスト
- `test_analyze_dependencies_with_error`: エラー発生時の依存関係解析挙動をテスト
- `test_display_dependencies`: 依存関係表示をテスト
- `test_build_dependency_tree`: 依存関係ツリー構築をテスト

### ジェネレーターテスト (`tests/converter/test_generator.py`)

- dbtモデル生成に関するテスト

## 最近修正したテスト (2024/03/11)

### 1. `MockRow` クラスの改善

問題点:
- BigQueryの結果行は辞書のようにアクセスできるが、モックではそれをサポートしていなかった
- `rows[0]["view_definition"]` のような参照でエラーが発生していた

修正内容:
```python
class MockRow:
    def __init__(self, view_definition):
        self.view_definition = view_definition
        
    def __getitem__(self, key):
        # 辞書アクセスをサポート
        return getattr(self, key)
```

### 2. `test_get_view_definition` の修正

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

### 3. `get_table_dependencies` のエラー処理改善

問題点:
- 依存関係取得でエラーが発生した場合に例外を投げるようになっていたが、テストでは空リストを返すことを期待していた

修正内容:
```python
except Exception as e:
    logger.error(f"依存関係の取得に失敗しました: {fully_qualified_name} - {e}")
    # エラーが発生した場合は空のリストを返す
    return []
```

### 4. 依存関係解析機能のテスト追加

依存関係解析機能の実装に伴い、以下のテストを新たに追加：

- `test_analyze_dependencies`: 指定したデータセットのビューの依存関係を分析するテスト
- `test_analyze_dependencies_with_error`: 依存関係解析中にエラーが発生した場合の挙動をテスト
- `test_display_dependencies_new`: 新しい依存関係表示メソッドをテスト
- `test_build_dependency_tree`: 依存関係ツリーの構築をテスト

### 5. 型チェック改善

- py.typed ファイルの追加による内部モジュールの型チェックサポート
- ビルド設定の更新（py.typed ファイルの含め方）

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