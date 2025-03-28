# bq2dbt プロジェクト実装メモ

## プロジェクト概要
- BigQueryのビューをdbtモデルに自動変換するCLIツール
- 主要機能：ビュー検出、依存関係分析、モデル生成、参照変換、インタラクティブ確認
- 技術スタック：Python 3.9+、BigQuery API、Rich UI、Jinja2テンプレート

## 現状分析 (2024/03/13 更新)
- 基本的なプロジェクト構造が整備された
- jinjaテンプレートのwhite space制御についてhumanが修正を行った (commit: fbe87252)
- コア機能の一部が実装完了
  - BigQueryクライアント（ビュー一覧取得、定義取得、スキーマ取得）
  - モデル生成の基本部分（テンプレート適用）
  - **依存関係解析機能** (`google-cloud-datacatalog-lineage`を使用して実装完了)
  - **インタラクティブモード** (改善実装完了 - 2024/03/13)
  - エラー処理の一貫性を改善（エラー時に空リストを返すように変更）
- 重要なコア機能でまだ未実装のもの
  - **ref()参照変換機能** (ユーザーからの指示があるまで凍結 - 2024/03/13)
  - **最終変換結果のログ出力機能** (display_conversion_resultsの出力)
  - **naming-presetの微修正** (名称変更とデフォルト変更)
  - **ビューフィルタリングのタイミング変更** (依存関係探索後に適用するよう修正)
  - **コンソール出力とREADMEの英語化**
- CLIインターフェースの基本実装完了
  - インポートコマンドの引数処理
  - プログレス表示
  - ビューフィルタリング
  - インタラクティブ確認機能
- テストの品質向上の取り組み
  - BigQueryClientのモックテストを実際の実装と整合させるよう修正
  - MockRowクラスに__getitem__メソッドを追加して辞書アクセスをサポート
  - 依存関係解析機能のテスト実装
  - py.typedファイルの追加による型チェックの改善
  - テストの統合と簡素化（重複テストの削除、テスト関数の統合）
- モジュール構造の改善
  - ビジネスロジックとCLIインターフェースの分離
  - 単一責任の原則に基づいたモジュール設計
  - 依存関係の明確化と一方向の依存関係の確立
  - BigQueryClientとLineageClientの分離
- 新たに発見された課題
  - ログが保存・出力されない問題
  - ログコマンドの挙動が期待通りでない
  - リリース準備のためのバージョニングとパッケージング戦略が未整備
  - ビューフィルタリングのタイミングが適切でない（依存関係探索前に適用されている）
  - コンソール出力とREADMEが日本語で書かれており、国際的なユーザーにとって理解が難しい

## 必要な機能コンポーネント
1. BigQuery クライアント ✅
   - ✅ データセットからビュー一覧取得
   - ✅ ビュー定義の取得
   - ✅ テーブルスキーマの取得
   - ✅ Lineage API接続（google-cloud-datacatalog-lineageを使用）

2. 依存関係リゾルバー ✅
   - ✅ ビュー間の依存関係解析（`google-cloud-datacatalog-lineage`を使用）
   - ✅ 指定されたdataset_idのビューのみ抽出するフィルタリング
   - ✅ 取得したビュー一覧と依存ビューの統合
   - ✅ Richを使用した依存関係の視覚的表示
   - ⚠️ ビューフィルタリングのタイミング変更（依存関係探索後に適用するよう修正）

3. モデルジェネレーター ⚠️
   - ✅ ビュー定義からdbtモデル生成 (テンプレート適用のみ)
   - ❄️ 参照をref()関数に変換 (凍結中 - ユーザーからの指示があるまで延期)
   - ✅ テンプレート適用

4. CLIインターフェース ⚠️
   - ✅ コマンドライン引数処理
   - ✅ インタラクティブプロンプト (humanフィードバックに基づいて改善実装完了)
   - ✅ 進捗表示
   - ✅ モジュール構造の改善（ビジネスロジックとCLIの分離）
   - ⚠️ ログ表示コマンド (未実装)
   - ⚠️ 最終変換結果のログ出力機能 (未実装)
   - ⚠️ コンソール出力の英語化 (未実装)

5. ユーティリティ ⚠️
   - ⚠️ ロギング (部分的に実装、保存・出力の問題あり)
   - ⚠️ 名前付け規則 (微修正が必要)
   - ✅ エラーハンドリング (一貫性のある例外処理を実装)

6. パッケージング・リリース ⚠️
   - ⚠️ hatchを使ったバージョニング (未実装)
   - ⚠️ ビルドとPyPIへのアップロード戦略 (未実装)
   - ⚠️ リリースプロセスの確立 (未実装)

7. ドキュメント ⚠️
   - ⚠️ READMEの英語化 (未実装)
   - ⚠️ インストール手順 (未実装)
   - ⚠️ 使用方法と例 (未実装)

## 今後のフォーカスエリア

### 短期目標 (2024/03/13更新・1-2週間)
1. ✅ 依存関係解析機能の実装
   - ✅ `google-cloud-datacatalog-lineage`ライブラリの追加
   - ✅ BigQuery Data Catalog Lineage APIを使用した依存関係取得
   - ✅ 指定されたdataset_idのビューのみ抽出するフィルタリング
   - ✅ 取得したビュー一覧と依存ビューの統合
   - ✅ Richを使用した依存関係の視覚的表示

2. ✅ インタラクティブモードの改善
   - ✅ 入力受付中にビュー依存関係分析の表示が出ないように修正
   - ✅ 質問にどのビューに対するものかを明示するよう改善
   - ✅ SQLとYAMLを分けず、ビューごとにインポートするかを一括で質問するよう変更
   - ✅ 既存ファイルの上書き確認と、拒否時の処理スキップを実装
   - ✅ 依存関係取得中にスピナーアニメーションを表示（Liveコンテキストを使用）
   - ✅ Rich TUIを使用したインタラクティブプロンプト
   - ✅ 依存関係のあるビューの確認UI
   - ✅ ファイル上書き確認の実装
   - ✅ 進捗表示の強化

3. ✅ モジュール構造の改善
   - ✅ ビジネスロジックとCLIインターフェースの分離
   - ✅ `commands/importer.py`の肥大化を解消
   - ✅ `converter/importer.py`にビジネスロジックを移動
   - ✅ `commands/import_views.py`にCLIコマンド定義を実装
   - ✅ 単一責任の原則に基づいたモジュール設計
   - ✅ 依存関係の明確化と一方向の依存関係の確立
   - ✅ テストの更新と改善

4. ✅ BigQueryClientとLineageClientの分離
   - ✅ LineageClientを別ファイルに移動
   - ✅ 単一責任の原則に基づいた設計
   - ✅ 依存関係の明確化
   - ✅ テストの更新

5. ⚠️ ビューフィルタリングのタイミング変更
   - ⚠️ `--include-views`と`--exclude-views`を依存関係探索後のview一覧を対象とするよう変更
   - ⚠️ フィルタリングロジックの移動
   - ⚠️ 関連するテストの更新

6. ⚠️ コンソール出力とREADMEの英語化
   - ⚠️ すべてのユーザー向けメッセージを英語に変換
   - ⚠️ READMEを英語で書き直し
   - ⚠️ ヘルプテキストとエラーメッセージの英語化
   - ⚠️ 関連するテストの更新

7. ⚠️ ログ機能の改善と修正
   - ⚠️ ログが保存・出力されない問題の解決
   - ⚠️ 最終変換結果のログ出力機能の追加
   - ⚠️ ログコマンドの実装

8. ⚠️ naming-presetの微修正
   - ⚠️ 名称の変更（humanからの具体案に基づく）
   - ⚠️ デフォルト値の変更（humanからの具体案に基づく）
   - ⚠️ 関連するテストの更新

9. ❄️ ref()参照変換機能の実装 (凍結中 - 2024/03/13)
   - ❄️ 参照パターン検出機能 (凍結中)
   - ❄️ dbt ref()関数への変換ロジック (凍結中)
   - ❄️ エッジケース対応 (凍結中)

10. ✅ テスト品質の向上
    - ✅ 依存関係解析機能のテスト実装
    - ✅ モック設計の最適化
    - ✅ 型チェックの改善（py.typedファイルの追加）
    - ✅ モジュール構造改善に伴うテストの更新
    - ✅ テストの統合と簡素化（重複テストの削除、テスト関数の統合）

### 中期目標 (2-4週間)
1. エラーハンドリングの強化
   - 例外処理の追加
   - ユーザーフレンドリーなエラーメッセージ

2. リリース準備とパッケージング
   - hatchを使ったバージョニングの追加
   - ビルドとPyPIへのアップロード戦略の策定
   - セマンティックバージョニングの適用
   - CHANGELOGの更新

3. ドキュメントの作成
   - インストール手順
   - 使用方法と例
   - コマンドリファレンス
   - アーキテクチャ図の更新

### 長期目標 (1-2ヶ月)
1. リリースプロセスの確立
   - パッケージングとリリースフロー
   - CHANGELOG管理
   - CIパイプラインとの統合

2. ユーザーフィードバック収集
   - テスターからのフィードバック収集体制
   - 課題追跡システムの設定

3. 拡張機能の検討
   - インクリメンタルモデル対応
   - 高度なテンプレートオプション

## テスト実装状況

現在のテスト実装状況:
- BigQueryClient テスト: 依存関係取得を含めた全機能のテスト完了
- LineageClient テスト: 基本機能のテスト完了
- 依存関係解析テスト: analyze_dependencies メソッドを含む完全なテスト実装
- モデル生成テスト: 基本機能のテスト完了、ref()変換機能のテストは凍結中
- コマンドテスト: 基本コマンドのテスト完了
- モジュール構造改善に伴うテスト更新: `test_importer.py`と`test_import_cmd.py`の更新完了
- テストの統合と簡素化: 重複テストの削除、テスト関数の統合

最近対応した課題:
- BigQueryClientのモックに__getitem__メソッドを追加し、辞書アクセスに対応
- get_table_dependencies メソッドのエラー処理を改善（エラー時に空リストを返す）
- 依存関係解析機能のテストを実装
- 型チェック改善のためにpy.typedファイルを追加
- モジュール構造改善に伴うテストの更新と修正
- `BigQueryClient`の初期化時に`location`パラメータをキーワード引数として渡すように修正
- テストの統合と簡素化（重複テストの削除、テスト関数の統合）

今後のテスト改善点:
- ビューフィルタリングのタイミング変更に伴うテスト更新
- コンソール出力の英語化に伴うテスト更新
- naming-presetの微修正に伴うテスト更新
- ログ機能の改善に伴うテスト実装
- hatchを使ったバージョニングのテスト
- コードカバレッジの計測と向上
- より複雑なユースケースのテスト追加

## 開発メモ

### 人間からのフィードバック (2024/03/13)
- インタラクティブモードの改善要望を反映（ビュー名表示、一括確認、上書き確認など）
- 依存関係取得中のUI改善（スピナーアニメーションの追加）
- ref()参照変換機能の実装は一時凍結（ユーザーからの指示があるまで）
- 最終変換結果のログ出力機能の追加要望
- naming-presetの微修正要望（名称変更とデフォルト変更）
- ログ機能の問題（保存・出力されない）の指摘
- hatchを使ったバージョニングの追加要望
- ビルドとPyPIへのアップロード戦略の策定要望
- ビューフィルタリングのタイミング変更要望（依存関係探索後に適用するよう修正）
- コンソール出力とREADMEの英語化要望

### 実装した依存関係解析機能の概要
- LineageClientの`get_table_dependencies`メソッドをGoogle Cloud Datacatalog Lineage APIを使用して実装
- DependencyResolverクラスに`analyze_dependencies`メソッドを追加
- 指定されたデータセット内のビューのみをフィルタリングする機能
- 依存関係の視覚的表示機能（Richライブラリを使用）
- エラー処理の強化（例外発生時に空リストを返すよう変更）

### インタラクティブモード改善の実装内容 (2024/03/13)
- 依存関係分析の前にユーザー確認を取るよう順序を変更
- Richの`Live`コンテキストを使用して依存関係分析中のアニメーション表示を実装
- ステータスコールバック関数を追加し、処理中のビュー名と進捗を表示
- ビューごとのインポート確認を実装（ビュー名を明示）
- 既存ファイルのチェックと上書き確認機能の追加
- ユーザーがスキップしたビューの一覧表示機能を追加

### モジュール構造改善の実装内容 (2024/03/13)
- `commands/importer.py`から`converter/importer.py`にビジネスロジックを移動
- `commands/import_views.py`にCLIコマンド定義を実装
- `commands/importer.py`をコマンドグループの定義のみに簡素化
- 大きな`import_views`関数を複数の小さな関数に分割
  - `initialize_bigquery_client`: BigQueryクライアントの初期化
  - `initialize_model_generator`: モデルジェネレーターの初期化
  - `setup_output_directory`: 出力ディレクトリの設定
  - `fetch_views`: ビュー一覧の取得
  - `display_views_table`: ビュー一覧の表示
  - `analyze_dependencies`: 依存関係の分析
  - `display_added_views`: 追加されたビューの表示
  - `display_ordered_views`: 順序付けられたビューの表示
  - `check_file_exists`: ファイルの存在確認
  - `confirm_view_import`: ビューのインポート確認
  - `convert_view`: ビューの変換
  - `display_conversion_results`: 変換結果の表示
- `BigQueryClient`の初期化時に`location`パラメータをキーワード引数として渡すように修正
- `ModelGenerator`の初期化方法を改善し、出力ディレクトリを後から設定できるように変更
- テストの更新と改善
  - `test_importer.py`を更新して、ビジネスロジックの関数をテスト
  - `test_import_cmd.py`を更新して、CLIコマンドをテスト
  - モックの設定を適切に更新

### BigQueryClientとLineageClientの分離 (2024/03/13)
- `BigQueryClient`と`LineageClient`を別々のファイルに分離
- 単一責任の原則に基づいた設計
  - `BigQueryClient`: BigQueryのテーブル/ビュー操作のみに責任
  - `LineageClient`: Data Catalog Lineage APIとの通信のみに責任
- 依存関係の明確化
  - `DependencyResolver`が`LineageClient`を明示的に使用
- テストの更新
  - `test_bigquery.py`と`test_dependency.py`を更新

### 新規タスク詳細 (2024/03/13 追加)

#### 1. ビューフィルタリングのタイミング変更
- 現在、`--include-views`と`--exclude-views`オプションは入力データセット内のビューに対してのみ適用されている
- 依存関係のあるビューが別のデータセットにある場合、これらのフィルタが適用されない
- フィルタリングロジックを依存関係解析後に移動する必要がある
- `converter/importer.py`の`analyze_dependencies`関数の後にフィルタリングを適用するよう変更

#### 2. コンソール出力とREADMEの英語化
- 現在、コンソール出力やREADMEが日本語で書かれており、国際的なユーザーにとって理解が難しい
- すべてのユーザー向けメッセージを英語に変換する必要がある
- READMEを英語で書き直す必要がある
- コマンドのヘルプテキストを英語に更新する必要がある

#### 3. 最終変換結果のログ出力機能
- 現在、変換処理の最終結果（display_conversion_results）がログに出力されていない
- `converter/importer.py`の`display_conversion_results`関数を修正し、ログ出力を追加する
- 変換されたモデル数、スキップされたモデル数、エラーが発生したモデル数などの統計情報をログに記録
- 変換されたファイルのパスリストをログに記録

#### 4. naming-presetの微修正
- 現在のnaming-presetの名称が直感的でない
- デフォルト値が最適でない
- humanからの具体的な指示に基づいて名称を変更
- デフォルト値を変更
- 関連するテストを更新

#### 5. ログコマンドの問題解決
- ログが正しく保存されない、または出力されない
- ロギング設定の見直し（ログファイルのパス、ログレベル、フォーマット）
- ログハンドラーの実装確認（ファイルハンドラー、コンソールハンドラー）
- ログコマンドの実装（最近のログ表示、フィルタリング機能）

#### 6. hatchを使ったバージョニング
- 現在、バージョン管理の仕組みが整備されていない
- hatchのインストールと設定（pyproject.tomlにhatch設定を追加）
- バージョン管理スキームの設定（セマンティックバージョニング）
- バージョン更新コマンドの整備
- CIパイプラインとの統合

#### 7. ビルドとPyPIへのアップロード戦略
- パッケージのビルドプロセスが確立されていない
- ビルド設定の整備（pyproject.tomlのビルド設定、メタデータ）
- ビルドプロセスの自動化（ビルドスクリプト、CI）
- PyPIアップロードの自動化（テストPyPI、本番PyPI、APIトークン）
- リリースチェックリストの作成

### ref()参照変換機能の実装計画 (凍結中 - 2024/03/13)
- ユーザーからの指示があるまで実装を凍結
- 凍結解除後に実装予定の機能:
  - 参照パターン検出方法
  - 依存関係情報を活用した変換ルール
  - エッジケース対応 

## 2024-03-22: filter_views 関数の改修

### 改修内容
`filter_views` 関数をFQN (Fully Qualified Name) 全体に対するパターンマッチングに対応するよう改修しました。

#### 問題点
- 以前の実装では、view名部分のみのマッチングに対応していた
- 新しいテストケースではFQN全体 (`project.dataset.view`) に対するマッチングが必要
- 既存のテストと新テストの両方に対応する必要があった

#### 解決策
1. `filter_views` 関数を以下のように修正:
   - パターン内に `.` が含まれている場合はFQN全体 (`project.dataset.view`) に対してマッチング
   - パターン内に `.` が含まれていない場合はview部分のみに対してマッチング (後方互換性のため)

2. テストケースを整理:
   - 既存の `tests/commands/test_importer.py` から新しいFQNパターンマッチングテストを
   - 正しい場所である `tests/converter/test_importer.py` に統合

#### 改修後のコード
```python
def filter_views(
    views: List[str],
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[str]:
    """ビュー一覧に対してinclude/excludeパターンによるフィルタリングを適用します。"""
    # ... 既存コード ...
    
    filtered_views = []
    for view in views:
        parts = view.split(".")
        if len(parts) != 3:
            if logger:
                logger.warning(f"無効なビュー名形式: {view}")
            continue

        project, dataset, view_name = parts

        # 含めるパターンによるフィルタリング
        if include_patterns:
            include_match = False
            for pattern in include_patterns:
                # パターンに"."が含まれている場合はFQN全体に対してマッチング
                if "." in pattern:
                    # FQN全体に対してパターンマッチング
                    if _match_pattern(view, pattern):
                        include_match = True
                        break
                else:
                    # view部分のみに対してパターンマッチング（後方互換性のため）
                    if _match_pattern(view_name, pattern):
                        include_match = True
                        break
            # ... 以下同様 ...
```

#### ユースケース例
この改修により、以下のようなパターンマッチングが可能になりました:

1. 特定のデータセット内のすべてのビュー: `*.dataset1.*`
2. 特定のプロジェクト内のすべてのビュー: `project1.*.*`
3. 特定のパターンに一致するデータセット内のビュー: `*.sample_dataset_*.*`
4. 特定のプロジェクトとデータセット内の特定パターンのビュー: `project1.dataset1.view*`

これにより、ユーザーはより柔軟にビューをフィルタリングできるようになりました。 