# dbt-view-importer

BigQueryビューをdbtモデルに自動変換するツール。

## 概要

このツールは、指定したBigQueryデータセット内のビューを検出し、適切なdbtモデル（SQLファイルとYAMLファイル）に変換します。また、ビュー間の依存関係も解析し、変換順序を最適化します。

## インストール

### 前提条件

- Python 3.9以上
- uv（Pythonパッケージマネージャー）

### インストール手順

```bash
# リポジトリをクローン
git clone https://github.com/K-Oxon/dbt-view-importer.git
cd dbt-view-importer

# uvを使って開発モードでインストール
uv pip install -e .
```

## 使用方法

### コマンドの基本構造

```bash
uv run bq2dbt [コマンド] [オプション]
```

### 主要コマンド

#### ビューのインポート

```bash
uv run bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR>
```

#### ログの表示

```bash
# 最近のログ一覧を表示
uv run bq2dbt logs list

# 最新のログを表示
uv run bq2dbt logs show --last
```

### デバッグモード

より詳細な情報を表示するデバッグモードを使用することができます：

```bash
uv run bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR> \
  --debug
```

## 開発者向け情報

### テストの実行

```bash
# すべてのテストを実行
./scripts/run_tests.sh

# BigQuery連携テストも実行（環境変数の設定が必要）
export BQ_PROJECT_ID=your-project
export BQ_DATASET_ID=your_dataset
./scripts/run_tests.sh --with-bq

# 特定のテストを実行
./scripts/run_tests.sh "" tests/converter/test_generator.py
```

### デバッグ用インポート

```bash
# 環境変数の設定
export BQ_PROJECT_ID=your-project
export BQ_DATASET_ID=your_dataset

# デバッグモードでインポート実行
./scripts/debug_import.sh output_dir
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。
