#!/bin/bash
# テスト実行スクリプト

set -e

# カレントディレクトリをプロジェクトルートに設定
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

# テスト環境の設定
echo "テスト環境のセットアップ中..."
if [ -z "$BQ_PROJECT_ID" ]; then
  echo "注意: 環境変数 BQ_PROJECT_ID が設定されていません。モックテストのみ実行します。"
fi

if [ -z "$BQ_DATASET_ID" ]; then
  echo "注意: 環境変数 BQ_DATASET_ID が設定されていません。モックテストのみ実行します。"
fi

if [ "$1" == "--with-bq" ]; then
  echo "BigQuery 連携テストを有効化します"
  export ENABLE_BQ_TESTS=1
fi

# デバッグモードを有効化
export PYTHONPATH="$PYTHONPATH:$(pwd)"
export LOG_LEVEL=DEBUG

# テスト実行
echo "テストを実行中..."
if [ -n "$2" ]; then
  # 特定のテストを実行
  uv run pytest "$2" -v
else
  # すべてのテストを実行
  uv run pytest tests/ -v
fi

# 終了メッセージ
echo "テスト完了" 