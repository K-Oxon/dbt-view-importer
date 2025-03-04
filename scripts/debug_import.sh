#!/bin/bash
# デバッグ用のインポートスクリプト

set -e

# カレントディレクトリをプロジェクトルートに設定
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

# 出力ディレクトリ
OUTPUT_DIR=${1:-"debug_output"}

# デバッグ用の実行
export LOG_LEVEL=DEBUG
echo "デバッグモードでBigQueryビューをインポートします"
echo "出力ディレクトリ: $OUTPUT_DIR"

# ビューのインポートを実行
uv run bq2dbt import views \
  --project-id "$BQ_PROJECT_ID" \
  --dataset "$BQ_DATASET_ID" \
  --output-dir "$OUTPUT_DIR" \
  --debug

# 完了メッセージ
echo "デバッグ実行が完了しました"
echo "出力ディレクトリ: $OUTPUT_DIR" 