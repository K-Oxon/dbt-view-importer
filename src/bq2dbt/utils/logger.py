"""ロギングユーティリティモジュール。"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List

from rich.console import Console
from rich.logging import RichHandler

# ログファイルを保存するディレクトリ
LOG_DIR = Path.home() / ".bq2dbt" / "logs"

# コンソール出力用のリッチハンドラー
console = Console()


def setup_logging(verbose: bool = False) -> logging.Logger:
    """アプリケーションのロギング設定を行う。

    Args:
        verbose: 詳細なログ出力を有効にするかどうか

    Returns:
        設定済みのロガーオブジェクト
    """
    # ログディレクトリが存在しない場合は作成
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # ログファイル名を生成 (YYYY-MM-DD_HH-MM-SS.log)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = LOG_DIR / f"{timestamp}.log"

    # ルートロガーの設定
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # リッチハンドラーを設定 (コンソール出力用)
    console_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_time=False,
        show_path=False,
    )
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)

    # ファイルハンドラーを設定 (ファイル出力用)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)  # ファイルには常に詳細なログを出力

    # フォーマットを設定
    console_format = logging.Formatter("%(message)s")
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    # ハンドラーをロガーに追加
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # bq2dbt用のロガーを取得
    logger = logging.getLogger("bq2dbt")

    # ログファイルのパスをログに残す
    logger.debug(f"ログファイル: {log_file}")

    return logger


def get_recent_logs(limit: int = 5) -> List[Path]:
    """最近のログファイルを取得する。

    Args:
        limit: 取得するログファイルの数

    Returns:
        最近のログファイルのパスのリスト
    """
    if not LOG_DIR.exists():
        return []

    # ログファイルを作成日時の降順でソート
    log_files = sorted(
        LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True
    )

    return log_files[:limit]


def display_log_content(log_file: Path) -> None:
    """ログファイルの内容を表示する。

    Args:
        log_file: 表示するログファイルのパス
    """
    if not log_file.exists():
        console.print(f"[bold red]ログファイルが見つかりません: {log_file}[/bold red]")
        return

    console.print(f"[bold]ログファイル: {log_file.name}[/bold]")
    console.print("=" * 80)

    with open(log_file, "r", encoding="utf-8") as f:
        content = f.read()
        console.print(content)

    console.print("=" * 80)
