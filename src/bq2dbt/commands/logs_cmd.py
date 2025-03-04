"""ログ表示コマンドモジュール。"""

import click
from rich.console import Console
from rich.table import Table

from bq2dbt.utils.logger import display_log_content, get_recent_logs


@click.group(name="logs")
def logs_cmd() -> None:
    """ログ関連のコマンド。

    変換ログの表示や管理を行います。
    """
    pass


@logs_cmd.command(name="list")
@click.option(
    "--limit", "-n", type=int, default=5, help="表示するログの数（デフォルト: 5）"
)
def list_logs(limit: int) -> None:
    """最近のログファイルの一覧を表示する。"""
    console = Console()
    log_files = get_recent_logs(limit=limit)

    if not log_files:
        console.print("[yellow]ログファイルが見つかりません。[/yellow]")
        return

    table = Table(title="最近のログファイル")
    table.add_column("No.", style="cyan")
    table.add_column("ファイル名", style="green")
    table.add_column("日時", style="magenta")

    for i, log_file in enumerate(log_files, 1):
        timestamp = log_file.stem  # ファイル名から拡張子を除いたもの
        table.add_row(str(i), log_file.name, timestamp.replace("_", " "))

    console.print(table)


@logs_cmd.command(name="show")
@click.option("--last", "-l", is_flag=True, help="最新のログファイルを表示する")
@click.option("--number", "-n", type=int, help="インデックス番号でログファイルを指定")
def show_log(last: bool, number: int) -> None:
    """ログファイルの内容を表示する。"""
    console = Console()
    log_files = get_recent_logs()

    if not log_files:
        console.print("[yellow]ログファイルが見つかりません。[/yellow]")
        return

    if last:
        # 最新のログファイルを表示
        display_log_content(log_files[0])
    elif number is not None:
        # 指定されたインデックスのログファイルを表示
        if 1 <= number <= len(log_files):
            display_log_content(log_files[number - 1])
        else:
            console.print(
                f"[bold red]エラー: 有効なログ番号を指定してください（1-{len(log_files)}）[/bold red]"
            )
    else:
        # オプションが指定されていない場合は最新のログを表示
        display_log_content(log_files[0])
