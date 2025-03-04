"""インポートコマンドモジュール。"""

from typing import Optional

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from bq2dbt.utils.logger import setup_logging


@click.group(name="import")
def import_cmd() -> None:
    """BigQueryビューをdbtモデルにインポートするコマンド。

    指定したデータセットからビューを検出し、dbtモデルに変換します。
    """
    pass


@import_cmd.command(name="views")
@click.option("--project-id", "-p", required=True, help="BigQueryプロジェクトID")
@click.option(
    "--dataset", "-d", required=True, help="インポート元のBigQueryデータセットID"
)
@click.option(
    "--output-dir",
    "-o",
    required=True,
    type=click.Path(file_okay=False),
    help="dbtモデルの出力先ディレクトリ",
)
@click.option(
    "--naming-preset",
    "-n",
    type=click.Choice(["dataset_prefix", "table_only", "full"]),
    default="dataset_prefix",
    help="ファイル名の命名規則",
)
@click.option(
    "--dry-run", is_flag=True, help="実際にファイルを生成せずにシミュレーションを実行"
)
@click.option("--include-views", help="インポートするビューのパターン（カンマ区切り）")
@click.option(
    "--exclude-views", help="インポートから除外するビューのパターン（カンマ区切り）"
)
@click.option("--non-interactive", is_flag=True, help="インタラクティブモードを無効化")
@click.pass_context
def import_views(
    ctx: click.Context,
    project_id: str,
    dataset: str,
    output_dir: str,
    naming_preset: str,
    dry_run: bool,
    include_views: Optional[str],
    exclude_views: Optional[str],
    non_interactive: bool,
) -> None:
    """BigQueryビューをdbtモデルにインポートする。"""
    # ロガーの設定
    verbose = ctx.obj.get("VERBOSE", False)
    logger = setup_logging(verbose=verbose)
    console = Console()

    # 現時点では、シンプルなメッセージを表示するだけ
    console.print("[bold green]BigQueryビューのインポートを開始します...[/bold green]")

    # 設定情報をログに出力
    logger.info(f"プロジェクトID: {project_id}")
    logger.info(f"データセット: {dataset}")
    logger.info(f"出力ディレクトリ: {output_dir}")
    logger.info(f"命名規則: {naming_preset}")

    if include_views:
        include_patterns = include_views.split(",")
        logger.info(f"インポート対象パターン: {include_patterns}")

    if exclude_views:
        exclude_patterns = exclude_views.split(",")
        logger.info(f"除外パターン: {exclude_patterns}")

    logger.info(f"インタラクティブモード: {'無効' if non_interactive else '有効'}")
    logger.info(f"ドライラン: {'有効' if dry_run else '無効'}")

    # この実装ではまだ実際の変換は行わず、将来実装予定のメッセージを表示
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold green]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task("実装中...", total=None)
        import time

        time.sleep(2)  # 進捗表示のデモ

    console.print("\n[bold yellow]注意: このコマンドは現在実装中です。[/bold yellow]")
    console.print(
        "将来のバージョンでBigQueryビューの検出と変換機能が実装される予定です。"
    )
