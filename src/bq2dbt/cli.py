#!/usr/bin/env python
"""BigQuery View to dbt Model Converter CLI."""

import sys

import click
from rich.console import Console

from bq2dbt.commands.import_cmd import import_cmd
from bq2dbt.commands.logs_cmd import logs_cmd

# バージョン情報
__version__ = "0.1.0"

# コンソール設定
console = Console()


@click.group()
@click.version_option(version=__version__)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """BigQuery View to dbt Model Converter.

    This tool helps you import BigQuery views and convert them to dbt models.
    """
    # コンテキストオブジェクトにオプションを保存
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose


# サブコマンドの登録
cli.add_command(import_cmd)
cli.add_command(logs_cmd)


def main() -> int:
    """CLIエントリポイント。"""
    try:
        return cli(standalone_mode=False) or 0
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        return 1


if __name__ == "__main__":
    sys.exit(main())
