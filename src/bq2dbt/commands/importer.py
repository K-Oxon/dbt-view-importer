"""インポートコマンドモジュール。"""

import click

from bq2dbt.commands.import_views import import_views


@click.group(name="import")
def import_cmd() -> None:
    """BigQueryビューをdbtモデルにインポートするコマンド。

    指定したデータセットからビューを検出し、dbtモデルに変換します。
    """
    pass


# サブコマンドの登録
import_cmd.add_command(import_views)
