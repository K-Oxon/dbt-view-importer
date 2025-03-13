"""BigQueryビューをdbtモデルにインポートするコマンド"""
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

from bq2dbt.converter.importer import import_views as import_views_func
from bq2dbt.utils.naming import NamingPreset


@click.command(name="views")
@click.option(
    "--project-id",
    required=True,
    help="BigQueryプロジェクトID",
)
@click.option(
    "--dataset",
    required=True,
    help="インポート対象のBigQueryデータセット",
)
@click.option(
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False),
    help="dbtモデルの出力先ディレクトリ",
)
@click.option(
    "--naming-preset",
    type=click.Choice([p.value for p in NamingPreset]),
    default=NamingPreset.DATASET_PREFIX.value,
    help="モデル命名規則のプリセット",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="実際にファイルを作成せずに実行",
)
@click.option(
    "--include-views",
    help="インポート対象のビュー名パターン（カンマ区切り）",
)
@click.option(
    "--exclude-views",
    help="インポート対象から除外するビュー名パターン（カンマ区切り）",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="インタラクティブな確認をスキップ",
)
@click.option(
    "--sql-template",
    type=click.Path(exists=True, dir_okay=False),
    help="SQLモデル用のJinja2テンプレートファイル",
)
@click.option(
    "--yml-template",
    type=click.Path(exists=True, dir_okay=False),
    help="YAMLモデル用のJinja2テンプレートファイル",
)
@click.option(
    "--include-dependencies",
    is_flag=True,
    help="依存関係にあるビューも含めてインポート",
)
@click.option(
    "--location",
    default="asia-northeast1",
    help="BigQueryのロケーション",
)
@click.option(
    "--debug",
    is_flag=True,
    help="デバッグモードを有効化",
)
@click.option(
    "--max-depth",
    type=int,
    default=3,
    help="依存関係の最大深度（--include-dependencies使用時）",
)
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
    sql_template: Optional[str],
    yml_template: Optional[str],
    include_dependencies: bool,
    location: str,
    debug: bool,
    max_depth: int,
) -> None:
    """BigQueryビューをdbtモデルにインポートします。

    指定したBigQueryデータセット内のビューをdbtモデル（SQLとYAML）に変換します。
    """
    verbose = ctx.obj.get("VERBOSE", False)
    console = Console(highlight=False)

    # include_viewsとexclude_viewsをリストに変換
    include_patterns = include_views.split(",") if include_views else None
    exclude_patterns = exclude_views.split(",") if exclude_views else None

    # 出力ディレクトリをPathオブジェクトに変換
    output_path = Path(output_dir)

    # 実際のインポート処理を実行
    import_views_func(
        project_id=project_id,
        dataset=dataset,
        output_dir=output_path,
        naming_preset=naming_preset,
        dry_run=dry_run,
        include_views=include_patterns,
        exclude_views=exclude_patterns,
        non_interactive=non_interactive,
        sql_template=sql_template,
        yml_template=yml_template,
        include_dependencies=include_dependencies,
        location=location,
        debug=debug,
        max_depth=max_depth,
    )
