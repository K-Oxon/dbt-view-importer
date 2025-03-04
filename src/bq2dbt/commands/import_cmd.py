"""インポートコマンドモジュール。"""

import os
import traceback
from pathlib import Path
from typing import Optional

import click
import jinja2
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.table import Table

from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.dependency import DependencyResolver
from bq2dbt.converter.generator import ModelGenerator
from bq2dbt.utils.logger import setup_logging
from bq2dbt.utils.naming import NamingPreset


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
@click.option(
    "--sql-template",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="SQLモデル用のカスタムテンプレートファイル",
)
@click.option(
    "--yml-template",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="YAMLモデル用のカスタムテンプレートファイル",
)
@click.option(
    "--debug", is_flag=True, help="デバッグモードを有効化（詳細なエラー情報を表示）"
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
    debug: bool,
) -> None:
    """BigQueryビューをdbtモデルにインポートする。"""
    # ロガーの設定
    verbose = ctx.obj.get("VERBOSE", False)
    logger = setup_logging(verbose=verbose)
    console = Console()

    # インポート開始メッセージ
    console.print("[bold green]BigQueryビューのインポートを開始します...[/bold green]")

    # 設定情報をログに出力
    logger.info(f"プロジェクトID: {project_id}")
    logger.info(f"データセット: {dataset}")
    logger.info(f"出力ディレクトリ: {output_dir}")
    logger.info(f"命名規則: {naming_preset}")
    logger.info(f"インタラクティブモード: {'無効' if non_interactive else '有効'}")
    logger.info(f"ドライラン: {'有効' if dry_run else '無効'}")
    logger.info(f"デバッグモード: {'有効' if debug else '無効'}")

    # フィルターパターンの初期化
    include_patterns = None
    exclude_patterns = None

    if include_views:
        include_patterns = include_views.split(",")
        logger.info(f"インポート対象パターン: {include_patterns}")

    if exclude_views:
        exclude_patterns = exclude_views.split(",")
        logger.info(f"除外パターン: {exclude_patterns}")

    # 出力ディレクトリの確認
    output_path = Path(output_dir)
    if not output_path.exists():
        if not non_interactive and Confirm.ask(
            f"出力ディレクトリ {output_dir} が存在しません。作成しますか？"
        ):
            output_path.mkdir(parents=True)
            logger.info(f"出力ディレクトリを作成しました: {output_dir}")
        else:
            logger.error(f"出力ディレクトリが存在しません: {output_dir}")
            return

    try:
        # BigQueryクライアントの初期化
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("BigQueryに接続しています...", total=None)
            bq_client = BigQueryClient(project_id)

        # ビュー一覧の取得
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(
                f"データセット {dataset} からビュー一覧を取得しています...", total=None
            )
            views = bq_client.list_views(
                dataset,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
            )

        if not views:
            console.print(
                f"[bold yellow]データセット {project_id}.{dataset} にビューが見つかりませんでした。[/bold yellow]"
            )
            return

        console.print(f"[green]{len(views)}個のビューが見つかりました。[/green]")

        # ビュー一覧の表示
        table = Table(title=f"データセット {project_id}.{dataset} のビュー一覧")
        table.add_column("No.", style="cyan")
        table.add_column("ビュー名", style="green")

        for i, view in enumerate(views, 1):
            _, _, view_name = view.split(".")
            table.add_row(str(i), view_name)

        console.print(table)

        # 依存関係の分析
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(
                "ビュー間の依存関係を分析しています...", total=None
            )

            # 依存関係リゾルバーの初期化
            resolver = DependencyResolver(bq_client)

            # 依存関係グラフの構築
            resolver.build_dependency_graph(views)

            try:
                # 変換順序の取得
                ordered_views = resolver.get_topological_order()
                logger.info(f"ビューの変換順序を決定しました: {len(ordered_views)}個")
            except ValueError as e:
                logger.warning(f"依存関係の分析中にエラーが発生しました: {e}")
                ordered_views = views  # エラーが発生した場合は元の順序を使用

        # モデルジェネレーターの初期化
        sql_template_path = Path(sql_template) if sql_template else None
        yml_template_path = Path(yml_template) if yml_template else None

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(
                "モデルジェネレーターを初期化しています...", total=None
            )
            generator = ModelGenerator(
                output_dir=output_path,
                sql_template_path=sql_template_path,
                yml_template_path=yml_template_path,
            )

        # ビューの変換
        with Progress() as progress:
            task = progress.add_task(
                "ビューを変換しています...", total=len(ordered_views)
            )

            converted_models = []
            for view in ordered_views:
                # 進捗表示の更新
                _, _, view_name = view.split(".")
                progress.update(
                    task, advance=1, description=f"ビューを変換しています: {view_name}"
                )

                try:
                    # ビュー定義の取得
                    sql_definition = bq_client.get_view_definition(view)

                    # スキーマ情報の取得
                    schema_fields = bq_client.get_view_schema(view)

                    # SQLモデルの生成
                    naming_preset_enum = NamingPreset(naming_preset)
                    try:
                        sql_content, sql_path = generator.generate_sql_model(
                            view,
                            sql_definition,
                            naming_preset=naming_preset_enum,
                            dry_run=dry_run,
                        )
                    except jinja2.exceptions.UndefinedError as e:
                        logger.error(f"テンプレート変数が未定義です: {str(e)}")
                        if debug:
                            logger.debug(
                                f"SQL テンプレート内容: {generator._load_template(generator.sql_template_path).render()}"
                            )
                        raise
                    except Exception as e:
                        logger.error(f"SQLモデル生成中にエラーが発生しました: {str(e)}")
                        if debug:
                            logger.debug(f"例外の詳細: {traceback.format_exc()}")
                        raise

                    # YAMLモデルの生成
                    try:
                        yml_content, yml_path = generator.generate_yaml_model(
                            view,
                            schema_fields,
                            naming_preset=naming_preset_enum,
                            dry_run=dry_run,
                        )
                    except jinja2.exceptions.UndefinedError as e:
                        logger.error(f"テンプレート変数が未定義です: {str(e)}")
                        if debug:
                            logger.debug(
                                f"YAML テンプレート内容: {generator._load_template(generator.yml_template_path).render()}"
                            )
                        raise
                    except Exception as e:
                        logger.error(
                            f"YAMLモデル生成中にエラーが発生しました: {str(e)}"
                        )
                        if debug:
                            logger.debug(f"例外の詳細: {traceback.format_exc()}")
                        raise

                    converted_models.append((view, sql_path, yml_path))

                except Exception as e:
                    logger.error(
                        f"ビュー {view} の変換中にエラーが発生しました: {str(e)}"
                    )
                    if debug:
                        logger.debug(f"例外の詳細: {traceback.format_exc()}")

        # 変換結果の表示
        console.print(
            f"\n[bold green]変換完了！[/bold green] {len(converted_models)}個のビューを変換しました。"
        )

        if dry_run:
            console.print(
                "[bold yellow]注意: ドライランモードのため、実際にはファイルは生成されていません。[/bold yellow]"
            )

        # 変換されたモデルの一覧表示
        if converted_models:
            table = Table(title="変換されたモデル")
            table.add_column("No.", style="cyan")
            table.add_column("ビュー名", style="green")
            table.add_column("SQLモデル", style="blue")
            table.add_column("YAMLモデル", style="magenta")

            for i, (view, sql_path, yml_path) in enumerate(converted_models, 1):
                _, _, view_name = view.split(".")
                table.add_row(
                    str(i),
                    view_name,
                    os.path.basename(sql_path),
                    os.path.basename(yml_path),
                )

            console.print(table)

    except Exception as e:
        logger.error(f"インポート中にエラーが発生しました: {e}", exc_info=True)
        console.print(f"[bold red]エラー: {e}[/bold red]")
