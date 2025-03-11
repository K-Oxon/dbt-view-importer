"""インポートコマンドモジュール。"""

import os
import traceback
from pathlib import Path
from typing import Optional, Tuple

import click
import jinja2
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.dependency import DependencyResolver
from bq2dbt.converter.generator import ModelGenerator
from bq2dbt.utils.logger import setup_logging
from bq2dbt.utils.naming import NamingPreset, generate_model_filename


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
    "--include-dependencies",
    is_flag=True,
    help="データセット外の依存ビューも含め、ビュー間の依存関係を分析する",
)
@click.option(
    "--location",
    default="asia-northeast1",
    help="Google Cloudのロケーション（デフォルト: asia-northeast1）",
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
    include_dependencies: bool,
    location: str,
    debug: bool,
) -> None:
    """BigQueryビューをdbtモデルにインポートします。

    Args:
        ctx: クリックコンテキスト
        project_id: BigQueryプロジェクトID
        dataset: インポート元のBigQueryデータセットID
        output_dir: dbtモデルの出力先ディレクトリ
        naming_preset: ファイル名の命名規則
        dry_run: 実際にファイルを生成せずにシミュレーションを実行するかどうか
        include_views: インポートするビューのパターン（カンマ区切り）
        exclude_views: インポートから除外するビューのパターン（カンマ区切り）
        non_interactive: インタラクティブモードを無効化するかどうか
        sql_template: SQLモデル用のカスタムテンプレートファイル
        yml_template: YAMLモデル用のカスタムテンプレートファイル
        include_dependencies: データセット外の依存ビューも含め、ビュー間の依存関係を分析するかどうか
        location: Google Cloudのロケーション
        debug: デバッグモードを有効化するかどうか
    """
    # ロガーの設定
    logger = setup_logging(verbose=debug)
    console = Console()

    logger.info(f"BigQueryビューのインポートを開始します: {project_id}.{dataset}")
    logger.info(f"出力先: {output_dir}")
    logger.info(f"命名規則: {naming_preset}")
    logger.info(f"インタラクティブモード: {'無効' if non_interactive else '有効'}")
    logger.info(f"ドライラン: {'有効' if dry_run else '無効'}")
    logger.info(f"デバッグモード: {'有効' if debug else '無効'}")
    logger.info(f"依存関係分析: {'有効' if include_dependencies else '無効'}")
    logger.info(f"ロケーション: {location}")

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
            f"出力ディレクトリ {output_dir} が存在しません。作成しますか？",
            default=True,
        ):
            output_path.mkdir(parents=True)
            logger.info(f"出力ディレクトリを作成しました: {output_dir}")
        else:
            logger.error(f"出力ディレクトリが存在しません: {output_dir}")
            return

    try:
        # BigQueryクライアントの初期化
        with console.status("[bold green]BigQueryに接続しています...") as status:
            bq_client = BigQueryClient(project_id, location=location)

        # ビュー一覧の取得
        with console.status(
            f"[bold green]データセット {dataset} からビュー一覧を取得しています..."
        ) as status:
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

        # モデルジェネレーターの初期化 (依存関係分析の前に初期化)
        with console.status(
            "[bold green]モデルジェネレーターを初期化しています..."
        ) as status:
            sql_template_path = Path(sql_template) if sql_template else None
            yml_template_path = Path(yml_template) if yml_template else None
            generator = ModelGenerator(
                output_dir=output_path,
                sql_template_path=sql_template_path,
                yml_template_path=yml_template_path,
            )

        # 依存関係の分析
        console.print("\n[bold]ビュー間の依存関係を分析します...[/bold]")

        # 依存関係分析を行わない場合は簡易的な処理
        if not include_dependencies:
            console.print("[yellow]依存関係分析をスキップします。[/yellow]")
            all_views = views
            ordered_views = views
        else:
            # シンプルなスピナーで依存関係分析の進行中を表示
            with console.status(
                "[bold blue]ビュー間の依存関係を分析しています...", spinner="dots"
            ) as status:
                # 依存関係リゾルバーの初期化
                resolver = DependencyResolver(bq_client)

                # 処理中のビューを表示するための関数
                def update_status(
                    current_view: str, processed: int, total: int
                ) -> None:
                    status.update(
                        f"[bold blue]ビュー間の依存関係を分析しています... ({processed}/{total})"
                    )

                # 新しい解析メソッドを使用して依存関係を分析（進捗表示機能付き）
                all_views, dependency_graph = resolver.analyze_dependencies(
                    views, dataset, status_callback=update_status
                )

                # 新しく追加されたビューがあるか確認
                added_views = [v for v in all_views if v not in views]

            # 依存関係の視覚的表示
            console.print("\n[bold]ビュー間の依存関係:[/bold]")
            resolver.display_dependencies(all_views, console=console)

            # 依存関係がある場合の追加ビューの確認
            if added_views:
                console.print(
                    f"[yellow]依存関係により{len(added_views)}個のビューが追加されました。[/yellow]"
                )

                # 追加されたビューの表示
                added_table = Table(title="依存関係により追加されたビュー")
                added_table.add_column("No.", style="cyan")
                added_table.add_column("ビュー名", style="yellow")

                for i, view in enumerate(added_views, 1):
                    added_table.add_row(str(i), view)

                console.print(added_table)

            try:
                # 変換順序の取得
                ordered_views = resolver.get_topological_order()
                logger.info(f"ビューの変換順序を決定しました: {len(ordered_views)}個")
            except ValueError as e:
                logger.warning(f"依存関係の分析中にエラーが発生しました: {e}")
                ordered_views = all_views  # エラーが発生した場合は元の順序を使用

            # ビューの変換順序を表示
            order_table = Table(title="ビューの変換順序")
            order_table.add_column("順序", style="cyan")
            order_table.add_column("ビュー名", style="green")

            for i, view in enumerate(ordered_views, 1):
                order_table.add_row(str(i), view)

            console.print(order_table)

        # ビューの変換
        console.print("\n[bold]ビューの変換を開始します...[/bold]")

        converted_models = []
        skipped_views = []

        # ファイル存在チェック用の関数
        def check_file_exists(
            view: str, naming_preset_enum: NamingPreset
        ) -> Tuple[bool, bool, Path, Path]:
            """ビューに対応するSQLとYAMLファイルが存在するかをチェックします。

            Args:
                view: ビュー名 (project.dataset.table形式)
                naming_preset_enum: 命名規則

            Returns:
                Tuple[bool, bool, Path, Path]: SQLファイル存在フラグ, YAMLファイル存在フラグ, SQLファイルパス, YAMLファイルパス
            """
            sql_file = generate_model_filename(
                view, naming_preset_enum, extension="sql"
            )
            yml_file = generate_model_filename(
                view, naming_preset_enum, extension="yml"
            )

            sql_path = output_path / sql_file
            yml_path = output_path / yml_file

            return sql_path.exists(), yml_path.exists(), sql_path, yml_path

        for view in ordered_views:
            # ビュー名を取得
            _, _, view_name = view.split(".")

            # インタラクティブモードでの確認 - ビューごとにインポートするか確認
            naming_preset_enum = NamingPreset(naming_preset)
            sql_exists, yml_exists, sql_path, yml_path = check_file_exists(
                view, naming_preset_enum
            )

            # ファイルの存在確認メッセージを表示
            files_exist = sql_exists or yml_exists
            overwrite_message = ""
            if files_exist:
                existing_files = []
                if sql_exists:
                    existing_files.append(f"SQL: {os.path.basename(sql_path)}")
                if yml_exists:
                    existing_files.append(f"YAML: {os.path.basename(yml_path)}")
                overwrite_message = (
                    f" [yellow](既存ファイル: {', '.join(existing_files)})[/yellow]"
                )

            # ビューのインポート確認
            import_this_view = True
            overwrite = True

            if not non_interactive:
                # 既存ファイルがある場合は上書き確認を一緒に行う
                if files_exist:
                    import_this_view = Confirm.ask(
                        f"ビュー [bold cyan]{view}[/bold cyan] をインポートしますか？{overwrite_message}",
                        default=True,
                    )

                    if import_this_view:
                        overwrite = Confirm.ask(
                            "既存のファイルを上書きしますか？", default=True
                        )
                        if not overwrite:
                            skipped_views.append((view, "上書き拒否"))
                            continue
                else:
                    import_this_view = Confirm.ask(
                        f"ビュー [bold cyan]{view}[/bold cyan] をインポートしますか？",
                        default=True,
                    )

                if not import_this_view:
                    skipped_views.append((view, "ユーザー選択"))
                    continue

            # 既存ファイルが有り、かつ上書きが許可されていない場合はスキップ
            if files_exist and not overwrite:
                skipped_views.append((view, "上書き拒否"))
                continue

            # 変換を行わない場合はスキップ
            if not import_this_view:
                skipped_views.append((view, "ユーザー選択"))
                continue

            try:
                # ビュー定義の取得
                sql_definition = bq_client.get_view_definition(view)

                # スキーマ情報の取得
                schema_fields = bq_client.get_view_schema(view)

                # SQLモデルとYAMLモデルを一括で生成
                try:
                    # SQLモデルの生成
                    sql_content, sql_path = generator.generate_sql_model(
                        view,
                        sql_definition,
                        naming_preset=naming_preset_enum,
                        dry_run=dry_run,
                    )

                    # YAMLモデルの生成
                    yml_content, yml_path = generator.generate_yaml_model(
                        view,
                        schema_fields,
                        naming_preset=naming_preset_enum,
                        dry_run=dry_run,
                    )

                    converted_models.append((view, sql_path, yml_path))

                except jinja2.exceptions.UndefinedError as e:
                    logger.error(f"テンプレート変数が未定義です: {str(e)}")
                    if debug:
                        logger.debug(
                            f"テンプレート内容: {generator._load_template(generator.sql_template_path).render()}"
                        )
                    raise
                except Exception as e:
                    logger.error(f"モデル生成中にエラーが発生しました: {str(e)}")
                    if debug:
                        logger.debug(f"例外の詳細: {traceback.format_exc()}")
                    raise

            except Exception as e:
                logger.error(f"ビュー {view} の変換中にエラーが発生しました: {str(e)}")
                if debug:
                    logger.debug(f"例外の詳細: {traceback.format_exc()}")
                skipped_views.append((view, f"エラー: {str(e)}"))

        # 変換結果の表示
        console.print(
            f"\n[bold green]変換完了！[/bold green] {len(converted_models)}個のビューを変換しました。"
        )

        if skipped_views:
            console.print(
                f"[bold yellow]{len(skipped_views)}個のビューがスキップされました。[/bold yellow]"
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

        # スキップされたビューの一覧表示
        if skipped_views:
            skip_table = Table(title="スキップされたビュー")
            skip_table.add_column("No.", style="cyan")
            skip_table.add_column("ビュー名", style="yellow")
            skip_table.add_column("理由", style="red")

            for i, (view, reason) in enumerate(skipped_views, 1):
                _, _, view_name = view.split(".")
                skip_table.add_row(
                    str(i),
                    view_name,
                    reason,
                )

            console.print(skip_table)

    except Exception as e:
        logger.error(f"インポート中にエラーが発生しました: {e}", exc_info=True)
        console.print(f"[bold red]エラー: {e}[/bold red]")
