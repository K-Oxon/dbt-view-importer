"""インポートコマンドモジュール。"""

import os
import traceback
from pathlib import Path
from typing import Any, List, Optional, Tuple

import click
import jinja2
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.dependency import DependencyResolver
from bq2dbt.converter.generator import ModelGenerator
from bq2dbt.converter.lineage import LineageClient
from bq2dbt.utils.logger import setup_logging
from bq2dbt.utils.naming import NamingPreset, generate_model_filename


@click.group(name="import")
def import_cmd() -> None:
    """BigQueryビューをdbtモデルにインポートするコマンド。

    指定したデータセットからビューを検出し、dbtモデルに変換します。
    """
    pass


def initialize_bigquery_client(
    project_id: str, location: str, console: Console
) -> BigQueryClient:
    """BigQueryクライアントを初期化します。

    Args:
        project_id: BigQueryプロジェクトID
        location: Google Cloudのロケーション
        console: Richコンソールインスタンス

    Returns:
        BigQueryClient: 初期化されたBigQueryクライアント
    """
    with console.status("[bold blue]BigQueryクライアントを初期化しています..."):
        bq_client = BigQueryClient(project_id, location=location)
    return bq_client


def initialize_lineage_client(
    project_id: str, location: str, console: Console
) -> LineageClient:
    """Lineageクライアントを初期化します。

    Args:
        project_id: Google Cloudプロジェクト
        location: Google Cloudのロケーション
        console: Richコンソールインスタンス

    Returns:
        LineageClient: 初期化されたLineageクライアント
    """
    with console.status("[bold blue]Lineageクライアントを初期化しています..."):
        lineage_client = LineageClient(project_id, location)
    return lineage_client


def fetch_views(
    bq_client: BigQueryClient,
    dataset: str,
    include_patterns: Optional[List[str]],
    exclude_patterns: Optional[List[str]],
    console: Console,
    project_id: str,
) -> Optional[List[str]]:
    """データセットからビュー一覧を取得します。

    Args:
        bq_client: BigQueryクライアント
        dataset: データセットID
        include_patterns: 含めるビューのパターン
        exclude_patterns: 除外するビューのパターン
        console: Richコンソールインスタンス
        project_id: BigQueryプロジェクトID

    Returns:
        Optional[List[str]]: ビュー一覧、エラー時はNone
    """
    try:
        with console.status("[bold blue]ビュー一覧を取得しています..."):
            views = bq_client.list_views(
                dataset,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
            )

        if not views:
            console.print(
                f"[bold red]データセット {project_id}.{dataset} にビューが見つかりませんでした。[/bold red]"
            )
            return None

        return views

    except Exception as e:
        console.print(f"[bold red]ビュー一覧の取得に失敗しました: {e}[/bold red]")
        return None


def display_views_table(
    views: List[str], project_id: str, dataset: str, console: Console
) -> None:
    """ビュー一覧をテーブル形式で表示します。

    Args:
        views: ビュー一覧
        project_id: BigQueryプロジェクトID
        dataset: データセットID
        console: Richコンソールインスタンス
    """
    console.print(
        f"\n[bold]データセット {project_id}.{dataset} から{len(views)}個のビューを検出しました:[/bold]"
    )

    # ビュー一覧をテーブル形式で表示
    table = Table(title=f"検出されたビュー一覧 ({project_id}.{dataset})")
    table.add_column("No.", style="cyan")
    table.add_column("ビュー名", style="green")

    for i, view in enumerate(views, 1):
        table.add_row(str(i), view)

    console.print(table)


def initialize_model_generator(
    output_path: Path,
    sql_template: Optional[str],
    yml_template: Optional[str],
    console: Console,
) -> ModelGenerator:
    """モデルジェネレーターを初期化します。

    Args:
        output_path: 出力先ディレクトリ
        sql_template: SQLテンプレートファイルパス
        yml_template: YAMLテンプレートファイルパス
        console: Richコンソールインスタンス

    Returns:
        ModelGenerator: 初期化されたモデルジェネレーター
    """
    with console.status("[bold blue]モデルジェネレーターを初期化しています..."):
        # テンプレートパスをPathオブジェクトに変換
        sql_template_path = Path(sql_template) if sql_template else None
        yml_template_path = Path(yml_template) if yml_template else None

        # モデルジェネレーターを初期化
        generator = ModelGenerator(
            output_dir=output_path,
            sql_template_path=sql_template_path,
            yml_template_path=yml_template_path,
        )

    return generator


def setup_output_directory(
    output_dir: str, non_interactive: bool, logger: Any
) -> Optional[Path]:
    """出力ディレクトリを設定します。

    Args:
        output_dir: 出力先ディレクトリパス
        non_interactive: インタラクティブモードを無効化するかどうか
        logger: ロガーインスタンス

    Returns:
        Optional[Path]: 出力先ディレクトリのPathオブジェクト、キャンセル時はNone
    """
    output_path = Path(output_dir)

    # ディレクトリが存在しない場合は作成
    if not output_path.exists():
        try:
            output_path.mkdir(parents=True)
            logger.info(f"出力ディレクトリを作成しました: {output_path}")
        except Exception as e:
            logger.error(f"ディレクトリの作成に失敗しました: {e}")
            return None
    else:
        # 既存ディレクトリの場合は確認
        if not non_interactive and not Confirm.ask(
            f"出力先ディレクトリ '{output_path}' は既に存在します。続行しますか？"
        ):
            logger.info("ユーザーによりキャンセルされました")
            return None

    return output_path


def analyze_dependencies(
    views: List[str],
    dataset: str,
    include_dependencies: bool,
    bq_client: BigQueryClient,
    console: Console,
    logger: Any,
    max_depth: int = 3,
) -> Tuple[List[str], List[str]]:
    """ビュー間の依存関係を分析します。

    Args:
        views: ビュー一覧
        dataset: データセットID
        include_dependencies: 依存関係分析を行うかどうか
        bq_client: BigQueryクライアント
        console: Richコンソールインスタンス
        logger: ロガーインスタンス
        max_depth: 依存関係を追跡する最大深さ

    Returns:
        Tuple[List[str], List[str]]: 全ビュー一覧と変換順序のタプル
    """
    console.print("\n[bold]ビュー間の依存関係を分析します...[/bold]")

    # 依存関係分析を行わない場合は簡易的な処理
    if not include_dependencies:
        console.print("[yellow]依存関係分析をスキップします。[/yellow]")
        return views, views

    # シンプルなスピナーで依存関係分析の進行中を表示
    with console.status(
        "[bold blue]ビュー間の依存関係を分析しています...", spinner="dots"
    ) as status:
        # 依存関係リゾルバーの初期化
        resolver = DependencyResolver(bq_client)

        # 処理中のビューを表示するための関数
        def update_status(current_view: str, processed: int, total: int) -> None:
            status.update(
                f"[bold blue]ビュー間の依存関係を分析しています... ({processed}/{total})"
            )

        # 新しい解析メソッドを使用して依存関係を分析（進捗表示機能付き）
        all_views, dependency_graph = resolver.analyze_dependencies(
            views, dataset, max_depth=max_depth, status_callback=update_status
        )

        # 新しく追加されたビューがあるか確認
        added_views = [v for v in all_views if v not in views]

    # 依存関係の視覚的表示
    console.print("\n[bold]ビュー間の依存関係:[/bold]")
    resolver.display_dependencies(all_views, console=console)

    # 依存関係がある場合の追加ビューの確認
    if added_views:
        display_added_views(added_views, console)

    try:
        # 変換順序の取得
        ordered_views = resolver.get_topological_order()
        logger.info(f"ビューの変換順序を決定しました: {len(ordered_views)}個")
    except ValueError as e:
        logger.warning(f"依存関係の分析中にエラーが発生しました: {e}")
        ordered_views = all_views  # エラーが発生した場合は元の順序を使用

    # ビューの変換順序を表示
    display_ordered_views(ordered_views, console)

    return all_views, ordered_views


def display_added_views(added_views: List[str], console: Console) -> None:
    """依存関係により追加されたビューを表示します。

    Args:
        added_views: 追加されたビュー一覧
        console: Richコンソールインスタンス
    """
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


def display_ordered_views(ordered_views: List[str], console: Console) -> None:
    """変換順序を表示します。

    Args:
        ordered_views: 変換順序のリスト
        console: Richコンソールインスタンス
    """
    console.print("\n[bold]ビューの変換順序:[/bold]")

    # 変換順序の表示
    order_table = Table(title="ビューの変換順序")
    order_table.add_column("順序", style="cyan")
    order_table.add_column("ビュー名", style="green")

    for i, view in enumerate(ordered_views, 1):
        order_table.add_row(str(i), view)

    console.print(order_table)


def check_file_exists(
    view: str, naming_preset: NamingPreset, output_path: Path
) -> Tuple[bool, bool, str, str]:
    """ファイルが既に存在するかどうかを確認します。

    Args:
        view: ビュー名
        naming_preset: 命名規則
        output_path: 出力先ディレクトリ

    Returns:
        Tuple[bool, bool, str, str]: (SQLファイルが存在するか, YAMLファイルが存在するか, SQLファイルパス, YAMLファイルパス)
    """
    # ファイル名を生成
    sql_filename = generate_model_filename(view, naming_preset, extension="sql")
    yml_filename = generate_model_filename(view, naming_preset, extension="yml")

    # ファイルパスを生成
    sql_path = output_path / sql_filename
    yml_path = output_path / yml_filename

    # ファイルの存在確認
    sql_exists = sql_path.exists()
    yml_exists = yml_path.exists()

    return sql_exists, yml_exists, str(sql_path), str(yml_path)


def confirm_view_import(
    view: str, files_exist: bool, existing_files: List[str], non_interactive: bool
) -> Tuple[bool, bool]:
    """ビューのインポートを確認します。

    Args:
        view: ビュー名
        files_exist: ファイルが存在するかどうか
        existing_files: 既存ファイルのリスト
        non_interactive: インタラクティブモードを無効化するかどうか

    Returns:
        Tuple[bool, bool]: (インポートするかどうか, 上書きするかどうか)
    """
    # 非インタラクティブモードの場合は常にインポートして上書き
    if non_interactive:
        return True, True

    # ファイルが存在しない場合は確認なしでインポート
    if not files_exist:
        return True, False

    # ファイルが存在する場合は確認
    print(f"\nビュー: {view}")
    print(f"既存ファイル: {', '.join(existing_files)}")

    # インポートするかどうかを確認
    import_this_view = Confirm.ask("このビューをインポートしますか？", default=True)
    if not import_this_view:
        return False, False

    # 上書きするかどうかを確認
    overwrite = Confirm.ask("既存のファイルを上書きしますか？", default=True)
    return True, overwrite


def convert_view(
    view: str,
    bq_client: BigQueryClient,
    generator: ModelGenerator,
    naming_preset_enum: NamingPreset,
    dry_run: bool,
    debug: bool,
    logger: Any,
) -> Optional[Tuple[str, Path, Path]]:
    """ビューをdbtモデルに変換します。

    Args:
        view: ビュー名
        bq_client: BigQueryクライアント
        generator: モデルジェネレーター
        naming_preset_enum: 命名規則
        dry_run: ドライランモードかどうか
        debug: デバッグモードかどうか
        logger: ロガーインスタンス

    Returns:
        Optional[Tuple[str, Path, Path]]: 変換結果のタプル (ビュー名, SQLパス, YAMLパス)、エラー時はNone
    """
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

            return view, sql_path, yml_path

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
        return None


def display_conversion_results(
    converted_models: List[Tuple[str, Path, Path]],
    skipped_views: List[Tuple[str, str]],
    dry_run: bool,
    console: Console,
) -> None:
    """変換結果を表示します。

    Args:
        converted_models: 変換されたモデルのリスト
        skipped_views: スキップされたビューのリスト
        dry_run: ドライランモードかどうか
        console: Richコンソールインスタンス
    """
    # 変換結果のサマリーを表示
    console.print("\n[bold]変換結果:[/bold]")
    console.print(
        f"変換されたモデル: {len(converted_models)}個, スキップされたビュー: {len(skipped_views)}個"
    )

    # ドライランモードの場合は注意書きを表示
    if dry_run:
        console.print(
            "[yellow]注意: ドライランモードのため、実際にはファイルは生成されていません。[/yellow]"
        )

    # 変換されたモデルの表示
    if converted_models:
        converted_table = Table(title="変換されたモデル")
        converted_table.add_column("No.", style="cyan")
        converted_table.add_column("ビュー名", style="green")
        converted_table.add_column("SQLファイル", style="blue")
        converted_table.add_column("YAMLファイル", style="magenta")

        for i, (view, sql_path, yml_path) in enumerate(converted_models, 1):
            converted_table.add_row(
                str(i), view, os.path.basename(sql_path), os.path.basename(yml_path)
            )

        console.print(converted_table)

    # スキップされたビューの表示
    if skipped_views:
        skipped_table = Table(title="スキップされたビュー")
        skipped_table.add_column("No.", style="cyan")
        skipped_table.add_column("ビュー名", style="yellow")
        skipped_table.add_column("理由", style="red")

        for i, (view, reason) in enumerate(skipped_views, 1):
            skipped_table.add_row(str(i), view, reason)

        console.print(skipped_table)


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
    "--max-depth",
    type=click.IntRange(1, 7),
    default=3,
    help="依存関係を追跡する最大深さ（1-7、デフォルト: 3）",
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
    max_depth: int,
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
        max_depth: 依存関係を追跡する最大深さ
        location: Google Cloudのロケーション
        debug: デバッグモードを有効化するかどうか
    """
    # ロガーの設定
    logger = setup_logging(verbose=debug)
    console = Console()

    options = {
        "BigQueryビューのインポート": f"{project_id}.{dataset}",
        "出力先": output_dir,
        "命名規則": naming_preset,
        "インタラクティブモード": "無効" if non_interactive else "有効",
        "ドライラン": "有効" if dry_run else "無効",
        "デバッグモード": "有効" if debug else "無効",
        "依存関係分析": "有効" if include_dependencies else "無効",
        "依存関係の最大深さ": max_depth,
        "ロケーション": location,
    }
    for key, value in options.items():
        logger.info(f"{key}: {value}")

    # フィルターパターンの初期化
    include_patterns = include_views.split(",") if include_views else None
    exclude_patterns = exclude_views.split(",") if exclude_views else None

    if include_patterns:
        logger.info(f"インポート対象パターン: {include_patterns}")
    if exclude_patterns:
        logger.info(f"除外パターン: {exclude_patterns}")

    # 出力ディレクトリの確認（早期returnでネストを削減）
    output_path = setup_output_directory(output_dir, non_interactive, logger)
    if output_path is None:
        return

    try:
        # BigQueryクライアントの初期化
        bq_client = initialize_bigquery_client(project_id, location, console)

        # ビュー一覧の取得
        views = fetch_views(
            bq_client, dataset, include_patterns, exclude_patterns, console, project_id
        )
        if views is None:
            return

        # ビュー一覧の表示
        display_views_table(views, project_id, dataset, console)

        # モデルジェネレーターの初期化
        generator = initialize_model_generator(
            output_path, sql_template, yml_template, console
        )

        # 依存関係の分析
        all_views, ordered_views = analyze_dependencies(
            views, dataset, include_dependencies, bq_client, console, logger, max_depth
        )

        # ビューの変換
        console.print("\n[bold]ビューの変換を開始します...[/bold]")

        converted_models = []
        skipped_views = []
        naming_preset_enum = NamingPreset(naming_preset)

        for view in ordered_views:
            # ビュー名を取得
            _, _, view_name = view.split(".")

            # ファイルの存在確認
            sql_exists, yml_exists, sql_path, yml_path = check_file_exists(
                view, naming_preset_enum, output_path
            )

            # 既存ファイルの確認メッセージを準備
            files_exist = sql_exists or yml_exists
            existing_files = []
            if sql_exists:
                existing_files.append(f"SQL: {os.path.basename(sql_path)}")
            if yml_exists:
                existing_files.append(f"YAML: {os.path.basename(yml_path)}")

            # ビューのインポートと上書き確認
            import_this_view, overwrite = confirm_view_import(
                view, files_exist, existing_files, non_interactive
            )

            # 既存ファイルが有り、かつ上書きが許可されていない場合はスキップ
            if files_exist and not overwrite:
                skipped_views.append((view, "上書き拒否"))
                continue

            # 変換を行わない場合はスキップ
            if not import_this_view:
                skipped_views.append((view, "ユーザー選択"))
                continue

            # ビューの変換
            result = convert_view(
                view, bq_client, generator, naming_preset_enum, dry_run, debug, logger
            )

            if result:
                converted_models.append(result)
            else:
                skipped_views.append((view, "変換エラー"))

        # 変換結果の表示
        display_conversion_results(converted_models, skipped_views, dry_run, console)

    except Exception as e:
        logger.error(f"インポート中にエラーが発生しました: {e}", exc_info=True)
        console.print(f"[bold red]エラー: {e}[/bold red]")
