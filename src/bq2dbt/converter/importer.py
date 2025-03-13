"""BigQueryビューをdbtモデルにインポートするビジネスロジック"""
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.dependency import DependencyResolver
from bq2dbt.converter.generator import ModelGenerator
from bq2dbt.utils.naming import NamingPreset


def initialize_bigquery_client(
    project_id: str, location: str, console: Console
) -> BigQueryClient:
    """BigQueryクライアントを初期化します。

    Args:
        project_id: BigQueryプロジェクトID
        location: BigQueryロケーション
        console: コンソールオブジェクト

    Returns:
        初期化されたBigQueryクライアント
    """
    with console.status("BigQueryクライアントを初期化中..."):
        bq_client = BigQueryClient(project_id, location=location)
    return bq_client


def initialize_model_generator(
    sql_template: Optional[str] = None, yml_template: Optional[str] = None
) -> ModelGenerator:
    """モデルジェネレーターを初期化します。

    Args:
        sql_template: SQLモデル用のテンプレートファイルパス
        yml_template: YAMLモデル用のテンプレートファイルパス

    Returns:
        初期化されたModelGeneratorオブジェクト
    """
    # 出力ディレクトリは後で設定するため、一時的に空のパスを指定
    output_dir = Path(".")
    sql_template_path = Path(sql_template) if sql_template else None
    yml_template_path = Path(yml_template) if yml_template else None
    return ModelGenerator(output_dir, sql_template_path, yml_template_path)


def setup_output_directory(output_path: Path, console: Console) -> None:
    """出力ディレクトリを設定します。

    Args:
        output_path: 出力ディレクトリのパス
        console: コンソールオブジェクト
    """
    if not output_path.exists():
        with console.status(f"出力ディレクトリを作成中: {output_path}"):
            output_path.mkdir(parents=True, exist_ok=True)
    else:
        console.print(f"出力ディレクトリが既に存在します: {output_path}")


def fetch_views(
    bq_client: BigQueryClient,
    dataset: str,
    include_patterns: Optional[List[str]],
    exclude_patterns: Optional[List[str]],
    console: Console,
    project_id: str,
) -> Optional[List[str]]:
    """BigQueryからビュー一覧を取得します。

    Args:
        bq_client: BigQueryクライアント
        dataset: データセットID
        include_patterns: 含めるビュー名のパターンリスト
        exclude_patterns: 除外するビュー名のパターンリスト
        console: コンソールオブジェクト
        project_id: プロジェクトID

    Returns:
        ビュー名のリスト、またはビューが見つからない場合はNone
    """
    with console.status(f"データセット '{dataset}' からビュー一覧を取得中..."):
        views = bq_client.list_views(
            dataset,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

    if not views:
        console.print(
            f"[bold red]エラー:[/] データセット '{dataset}' にビューが見つかりませんでした。"
        )
        return None

    return views


def display_views_table(views: List[str], console: Console) -> None:
    """ビュー一覧をテーブル形式で表示します。

    Args:
        views: ビュー名のリスト
        console: コンソールオブジェクト
    """
    table = Table(title=f"検出されたビュー ({len(views)})")
    table.add_column("ビュー名", style="cyan")

    for view in views:
        table.add_row(view)

    console.print(table)


def analyze_dependencies(
    views: List[str],
    dataset: str,
    include_dependencies: bool,
    bq_client: BigQueryClient,
    console: Console,
    logger: logging.Logger,
    max_depth: int = 3,
) -> Tuple[List[str], List[str]]:
    """ビュー間の依存関係を分析します。

    Args:
        views: ビュー名のリスト
        dataset: データセットID
        include_dependencies: 依存関係にあるビューも含めるかどうか
        bq_client: BigQueryクライアント
        console: コンソールオブジェクト
        logger: ロガーオブジェクト
        max_depth: 依存関係の最大深度

    Returns:
        (全てのビュー, 変換順序に並べられたビュー) のタプル
    """
    if not include_dependencies:
        console.print("依存関係の分析をスキップします。")
        return views, views

    console.print("ビュー間の依存関係を分析中...")
    resolver = DependencyResolver(bq_client)

    # 進捗状況を表示する関数
    def status_update(view_name: str, current: int, total: int) -> None:
        console.print(f"[{current}/{total}] {view_name} の依存関係を分析中...")

    try:
        # 依存関係の分析
        all_views, dependency_info = resolver.analyze_dependencies(
            views, dataset, max_depth=max_depth, status_callback=status_update
        )

        # 依存関係に基づいてビューの順序を決定
        ordered_views = resolver.get_topological_order()

        return all_views, ordered_views
    except Exception as e:
        logger.exception("依存関係の分析中にエラーが発生しました")
        console.print(
            f"[bold red]エラー:[/] 依存関係の分析中にエラーが発生しました: {e}"
        )
        return views, views


def display_added_views(added_views: List[str], console: Console) -> None:
    """依存関係により追加されたビューを表示します。

    Args:
        added_views: 追加されたビュー名のリスト
        console: コンソールオブジェクト
    """
    if not added_views:
        return

    table = Table(title=f"依存関係により追加されたビュー ({len(added_views)})")
    table.add_column("ビュー名", style="green")

    for view in added_views:
        table.add_row(view)

    console.print(table)


def display_ordered_views(ordered_views: List[str], console: Console) -> None:
    """変換順序に並べられたビューを表示します。

    Args:
        ordered_views: 順序付けられたビュー名のリスト
        console: コンソールオブジェクト
    """
    table = Table(title=f"変換順序 ({len(ordered_views)})")
    table.add_column("順序", style="dim")
    table.add_column("ビュー名", style="cyan")

    for i, view in enumerate(ordered_views, 1):
        table.add_row(str(i), view)

    console.print(table)


def check_file_exists(
    view: str, naming_preset: NamingPreset, output_path: Path
) -> Tuple[bool, bool, Path, Path]:
    """ファイルが既に存在するかどうかを確認します。

    Args:
        view: ビュー名
        naming_preset: 命名規則プリセット
        output_path: 出力ディレクトリのパス

    Returns:
        (SQLファイルが存在するか, YAMLファイルが存在するか, SQLファイルパス, YAMLファイルパス) のタプル
    """
    # ビュー名からファイル名を生成
    parts = view.split(".")
    if len(parts) != 3:
        raise ValueError(f"無効なビュー名: {view}")

    project_id, dataset, view_name = parts

    # 命名規則に基づいてファイル名を生成
    if naming_preset == NamingPreset.DATASET_PREFIX:
        base_name = f"{dataset}__{view_name}"
    elif naming_preset == NamingPreset.TABLE_ONLY:
        base_name = view_name
    else:  # NamingPreset.FULL
        base_name = f"{dataset}_{view_name}"

    sql_path = output_path / f"{base_name}.sql"
    yml_path = output_path / f"{base_name}.yml"

    return sql_path.exists(), yml_path.exists(), sql_path, yml_path


def confirm_view_import(
    view: str,
    files_exist: bool,
    existing_files: List[str],
    non_interactive: bool,
) -> Tuple[bool, bool]:
    """ビューのインポートを確認します。

    Args:
        view: ビュー名
        files_exist: ファイルが既に存在するかどうか
        existing_files: 既存のファイル名のリスト
        non_interactive: 非インタラクティブモードかどうか

    Returns:
        (インポートするかどうか, 上書きするかどうか) のタプル
    """
    if not files_exist:
        return True, False

    if non_interactive:
        return True, True

    print(f"\nビュー '{view}' のファイルが既に存在します:")
    for file in existing_files:
        print(f"  - {file}")

    import_this_view = Confirm.ask(
        f"このビューをインポートしますか? {view}", default=True
    )
    if not import_this_view:
        return False, False

    overwrite = Confirm.ask("既存のファイルを上書きしますか?", default=True)
    return True, overwrite


def convert_view(
    view: str,
    bq_client: BigQueryClient,
    generator: ModelGenerator,
    naming_preset_enum: NamingPreset,
    dry_run: bool,
    debug: bool,
    logger: logging.Logger,
) -> Tuple[str, Path, Path]:
    """ビューをdbtモデルに変換します。

    Args:
        view: ビュー名
        bq_client: BigQueryクライアント
        generator: モデルジェネレーター
        naming_preset_enum: 命名規則プリセット
        dry_run: ドライランモードかどうか
        debug: デバッグモードかどうか
        logger: ロガーオブジェクト

    Returns:
        (ビュー名, SQLファイルパス, YAMLファイルパス) のタプル

    Raises:
        RuntimeError: 変換中にエラーが発生した場合
    """
    try:
        # テーブルタイプを確認
        table_type = bq_client.get_table_type(view)
        if table_type != "VIEW":
            # ビューでない場合は専用の例外を発生させる
            if not table_type:
                raise ValueError(f"オブジェクトが存在しません: {view}")
            else:
                raise ValueError(
                    f"オブジェクトはビューではありません (タイプ: {table_type}): {view}"
                )

        # ビュー定義を取得
        view_definition = bq_client.get_view_definition(view)
        if debug:
            logger.debug(f"ビュー定義: {view_definition}")

        # ビューのスキーマを取得
        schema = bq_client.get_view_schema(view)
        if debug:
            logger.debug(f"ビュースキーマ: {schema}")

        # SQLモデルを生成
        sql_content, sql_path = generator.generate_sql_model(
            view, view_definition, naming_preset_enum, dry_run
        )

        # YAMLモデルを生成
        yml_content, yml_path = generator.generate_yaml_model(
            view, schema, "", naming_preset_enum, dry_run
        )

        return view, sql_path, yml_path
    except Exception as e:
        logger.exception(f"ビュー '{view}' の変換中にエラーが発生しました")
        raise RuntimeError(
            f"ビュー '{view}' の変換中にエラーが発生しました: {e}"
        ) from e


def display_conversion_results(
    converted_models: List[Tuple[str, Path, Path]],
    skipped_views: Dict[str, str],
    dry_run: bool,
    console: Console,
) -> None:
    """変換結果を表示します。

    Args:
        converted_models: 変換されたモデルのリスト (ビュー名, SQLパス, YAMLパス)
        skipped_views: スキップされたビューと理由の辞書
        dry_run: ドライランモードかどうか
        console: コンソールオブジェクト
    """
    if dry_run:
        console.print(
            "\n[bold yellow]ドライランモード:[/] ファイルは作成されていません"
        )

    console.print(
        f"\n[bold green]変換完了:[/] {len(converted_models)} モデルが変換されました"
        + ("（ドライラン）" if dry_run else "")
    )

    if converted_models:
        table = Table(title="変換されたモデル")
        table.add_column("ビュー名", style="cyan")
        table.add_column("SQLファイル", style="green")
        table.add_column("YAMLファイル", style="green")

        for view, sql_path, yml_path in converted_models:
            table.add_row(view, str(sql_path), str(yml_path))

        console.print(table)

    if skipped_views:
        table = Table(title="スキップされたビュー")
        table.add_column("ビュー名", style="cyan")
        table.add_column("理由", style="yellow")

        for view, reason in skipped_views.items():
            table.add_row(view, reason)

        console.print(table)


def import_views(
    project_id: str,
    dataset: str,
    output_dir: Path,
    naming_preset: str,
    dry_run: bool = False,
    include_views: Optional[List[str]] = None,
    exclude_views: Optional[List[str]] = None,
    non_interactive: bool = False,
    sql_template: Optional[str] = None,
    yml_template: Optional[str] = None,
    include_dependencies: bool = False,
    location: str = "asia-northeast1",
    debug: bool = False,
    max_depth: int = 3,
) -> None:
    """BigQueryビューをdbtモデルにインポートします。

    Args:
        project_id: BigQueryプロジェクトID
        dataset: データセットID
        output_dir: 出力ディレクトリのパス
        naming_preset: 命名規則プリセット
        dry_run: ドライランモードかどうか
        include_views: 含めるビュー名のパターンリスト
        exclude_views: 除外するビュー名のパターンリスト
        non_interactive: 非インタラクティブモードかどうか
        sql_template: SQLモデル用のテンプレートファイルパス
        yml_template: YAMLモデル用のテンプレートファイルパス
        include_dependencies: 依存関係にあるビューも含めるかどうか
        location: BigQueryロケーション
        debug: デバッグモードかどうか
        max_depth: 依存関係の最大深度
    """
    # ロギングの設定
    logger = logging.getLogger("bq2dbt")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # コンソールの設定
    console = Console(highlight=False)

    # インポート情報をログに記録
    options = {
        "project": project_id,
        "dataset": dataset,
        "output_dir": str(output_dir),
        "naming_preset": naming_preset,
        "dry_run": dry_run,
        "non_interactive": non_interactive,
        "include_dependencies": include_dependencies,
        "debug": debug,
    }
    logger.debug(f"インポートオプション: {options}")

    # フィルターパターンをログに記録
    if include_views:
        logger.debug(f"含めるビュー名パターン: {include_views}")
    if exclude_views:
        logger.debug(f"除外するビュー名パターン: {exclude_views}")

    # 出力ディレクトリの設定
    setup_output_directory(output_dir, console)

    # BigQueryクライアントの初期化
    bq_client = initialize_bigquery_client(project_id, location, console)

    # ビュー一覧の取得
    views = fetch_views(
        bq_client, dataset, include_views, exclude_views, console, project_id
    )
    if not views:
        return

    # ビュー一覧の表示
    display_views_table(views, console)

    # モデルジェネレーターの初期化
    generator = initialize_model_generator(sql_template, yml_template)
    # 出力ディレクトリを設定
    generator.output_dir = output_dir

    # 依存関係の分析
    all_views, ordered_views = analyze_dependencies(
        views, dataset, include_dependencies, bq_client, console, logger, max_depth
    )

    # 依存関係により追加されたビューの表示
    added_views = [view for view in all_views if view not in views]
    if added_views:
        display_added_views(added_views, console)

    # 変換順序の表示
    display_ordered_views(ordered_views, console)

    # 命名規則プリセットの設定
    naming_preset_enum = NamingPreset(naming_preset)

    # ビューの変換
    converted_models = []
    skipped_views = {}

    for view in ordered_views:
        # ファイルの存在確認
        sql_exists, yml_exists, sql_path, yml_path = check_file_exists(
            view, naming_preset_enum, output_dir
        )

        # 既存ファイルの確認
        files_exist = sql_exists or yml_exists
        existing_files = []
        if sql_exists:
            existing_files.append(f"SQL: {sql_path.name}")
        if yml_exists:
            existing_files.append(f"YAML: {yml_path.name}")

        # インポートの確認
        import_this_view, overwrite = confirm_view_import(
            view, files_exist, existing_files, non_interactive
        )

        if not import_this_view:
            skipped_views[view] = "ユーザーによりスキップ"
            continue

        if files_exist and not overwrite:
            skipped_views[view] = "既存ファイルを上書きしない"
            continue

        try:
            # ビューの変換
            result = convert_view(
                view, bq_client, generator, naming_preset_enum, dry_run, debug, logger
            )
            converted_models.append(result)
        except RuntimeError as e:
            logger.error(f"ビュー '{view}' の変換中にエラーが発生しました: {e}")

            # エラーメッセージからテーブルタイプに関する情報を抽出
            error_msg = str(e)
            if "オブジェクトはビューではありません" in error_msg:
                # テーブルタイプに関するエラーの場合は簡潔なメッセージを表示
                table_type = (
                    error_msg.split("タイプ: ")[1].split(")")[0]
                    if "タイプ: " in error_msg
                    else "不明"
                )
                skipped_views[view] = f"ビューではありません (タイプ: {table_type})"
            elif "オブジェクトが存在しません" in error_msg:
                skipped_views[view] = "オブジェクトが存在しません"
            else:
                # その他のエラーの場合
                skipped_views[view] = f"エラー: {str(e)}"

    # 変換結果の表示
    display_conversion_results(converted_models, skipped_views, dry_run, console)
