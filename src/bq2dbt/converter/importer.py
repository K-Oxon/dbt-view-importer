"""BigQueryビューをdbtモデルにインポートするビジネスロジック"""
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from bq2dbt.converter.bigquery import BigQueryClient
from bq2dbt.converter.dependency import DependencyResolver
from bq2dbt.converter.generator import ModelGenerator
from bq2dbt.utils.logger import setup_logging
from bq2dbt.utils.naming import NamingPreset, generate_model_name


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
    base_name = generate_model_name(view, naming_preset)

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
    yml_prefix: Optional[str] = None,
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
        yml_prefix: YAMLファイルの接頭辞（デフォルト: None）
                     e.g. "_" -> _model_name.yml
    Returns:
        (ビュー名, SQLファイルパス, YAMLファイルパス) のタプル

    Raises:
        RuntimeError: 変換中にエラーが発生した場合
        ValueError: テーブルタイプがVIEW以外、またはオブジェクトが存在しない場合
    """
    # テーブルタイプを確認
    table_type = bq_client.get_table_type(view)
    if table_type != "VIEW":
        # ビューでない場合はValueErrorを返す（呼び出し元でスキップ処理する）
        if not table_type:
            raise ValueError(f"オブジェクトが存在しません: {view}")
        else:
            raise ValueError(
                f"オブジェクトはビューではありません (タイプ: {table_type}): {view}"
            )

    try:
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
            view, schema, "", naming_preset_enum, dry_run, yml_prefix
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
    logger: logging.Logger,
) -> None:
    """変換結果を表示します。

    Args:
        converted_models: 変換されたモデルのリスト (ビュー名, SQLパス, YAMLパス)
        skipped_views: スキップされたビューと理由の辞書
        dry_run: ドライランモードかどうか
        console: コンソールオブジェクト
        logger: ロガーオブジェクト
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
            logger.info(f"  - {view}")
            logger.info(f"    SQL: {sql_path}")
            logger.info(f"    YAML: {yml_path}")

        console.print(table)

    if skipped_views:
        table = Table(title="スキップされたビュー")
        table.add_column("ビュー名", style="cyan")
        table.add_column("理由", style="yellow")

        for view, reason in skipped_views.items():
            table.add_row(view, reason)
            logger.info(f"スキップされたビュー: {view}: {reason}")
        console.print(table)


def _match_pattern(text: str, pattern: str) -> bool:
    """簡易的なパターンマッチングを行います。

    Args:
        text: マッチング対象のテキスト
        pattern: パターン（*をワイルドカードとして使用可能）

    Returns:
        マッチする場合はTrue、しない場合はFalse
    """
    # *をワイルドカードとして扱い、正規表現に変換
    regex_pattern = pattern.replace("*", ".*")
    return bool(re.match(f"^{regex_pattern}$", text))


def filter_views(
    views: List[str],
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[str]:
    """ビュー一覧に対してinclude/excludeパターンによるフィルタリングを適用します。

    Args:
        views: フィルタリング対象のビュー名のリスト
        include_patterns: 含めるビュー名のパターンリスト
        exclude_patterns: 除外するビュー名のパターンリスト
        logger: ロガーインスタンス

    Returns:
        フィルタリング後のビュー名のリスト
    """
    if not views:
        return []

    # パターンが指定されていない場合は全てのビューを返す
    if not include_patterns and not exclude_patterns:
        return views

    filtered_views = []
    for view in views:
        # ビュー名の形式を確認（project.dataset.viewの形式であることを確認）
        parts = view.split(".")
        if len(parts) != 3:
            if logger:
                logger.warning(f"無効なビュー名形式: {view}")
            continue

        project, dataset, view_name = parts

        # 含めるパターンによるフィルタリング
        if include_patterns:
            include_match = False
            for pattern in include_patterns:
                # パターンに"."が含まれている場合はFQN全体に対してマッチング
                if "." in pattern:
                    # FQN全体に対してパターンマッチング
                    if _match_pattern(view, pattern):
                        include_match = True
                        break
                else:
                    # view部分のみに対してパターンマッチング（後方互換性のため）
                    if _match_pattern(view_name, pattern):
                        include_match = True
                        break
            if not include_match:
                continue  # マッチしない場合はスキップ

        # 除外パターンによるフィルタリング
        if exclude_patterns:
            exclude_match = False
            for pattern in exclude_patterns:
                # パターンに"."が含まれている場合はFQN全体に対してマッチング
                if "." in pattern:
                    # FQN全体に対してパターンマッチング
                    if _match_pattern(view, pattern):
                        exclude_match = True
                        break
                else:
                    # view部分のみに対してパターンマッチング（後方互換性のため）
                    if _match_pattern(view_name, pattern):
                        exclude_match = True
                        break
            if exclude_match:
                continue  # マッチする場合はスキップ

        filtered_views.append(view)

    return filtered_views


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
    yml_prefix: Optional[str] = None,
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
        yml_prefix: YAMLファイルの接頭辞（デフォルト: None）
                     e.g. "_" -> _model_name.yml
        include_dependencies: 依存関係にあるビューも含めるかどうか
        location: BigQueryロケーション
        debug: デバッグモードかどうか
        max_depth: 依存関係の最大深度
    """
    # ロギングの設定
    logger = setup_logging(verbose=debug)

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

    # 依存関係で追加されたビューも含めて、フィルタリングを適用
    if include_dependencies and (include_views or exclude_views):
        logger.info("依存関係で追加されたビューにもフィルタリングを適用します")
        ordered_views = filter_views(
            ordered_views, include_views, exclude_views, logger
        )
        logger.info(f"フィルタリング後のビュー数: {len(ordered_views)}")

    # 変換順序の表示
    display_ordered_views(ordered_views, console)

    # 命名規則プリセットの設定
    naming_preset_enum = NamingPreset(naming_preset)

    # ビューの変換
    converted_models = []
    skipped_views = {}

    for view in ordered_views:
        # テーブルタイプを確認（ビューでない場合はスキップ）
        try:
            table_type = bq_client.get_table_type(view)
            if table_type != "VIEW":
                if not table_type:
                    skipped_views[view] = "オブジェクトが存在しません"
                else:
                    skipped_views[view] = f"ビューではありません (タイプ: {table_type})"
                # ビューでない場合は次のオブジェクトへ
                continue
        except Exception as e:
            logger.warning(
                f"テーブルタイプの確認中にエラーが発生しました: {view} - {e}"
            )
            skipped_views[view] = f"テーブルタイプの確認に失敗: {str(e)}"
            continue

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
                view,
                bq_client,
                generator,
                naming_preset_enum,
                dry_run,
                debug,
                logger,
                yml_prefix,
            )
            converted_models.append(result)
        except Exception as e:
            logger.error(f"ビュー '{view}' の変換中にエラーが発生しました: {e}")
            skipped_views[view] = f"エラー: {str(e)}"

    # 変換結果の表示
    display_conversion_results(
        converted_models, skipped_views, dry_run, console, logger
    )
