"""名前付け規則ユーティリティモジュール。"""

import re
from enum import Enum
from typing import Optional, Tuple


class NamingPreset(str, Enum):
    """命名規則プリセット。"""

    FULL = "full"  # データセット名とテーブル名を使用
    TABLE_ONLY = "table_only"  # テーブル名のみを使用
    DATASET_WITHOUT_PREFIX = (
        "dataset_without_prefix"  # データセットプレフィックスを使用
    )


def parse_bigquery_name(fully_qualified_name: str) -> Tuple[str, str, str]:
    """BigQueryの完全修飾名をプロジェクト、データセット、テーブル名に分解する。

    Args:
        fully_qualified_name: BigQueryの完全修飾名 (project.dataset.table または dataset.table)

    Returns:
        プロジェクト、データセット、テーブル名のタプル

    Raises:
        ValueError: 無効なフォーマットの場合
    """
    parts = fully_qualified_name.split(".")

    if len(parts) == 3:
        # project.dataset.table 形式
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        # dataset.table 形式
        return "", parts[0], parts[1]
    else:
        raise ValueError(f"無効なBigQuery名: {fully_qualified_name}")


def extract_dataset_prefix(dataset_name: str) -> str:
    """データセット名からプレフィックスを抽出する。

    例:
        dm_sales -> sales
        sales -> sales

    Args:
        dataset_name: データセット名

    Returns:
        データセットプレフィックス
    """
    # データセット名から共通プレフィックス（dm_, dwh_, stg_など）を削除
    match = re.match(r"^([a-z]+_)(.+)$", dataset_name)
    if match:
        prefix, name = match.groups()
        return name

    return dataset_name


def generate_model_name(
    fully_qualified_name: str, naming_preset: NamingPreset = NamingPreset.FULL
) -> str:
    """BigQueryビュー名からdbtモデル名を生成する。

    Args:
        fully_qualified_name: BigQueryの完全修飾名
        naming_preset: 使用する命名規則プリセット

    Returns:
        dbtモデル名
    """
    project, dataset, table = parse_bigquery_name(fully_qualified_name)

    if naming_preset == NamingPreset.TABLE_ONLY:
        # テーブル名のみを使用
        return table
    elif naming_preset == NamingPreset.FULL:
        # データセット名とテーブル名を使用
        return f"{dataset}__{table}"
    else:
        # データセットプレフィックスを抽出して使用
        dataset_prefix = extract_dataset_prefix(dataset)
        return f"{dataset_prefix}__{table}"


def generate_model_filename(
    fully_qualified_name: str,
    naming_preset: NamingPreset = NamingPreset.FULL,
    extension: str = "sql",
    yml_prefix: Optional[str] = None,
) -> str:
    """BigQueryビュー名からdbtモデルのファイル名を生成する。

    Args:
        fully_qualified_name: BigQueryの完全修飾名
        naming_preset: 使用する命名規則プリセット
        extension: ファイル拡張子（デフォルト: sql）
        yml_prefix: YAMLファイルの接頭辞（デフォルト: None）
                     e.g. "_" -> _model_name.yml
    Returns:
        dbtモデルのファイル名
    """
    model_name = generate_model_name(fully_qualified_name, naming_preset)
    if extension == "yml" and yml_prefix:
        return f"{yml_prefix}{model_name}.yml"
    else:
        return f"{model_name}.{extension}"
