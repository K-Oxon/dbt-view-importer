"""dbtモデルジェネレーターモジュール。

BigQueryビュー定義からdbtモデルを生成する機能を提供します。
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import jinja2

from bq2dbt.utils.naming import (
    NamingPreset,
    generate_model_filename,
    generate_model_name,
)

logger = logging.getLogger(__name__)


class ModelGenerator:
    """dbtモデルを生成するクラス。"""

    def __init__(
        self,
        output_dir: Union[str, Path],
        sql_template_path: Optional[Path] = None,
        yml_template_path: Optional[Path] = None,
    ):
        """モデルジェネレーターを初期化します。

        Args:
            output_dir: dbtモデルの出力先ディレクトリ
            sql_template_path: SQLテンプレートファイルのパス（省略時はデフォルトテンプレート）
            yml_template_path: YAMLテンプレートファイルのパス（省略時はデフォルトテンプレート）
        """
        self.output_dir = Path(output_dir)

        # テンプレート環境を設定
        template_dir = Path(__file__).parent.parent / "templates"
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # テンプレートを読み込む
        self.sql_template_path = sql_template_path or template_dir / "model.sql"
        self.yml_template_path = yml_template_path or template_dir / "model.yml"

        # パスが存在するか確認
        if not self.output_dir.exists():
            raise ValueError(f"出力ディレクトリが存在しません: {output_dir}")
        if sql_template_path and not sql_template_path.exists():
            raise ValueError(
                f"SQLテンプレートファイルが存在しません: {sql_template_path}"
            )
        if yml_template_path and not yml_template_path.exists():
            raise ValueError(
                f"YAMLテンプレートファイルが存在しません: {yml_template_path}"
            )

        logger.debug(f"モデルジェネレーターを初期化しました: 出力先={output_dir}")

    def _load_template(self, path: Path) -> jinja2.Template:
        """テンプレートファイルを読み込みます。

        Args:
            path: テンプレートファイルのパス

        Returns:
            読み込まれたテンプレート
        """
        logger.debug(f"テンプレートファイルを読み込みます: {path}")
        with open(path, "r") as f:
            template_str = f.read()

        try:
            template = jinja2.Template(template_str)
            logger.debug("テンプレートを正常に読み込みました")
            return template
        except Exception as e:
            logger.error(f"テンプレートの読み込み中にエラーが発生しました: {str(e)}")
            raise

    def generate_sql_model(
        self,
        fully_qualified_name: str,
        sql_definition: str,
        naming_preset: NamingPreset = NamingPreset.FULL,
        dry_run: bool = False,
    ) -> Tuple[str, Path]:
        """SQLモデルファイルを生成します。

        Args:
            fully_qualified_name: ビューの完全修飾名
            sql_definition: ビューのSQL定義
            naming_preset: ファイル名の命名規則
            dry_run: ファイルを実際に生成せずに内容だけ返す

        Returns:
            生成されたモデル内容とファイルパスのタプル
        """
        # テンプレートを読み込む
        template = self._load_template(self.sql_template_path)

        # モデル名とファイル名を生成
        model_name = generate_model_name(fully_qualified_name, naming_preset)
        file_name = generate_model_filename(
            fully_qualified_name, naming_preset, extension="sql"
        )
        file_path = self.output_dir / file_name

        logger.debug(f"SQLモデル生成: fully_qualified_name={fully_qualified_name}")
        logger.debug(f"SQLモデル生成: model_name={model_name}")
        logger.debug(f"SQLモデル生成: file_path={file_path}")

        # テンプレート変数を設定
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        template_vars = {
            "source_view": fully_qualified_name,
            "model_name": model_name,
            "sql_definition": sql_definition,
            "timestamp": timestamp,
        }

        # テンプレート変数のログ
        logger.debug(f"テンプレート変数: {template_vars.keys()}")

        try:
            # テンプレートをレンダリング
            rendered_content = template.render(**template_vars)
            logger.debug("テンプレートのレンダリングに成功しました")
        except Exception as e:
            logger.error(
                f"テンプレートのレンダリング中にエラーが発生しました: {str(e)}"
            )
            raise

        # ファイルに書き込む（dry_run=Falseの場合）
        if not dry_run:
            with open(file_path, "w") as f:
                f.write(rendered_content)
            logger.debug(f"SQLモデルファイルを生成しました: {file_path}")

        return rendered_content, file_path

    def generate_yaml_model(
        self,
        fully_qualified_name: str,
        schema_fields: List[Dict[str, str]],
        description: str = "",
        naming_preset: NamingPreset = NamingPreset.FULL,
        dry_run: bool = False,
        yml_prefix: Optional[str] = None,
    ) -> Tuple[str, Path]:
        """YAMLモデルファイルを生成します。

        Args:
            fully_qualified_name: ビューの完全修飾名
            schema_fields: スキーマフィールドのリスト
            description: モデルの説明
            naming_preset: ファイル名の命名規則
            dry_run: ファイルを実際に生成せずに内容だけ返す

        Returns:
            生成されたモデル内容とファイルパスのタプル
        """
        # テンプレートを読み込む
        template = self._load_template(self.yml_template_path)

        # モデル名とファイル名を生成
        model_name = generate_model_name(fully_qualified_name, naming_preset)
        file_name = generate_model_filename(
            fully_qualified_name,
            naming_preset,
            extension="yml",
            yml_prefix=yml_prefix,
        )
        file_path = self.output_dir / file_name

        logger.debug(f"YAMLモデル生成: fully_qualified_name={fully_qualified_name}")
        logger.debug(f"YAMLモデル生成: model_name={model_name}")
        logger.debug(f"YAMLモデル生成: schema_fields数={len(schema_fields)}")
        logger.debug(f"YAMLモデル生成: file_path={file_path}")

        # テンプレート変数を設定
        template_vars = {
            "model_name": model_name,
            "description": description,
            "columns": schema_fields,
        }

        # テンプレート変数のログ
        logger.debug(f"テンプレート変数: {template_vars.keys()}")

        try:
            # テンプレートをレンダリング
            rendered_content = template.render(**template_vars)
            logger.debug("テンプレートのレンダリングに成功しました")
        except Exception as e:
            logger.error(
                f"テンプレートのレンダリング中にエラーが発生しました: {str(e)}"
            )
            raise

        # ファイルに書き込む（dry_run=Falseの場合）
        if not dry_run:
            with open(file_path, "w") as f:
                f.write(rendered_content)
            logger.debug(f"YAMLモデルファイルを生成しました: {file_path}")

        return rendered_content, file_path
