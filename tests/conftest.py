"""pytest共通設定"""
import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_output_dir():
    """一時的な出力ディレクトリを提供するフィクスチャ"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


@pytest.fixture
def template_dir():
    """テンプレートディレクトリのパスを提供するフィクスチャ"""
    return Path(__file__).parent.parent / "src" / "bq2dbt" / "templates"


@pytest.fixture
def sql_template_path(template_dir):
    """SQLテンプレートのパスを提供するフィクスチャ"""
    return template_dir / "model.sql"


@pytest.fixture
def yml_template_path(template_dir):
    """YAMLテンプレートのパスを提供するフィクスチャ"""
    return template_dir / "model.yml"


@pytest.fixture
def bq_project_id():
    """BigQueryプロジェクトIDを提供するフィクスチャ

    環境変数BQ_PROJECT_IDから取得するか、デフォルト値を使用
    """
    return os.environ.get("BQ_PROJECT_ID", "test-project")


@pytest.fixture
def bq_dataset_id():
    """BigQueryデータセットIDを提供するフィクスチャ

    環境変数BQ_DATASET_IDから取得するか、デフォルト値を使用
    """
    return os.environ.get("BQ_DATASET_ID", "test_dataset")
