"""Shared pytest setup: make the app modules importable and provide a local Spark session."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for app_dir in ("producer", "spark_app", "loader", "sheets_sink"):
    sys.path.insert(0, str(ROOT / "apps" / app_dir))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))


@pytest.fixture(scope="session")
def spark():
    """A tiny local SparkSession (UTC) shared by all Spark tests."""
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("flight-pipeline-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
