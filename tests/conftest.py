"""Shared fixtures: one local Spark session per test run and the repository config."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"

# Spark workers must use the same interpreter as the driver (pandas / pyarrow for UDFs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def settings():  # type: ignore[no-untyped-def]
    from goldenrecord.config import load_settings

    return load_settings(CONFIG_DIR)


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[object]:
    from pyspark.sql import SparkSession

    session = (  # one session for the whole run: starting Spark is the slow part
        SparkSession.builder.master("local[2]")
        .appName("goldenrecord-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.environ.get("TEST_SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sparkContext.setCheckpointDir(str(tmp_path_factory.mktemp("checkpoints")))
    yield session
    session.stop()


@pytest.fixture
def token_key() -> bytes:
    return b"unit-test-key"


@pytest.fixture(autouse=True, scope="module")
def _clear_spark_cache() -> Iterator[None]:
    """Drop cached DataFrames between test modules so one long session stays small."""
    yield
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.catalog.clearCache()
