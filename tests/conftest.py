"""
Shared pytest fixtures for transportation-splitter tests.
"""

import logging
from importlib.metadata import version
from pathlib import Path

import pytest
from sedona.spark import SedonaContext


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


# Path to test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"


def get_test_data_path(pattern: str = "{segment,connector}.parquet") -> str:
    """Get the path to test data files (original segment and connector files only)."""
    return str(TEST_DATA_DIR / pattern)


@pytest.fixture(scope="session")
def spark_session(request):
    """
    Fixture for creating a Spark session with Sedona.

    This fixture is session-scoped to avoid the overhead of creating
    multiple Spark contexts during test runs.
    """
    spark_version = version("pyspark")
    minor_version = int(spark_version.split(".")[1])

    # Sedona recommends using matching major.minor version for spark >= 3.3
    # See: https://sedona.apache.org/latest/setup/install-python/
    spark_version_for_sedona = "3.0"
    if minor_version >= 3:
        spark_version_for_sedona = f"3.{minor_version}"

    # Use Sedona 1.8.0 shaded JAR (includes all dependencies) with Scala 2.12
    # Note: PySpark from PyPI only ships with Scala 2.12, regardless of version
    sedona_version = "1.8.0"

    spark = (
        SedonaContext.builder()
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            f"org.apache.sedona:sedona-spark-shaded-{spark_version_for_sedona}_2.12:{sedona_version}",
        )
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()

    # SedonaContext.create() registers geospatial functions
    return SedonaContext.create(spark)


@pytest.fixture
def test_data_path():
    """Fixture providing the path to test data."""
    return get_test_data_path()
