import logging

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    MapType,
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    IntegerType,
)
from sedona.spark import *
from transportation_splitter import split_transportation, SplitConfig, PROHIBITED_TRANSITIONS_COLUMN, DESTINATIONS_COLUMN


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = SedonaContext.builder() \
        .master("local[*]") \
    .config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,'
           'org.datasyslab:geotools-wrapper:1.6.1-28.2'). \
    getOrCreate()
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()

    # Needed to enable geospatial functions
    return SedonaContext.create(spark)

def test_split_preserve_connectors(spark_session):
    input_path = "test/data/*.parquet"
    output_path = "test/out/out"

    test_config = SplitConfig(
        split_at_connectors=False,
        lr_columns_to_exclude=[PROHIBITED_TRANSITIONS_COLUMN, DESTINATIONS_COLUMN],
        reuse_existing_intermediate_outputs=False,
    )

    sc = spark_session.sparkContext
    result_df = split_transportation(spark_session, sc, input_path, output_path, filter_wkt=None, cfg=test_config)
    result_df.filter('type == "segment"').show(20, False)
