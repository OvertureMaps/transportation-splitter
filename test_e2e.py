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
from transportation_splitter import (
    split_transportation,
    SplitConfig,
    PROHIBITED_TRANSITIONS_COLUMN,
    DESTINATIONS_COLUMN,
)


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (
        SedonaContext.builder()
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1,"
            "org.datasyslab:geotools-wrapper:1.6.1-28.2",
        )
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()

    # Needed to enable geospatial functions
    return SedonaContext.create(spark)


def test_split_no_connector_split(spark_session):
    input_path = "test/data/*.parquet"
    output_path = "test/out/preserve_connectors"

    test_config = SplitConfig(
        split_at_connectors=False,
        lr_columns_to_exclude=[PROHIBITED_TRANSITIONS_COLUMN, DESTINATIONS_COLUMN],
        reuse_existing_intermediate_outputs=False,
    )

    result_df = split_transportation(
        spark_session,
        spark_session.sparkContext,
        input_path,
        output_path,
        cfg=test_config,
    )

    actual_df = (
        result_df.filter("id = '08728d5430ffffff04767d5cdb906cb5'")
        .sort("start_lr")
        .collect()
    )
    assert len(actual_df) == 3
    for split in actual_df:
        print(split)
    assert all(
        [
            split["class"] == "motorway"
            and split.subclass == "link"
            and split.subclass_rules == [Row(value="link", between=None)]
            for split in actual_df
        ]
    )

    # Check attributes
    assert actual_df[0].road_flags == [
        Row(values=["is_bridge", "is_link"], between=None)
    ]
    assert actual_df[1].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[2].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[0].road_surface is None
    assert actual_df[1].road_surface is None
    assert actual_df[2].road_surface == [Row(value="paved", between=None)]

    # Interior connector is preserved on one split segment
    assert actual_df[0].connectors == [
        Row(connector_id="08f28d5430ce156b042ffd6280a10651", at=0.0),
        Row(connector_id="08f28d5430ce146b0432776290e587d8", at=0.582983938),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=1.0),
    ]
    assert actual_df[1].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=0.0),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=1.0),
    ]
    assert actual_df[2].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=0.0),
        Row(connector_id="08f28d543056a114043a7d54efe962ca", at=1.0),
    ]


def test_split_all_connectors(spark_session):
    input_path = "test/data/*.parquet"
    output_path = "test/out/all_connectors"

    result_df = split_transportation(
        spark_session,
        spark_session.sparkContext,
        input_path,
        output_path,
    )

    actual_df = (
        result_df.filter("id = '08728d5430ffffff04767d5cdb906cb5'")
        .sort("start_lr")
        .collect()
    )
    assert len(actual_df) == 4
    for split in actual_df:
        print(split)
    assert all(
        [
            split["class"] == "motorway"
            and split.subclass == "link"
            and split.subclass_rules == [Row(value="link", between=None)]
            for split in actual_df
        ]
    )

    # Check attributes
    assert actual_df[0].road_flags == [
        Row(values=["is_bridge", "is_link"], between=None)
    ]
    assert actual_df[1].road_flags == [
        Row(values=["is_bridge", "is_link"], between=None)
    ]
    assert actual_df[2].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[3].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[0].road_surface is None
    assert actual_df[1].road_surface is None
    assert actual_df[2].road_surface is None
    assert actual_df[3].road_surface == [Row(value="paved", between=None)]

    assert actual_df[0].connectors == [
        Row(connector_id="08f28d5430ce156b042ffd6280a10651", at=0.0),
        Row(connector_id="08f28d5430ce146b0432776290e587d8", at=1.0),
    ]
    assert actual_df[1].connectors == [
        Row(connector_id="08f28d5430ce146b0432776290e587d8", at=0.0),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=1.0),
    ]
    assert actual_df[2].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=0.0),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=1.0),
    ]
    assert actual_df[3].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=0.0),
        Row(connector_id="08f28d543056a114043a7d54efe962ca", at=1.0),
    ]
