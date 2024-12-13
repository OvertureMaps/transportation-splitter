import logging
from importlib.metadata import version

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import expr
from sedona.spark import *
from transportation_splitter import (
    DESTINATIONS_COLUMN,
    PROHIBITED_TRANSITIONS_COLUMN,
    split_transportation,
    SplitConfig,
    SplitterDataWrangler,
    SplitterStep,
)


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark_version = version("pyspark")
    minor_version = int(spark_version.split(".")[1])

    # Sedona recommends using matching major.minor version for spark >= 3.3
    # See latest instructions here: https://sedona.apache.org/latest/setup/install-python/
    spark_version_for_sedona = "3.0"
    if minor_version >= 3:
        spark_version_for_sedona = f"3.{minor_version}"

    spark = (
        SedonaContext.builder()
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            f"org.apache.sedona:sedona-spark-{spark_version_for_sedona}_2.12:1.7.0,"
            "org.datasyslab:geotools-wrapper:1.7.0-28.5",
        )
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all",
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
        SplitterDataWrangler(input_path=input_path, output_path_prefix=output_path),
        cfg=test_config,
    )

    actual_df = (
        result_df.filter("id = '08728d5430ffffff04767d5cdb906cb5'")
        .sort("start_lr")
        .collect()
    )
    assert len(actual_df) == 3

    # Check attributes
    assert all(
        [
            split["class"] == "motorway"
            and split.subclass == "link"
            and split.subclass_rules == [Row(value="link", between=None)]
            for split in actual_df
        ]
    )
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

    test_config = SplitConfig(
        split_at_connectors=True,
        reuse_existing_intermediate_outputs=False,
    )

    result_df = split_transportation(
        spark_session,
        spark_session.sparkContext,
        SplitterDataWrangler(input_path=input_path, output_path_prefix=output_path),
        cfg=test_config,
    )

    validate_all_connector_split(result_df)

def test_custom_hooks(spark_session):
    input_path = "test/data/*.parquet"
    output_path = "test/out/custom_hooks"

    test_config = SplitConfig(
        split_at_connectors=True,
        reuse_existing_intermediate_outputs=False,
    )

    def read(spark, step, base_path):
        read_path = base_path + f"_{step.name.lower()}/"
        print(f"Test reading from {read_path}")
        if step == SplitterStep.read_input:
            read_path = base_path
        if step == SplitterStep.raw_split:
            return spark.read.option("mergeSchema", "true").parquet(read_path)
        elif step == SplitterStep.read_input:
            return spark.read.option("mergeSchema", "true").parquet(read_path) \
                .withColumn("geometry", expr("ST_GeomFromWKB(geometry)"))
        else:
            return spark.read.option("mergeSchema", "true").format("geoparquet").load(read_path)
        
    def check_exists(_spark, _step, _output_path_prefix):
        return False

    def write(df, step, output_path_prefix):
        write_path = output_path_prefix + f"_{step.name.lower()}/"
        print(f"Test writing to {write_path}")
        if step == SplitterStep.raw_split:
            return df.write.mode("overwrite").option("mergeSchema", "true").parquet(write_path)
        else:
            return df.write.format("geoparquet").mode("overwrite").option("mergeSchema", "true").save(write_path)

    result_df = split_transportation(
        spark_session,
        spark_session.sparkContext,
        SplitterDataWrangler(input_path=input_path, output_path_prefix=output_path, custom_read_hook=read, custom_write_hook=write, custom_exists_hook=check_exists),
        cfg=test_config,
    )

    validate_all_connector_split(result_df)

def validate_all_connector_split(result_df):
    actual_df = (
        result_df.filter("id = '08728d5430ffffff04767d5cdb906cb5'")
        .sort("start_lr")
        .collect()
    )
    assert len(actual_df) == 4

    # Check attributes
    assert all(
        [
            split["class"] == "motorway"
            and split.subclass == "link"
            and split.subclass_rules == [Row(value="link", between=None)]
            for split in actual_df
        ]
    )
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
