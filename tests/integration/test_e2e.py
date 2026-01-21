"""Integration tests for end-to-end transportation splitting."""

import logging

from pyspark.sql import Row
from tests.conftest import get_test_data_path
from transportation_splitter import (
    DESTINATIONS_COLUMN,
    PROHIBITED_TRANSITIONS_COLUMN,
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

logger = logging.getLogger(__name__)


def test_split_no_connector_split(spark_session):
    input_path = get_test_data_path()
    output_path = "tests/out/preserve_connectors"

    test_config = SplitConfig(
        split_at_connectors=False,
        lr_columns_to_exclude=[PROHIBITED_TRANSITIONS_COLUMN, DESTINATIONS_COLUMN],
        reuse_existing_intermediate_outputs=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(input_path=input_path, output_path=output_path),
        cfg=test_config,
    )
    result_df = splitter.split()

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
    input_path = get_test_data_path()
    output_path = "tests/out/all_connectors"

    test_config = SplitConfig(
        split_at_connectors=True,
        reuse_existing_intermediate_outputs=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(input_path=input_path, output_path=output_path),
        cfg=test_config,
    )
    result_df = splitter.split()

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


def test_boulder_data_split(spark_session):
    """End-to-end test using Boulder, CO test data."""
    input_path = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder"

    test_config = SplitConfig(
        split_at_connectors=True,
        reuse_existing_intermediate_outputs=False,
        skip_debug_output=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            write_intermediate_files=True,
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    # Validate basic structure
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(f"\n[BOULDER] Segments: {segment_count}, Connectors: {connector_count}")

    # Verify we got results
    assert segment_count > 0, "Expected at least one segment in Boulder data"
    assert connector_count > 0, "Expected at least one connector in Boulder data"

    # Verify all segments have exactly 2 connectors (start and end)
    segments_with_connector_count = (
        result_df.filter("type = 'segment'")
        .selectExpr("id", "size(connectors) as connector_count")
        .collect()
    )

    for row in segments_with_connector_count:
        assert (
            row.connector_count >= 2
        ), f"Segment {row.id} has {row.connector_count} connectors, expected >= 2"

    # Verify start_lr and end_lr are present and valid
    lr_check = (
        result_df.filter("type = 'segment'")
        .selectExpr("id", "start_lr", "end_lr", "end_lr - start_lr as lr_span")
        .collect()
    )

    for row in lr_check:
        assert row.start_lr is not None, f"Segment {row.id} has null start_lr"
        assert row.end_lr is not None, f"Segment {row.id} has null end_lr"
        assert (
            row.start_lr >= 0.0
        ), f"Segment {row.id} has invalid start_lr: {row.start_lr}"
        assert row.end_lr <= 1.0, f"Segment {row.id} has invalid end_lr: {row.end_lr}"
        assert (
            row.lr_span > 0
        ), f"Segment {row.id} has non-positive lr_span: {row.lr_span}"

    logger.info(f"[BOULDER] All {segment_count} segments validated successfully")


def test_boulder_data_split_with_filter(spark_session):
    """End-to-end test using Boulder, CO test data with spatial filter."""
    input_path: str = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder"

    test_config = SplitConfig(
        split_at_connectors=True,
        reuse_existing_intermediate_outputs=False,
        skip_debug_output=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            write_intermediate_files=True,
            filter_wkt="POLYGON((-105.279742 40.014376, -105.248896 40.014376, -105.248896 40.000703, -105.279742 40.000703, -105.279742 40.014376))",
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    # Validate basic structure
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(
        f"\n[BOULDER-FILTERED] Segments: {segment_count}, Connectors: {connector_count}"
    )

    # Verify we got results
    assert (
        segment_count > 0 and segment_count < 10000
    ), "Expected at least one segment in Boulder data, but fewer than in the full dataset"
    assert (
        connector_count > 0 and connector_count < 10000
    ), "Expected at least one connector in Boulder data, but fewer than in the full dataset"

    # Verify all segments have exactly 2 connectors (start and end)
    segments_with_connector_count = (
        result_df.filter("type = 'segment'")
        .selectExpr("id", "size(connectors) as connector_count")
        .collect()
    )

    for row in segments_with_connector_count:
        assert (
            row.connector_count >= 2
        ), f"Segment {row.id} has {row.connector_count} connectors, expected >= 2"

    # Verify start_lr and end_lr are present and valid
    lr_check = (
        result_df.filter("type = 'segment'")
        .selectExpr("id", "start_lr", "end_lr", "end_lr - start_lr as lr_span")
        .collect()
    )

    for row in lr_check:
        assert row.start_lr is not None, f"Segment {row.id} has null start_lr"
        assert row.end_lr is not None, f"Segment {row.id} has null end_lr"
        assert (
            row.start_lr >= 0.0
        ), f"Segment {row.id} has invalid start_lr: {row.start_lr}"
        assert row.end_lr <= 1.0, f"Segment {row.id} has invalid end_lr: {row.end_lr}"
        assert (
            row.lr_span > 0
        ), f"Segment {row.id} has non-positive lr_span: {row.lr_span}"

    logger.info(f"[BOULDER] All {segment_count} segments validated successfully")
