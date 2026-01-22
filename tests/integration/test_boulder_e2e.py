"""Integration tests for end-to-end transportation splitting."""

import logging

from tests.conftest import get_test_data_path
from transportation_splitter import (
    OutputFormat,
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

logger: logging.Logger = logging.getLogger(__name__)


def test_boulder_data_split(spark_session):
    """End-to-end test using Boulder, CO test data."""
    input_path = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder"

    test_config = SplitConfig(
        split_at_connectors=True,
        skip_debug_output=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            write_intermediate_files=True,
            reuse_existing_intermediate_outputs=False,
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
        result_df.filter("type = 'segment'").selectExpr("id", "size(connectors) as connector_count").collect()
    )

    for row in segments_with_connector_count:
        assert row.connector_count >= 2, f"Segment {row.id} has {row.connector_count} connectors, expected >= 2"

    # Verify start_lr and end_lr are present and valid
    lr_check = (
        result_df.filter("type = 'segment'")
        .selectExpr("id", "start_lr", "end_lr", "end_lr - start_lr as lr_span")
        .collect()
    )

    for row in lr_check:
        assert row.start_lr is not None, f"Segment {row.id} has null start_lr"
        assert row.end_lr is not None, f"Segment {row.id} has null end_lr"
        assert row.start_lr >= 0.0, f"Segment {row.id} has invalid start_lr: {row.start_lr}"
        assert row.end_lr <= 1.0, f"Segment {row.id} has invalid end_lr: {row.end_lr}"
        assert row.lr_span > 0, f"Segment {row.id} has non-positive lr_span: {row.lr_span}"

    logger.info(f"[BOULDER] All {segment_count} segments validated successfully")


def test_boulder_data_split_with_filter(spark_session):
    """End-to-end test using Boulder, CO test data with spatial filter."""
    input_path: str = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder"

    test_config = SplitConfig(
        split_at_connectors=False,
        skip_debug_output=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            output_format=OutputFormat.PARQUET_WKB,
            write_intermediate_files=True,
            reuse_existing_intermediate_outputs=False,
            filter_wkt="POLYGON((-105.279742 40.014376, -105.248896 40.014376, -105.248896 40.000703, -105.279742 40.000703, -105.279742 40.014376))",
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    # Validate basic structure
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(f"\n[BOULDER-FILTERED] Segments: {segment_count}, Connectors: {connector_count}")

    # Verify we got results - should match the original filtered test
    assert segment_count > 0 and segment_count < 10000, (
        "Expected at least one segment in Boulder data, but fewer than in the full dataset"
    )
    assert connector_count > 0 and connector_count < 10000, (
        "Expected at least one connector in Boulder data, but fewer than in the full dataset"
    )


def test_boulder_data_split_with_filter_geoparquet(spark_session):
    """End-to-end test using Boulder, CO test data with spatial filter and GeoParquet output."""
    input_path: str = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder_geoparquet"

    test_config = SplitConfig(
        split_at_connectors=False,
        skip_debug_output=True,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            output_format=OutputFormat.GEOPARQUET,
            write_intermediate_files=True,
            reuse_existing_intermediate_outputs=False,
            filter_wkt="POLYGON((-105.279742 40.014376, -105.248896 40.014376, -105.248896 40.000703, -105.279742 40.000703, -105.279742 40.014376))",
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    # Validate basic structure
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(f"\n[BOULDER-FILTERED-GEOPARQUET] Segments: {segment_count}, Connectors: {connector_count}")

    # Verify we got results
    assert segment_count > 0 and segment_count < 10000, (
        "Expected at least one segment in Boulder data, but fewer than in the full dataset"
    )
    assert connector_count > 0 and connector_count < 10000, (
        "Expected at least one connector in Boulder data, but fewer than in the full dataset"
    )

    # Verify all segments have at least 2 connectors (start and end)
    segments_with_connector_count = (
        result_df.filter("type = 'segment'").selectExpr("id", "size(connectors) as connector_count").collect()
    )

    for row in segments_with_connector_count:
        assert row.connector_count >= 2, f"Segment {row.id} has {row.connector_count} connectors, expected >= 2"

    logger.info(f"[BOULDER-FILTERED-GEOPARQUET] All {segment_count} segments validated successfully")


def test_boulder_data_split_with_filter_geoparquet_reuse_existing(spark_session):
    """End-to-end test using Boulder, CO test data with spatial filter, GeoParquet output, and cache reuse.

    This test verifies that intermediate files written in GeoParquet format by
    test_boulder_data_split_with_filter_geoparquet can be correctly reused.
    """
    input_path: str = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder_geoparquet"

    test_config = SplitConfig(
        split_at_connectors=False,
        skip_debug_output=True,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            output_format=OutputFormat.GEOPARQUET,
            write_intermediate_files=False,
            reuse_existing_intermediate_outputs=True,
            filter_wkt="POLYGON((-105.279742 40.014376, -105.248896 40.014376, -105.248896 40.000703, -105.279742 40.000703, -105.279742 40.014376))",
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    # Validate basic structure
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(f"\n[BOULDER-FILTERED-GEOPARQUET-REUSE] Segments: {segment_count}, Connectors: {connector_count}")

    # Verify we got results - should match the original GeoParquet test
    assert segment_count > 0 and segment_count < 10000, (
        "Expected at least one segment in Boulder data, but fewer than in the full dataset"
    )
    assert connector_count > 0 and connector_count < 10000, (
        "Expected at least one connector in Boulder data, but fewer than in the full dataset"
    )

    # Verify all segments have exactly 2 connectors (start and end)
    segments_with_connector_count = (
        result_df.filter("type = 'segment'").selectExpr("id", "size(connectors) as connector_count").collect()
    )

    for row in segments_with_connector_count:
        assert row.connector_count >= 2, f"Segment {row.id} has {row.connector_count} connectors, expected >= 2"

    # Verify start_lr and end_lr are present and valid
    lr_check = (
        result_df.filter("type = 'segment'")
        .selectExpr("id", "start_lr", "end_lr", "end_lr - start_lr as lr_span")
        .collect()
    )

    for row in lr_check:
        assert row.start_lr is not None, f"Segment {row.id} has null start_lr"
        assert row.end_lr is not None, f"Segment {row.id} has null end_lr"
        assert row.start_lr >= 0.0, f"Segment {row.id} has invalid start_lr: {row.start_lr}"
        assert row.end_lr <= 1.0, f"Segment {row.id} has invalid end_lr: {row.end_lr}"
        assert row.lr_span > 0, f"Segment {row.id} has non-positive lr_span: {row.lr_span}"

    logger.info(f"[BOULDER-FILTERED] All {segment_count} segments validated successfully")


def test_boulder_data_split_with_filter_reuse_existing(spark_session):
    """End-to-end test using Boulder, CO test data with spatial filter and cache reuse."""
    input_path: str = get_test_data_path(pattern="boulder_*.parquet")
    output_path = "tests/out/boulder"

    test_config = SplitConfig(
        split_at_connectors=False,
        skip_debug_output=False,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            output_format=OutputFormat.PARQUET_WKB,  # Must match the format used by test_boulder_data_split_with_filter
            write_intermediate_files=False,
            reuse_existing_intermediate_outputs=True,
            filter_wkt="POLYGON((-105.279742 40.014376, -105.248896 40.014376, -105.248896 40.000703, -105.279742 40.000703, -105.279742 40.014376))",
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    # Validate basic structure
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(f"\n[BOULDER-FILTERED-REUSE] Segments: {segment_count}, Connectors: {connector_count}")

    # Verify we got results
    assert segment_count > 0 and segment_count < 10000, (
        "Expected at least one segment in Boulder data, but fewer than in the full dataset"
    )
    assert connector_count > 0 and connector_count < 10000, (
        "Expected at least one connector in Boulder data, but fewer than in the full dataset"
    )
