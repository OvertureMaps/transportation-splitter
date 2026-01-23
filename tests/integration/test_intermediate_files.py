"""
Integration tests for transportation splitting with intermediate file reuse in multiple
formats.

Test plan:
1. Run the split pipeline and write intermediate files without GeoParquet
2. Run the split pipeline again and reuse intermediate files without GeoParquet Output
3. Run the split pipeline again and reuse intermediate files with GeoParquet Output

4. Run the split pipeline and write intermediate files with GeoParquet Output
5. Run the split pipeline again and reuse intermediate files without GeoParquet Output
6. Run the split pipeline again and reuse intermediate files with GeoParquet Output
"""

import logging

from tests.conftest import get_test_data_path
from transportation_splitter import (
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

logger: logging.Logger = logging.getLogger(__name__)

# Common test configuration
TEST_CONFIG = SplitConfig(
    split_at_connectors=False,
    skip_debug_output=True,
)

# Common spatial filter for Boulder area
BOULDER_FILTER_WKT = "POLYGON((-105.279742 40.014376, -105.248896 40.014376, -105.248896 40.000703, -105.279742 40.000703, -105.279742 40.014376))"


def validate_split_results(result_df, test_name: str) -> tuple[int, int]:
    """Validate the split results and return segment/connector counts."""
    segment_count = result_df.filter("type = 'segment'").count()
    connector_count = result_df.filter("type = 'connector'").count()

    logger.info(f"\n[{test_name}] Segments: {segment_count}, Connectors: {connector_count}")

    # Verify we got results
    assert segment_count > 0, f"[{test_name}] Expected at least one segment"
    assert connector_count > 0, f"[{test_name}] Expected at least one connector"

    return segment_count, connector_count


# =============================================================================
# Group 1: Start with Parquet+WKB intermediate files (write_geoparquet=False)
# =============================================================================


class TestIntermediateFilesParquetWKB:
    """Test suite for intermediate file reuse starting with Parquet+WKB format."""

    # Store counts from test 1 for comparison in tests 2 and 3
    expected_segment_count: int = 0
    expected_connector_count: int = 0

    def test_1_write_intermediate_parquet_wkb(self, spark_session):
        """Step 1: Write intermediate files in Parquet+WKB format."""
        input_path = get_test_data_path(pattern="boulder*.parquet")
        output_path = "tests/out/intermediate_parquet_wkb"

        splitter = OvertureTransportationSplitter(
            spark=spark_session,
            wrangler=SplitterDataWrangler(
                input_path=input_path,
                output_path=output_path,
                write_geoparquet=False,  # Parquet+WKB format
                write_intermediate_files=True,
                reuse_existing_intermediate_outputs=False,  # Force fresh write
                filter_wkt=BOULDER_FILTER_WKT,
            ),
            cfg=TEST_CONFIG,
        )
        result_df = splitter.split()

        segment_count, connector_count = validate_split_results(result_df, "WRITE-PARQUET-WKB")

        # Store for comparison in subsequent tests
        TestIntermediateFilesParquetWKB.expected_segment_count = segment_count
        TestIntermediateFilesParquetWKB.expected_connector_count = connector_count

    def test_2_reuse_intermediate_output_parquet_wkb(self, spark_session):
        """Step 2: Reuse intermediate files, output as Parquet+WKB."""
        input_path = get_test_data_path(pattern="boulder*.parquet")
        output_path = "tests/out/intermediate_parquet_wkb"

        splitter = OvertureTransportationSplitter(
            spark=spark_session,
            wrangler=SplitterDataWrangler(
                input_path=input_path,
                output_path=output_path,
                write_geoparquet=False,  # Output as Parquet+WKB
                write_intermediate_files=False,  # Don't write new intermediate files
                reuse_existing_intermediate_outputs=True,  # Reuse existing
                filter_wkt=BOULDER_FILTER_WKT,
            ),
            cfg=TEST_CONFIG,
        )
        result_df = splitter.split()

        segment_count, connector_count = validate_split_results(result_df, "REUSE-PARQUET-WKB->PARQUET-WKB")

        # Verify counts match the original run
        assert segment_count == TestIntermediateFilesParquetWKB.expected_segment_count, (
            f"Segment count mismatch: got {segment_count}, "
            f"expected {TestIntermediateFilesParquetWKB.expected_segment_count}"
        )
        assert connector_count == TestIntermediateFilesParquetWKB.expected_connector_count, (
            f"Connector count mismatch: got {connector_count}, "
            f"expected {TestIntermediateFilesParquetWKB.expected_connector_count}"
        )

    def test_3_reuse_intermediate_output_geoparquet(self, spark_session):
        """Step 3: Reuse Parquet+WKB intermediate files, output as GeoParquet."""
        input_path = get_test_data_path(pattern="boulder*.parquet")
        output_path = "tests/out/intermediate_parquet_wkb"

        splitter = OvertureTransportationSplitter(
            spark=spark_session,
            wrangler=SplitterDataWrangler(
                input_path=input_path,
                output_path=output_path,
                write_geoparquet=True,  # Output as GeoParquet (different from intermediate)
                write_intermediate_files=False,  # Don't write new intermediate files
                reuse_existing_intermediate_outputs=True,  # Reuse existing Parquet+WKB files
                filter_wkt=BOULDER_FILTER_WKT,
            ),
            cfg=TEST_CONFIG,
        )
        result_df = splitter.split()

        segment_count, connector_count = validate_split_results(result_df, "REUSE-PARQUET-WKB->GEOPARQUET")

        # Verify counts match the original run
        assert segment_count == TestIntermediateFilesParquetWKB.expected_segment_count, (
            f"Segment count mismatch: got {segment_count}, "
            f"expected {TestIntermediateFilesParquetWKB.expected_segment_count}"
        )
        assert connector_count == TestIntermediateFilesParquetWKB.expected_connector_count, (
            f"Connector count mismatch: got {connector_count}, "
            f"expected {TestIntermediateFilesParquetWKB.expected_connector_count}"
        )


# =============================================================================
# Group 2: Start with GeoParquet intermediate files (write_geoparquet=True)
# =============================================================================


class TestIntermediateFilesGeoParquet:
    """Test suite for intermediate file reuse starting with GeoParquet format."""

    # Store counts from test 4 for comparison in tests 5 and 6
    expected_segment_count: int = 0
    expected_connector_count: int = 0

    def test_4_write_intermediate_geoparquet(self, spark_session):
        """Step 4: Write intermediate files in GeoParquet format."""
        input_path = get_test_data_path(pattern="boulder*.parquet")
        output_path = "tests/out/intermediate_geoparquet"

        splitter = OvertureTransportationSplitter(
            spark=spark_session,
            wrangler=SplitterDataWrangler(
                input_path=input_path,
                output_path=output_path,
                write_geoparquet=True,  # GeoParquet format
                write_intermediate_files=True,
                reuse_existing_intermediate_outputs=False,  # Force fresh write
                filter_wkt=BOULDER_FILTER_WKT,
            ),
            cfg=TEST_CONFIG,
        )
        result_df = splitter.split()

        segment_count, connector_count = validate_split_results(result_df, "WRITE-GEOPARQUET")

        # Store for comparison in subsequent tests
        TestIntermediateFilesGeoParquet.expected_segment_count = segment_count
        TestIntermediateFilesGeoParquet.expected_connector_count = connector_count

    def test_5_reuse_intermediate_output_parquet_wkb(self, spark_session):
        """Step 5: Reuse GeoParquet intermediate files, output as Parquet+WKB."""
        input_path = get_test_data_path(pattern="boulder*.parquet")
        output_path = "tests/out/intermediate_geoparquet"

        splitter = OvertureTransportationSplitter(
            spark=spark_session,
            wrangler=SplitterDataWrangler(
                input_path=input_path,
                output_path=output_path,
                write_geoparquet=False,  # Output as Parquet+WKB (different from intermediate)
                write_intermediate_files=False,  # Don't write new intermediate files
                reuse_existing_intermediate_outputs=True,  # Reuse existing GeoParquet files
                filter_wkt=BOULDER_FILTER_WKT,
            ),
            cfg=TEST_CONFIG,
        )
        result_df = splitter.split()

        segment_count, connector_count = validate_split_results(result_df, "REUSE-GEOPARQUET->PARQUET-WKB")

        # Verify counts match the original run
        assert segment_count == TestIntermediateFilesGeoParquet.expected_segment_count, (
            f"Segment count mismatch: got {segment_count}, "
            f"expected {TestIntermediateFilesGeoParquet.expected_segment_count}"
        )
        assert connector_count == TestIntermediateFilesGeoParquet.expected_connector_count, (
            f"Connector count mismatch: got {connector_count}, "
            f"expected {TestIntermediateFilesGeoParquet.expected_connector_count}"
        )

    def test_6_reuse_intermediate_output_geoparquet(self, spark_session):
        """Step 6: Reuse GeoParquet intermediate files, output as GeoParquet."""
        input_path = get_test_data_path(pattern="boulder*.parquet")
        output_path = "tests/out/intermediate_geoparquet"

        splitter = OvertureTransportationSplitter(
            spark=spark_session,
            wrangler=SplitterDataWrangler(
                input_path=input_path,
                output_path=output_path,
                write_geoparquet=True,  # Output as GeoParquet
                write_intermediate_files=False,  # Don't write new intermediate files
                reuse_existing_intermediate_outputs=True,  # Reuse existing GeoParquet files
                filter_wkt=BOULDER_FILTER_WKT,
            ),
            cfg=TEST_CONFIG,
        )
        result_df = splitter.split()

        segment_count, connector_count = validate_split_results(result_df, "REUSE-GEOPARQUET->GEOPARQUET")

        # Verify counts match the original run
        assert segment_count == TestIntermediateFilesGeoParquet.expected_segment_count, (
            f"Segment count mismatch: got {segment_count}, "
            f"expected {TestIntermediateFilesGeoParquet.expected_segment_count}"
        )
        assert connector_count == TestIntermediateFilesGeoParquet.expected_connector_count, (
            f"Connector count mismatch: got {connector_count}, "
            f"expected {TestIntermediateFilesGeoParquet.expected_connector_count}"
        )
