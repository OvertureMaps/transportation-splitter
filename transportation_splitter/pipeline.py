"""Main Spark pipeline for splitting transportation segments."""

import logging
import traceback
from timeit import default_timer as timer

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, size, struct, udf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    Row,
    StringType,
    StructField,
    StructType,
)
from shapely.geometry import LineString
from transportation_splitter._pipeline_helpers import (
    SEGMENT_LENGTH_COLUMN,
    add_segment_length_column,
    get_aggregated_metrics,
    get_split_segment_dict,
    join_segments_with_connectors,
)
from transportation_splitter._resolve_refs import (
    resolve_destinations_references,
    resolve_tr_references,
)
from transportation_splitter._spark_udfs import (
    additional_fields_in_split_segments,
    flattened_tr_info_schema,
    get_columns_with_struct_field_name,
    get_filtered_columns,
    resolved_destinations_schema,
    resolved_prohibited_transitions_schema,
)
from transportation_splitter.config import (
    DEFAULT_CFG,
    DESTINATIONS_COLUMN,
    LR_SCOPE_KEY,
    PROHIBITED_TRANSITIONS_COLUMN,
    SplitConfig,
)
from transportation_splitter.geometry import are_different_coords, split_line
from transportation_splitter.linear_reference import (
    add_lr_split_points,
    get_connector_split_points,
    get_lrs,
)
from transportation_splitter.wrangler import SplitterDataWrangler, SplitterStep

logger = logging.getLogger(__name__)


# Re-export commonly used items
__all__ = [
    "OvertureTransportationSplitter",
    "join_segments_with_connectors",
    "get_aggregated_metrics",
]


class OvertureTransportationSplitter:
    """
    Splits Overture transportation segments into simpler sub-segments.

    This class processes GeoParquet files containing road segments and connectors,
    splitting segments at connector points and linear reference boundaries.

    The result is a transportation dataset where segments have exactly two connectors
    (one at each end) and no linear references.

    Example:
        >>> from transportation_splitter import (
        ...     OvertureTransportationSplitter,
        ...     SplitConfig,
        ...     SplitterDataWrangler,
        ... )
        >>>
        >>> splitter = OvertureTransportationSplitter(
        ...     spark=spark_session,
        ...     wrangler=SplitterDataWrangler(
        ...         input_path="path/to/data/*.parquet",
        ...         output_path="output/"
        ...     ),
        ...     cfg=SplitConfig(split_at_connectors=True)
        ... )
        >>>
        >>> # Run the splitting pipeline
        >>> result_df = splitter.split()
        >>>
        >>> # Or with a spatial filter (via wrangler)
        >>> splitter = OvertureTransportationSplitter(
        ...     spark=spark_session,
        ...     wrangler=SplitterDataWrangler(
        ...         input_path="path/to/data/*.parquet",
        ...         output_path="output/",
        ...         filter_wkt="POLYGON(...)"
        ...     ),
        ...     cfg=SplitConfig(split_at_connectors=True)
        ... )
        >>> result_df = splitter.split()
    """

    def __init__(
        self,
        spark: SparkSession,
        wrangler: SplitterDataWrangler,
        cfg: SplitConfig = DEFAULT_CFG,
    ):
        """
        Initialize the transportation splitter.

        Args:
            spark: SparkSession with Sedona configured
            wrangler: Data wrangler for I/O operations and state management
            cfg: Configuration for the splitting process
        """
        self.spark = spark
        self.sc = spark.sparkContext
        self.wrangler = wrangler
        self.cfg = cfg

    def split(self) -> DataFrame:
        """
        Run the splitting pipeline.

        If a spatial filter is configured via `wrangler.filter_wkt`, only features
        intersecting that polygon will be processed. The filter also causes
        intermediate and output paths to use a `_filtered` suffix, ensuring
        cache isolation from unfiltered runs.

        Returns:
            DataFrame containing split segments and connectors
        """
        logger.info("Starting split pipeline")
        logger.info(f"wrangler: {self.wrangler}")
        logger.info(f"config: {self.cfg}")

        # Step 1: Get input data (wrangler handles spatial filtering if configured)
        filtered_df = self.wrangler.get(self.spark, SplitterStep.read_input)

        lr_columns = get_filtered_columns(
            get_columns_with_struct_field_name(filtered_df, LR_SCOPE_KEY),
            self.cfg.lr_columns_to_include,
            self.cfg.lr_columns_to_exclude,
        )
        logger.info(f"lr_columns_for_splitting: {lr_columns}")

        # Step 2: Join connectors and pre-compute segment length
        joined_df = self._join_and_prepare(filtered_df)

        # Step 3: Split segments
        split_df = self._split_segments(joined_df, lr_columns)

        # Step 4: Format output
        final_df = self._format_output(split_df, filtered_df)

        return final_df

    def _join_and_prepare(self, filtered_df: DataFrame) -> DataFrame:
        """Join segments with connectors and pre-compute segment length.

        Uses wrangler.get() and wrangler.store() for state management.

        Args:
            filtered_df: The filtered input DataFrame

        Returns:
            DataFrame with segments joined with connectors and segment length computed.
        """
        # Try to get cached joined result
        cached = self.wrangler.get(self.spark, SplitterStep.joined)
        if cached is not None:
            # Ensure segment length column exists when reading from cache
            if SEGMENT_LENGTH_COLUMN not in cached.columns:
                cached = add_segment_length_column(cached)
            return cached

        # Need to compute
        joined_df = join_segments_with_connectors(filtered_df)
        # Pre-compute segment length using Sedona's ST_LengthSpheroid
        joined_df = add_segment_length_column(joined_df)

        # Store the result
        return self.wrangler.store(SplitterStep.joined, joined_df)

    def _split_segments(self, joined_df: DataFrame, lr_columns: list[str]) -> DataFrame:
        """Apply the split_segment UDF to each joined segment.

        Uses wrangler.get() and wrangler.store() for state management.

        Args:
            joined_df: The joined DataFrame with connectors
            lr_columns: List of columns containing linear references

        Returns:
            DataFrame with split results.
        """
        # Try to get cached split result
        cached = self.wrangler.get(self.spark, SplitterStep.raw_split)
        if cached is not None:
            return cached

        # Need to compute
        split_df = self._apply_split_udf(joined_df, lr_columns)

        # Store the result
        return self.wrangler.store(SplitterStep.raw_split, split_df)

    def _apply_split_udf(
        self, df: DataFrame, lr_columns_for_splitting: list[str]
    ) -> DataFrame:
        """Apply the split_segment UDF to each joined segment."""
        broadcast_lr_columns = self.sc.broadcast(lr_columns_for_splitting)
        input_fields_to_drop = ["joined_connectors", SEGMENT_LENGTH_COLUMN]
        feature_schema = StructType(
            [f for f in df.schema.fields if f.name not in input_fields_to_drop]
        )
        split_fields = list(feature_schema.fields) + additional_fields_in_split_segments
        if PROHIBITED_TRANSITIONS_COLUMN in df.columns:
            split_fields += [
                StructField("turn_restrictions", flattened_tr_info_schema, True)
            ]

        split_segment_schema = StructType(split_fields)
        cfg = self.cfg  # Capture for closure

        return_schema = StructType(
            [
                StructField("is_success", BooleanType(), nullable=False),
                StructField("error_message", StringType(), nullable=True),
                StructField(
                    "exception_traceback", ArrayType(StringType()), nullable=True
                ),
                StructField("debug_messages", ArrayType(StringType()), nullable=True),
                StructField("elapsed", DoubleType(), nullable=True),
                StructField(
                    "split_segments_rows",
                    ArrayType(split_segment_schema),
                    nullable=True,
                ),
                StructField(
                    "added_connectors_rows", ArrayType(feature_schema), nullable=True
                ),
                StructField("length_before_split", DoubleType(), nullable=True),
                StructField("length_after_split", DoubleType(), nullable=True),
                StructField("length_diff", DoubleType(), nullable=True),
            ]
        )

        @udf(returnType=return_schema)
        def split_segment(input_segment):
            start = timer()
            udf_debug_messages = []
            length_before, length_after = 0.0, 0.0
            try:
                lr_cols = broadcast_lr_columns.value
                split_rows, connector_rows = [], []
                error_message = ""

                udf_debug_messages.append(input_segment.id)
                if not isinstance(input_segment.geometry, LineString):
                    raise Exception(
                        f"geometry type {type(input_segment.geometry)} is not LineString!"
                    )

                segment_dict = input_segment.asDict(recursive=True)
                for field in input_fields_to_drop:
                    segment_dict.pop(field, None)

                # Use pre-computed segment length from Sedona's ST_LengthSpheroid (required)
                if (
                    hasattr(input_segment, SEGMENT_LENGTH_COLUMN)
                    and input_segment[SEGMENT_LENGTH_COLUMN] is not None
                ):
                    segment_length = input_segment[SEGMENT_LENGTH_COLUMN]
                else:
                    raise Exception(
                        f"Missing required {SEGMENT_LENGTH_COLUMN} column. "
                        "Ensure add_segment_length_column() was called before splitting."
                    )
                # Remove segment_length_meters from output dict since it's an internal column
                segment_dict.pop(SEGMENT_LENGTH_COLUMN, None)
                length_before = segment_length

                split_points = get_connector_split_points(
                    input_segment.joined_connectors,
                    input_segment.geometry,
                    segment_length,
                )
                if not cfg.split_at_connectors:
                    split_points = sorted(split_points, key=lambda p: p.lr)
                    split_points = [split_points[0], split_points[-1]]

                lrs_set = set()
                for column in lr_cols:
                    if column in segment_dict and segment_dict[column] is not None:
                        lrs_set.update(get_lrs(segment_dict[column]))

                add_lr_split_points(
                    split_points,
                    sorted(lrs_set),
                    segment_dict["id"],
                    input_segment.geometry,
                    segment_length,
                    cfg.point_precision,
                    cfg.lr_split_point_min_dist_meters,
                )

                sorted_points = sorted(split_points, key=lambda p: p.lr)
                if len(sorted_points) < 2:
                    raise Exception(
                        f"Unexpected split points count: {len(sorted_points)}"
                    )

                split_segments = split_line(input_segment.geometry, sorted_points)
                for seg in split_segments:
                    # Use pre-computed length from split points (lr_meters difference)
                    # instead of calling Python get_length()
                    split_len = seg.length
                    length_after += split_len
                    if not are_different_coords(
                        list(seg.geometry.coords)[0], list(seg.geometry.coords)[-1]
                    ):
                        error_message += f"Invalid segment: {seg.start_split_point.lr}-{seg.end_split_point.lr}"
                    mod_dict = get_split_segment_dict(
                        segment_dict,
                        input_segment.geometry,
                        segment_length,
                        seg,
                        lr_cols,
                        cfg.lr_split_point_min_dist_meters,
                    )
                    split_rows.append(Row(**mod_dict))

                for pt in split_points:
                    if pt.is_lr_added:
                        new_conn = {f.name: None for f in feature_schema.fields}
                        new_conn.update(
                            {"id": pt.id, "type": "connector", "geometry": pt.geometry}
                        )
                        connector_rows.append(Row(**new_conn))

                is_success = True
                exception_tb = []
                if error_message:
                    raise Exception(error_message)
            except Exception as e:
                is_success, error_message = False, str(e)
                exception_tb = traceback.format_exc().splitlines()
                split_rows, connector_rows = [], []

            return (
                is_success,
                error_message,
                exception_tb,
                udf_debug_messages,
                timer() - start,
                split_rows,
                connector_rows,
                length_before,
                length_after,
                length_after - length_before,
            )

        df_struct = df.withColumn(
            "input_segment", struct([col(c) for c in df.columns])
        ).select("id", "input_segment")
        return df_struct.withColumn("split_result", split_segment("input_segment"))

    def _format_output(self, split_df: DataFrame, filtered_df: DataFrame) -> DataFrame:
        """Format the split results into the final output DataFrame.

        Uses wrangler.get() and wrangler.store() for state management.

        Args:
            split_df: The DataFrame with split results
            filtered_df: The original filtered DataFrame

        Note:
            The final output is always regenerated (not reused from cache).
            Only intermediate outputs are cached for convenience.
        """
        flat_df = split_df.select("input_segment", "split_result.*")

        # Try to get cached segment splits
        cached_splits = self.wrangler.get(
            self.spark, SplitterStep.segment_splits_exploded
        )
        if cached_splits is not None:
            final_segments_df = cached_splits
        else:
            # Need to compute
            exploded_df = flat_df.withColumn(
                "split_segment_row", F.explode_outer("split_segments_rows")
            ).drop("split_segments_rows")
            flat_splits_df = exploded_df.select("*", "split_segment_row.*")

            # Store the result
            final_segments_df = self.wrangler.store(
                SplitterStep.segment_splits_exploded, flat_splits_df
            )

        # Process added connectors (always recomputed from split_df)
        added_connectors_df = (
            flat_df.filter(size("added_connectors_rows") > 0)
            .select("added_connectors_rows")
            .withColumn("connector", F.explode("added_connectors_rows"))
            .select("connector.*")
        )

        # Ensure geometry type consistency
        # added_connectors_df = ensure_geometry_udt(added_connectors_df)

        # Combine with existing connectors
        all_connectors_df = (
            filtered_df.filter("type == 'connector'")
            .unionByName(added_connectors_df)
            .select(filtered_df.columns)
        )

        # Resolve turn restrictions
        if PROHIBITED_TRANSITIONS_COLUMN in final_segments_df.columns:
            final_segments_df = resolve_tr_references(final_segments_df)
            all_connectors_df = all_connectors_df.drop(
                PROHIBITED_TRANSITIONS_COLUMN
            ).withColumn(
                PROHIBITED_TRANSITIONS_COLUMN,
                lit(None).cast(resolved_prohibited_transitions_schema),
            )

        # Resolve destinations
        if DESTINATIONS_COLUMN in final_segments_df.columns:
            final_segments_df = resolve_destinations_references(final_segments_df)
            all_connectors_df = all_connectors_df.drop(DESTINATIONS_COLUMN).withColumn(
                DESTINATIONS_COLUMN, lit(None).cast(resolved_destinations_schema)
            )

        # Add extra columns to connectors
        extra_cols = [
            f.name
            for f in additional_fields_in_split_segments
            if f.name != "turn_restrictions"
        ]
        for col_name in extra_cols:
            all_connectors_df = all_connectors_df.withColumn(col_name, lit(None))

        # Combine segments and connectors
        final_df = final_segments_df.select(
            filtered_df.columns + extra_cols
        ).unionByName(all_connectors_df)

        # Store the final output
        final_df = self.wrangler.store(SplitterStep.final_output, final_df)

        if not self.cfg.skip_debug_output:
            type_counts = (
                final_df.groupBy("type").agg(F.count("*").alias("count")).collect()
            )
            logger.info(
                f"Output counts: {[(row.type, row['count']) for row in type_counts]}"
            )
            metrics = get_aggregated_metrics(final_df).collect()
            logger.debug(
                f"Aggregated metrics: {[(row.key, row.value, row.value_count) for row in metrics]}"
            )

        return final_df
