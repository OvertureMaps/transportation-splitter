"""Helper functions for the pipeline module."""

from copy import deepcopy

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, collect_list, count, explode, expr, struct
from pyspark.sql.functions import round as _round
from shapely.geometry import LineString

from transportation_splitter.config import (
    DESTINATIONS_COLUMN,
    PROHIBITED_TRANSITIONS_COLUMN,
)
from transportation_splitter.geometry import get_length_bucket
from transportation_splitter.linear_reference import apply_lr_scope
from transportation_splitter.models import SplitSegment
from transportation_splitter.properties import get_destinations, get_trs

# Column name for pre-computed segment length using Sedona
SEGMENT_LENGTH_COLUMN = "segment_length_meters"

# Number of decimal places for Linear Reference (LR) rounding.
# LR values are unitless fractions (0.0 to 1.0) representing positions along a segment.
# We use 9 decimal places because:
#   1. Higher precision than coordinates (7 decimals) to avoid compounding errors in chained calculations
#   2. Provides sub-millimeter precision on typical road segments (e.g., 1km segment = 0.000001m precision)
#   3. Prevents floating-point artifacts from accumulating during LR recalculation on splits
LR_PRECISION_DECIMALS = 9


def add_segment_length_column(df: DataFrame) -> DataFrame:
    """
    Add a pre-computed segment length column using Sedona's ST_LengthSpheroid.
    https://sedona.apache.org/latest/api/sql/Function/#st_lengthspheroid

    The ST_LengthSpheroid function calculates the geodesic length of a geometry
    on the WGS84 spheroid, returning the length in meters.

    Args:
        df: DataFrame with a 'geometry' column containing LineString geometries

    Returns:
        DataFrame with an additional 'segment_length_meters' column
    """
    return df.withColumn(SEGMENT_LENGTH_COLUMN, expr("ST_LengthSpheroid(geometry)"))


def join_segments_with_connectors(input_df: DataFrame) -> DataFrame:
    """
    Join segment features with their connector geometries.

    For planet-scale data, this function disables broadcast joins to prevent
    memory issues when the connectors DataFrame is too large to broadcast.
    """
    segments_df = input_df.filter(col("type") == "segment").withColumnRenamed("id", "segment_id")
    connectors_df = (
        input_df.filter(col("type") == "connector")
        .withColumnRenamed("id", "connector_id")
        .withColumnRenamed("geometry", "connector_geometry")
    )
    segments_with_index = segments_df.withColumn(
        "connectors_with_index",
        F.expr("TRANSFORM(connectors, (c, i) -> STRUCT(c.connector_id AS id, c.at AS at, i AS index))"),
    )
    segments_connectors_exploded = segments_with_index.select(
        col("segment_id"),
        explode("connectors_with_index").alias("connector_with_index"),
    ).select(
        col("segment_id"),
        col("connector_with_index.id").alias("connector_id"),
        col("connector_with_index.at").alias("connector_at"),
        col("connector_with_index.index").alias("connector_index"),
    )

    # Use shuffle hash join hint to prevent broadcast join failures on large datasets
    # The /*+ SHUFFLE_HASH */ hint forces a shuffle-based join instead of broadcast
    joined_df = (
        segments_connectors_exploded.hint("shuffle_hash")
        .join(
            connectors_df,
            segments_connectors_exploded.connector_id == connectors_df.connector_id,
            "left",
        )
        .select(
            segments_connectors_exploded.segment_id,
            segments_connectors_exploded.connector_id,
            segments_connectors_exploded.connector_at,
            segments_connectors_exploded.connector_index,
            connectors_df.connector_geometry,
        )
    )

    aggregated_connectors = joined_df.groupBy("segment_id").agg(
        collect_list(
            struct(
                col("connector_id"),
                col("connector_geometry"),
                col("connector_at"),
                col("connector_index"),
            )
        ).alias("joined_connectors")
    )

    # Also use shuffle_hash for the final join to avoid broadcast issues
    final_df = (
        segments_df.hint("shuffle_hash")
        .join(aggregated_connectors, on="segment_id", how="left")
        .withColumnRenamed("segment_id", "id")
    )
    return final_df


def get_connector_dict(connector_id: str, lr: float) -> dict:
    """Create a connector dictionary."""
    return {"connector_id": connector_id, "at": lr}


def get_connectors_for_split(
    split_segment: SplitSegment,
    original_connectors: list[dict],
    original_segment_length: float,
) -> list[dict]:
    """
    Get the list of connectors for a split segment.

    Args:
        split_segment: The split segment containing start and end split points
        original_connectors: List of connector dictionaries from the original segment
        original_segment_length: Length of the original segment in meters

    Returns:
        List of connector dictionaries with recalculated LR positions for the split
    """
    connectors_for_split = [
        get_connector_dict(p.id, p.lr) for p in [split_segment.start_split_point, split_segment.end_split_point]
    ]
    connectors_for_split += [
        c
        for c in original_connectors
        if c["at"] > split_segment.start_split_point.lr
        and c["at"] < split_segment.end_split_point.lr
        and not any(x["connector_id"] == c["connector_id"] for x in connectors_for_split)
    ]
    connectors_for_split = sorted(connectors_for_split, key=lambda c: c["at"])
    for c in connectors_for_split:
        if c["at"] is not None:
            c["at"] = round(
                (c["at"] * original_segment_length - split_segment.start_split_point.lr_meters) / split_segment.length,
                LR_PRECISION_DECIMALS,
            )
    return connectors_for_split


def get_split_segment_dict(
    original_segment_dict: dict,
    original_segment_geometry: LineString,
    original_segment_length: float,
    split_segment: SplitSegment,
    lr_columns_for_splitting: list[str],
    lr_min_overlap_meters: float,
) -> dict:
    """
    Create a dictionary for a split segment.

    Args:
        original_segment_dict: Dictionary containing the original segment properties
        original_segment_geometry: LineString geometry of the original segment
        original_segment_length: Length of the original segment in meters
        split_segment: The split segment containing geometry and split points
        lr_columns_for_splitting: List of column names that contain LR-scoped properties
        lr_min_overlap_meters: Minimum overlap in meters for LR properties to be included

    Returns:
        Dictionary containing the split segment properties with recalculated LRs
    """
    modified_segment_dict = deepcopy(original_segment_dict)
    modified_segment_dict["start_lr"] = float(split_segment.start_split_point.lr)
    modified_segment_dict["end_lr"] = float(split_segment.end_split_point.lr)
    modified_segment_dict["metrics"] = {}
    modified_segment_dict["metrics"]["length"] = get_length_bucket(split_segment.length)
    if not original_segment_geometry.is_simple:
        modified_segment_dict["metrics"]["original_self_intersecting"] = "true"
    if not split_segment.geometry.is_simple:
        modified_segment_dict["metrics"]["split_self_intersecting"] = "true"
    modified_segment_dict["geometry"] = split_segment.geometry
    modified_segment_dict["connectors"] = get_connectors_for_split(
        split_segment, modified_segment_dict["connectors"], original_segment_length
    )
    if "connector_ids" in modified_segment_dict:
        modified_segment_dict["connector_ids"] = [c["connector_id"] for c in modified_segment_dict["connectors"]]
    for column in lr_columns_for_splitting:
        if column not in modified_segment_dict or modified_segment_dict[column] is None:
            continue
        modified_segment_dict[column] = apply_lr_scope(
            modified_segment_dict[column],
            split_segment,
            original_segment_length,
            lr_min_overlap_meters,
        )

    if PROHIBITED_TRANSITIONS_COLUMN in modified_segment_dict:
        prohibited_transitions = modified_segment_dict.get(PROHIBITED_TRANSITIONS_COLUMN) or {}
        (
            modified_segment_dict[PROHIBITED_TRANSITIONS_COLUMN],
            modified_segment_dict["turn_restrictions"],
        ) = get_trs(prohibited_transitions, modified_segment_dict["connectors"])

    if DESTINATIONS_COLUMN in original_segment_dict:
        destinations = modified_segment_dict.get(DESTINATIONS_COLUMN)
        modified_segment_dict[DESTINATIONS_COLUMN] = get_destinations(destinations, modified_segment_dict["connectors"])

    return modified_segment_dict


def get_aggregated_metrics(result_df: DataFrame) -> DataFrame:
    """Get aggregated metrics from split results."""
    segments_df = result_df.filter("type='segment'")
    total_row_count = segments_df.count()
    metrics_df = (
        result_df.select(col("id"), explode(col("metrics")).alias("key", "value"))
        .groupBy("key", "value")
        .agg(count("value").alias("value_count"))
    )
    metrics_df = metrics_df.withColumn("percentage", _round((col("value_count") / total_row_count) * 100, 2))
    return metrics_df.orderBy("key", "value")
