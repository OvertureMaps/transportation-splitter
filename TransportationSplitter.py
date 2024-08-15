# Databricks notebook source
# MAGIC %md
# MAGIC Please see instructions and details [here](https://github.com/OvertureMaps/transportation-splitter/blob/main/README.md).
# MAGIC
# MAGIC # AWS Glue notebook - see instructions for magic commands

# COMMAND ----------

from collections import deque
from pyspark.sql.functions import expr, lit, col, explode, collect_list, struct, udf, struct, count, when, size, split, element_at, coalesce, round as _round
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, functions as F
import pyproj
from shapely.geometry import Point, LineString
from shapely import wkt, wkb, ops, equals
from timeit import default_timer as timer
from typing import NamedTuple, Optional, Any
import traceback
import json
import os
import unittest

SPLIT_AT_CONNECTORS = True
PROHIBITED_TRANSITIONS_COLUMN = "prohibited_transitions"
DESTINATIONS_COLUMN = "destinations"

# List of columns that are considered for identifying the LRs values to split at is constructed at runtime out of input parquet's schema columns that have LR_SCOPE_KEY = "between" anywhere in their structure. The include/exclude constants below are applied to that list.
# Explicit list of column names to use for finding LR "between" values to split at. Set to None or empty list to use all columns that have "between" fields.
LR_COLUMNS_TO_INCLUDE = []
# list columns to exclude from finding the LR values to split at. Set to None or empty list to use all columns that have "between" fields.
LR_COLUMNS_TO_EXCLUDE = [] 

LR_SCOPE_KEY = "between"
POINT_PRECISION = 7
LR_SPLIT_POINT_MIN_DIST_METERS = 0.01 # New split points are needed for linear references. How far in meters from other existing splits (from either connectors or other LRs) do these LRs need to be for us to create a new connector for them instead of using the existing connector.
IS_ON_SUB_SEGMENT_THRESHOLD_METERS = 0.001 # 1mm -> max distance from a connector point to a sub-segment for the connector to be considered "on" that sub-segment

class SplitPoint:
    """POCO to represent a segment split point."""
    def __init__(self, id=None, geometry=None, lr=None, lr_meters=None, is_lr_added=False, at_coord_idx=None):
        self.id = id
        self.geometry = geometry
        self.lr = lr
        self.lr_meters = lr_meters
        self.is_lr_added = is_lr_added
        self.at_coord_idx = at_coord_idx

    def __repr__(self):
        return f"SplitPoint(at_coord_idx={str(self.at_coord_idx).rjust(3)}, geometry={str(self.geometry).ljust(40)}, lr={str(self.lr).ljust(22)}) ({str(self.lr_meters).ljust(22)}m), is_lr_added={self.is_lr_added}"

class SplitSegment:
    """POCO to represent a split segment."""
    def __init__(self, id=None, geometry=None, start_split_point=None, end_split_point=None):
        self.id = id
        self.geometry = geometry
        self.start_split_point = start_split_point
        self.end_split_point = end_split_point

    def __repr__(self):
        return f"SplitSegment(id={self.id}, @{str(self.start_split_point.lr).ljust(20)} -{str(self.end_split_point.lr).rjust(20)} length={str(self.length).rjust(22)}, geometry={str(self.geometry)})"
    
    @property
    def length(self) -> float:
        return self.end_split_point.lr_meters - self.start_split_point.lr_meters


def read_parquet(spark, path, merge_schema=False):
    return spark.read.option("mergeSchema", str(merge_schema).lower()).parquet(path)

def is_geoparquet(spark, input_path, limit=1, geometry_column="geometry", merge_schema=False):
    try:
        sample_data = spark.read.format("geoparquet").option("mergeSchema", str(merge_schema).lower()).load(input_path).limit(limit)
        geometry_column_data_type = sample_data.schema[geometry_column].dataType
        # GeoParquet uses GeometryType, and WKB uses BinaryType
        return str(geometry_column_data_type) == "GeometryType()"
    except Exception as e:
        # read_geoparquet would throw an exception if it's not a geoparquet file.
        # Assume it's parquet format here.
        return False


def read_geoparquet(spark, path, merge_schema=True, geometry_column="geometry"):
    if is_geoparquet(spark, path, geometry_column=geometry_column):
        return spark.read.format("geoparquet").option("mergeSchema", str(merge_schema).lower()).load(path)
    else:
        return spark.read.option("mergeSchema", str(merge_schema).lower()).parquet(path) \
            .withColumn(geometry_column, expr("ST_GeomFromWKB(geometry)"))

def write_geoparquet(df, path):
    # partitionBy('theme','type') used to create the subfolder structure like parquet/theme=*/type=*/
    # maxRecordsPerFile is used to control single file containing max 10m rows to control the file size, will ensure that your output files don't exceed a certain number of rows, but only a single task will be able to write out these files serially. One task will have to work through the entire data partition, instead of being able to write out that large data partition with multiple tasks.
    #.option("maxRecordsPerFile", 10000000) \
    # parquet.block.size is used to control the row group size for parquet file
    df.write.format("geoparquet") \
        .option("compression", "zstd") \
        .option("parquet.block.size", 16 * 1024 * 1024) \
        .mode("overwrite").save(path)

def parquet_exists(spark, path):
    try:
        spark.read.format("parquet").load(path).limit(0)
        return True
    except AnalysisException:
        # If an AnalysisException is thrown, it likely means the path does not exist or is inaccessible
        return False

def contains_field_name(data_type, field_name):
    if isinstance(data_type, StructType):
        for field in data_type.fields:
            if field.name == field_name or contains_field_name(field.dataType, field_name):
                return True
    elif isinstance(data_type, ArrayType):
        return contains_field_name(data_type.elementType, field_name)
    
    return False                    

def get_columns_with_struct_field_name(df, field_name):
    return [field.name for field in df.schema.fields if contains_field_name(field.dataType, field_name)]

def get_filtered_columns(existing_columns, columns_to_include, columns_to_exclude):
    # Convert lists to sets for set operations
    existing_columns_set = set(existing_columns)
    include_set = set(columns_to_include) if columns_to_include else set()
    exclude_set = set(columns_to_exclude) if columns_to_exclude else set()

    if not include_set.issubset(existing_columns_set):
        missing_columns = include_set - existing_columns_set
        raise ValueError(f"The following columns to include are not present in existing columns: {missing_columns}")

    # Find common elements
    common_elements = existing_columns_set & include_set if include_set else existing_columns_set

    # Remove excluded elements
    result_set = common_elements - exclude_set

    # Convert result back to list
    return list(result_set)    
    
def sanitize_wkt(wkt_str):
    geom = wkt.loads(wkt_str)  # This will throw an error if the WKT is invalid.
    # Escape single quotes by replacing them with two single quotes
    return str(geom).replace("'", "''")

def filter_df(input_df, filter_wkt, condition_function = "ST_Intersects"):
    filter_expression = f"{condition_function}(ST_GeomFromWKT('{sanitize_wkt(filter_wkt)}'), geometry) = true"
    return input_df.filter(expr(filter_expression))

def join_segments_with_connectors(input_df):
    segments_df = input_df.filter(col("type") == "segment").withColumnRenamed("id", "segment_id")
    connectors_df = input_df.filter(col("type") == "connector")\
        .withColumnRenamed("id", "connector_id")\
        .withColumnRenamed("geometry", "connector_geometry")\

    segments_with_index = segments_df.withColumn(
        "connector_ids_with_index",
        F.expr("TRANSFORM(connector_ids, (x, i) -> STRUCT(x AS id, i AS index))")
    )
    segments_connectors_exploded = segments_with_index.select(
        col("segment_id"),
        explode("connector_ids_with_index").alias("connector_with_index")
    ).select(
        col("segment_id"),
        col("connector_with_index.id").alias("connector_id"),
        col("connector_with_index.index").alias("connector_index")
    )

    # Step 2: Join with connectors_df to get connector geometry
    joined_df = segments_connectors_exploded.join(
        connectors_df,
        segments_connectors_exploded.connector_id == connectors_df.connector_id,
        "left"
    ).select(
        segments_connectors_exploded.segment_id,
        segments_connectors_exploded.connector_id,
        segments_connectors_exploded.connector_index,
        connectors_df.connector_geometry
    )

    # Step 3: Group by segment_id and aggregate connector information
    aggregated_connectors = joined_df.groupBy("segment_id").agg(
        collect_list(
            struct(
                col("connector_id"),
                col("connector_geometry"),
                col("connector_index")
            )
        ).alias("joined_connectors")
    )

    # Step 4: Join aggregated connector information back to the original segments_df
    final_df = segments_df.join(
        aggregated_connectors,
        on="segment_id",
        how="left"
    ).withColumnRenamed("segment_id", "id")#.cache()
    return final_df

def has_consecutive_dupe_coords(line: LineString) -> bool:
    coordinates = list(line.coords)
    return any(coordinates[i] == coordinates[i - 1] for i in range(1, len(coordinates)))

def get_split_line_geometry(coordinates):
    filtered_coords = [coordinates[i] for i in range(len(coordinates)) if i == 0 or coordinates[i] != coordinates[i - 1]]
    return LineString(filtered_coords)

def are_different_coords(coords1, coords2):
    return coords1[0] != coords2[0] or coords1[1] != coords2[1]

def round_point(point, precision=POINT_PRECISION):
    return Point(round(point.x, precision), round(point.y, precision))

def split_line(original_line_geometry: LineString, split_points: list[SplitPoint] ) -> list[SplitSegment]:
    """Split the LineString into segments at the given points"""

    # Special case to avoid processing when there are only start/end split points
    if len(split_points) == 2 and split_points[0].lr == 0 and split_points[1].lr == 1:
        return [SplitSegment(0, original_line_geometry, split_points[0], split_points[1])]

    split_segments = []
    i=0
    for split_point_start, split_point_end in zip(split_points[:-1], split_points[1:]):
        idx_start = split_point_start.at_coord_idx + 1
        idx_end = split_point_end.at_coord_idx + 1
        coords = \
            list(split_point_start.geometry.coords) +\
            original_line_geometry.coords[idx_start:idx_end] +\
            list(split_point_end.geometry.coords)

        geom = get_split_line_geometry(coords)
        split_segments.append(SplitSegment(i, geom, split_point_start, split_point_end))
        i += 1
    return split_segments

def get_lrs(x):
    output = [0, 1]
    if isinstance(x, list):
        for sub_item in x:
            if sub_item is None:
                continue
            for lr in get_lrs(sub_item):
                if lr and lr not in output:
                    output.append(lr)
        return output
    elif isinstance(x, dict):
        if LR_SCOPE_KEY in x and x[LR_SCOPE_KEY] is not None:
            for lr in x[LR_SCOPE_KEY]:
                if lr and lr not in output:
                    output.append(lr)
        for key in x.keys():
            if x[key] is None:
                continue
            for lr in get_lrs(x[key]):
                if lr and lr not in output:
                    output.append(lr)
    return output

def is_lr_scope_applicable_to_split(between, split_segment: SplitSegment, original_segment_length_meters, tolerance_meters=LR_SPLIT_POINT_MIN_DIST_METERS) -> bool:
    lr_start_meters = (between[0] if between[0] else 0) * original_segment_length_meters
    lr_end_meters = (between[1] if between[1] else 1) * original_segment_length_meters
    return (
        # either the between LR is completely covered by the split's LR:
        (lr_start_meters >= split_segment.start_split_point.lr_meters - tolerance_meters and 
        lr_end_meters <= split_segment.end_split_point.lr_meters + tolerance_meters) or 
        # OR the between LR completely covers the split's LR:
        (lr_start_meters <= split_segment.start_split_point.lr_meters + tolerance_meters and 
        lr_end_meters >= split_segment.end_split_point.lr_meters - tolerance_meters)
    )

def apply_lr_on_split(
        original_lr: list[float], 
        split_segment: SplitSegment, 
        original_segment_length, 
        min_overlapping_length_meters=LR_SPLIT_POINT_MIN_DIST_METERS) -> tuple[bool, Optional[list[float]]]:
    split_length = split_segment.length
    lr_start_meters = (original_lr[0] if original_lr[0] else 0) * original_segment_length
    lr_end_meters = (original_lr[1] if original_lr[1] else 1) * original_segment_length
    
    # make LRs relative to this split
    lr_start_within_split_meters = lr_start_meters - split_segment.start_split_point.lr_meters
    lr_end_within_split_meters = lr_end_meters - split_segment.start_split_point.lr_meters
    
    # [--|---(--|---)----]
    # 0  10  25 30  45  60
    # between = [25m to 45m]
    # split = |10m to 30m|
    # new between = [-15m to 35]

    # part overlapping with the split:
    overlapping_start_meters = max(lr_start_within_split_meters, 0)
    overlapping_end_meters = min(lr_end_within_split_meters, split_length)

    overlapping_length_meters = overlapping_end_meters - overlapping_start_meters
    if overlapping_length_meters < min_overlapping_length_meters:
        return (False, None)

    new_lr = (
        None if split_length - overlapping_length_meters < min_overlapping_length_meters # set new LR to null if it covers the whole split within tolerance
        else [overlapping_start_meters / split_length, overlapping_end_meters / split_length] # new LR relative to split legth
    )

    return (True, new_lr)

def apply_lr_scope(
        x: Any, 
        split_segment: SplitSegment, 
        original_segment_length:float, 
        xpath:str = "$", 
        min_overlapping_length_meters:float=LR_SPLIT_POINT_MIN_DIST_METERS):
    if x is None:
        return None

    #print(f"apply_lr_scope({json.dumps(x)}, {xpath})")
    if isinstance(x, list):
        output = []
        for index, sub_item in enumerate(x):
            cleaned_sub_item = apply_lr_scope(sub_item, split_segment, original_segment_length, f"{xpath}[{index}]")
            if cleaned_sub_item is not None:
                output.append(cleaned_sub_item)
        return output if output else None
    elif isinstance(x, dict):
        output = {}
        if LR_SCOPE_KEY in x and x[LR_SCOPE_KEY] is not None:
            original_lr = x[LR_SCOPE_KEY]
            if not isinstance(original_lr, list):
                raise Exception(f"{xpath}.{LR_SCOPE_KEY} is of type {str(type(original_lr))}, expecting list!")
            if len(original_lr) != 2:
                raise Exception(f"{xpath}.{LR_SCOPE_KEY} has {str(len(original_lr))} items, expecting 2 for a LR!")

            is_applicable, new_lr = apply_lr_on_split(original_lr, split_segment, original_segment_length, min_overlapping_length_meters)
            if not is_applicable:
                return None
            if new_lr:
                output[LR_SCOPE_KEY] = new_lr

        for key in x.keys():
            if key == LR_SCOPE_KEY:
                continue
            clean_sub_prop = apply_lr_scope(x[key], split_segment, original_segment_length, f"{xpath}.{key}")
            if clean_sub_prop is not None:
                output[key] = clean_sub_prop

        return output if output else None
    else:
        return x

def get_trs(turn_restrictions, connector_ids):
    # extract TR references structure;
    # this will be used after split is complete to identify for each segment_id reference which of the splits of the original segment_id to use;
    # this step includes pruning out the TRs that don't apply for this split - we check that the TR's first connector id appears in the correct index in connector_ids corresponding to the TR's heading scope (for forward: index=1, for backward: index=0)
    if turn_restrictions is None:
        return None, None

    flattened_tr_seq_items = []
    trs_to_keep = []
    for tr in turn_restrictions:
        tr_heading = (tr.get("when") or {}).get("heading")
        tr_sequence = tr.get("sequence")
        if not tr_sequence or len(tr_sequence) == 0:
            continue

        if not connector_ids or len(connector_ids) != 2:
            # at this point modified segments are expected to have exactly two connector ids, skip edge cases that don't
            continue

        first_connector_id_ref = tr_sequence[0].get("connector_id")

        if tr_heading == "forward" and connector_ids[1] != first_connector_id_ref:
            # the second connector id on this segment split needs to match the first connector id in the sequence because heading scope applies only to forward
            continue

        if tr_heading == "backward" and connector_ids[0] != first_connector_id_ref:
            # the first connector id on this segment split needs to match the first connector id in the sequence because heading scope applies only to backward
            continue

        if first_connector_id_ref not in connector_ids:
            # the first connector id in the sequence needs to be in this split's connector_ids no matter what
            continue

        tr_idx = len(trs_to_keep)
        trs_to_keep.append(tr)

        for seq_idx, seq in enumerate(tr_sequence):
            flattened_tr_seq_items.append({
                "tr_index": tr_idx,
                "sequence_index": seq_idx,
                "segment_id": seq.get("segment_id"),
                "connector_id": seq.get("connector_id"),
                "next_connector_id": tr_sequence[seq_idx + 1]["connector_id"] if seq_idx + 1 < len(tr_sequence) else None,
                "final_heading": tr.get("final_heading") # final_heading is required, so it should always be not null
            })

    if len(trs_to_keep) == 0:
        return None, None

    return trs_to_keep, flattened_tr_seq_items

def get_destinations(destinations, connector_ids):
    if destinations is None:
        return None    
    destinations_to_keep = [d for d in destinations if destination_applies_to_split_connectors(d, connector_ids)]     
    return destinations_to_keep if destinations_to_keep else None

def destination_applies_to_split_connectors(d, connector_ids):
    if not connector_ids or len(connector_ids) != 2:
        # at this point modified segments are expected to have exactly two connector ids, skip edge cases that don't
        return False
    when_heading = d.get("when", {}).get("heading")
    from_connector_id = d.get("from_connector_id")
    if not from_connector_id or not when_heading:
        #these are required properties and need them to exist to resolve the split reference
        return False
    if when_heading == "forward" and connector_ids[1] != from_connector_id:
        # the second connector id on this segment split needs to match the from_connector_id in the destination because heading scope applies only to forward
        return False
    if when_heading == "backward" and connector_ids[0] != from_connector_id:
        # the first connector id on this segment split needs to match the from_connector_id in the destination because heading scope applies only to backward
        return False
    if from_connector_id not in connector_ids:
        # the from_connector_id needs to be in this split's connector_ids no matter what
        return False
    return True

def get_length_bucket(length):
    if length <= 0.01:
        return "A. <=1cm"
    if length <= 0.1:
        return "B. 1cm-10cm"
    if length <= 1:
        return "C. 10cm-1m"
    if length <= 100:
        return "D. 1m-100m"
    if length <= 1000:
        return "E. 100m-1km"
    if length <= 10000:
        return "F. 1km-10km"
    return "G. >10km"

def get_split_segment_dict(original_segment_dict, original_segment_geometry, original_segment_length, split_segment, lr_columns_for_splitting):
    modified_segment_dict = original_segment_dict.copy()
    #debug_messages.append("type(start_lr)=" + str(type(split_segment.start_split_point.lr)))
    #debug_messages.append("type(geometry)=" + str(type(wkb.dumps(split_segment.geometry))))
    modified_segment_dict["start_lr"] = float(split_segment.start_split_point.lr)
    modified_segment_dict["end_lr"] = float(split_segment.end_split_point.lr)
    modified_segment_dict["metrics"] = {}
    modified_segment_dict["metrics"]["length"] = get_length_bucket(split_segment.length)
    if not original_segment_geometry.is_simple:
        modified_segment_dict["metrics"]["original_self_intersecting"] = "true"
    if not split_segment.geometry.is_simple:
        modified_segment_dict["metrics"]["split_self_intersecting"] = "true"    
    modified_segment_dict["geometry"] = split_segment.geometry
    modified_segment_dict["connector_ids"] = [split_segment.start_split_point.id, split_segment.end_split_point.id]
    for column in lr_columns_for_splitting:
        if column not in modified_segment_dict or modified_segment_dict[column] is None:
            continue
        modified_segment_dict[column] = apply_lr_scope(modified_segment_dict[column], split_segment, original_segment_length)

    if PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
        # remove turn restrictions that we're sure don't apply to this split and construct turn_restrictions field -
        # this is a flat list of all segment_id referenced in sequences;
        # used to resolve segment references after split - for each TR segment_id reference we need to identify which of the splits to retain as reference
        prohibited_transitions = modified_segment_dict.get(PROHIBITED_TRANSITIONS_COLUMN) or {}
        modified_segment_dict[PROHIBITED_TRANSITIONS_COLUMN], modified_segment_dict["turn_restrictions"] = get_trs(prohibited_transitions, modified_segment_dict["connector_ids"])

    if DESTINATIONS_COLUMN in original_segment_dict:
        destinations = modified_segment_dict.get(DESTINATIONS_COLUMN)
        modified_segment_dict[DESTINATIONS_COLUMN] = get_destinations(destinations, modified_segment_dict["connector_ids"])

    return modified_segment_dict

def is_distinct_split_lr(lr1, lr2, segment_length, min_dist_meters=LR_SPLIT_POINT_MIN_DIST_METERS):
    return abs(lr1 - lr2) * segment_length > min_dist_meters

def get_length(line_geometry: LineString):
    geod = pyproj.Geod(ellps="WGS84")
    return geod.geometry_length(line_geometry)

def add_lr_split_points(split_points, lrs, segment_id, original_segment_geometry, line_length):
    """
    Split a line at linear reference points along its length

    This uses the pyproj.Geod object to find a point located at the specified
    length along a segment. We do this by iterating over pairs of coordinates
    along the line, which are called "subsegments". For each subsegment we use
    Geod.inv() which returns the length of the segment and the azimuth from the
    first point to the second point of the subsegment. We accumulate the lengths
    until we reach the subsegment that contains the target point. We now know
    the length along this subsegment where the point is located. We use
    Geod.fwd() which takes a starting coordinate (the first point of the
    subsegment), the azimuth, and a distance to return the final point.

    """

    if not lrs:
        return split_points

    # Get the length of the projected segment
    geod = pyproj.Geod(ellps="WGS84")
    line_length = geod.geometry_length(original_segment_geometry)
    coords = list(original_segment_geometry.coords)

    for lr in lrs:
        add_split_point = False
        if len(split_points) == 0:
            add_split_point = True
        else:
            closest_existing_split_point = min(split_points, key=lambda existing: abs(lr - existing.lr))
            if is_distinct_split_lr(closest_existing_split_point.lr, lr, line_length):
                add_split_point = True

        if add_split_point:
            target_length = lr * line_length
            coord_idx = 0 # remember the index of the original geometry's coordinate
            for (lon1, lat1), (lon2, lat2) in zip(coords[:-1], coords[1:]):
                (azimuth, _, subsegment_length) = geod.inv(lon1, lat1, lon2, lat2, return_back_azimuth=False)
                if round(target_length - subsegment_length, 6) <= 0:
                    # Compute final point on this subsegment
                    break
                target_length -= subsegment_length
                coord_idx += 1

            # target_length is the length along this subsegment where the point is located. Use geod.fwd()
            # with the azimuth to get the final point
            split_lon, split_lat, _ = geod.fwd(lon1, lat1, azimuth, target_length, return_back_azimuth=False)

            point_geometry = round_point(Point(split_lon, split_lat))
            # see if after rounding we have a point identical to last one;
            # if yes, then don't add a new split point; if we did, we would end up with invalid lines 
            # that start and end in the same point, or, more accurately,  because we have a step that 
            # removes consecutive identical coordinates from splits, it would result in a line with a 
            # single point, which would be invalid.
            # this effectively is an additional tollerance on top of LR_SPLIT_POINT_MIN_DIST_METERS,
            # which is controlled by the rouding parameter POINT_PRECISION, default value=7, which is ~centimeter size
            if not split_points or are_different_coords(split_points[-1].geometry.coords[0], point_geometry.coords[0]):
                split_points.append(SplitPoint(f"{segment_id}@{str(lr)}", point_geometry, lr, lr_meters=lr * line_length, is_lr_added=True, at_coord_idx=coord_idx))
    return split_points

def get_connector_split_points(connectors, original_segment_geometry, original_segment_length):
    split_points = []
    if not connectors:
        return split_points

    # Get the length of the projected segment
    geod = pyproj.Geod(ellps="WGS84")
    original_segment_coords = list(original_segment_geometry.coords)
    sorted_valid_connectors = sorted([c for c in connectors if c.connector_geometry], key=lambda p: p.connector_index)
    connectors_queue = deque(sorted_valid_connectors)
        
    # Pass 1 - connector coordinates have exact match in the segment's coordinates
    coord_count = len(original_segment_coords)
    
    # edge case first - if last connector matches the last coordinate then LR should be 1,
    # no matter if the same coordinate may also appear somewhere else in the middle of the segment
    last_connector = connectors_queue[-1]
    if not are_different_coords(last_connector.connector_geometry.coords[0], original_segment_coords[-1]):
        split_points.append(SplitPoint(last_connector.connector_id, last_connector.connector_geometry, lr=1, lr_meters=original_segment_length, is_lr_added=False, at_coord_idx=coord_count-1))
        connectors_queue.pop()

    lr_meters = 0
    for coord_idx in range(0, coord_count):
        for connector in list(connectors_queue):
            connector_geometry = connector.connector_geometry
            if not are_different_coords(connector_geometry.coords[0], original_segment_coords[coord_idx]):
                lr = lr_meters / original_segment_length
                split_points.append(SplitPoint(connector.connector_id, connector_geometry, lr, lr_meters, is_lr_added=False, at_coord_idx=coord_idx))
                connectors_queue.remove(connector)
        if coord_idx < coord_count - 1:
            sub_segment = LineString([original_segment_coords[coord_idx], original_segment_coords[coord_idx+1]])
            sub_segment_length = geod.geometry_length(sub_segment)
            lr_meters += sub_segment_length
    
    if not connectors_queue:
        return split_points

    # Pass 2 - fallback if some connectors don't match exactly the coordinates, 
    # find which consecutive coord pair sub-segment it lies on
    coord_idx = 0 
    accumulated_segment_length = 0
    for (lon1, lat1), (lon2, lat2) in zip(original_segment_coords[:-1], original_segment_coords[1:]):
        sub_segment = LineString([(lon1, lat1), (lon2, lat2)])
        sub_segment_length = geod.geometry_length(sub_segment)
        
        while connectors_queue:
            connector = connectors_queue[0]
            connector_geometry = connector.connector_geometry

            # Calculate the distances between points 1 and 2 of the sub-segment and the connector
            _, _, dist12 = geod.inv(lon1, lat1, lon2, lat2)
            _, _, dist1c = geod.inv(lon1, lat1, connector_geometry.x, connector_geometry.y)
            _, _, dist2c = geod.inv(lon2, lat2, connector_geometry.x, connector_geometry.y)            
                    
            dist_diff = abs(dist1c + dist2c - dist12)
            if dist_diff < IS_ON_SUB_SEGMENT_THRESHOLD_METERS:

                if len(connectors_queue) == 1 and not are_different_coords(connector_geometry.coords[0], original_segment_coords[-1]):
                    # edge case first - if this is the last connector, and it matches the last coordinate then LR should be 1,
                    # no matter if the same coordinate may also appear here, somewhere else in the middle of the segment
                    at_coord_idx = len(original_segment_coords) - 1
                    lr_meters = original_segment_length
                else:
                    offset_on_segment_meters = dist1c
                    at_coord_idx = coord_idx + 1 if offset_on_segment_meters == sub_segment_length else coord_idx
                    lr_meters = accumulated_segment_length + offset_on_segment_meters

                lr = lr_meters / original_segment_length
                split_points.append(SplitPoint(connector.connector_id, connector_geometry, lr, lr_meters, is_lr_added=False, at_coord_idx=at_coord_idx))
                connectors_queue.popleft()
            else:
                # next connector is not on this segment, move to next segment
                break
        coord_idx += 1
        accumulated_segment_length += sub_segment_length

    if connectors_queue:
        raise Exception(f"Could not find split point locations for all connectors")
    return split_points

additional_fields_in_split_segments = [
    StructField("start_lr", DoubleType(), True),
    StructField("end_lr", DoubleType(), True),
    StructField("metrics", MapType(StringType(), StringType()), True)
]
flattened_tr_info_schema = ArrayType(StructType([
    StructField("tr_index", IntegerType(), True),
    StructField("sequence_index", IntegerType(), True),
    StructField("segment_id", StringType(), True),
    StructField("connector_id", StringType(), True),
    StructField("next_connector_id", StringType(), True),
    StructField("final_heading", StringType(), True)
]))


resolved_prohibited_transitions_schema = ArrayType(
    StructType([
        StructField("sequence", ArrayType(
            StructType([
                StructField("connector_id", StringType(), True),
                StructField("segment_id", StringType(), True),
                StructField("start_lr", DoubleType(), True),
                StructField("end_lr", DoubleType(), True),
            ])
        ), True),
        StructField("final_heading", StringType(), True),
        StructField("when", StructType([
            StructField("heading", StringType(), True),
            StructField("during", StringType(), True),
            StructField("using", ArrayType(StringType()), True),
            StructField("recognized", ArrayType(StringType()), True),
            StructField("mode", ArrayType(StringType()), True),
            StructField("vehicle", ArrayType(
                StructType([
                    StructField("dimension", StringType(), True),
                    StructField("comparison", StringType(), True),
                    StructField("value", DoubleType(), True),
                    StructField("unit", StringType(), True)
                ])
            ), True)
        ]), True),
        StructField("between", ArrayType(DoubleType()), True)
    ])
)

resolved_destinations_schema = ArrayType(
    StructType([
        StructField('labels', ArrayType(
            StructType([
                StructField('value', StringType(), True), 
                StructField('type', StringType(), True)
                ]), True), 
            True), 
        StructField('symbols', ArrayType(StringType(), True), True), 
        StructField('from_connector_id', StringType(), True), 
        StructField('to_segment_id', StringType(), True), 
        StructField('to_segment_start_lr', DoubleType(), True), 
        StructField('to_segment_end_lr', DoubleType(), True), 
        StructField('to_connector_id', StringType(), True), 
        StructField('when', StructType([
            StructField('heading', StringType(), True)
            ]), True), 
        StructField('final_heading', StringType(), True)
        ]), False)

def split_joined_segments(df: DataFrame, lr_columns_for_splitting: list[str]) -> DataFrame:
    broadcast_lr_columns_for_splitting = sc.broadcast(lr_columns_for_splitting)    
    input_fields_to_drop_in_splits = ["joined_connectors"]
    transportation_feature_schema = StructType([field for field in df.schema.fields if field.name not in input_fields_to_drop_in_splits])
    split_segment_fields = [field for field in transportation_feature_schema] + additional_fields_in_split_segments
    if PROHIBITED_TRANSITIONS_COLUMN in df.columns:
        split_segment_fields += [ StructField("turn_restrictions", flattened_tr_info_schema, True) ]

    split_segment_schema = StructType(split_segment_fields)

    # Define the return schema including is_success, error_message, and the array of input struct type
    return_schema = StructType([
        StructField("is_success", BooleanType(), nullable=False),
        StructField("error_message", StringType(), nullable=True),
        StructField("exception_traceback", ArrayType(StringType()), nullable=True),
        StructField("debug_messages", ArrayType(StringType()), nullable=True),
        StructField("elapsed", DoubleType(), nullable=True),
        StructField("split_segments_rows", ArrayType(split_segment_schema), nullable=True),
        StructField("added_connectors_rows", ArrayType(transportation_feature_schema), nullable=True)
    ])

    @udf(returnType=return_schema)
    def split_segment(input_segment):
        start = timer()
        debug_messages = []
        try:
            lr_columns_for_splitting = broadcast_lr_columns_for_splitting.value
            split_segments_rows = []
            added_connectors_rows = []
            error_message = ""

            debug_messages.append(input_segment.id)
            if not isinstance(input_segment.geometry, LineString):
                raise Exception(f"geometry type {type(input_segment.geometry)} is not LineString!")

            original_segment_dict = input_segment.asDict(recursive=True)
            for field_to_drop in input_fields_to_drop_in_splits:
                original_segment_dict.pop(field_to_drop, None)

            segment_length = get_length(input_segment.geometry)
            debug_messages.append(str(input_segment.geometry))
            debug_messages.append(f"length={segment_length}")

            split_points = get_connector_split_points(input_segment.joined_connectors, input_segment.geometry, segment_length) if SPLIT_AT_CONNECTORS else []

            debug_messages.append("adding lr split points...")
            lrs = []
            for column in lr_columns_for_splitting:
                debug_messages.append(f"LRs in [{column}]:")
                if column not in original_segment_dict or original_segment_dict[column] is None:
                    continue
                lrs_in_column = get_lrs(original_segment_dict[column])
                for lr in lrs_in_column:
                    debug_messages.append(str(lr))
                lrs = lrs + lrs_in_column

            add_lr_split_points(split_points, lrs, original_segment_dict["id"], input_segment.geometry, segment_length)

            sorted_split_points = sorted(split_points, key=lambda p: p.lr)
            debug_messages.append("sorted final split points:")
            for p in sorted_split_points:
                debug_messages.append(str(p))

            if len(sorted_split_points) < 2:
                raise Exception(f"Unexpected number of split points: {str(len(sorted_split_points))}; (expected at least 2)")

            #debug_messages.append("splitting into segments...")
            split_segments = split_line(input_segment.geometry, sorted_split_points)
            for split_segment in split_segments:
                debug_messages.append(f"{split_segment.start_split_point.lr}-{split_segment.end_split_point.lr}: " + str(split_segment))
                if not are_different_coords(list(split_segment.geometry.coords)[0], list(split_segment.geometry.coords)[-1]):
                    error_message += f"Wrong segment created: {split_segment.start_split_point.lr}-{split_segment.end_split_point.lr}: " + str(split_segment.geometry)
                modified_segment_dict = get_split_segment_dict(original_segment_dict, input_segment.geometry, segment_length, split_segment, lr_columns_for_splitting)
                split_segments_rows.append(Row(**modified_segment_dict))

            for split_point in split_points:
                if not split_point.is_lr_added:
                    continue
                new_connector_dict = {field.name: None for field in transportation_feature_schema.fields}
                new_connector_dict.update({
                    "id": split_point.id,
                    "type": "connector",
                    "geometry": split_point.geometry
                })
                added_connectors_rows.append(Row(**new_connector_dict))
            is_success = True
            exception_traceback = []
            if error_message:
                raise Exception(error_message)
        except Exception as e:
            is_success = False
            error_message = str(e)
            exception_traceback = traceback.format_exc().splitlines() # e
            split_segments_rows = []
            added_connectors_rows = []

        end = timer()
        elapsed = end - start
        return (is_success, error_message, exception_traceback, debug_messages, elapsed, split_segments_rows, added_connectors_rows)

    df_with_struct = df.withColumn("input_segment", struct([col(c) for c in df.columns])).select("id", "input_segment")
    split_segments_df = df_with_struct.withColumn("split_result", split_segment("input_segment"))

    return split_segments_df

def resolve_tr_references(result_df):
    splits_w_trs_df = result_df.filter("turn_restrictions is not null and size(turn_restrictions)>0").select("id", "start_lr", "end_lr", "turn_restrictions").withColumn("tr", F.explode("turn_restrictions")).drop("turn_restrictions").select("*", "tr.*").drop("tr")

    referenced_segment_ids_df = splits_w_trs_df.select(col("segment_id").alias("referenced_segment_id")).distinct()

    referenced_splits_info_df = referenced_segment_ids_df.join(
        result_df,
        result_df.id == referenced_segment_ids_df.referenced_segment_id,
        "inner").select(col("id").alias("ref_id"), col("start_lr").alias("ref_start_lr"), col("end_lr").alias("ref_end_lr"), col("connector_ids").alias("ref_connector_ids"))

    # alternative: just do the select relevant columns on the full corpus then do the inner join
    #referenced_splits_info_df = result_df.select(col("id").alias("ref_id"), col("start_lr").alias("ref_start_lr"), col("end_lr").alias("ref_end_lr"), col("connector_ids").alias("ref_connector_ids"))

    ref_joined_df = splits_w_trs_df.join(
        referenced_splits_info_df,
        splits_w_trs_df.segment_id == referenced_splits_info_df.ref_id,
        "inner"
    ).select(
        splits_w_trs_df["*"],
        referenced_splits_info_df["*"]
    ).drop("segment_id")

    # if next_connector_id is null then it's the last reference in the sequence - in this case final_heading is used - if forward then the referenced connector id is expected to be the first, if backward then second
    # if it's not the last in the sequence, then we need the segment to have references to both the connector in the TR for the current sequence index as well as the next sequence index - so the two connectors must be connector_id and next_connector_id, order not relevant (they could be swapped).
    referenced_split_condition = F.when(
        F.col("next_connector_id").isNull(),
        F.when(F.col("final_heading") == "forward",
            F.col("ref_connector_ids")[0] == F.col("connector_id")).otherwise(
            F.col("ref_connector_ids")[1] == F.col("connector_id"))
    ).otherwise(
        F.array_contains(F.col("ref_connector_ids"), F.col("connector_id")) &
        F.array_contains(F.col("ref_connector_ids"), F.col("next_connector_id"))
    )

    trs_refs_resolved_df = ref_joined_df.filter(referenced_split_condition).select(
        col("id").alias("trs_id"),
        col("start_lr").alias("trs_start_lr"),
        col("end_lr").alias("trs_end_lr"),
        struct("tr_index", "sequence_index", "ref_id", "ref_start_lr", "ref_end_lr").alias("tr_seq"))

    trs_refs_resolved_agg_df = trs_refs_resolved_df.groupBy("trs_id", "trs_start_lr", "trs_end_lr").agg(collect_list("tr_seq").alias("turn_restrictions"))

    result_w_trs_refs_df = result_df.drop("turn_restrictions").join(
        trs_refs_resolved_agg_df,
        (result_df.id == trs_refs_resolved_agg_df.trs_id) & (result_df.start_lr == trs_refs_resolved_agg_df.trs_start_lr) & (result_df.end_lr == trs_refs_resolved_agg_df.trs_end_lr),
        "left").drop("trs_id", "trs_start_lr", "trs_end_lr")

    def apply_tr_split_refs(prohibited_transitions, turn_restrictions):
        if prohibited_transitions is None:
            return None
        
        new_prohibited_transitions = [pt.asDict(recursive=True) for pt in prohibited_transitions]
        if turn_restrictions:
            for tr in turn_restrictions:
                sequence_item_to_update = new_prohibited_transitions[tr["tr_index"]]["sequence"][tr["sequence_index"]]
                sequence_item_to_update["start_lr"] = tr["ref_start_lr"]
                sequence_item_to_update["end_lr"] = tr["ref_end_lr"]

        # return only the trs that did resolved into a full start-end lr references
        resolved = [pt for pt in new_prohibited_transitions
                if "sequence" in pt and all(("start_lr" in seq_item and "end_lr" in seq_item) for seq_item in pt["sequence"])]

        return None if len(resolved) == 0 else [Row(**pt) for pt in resolved]
    
    apply_tr_split_refs_udf = udf(apply_tr_split_refs, resolved_prohibited_transitions_schema)

    result_trs_resolved_df = result_w_trs_refs_df\
        .withColumn(PROHIBITED_TRANSITIONS_COLUMN, apply_tr_split_refs_udf(col(PROHIBITED_TRANSITIONS_COLUMN), col("turn_restrictions")))
    return result_trs_resolved_df

def resolve_destinations_references(result_df):
    splits_w_destinations_df = result_df.filter(f"{DESTINATIONS_COLUMN} is not null and size({DESTINATIONS_COLUMN})>0").select("id", "start_lr", "end_lr", DESTINATIONS_COLUMN).withColumn("dr", F.explode(DESTINATIONS_COLUMN)).select("*", "dr.*").drop("dr")

    referenced_segment_ids_df = splits_w_destinations_df.select(col("to_segment_id").alias("referenced_segment_id")).distinct()

    referenced_splits_info_df = referenced_segment_ids_df.join(
        result_df,
        result_df.id == referenced_segment_ids_df.referenced_segment_id,
        "inner").select(col("id").alias("ref_id"), col("start_lr").alias("to_segment_start_lr"), col("end_lr").alias("to_segment_end_lr"), col("connector_ids").alias("ref_connector_ids"))

    ref_joined_df = splits_w_destinations_df.join(
        referenced_splits_info_df,
        splits_w_destinations_df.to_segment_id == referenced_splits_info_df.ref_id,
        "inner"
    ).select(
        splits_w_destinations_df["*"],
        referenced_splits_info_df["*"]
    ).drop("segment_id")

    referenced_split_condition = F.when(F.col("final_heading") == "forward",
        F.col("ref_connector_ids")[0] == F.col("to_connector_id")).otherwise(
        F.col("ref_connector_ids")[1] == F.col("to_connector_id"))

    destination_refs_resolved_df = ref_joined_df.filter(referenced_split_condition).select(
        col("id").alias("from_id"),
        col("start_lr").alias("from_start_lr"),
        col("end_lr").alias("from_end_lr"),
        struct( "labels", "symbols", "from_connector_id", "to_segment_id","to_segment_start_lr", "to_segment_end_lr", "to_connector_id", "when", "final_heading").alias("d"))

    destination_refs_resolved_agg_df = destination_refs_resolved_df.groupBy("from_id", "from_start_lr", "from_end_lr").agg(collect_list("d").alias(f"{DESTINATIONS_COLUMN}_resolved"))

    result_w_destinations_resolved_df = result_df.drop("destinations").join(
        destination_refs_resolved_agg_df,
        (result_df.id == destination_refs_resolved_agg_df.from_id) & (result_df.start_lr == destination_refs_resolved_agg_df.from_start_lr) & (result_df.end_lr == destination_refs_resolved_agg_df.from_end_lr),
        "left").drop("from_id", "from_start_lr", "from_end_lr").withColumnRenamed(f"{DESTINATIONS_COLUMN}_resolved", DESTINATIONS_COLUMN)
    
    return result_w_destinations_resolved_df

def get_aggregated_metrics(result_df):
    segments_df = result_df.filter("type='segment'")
    total_row_count = segments_df.count()
    print(total_row_count)
    metrics_df = result_df.select(col("id"), explode(col("metrics")).alias("key", "value")).groupBy("key", "value").agg(count("value").alias("value_count"))
    metrics_df = metrics_df.withColumn("percentage", _round((col("value_count") / total_row_count) * 100, 2))
    return metrics_df.orderBy('key', 'value')

def split_transportation(spark, input_path, output_path, filter_wkt=None, reuse_existing_intermediate_outputs=True):
    output_path = output_path.rstrip('/')
    print(f"filter_wkt: {filter_wkt}")
    print(f"input_path: {input_path}")
    print(f"output_path: {output_path}")

    spatially_filtered_path = output_path + "_1_spatially_filtered"
    joined_path = output_path + "_2_joined"
    raw_split_path = output_path + "_3_raw_split"
    segment_splits_exploded_path = output_path + "_4_segments_splits"

    print(f"spatially_filtered_path: {spatially_filtered_path}")
    print(f"joined_path: {joined_path}")
    print(f"raw_split_path: {raw_split_path}")
    print(f"segment_splits_exploded_path: {segment_splits_exploded_path}")

    if filter_wkt is None:
        filtered_df = read_geoparquet(spark, input_path)
    else:
        # Step 1 Filter only features that intersect with given polygon wkt
        if not parquet_exists(spark, spatially_filtered_path) or not reuse_existing_intermediate_outputs:
            input_df = read_geoparquet(spark, input_path)
            print(f"input_df.count() = {str(input_df.count())}")
            print(f"filter_df()...")
            filtered_df = filter_df(input_df, filter_wkt)
            write_geoparquet(filtered_df, spatially_filtered_path)
        else:
            filtered_df = read_geoparquet(spark, spatially_filtered_path)

    print(f"filtered_df.count() = {str(filtered_df.count())}")

    lr_columns_for_splitting = get_filtered_columns(get_columns_with_struct_field_name(filtered_df, LR_SCOPE_KEY), LR_COLUMNS_TO_INCLUDE, LR_COLUMNS_TO_EXCLUDE)
    print("lr_columns_for_splitting: ")
    print(lr_columns_for_splitting)

    # Step 2 Join connector geometries with segments
    if not parquet_exists(spark, joined_path) or not reuse_existing_intermediate_outputs:
        print(f"join_segments_with_connectors()...")
        joined_df = join_segments_with_connectors(filtered_df)
        write_geoparquet(joined_df, joined_path)
    else:
        joined_df = read_geoparquet(spark, joined_path)

    print(f"joined_df.count() = {str(joined_df.count())}")

    # Step 3 Split segments applying UDF on each segment+its connectors
    if not parquet_exists(spark, raw_split_path) or not reuse_existing_intermediate_outputs:
        print(f"split_joined_segments()...")
        split_segments_df = split_joined_segments(joined_df, lr_columns_for_splitting)        
        split_segments_df.write.format("parquet").mode("overwrite") \
                .option("compression", "zstd") \
                .option("parquet.block.size", 16 * 1024 * 1024) \
                .save(raw_split_path)
    else:
        split_segments_df = read_parquet(spark, raw_split_path)

    print(f"split_segments_df.count() = {str(split_segments_df.count())}")

    # Step 4 Format output (flatten result, explode rows, pick columns, unions)
    print("split_segments_df")

    flat_res_df = split_segments_df.select("input_segment", "split_result.*")
    print("flat_res_df")

    if not parquet_exists(spark, segment_splits_exploded_path) or not reuse_existing_intermediate_outputs:
        exploded_df = flat_res_df.withColumn("split_segment_row", F.explode_outer("split_segments_rows")).drop("split_segments_rows")
        print("exploded_df")

        flat_splits_df = exploded_df.select("*", "split_segment_row.*")
        print("flat_splits_df")

        write_geoparquet(flat_splits_df, segment_splits_exploded_path)

    added_connectors_df = flat_res_df.select("added_connectors_rows")
    added_connectors_df = flat_res_df.filter(size("added_connectors_rows") > 0).select("added_connectors_rows").withColumn("connector", F.explode("added_connectors_rows")).select("connector.*")

    final_segments_df = read_geoparquet(spark, segment_splits_exploded_path)

    # Output error count, example errors and split stats to identify potential issues
    final_segments_df.groupBy("is_success", coalesce(element_at(split(final_segments_df["error_message"], ":"), 1), "error_message")).agg(count("*").alias("count")).show(20, False)
    final_segments_df.groupBy("id").agg(count("*").alias("number_of_splits")).groupBy("number_of_splits").agg(count("*")).orderBy("number_of_splits").show()

    all_connectors_df = filtered_df.filter("type == 'connector'").unionByName(added_connectors_df).select(filtered_df.columns)
    if PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
        final_segments_df = resolve_tr_references(final_segments_df)

        # if we're resolving TR refs we need to set the schema with start_lr, end_lr fields for connectors too, so that the union can work:
        all_connectors_df = all_connectors_df.drop(PROHIBITED_TRANSITIONS_COLUMN).withColumn(PROHIBITED_TRANSITIONS_COLUMN, lit(None).cast(resolved_prohibited_transitions_schema))

    if DESTINATIONS_COLUMN in final_segments_df.columns:
        final_segments_df = resolve_destinations_references(final_segments_df)        
        all_connectors_df = all_connectors_df.drop(DESTINATIONS_COLUMN).withColumn(DESTINATIONS_COLUMN, lit(None).cast(resolved_destinations_schema))

    extra_columns = [field.name for field in additional_fields_in_split_segments if field.name != "turn_restrictions"]    
    for extra_col in extra_columns:
        all_connectors_df = all_connectors_df.withColumn(extra_col, lit(None))

    final_df = final_segments_df.select(filtered_df.columns + extra_columns).unionByName(all_connectors_df)
    write_geoparquet(final_df, output_path)
    loaded_final_df = read_geoparquet(spark, output_path)
    loaded_final_df.groupBy("type").agg(count("*").alias("count")).show()

    print("split segments metrics:")
    get_aggregated_metrics(loaded_final_df).show()
        
    return loaded_final_df

class Connector(NamedTuple):
    connector_id: str
    connector_geometry: Point
    connector_index: int

# COMMAND ----------
if 'spark' in globals():
    overture_release_version = "2024-07-22.0"
    overture_release_path = "wasbs://release@overturemapswestus2.blob.core.windows.net" #  "s3://overturemaps-us-west-2/release"
    base_output_path = "wasbs://test@ovtpipelinedev.blob.core.windows.net/transportation-splits" # "s3://<bucket>/transportation-split"

    wkt_filter = None

    # South America polygon
    wkt_filter = "POLYGON ((-180 -90, 180 -90, 180 -59, -25.8 -59, -25.8 28.459033, -79.20293 28.05483, -79.947494 24.627045, -86.352539 22.796439, -81.650495 15.845105, -82.60631 10.260871, -84.51781 8.331083, -107.538877 10.879395, -120 -59, -180 -59, -180 -90))"

    # Tiny test polygon in Bellevue WA
    #wkt_filter = "POLYGON ((-122.1896338 47.6185118, -122.1895695 47.6124029, -122.1792197 47.6129526, -122.1793771 47.6178368, -122.1896338 47.6185118))"

    input_path = f"{overture_release_path}/{overture_release_version}/theme=transportation"
    filter_target = "global" if not wkt_filter else "filtered"
    output_path = f"{base_output_path}/{overture_release_version}/{filter_target}"

    result_df = split_transportation(spark, input_path, output_path, wkt_filter, reuse_existing_intermediate_outputs=True)
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        display(result_df.filter('type == "segment"').limit(50))
    else:
        result_df.filter('type == "segment"').show(20, False)
