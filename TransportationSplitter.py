# Databricks notebook source
# MAGIC %md
# MAGIC Please see instructions and details [here](https://github.com/OvertureMaps/transportation-splitter/blob/main/README.md).
# MAGIC
# MAGIC # AWS Glue notebook - see instructions for magic commands

# COMMAND ----------

from collections import deque
from pyspark.sql.functions import expr, lit, col, explode, collect_list, struct, udf, struct, count, when, size, split, element_at, coalesce
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, functions as F
import pyproj
from shapely.geometry import Point, LineString
from shapely import wkt, wkb, ops, equals
from timeit import default_timer as timer
from typing import NamedTuple
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
        return f"SplitPoint(id={self.id!r}, at_coord_idx={self.at_coord_idx!r}, geometry={str(self.geometry)!r}, lr={self.lr!r}) ({self.lr_meters!r}m), is_lr_added={self.is_lr_added!r}"

class SplitSegment:
    """POCO to represent a split segment."""
    def __init__(self, id=None, geometry=None, start_split_point=None, end_split_point=None):
        self.id = id
        self.geometry = geometry
        self.start_split_point = start_split_point
        self.end_split_point = end_split_point

    def __repr__(self):
        return f"SplitSegment(id={self.id!r}, geometry={str(self.geometry)!r}, start_split_point={self.start_split_point!r}, end_split_point={self.end_split_point!r})"


def read_parquet(spark, path, merge_schema=False):
    return spark.read.option("mergeSchema", str(merge_schema).lower()).parquet(path)

def is_geoparquet(spark, input_path, limit=1, geometry_column="geometry"):
    try:
        sample_data = spark.read.format("geoparquet").option("mergeSchema", str(merge_schema).lower()).load(path).limit(1)
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

def is_lr_scope_covering_split(between, split_segment):
    return (between[0] if between[0] else 0) >= split_segment.start_split_point.lr - LR_SPLIT_POINT_MIN_DIST_METERS and \
           (between[1] if between[1] else 1) <= split_segment.end_split_point.lr + LR_SPLIT_POINT_MIN_DIST_METERS

def apply_lr_scope(x, split_segment, xpath = "$"):
    if x is None:
        return None

    #print(f"apply_lr_scope({json.dumps(x)}, {xpath})")
    if isinstance(x, list):
        output = []
        for index, sub_item in enumerate(x):
            cleaned_sub_item = apply_lr_scope(sub_item, split_segment, f"{xpath}[{index}]")
            if cleaned_sub_item is not None:
                output.append(cleaned_sub_item)
        return output if output else None
    elif isinstance(x, dict):
        if LR_SCOPE_KEY in x and x[LR_SCOPE_KEY] is not None:
            between = x[LR_SCOPE_KEY]
            if not isinstance(between, list):
                raise Exception(f"{xpath}.{LR_SCOPE_KEY} is of type {str(type(between))}, expecting list!")
            if len(between) != 2:
                raise Exception(f"{xpath}.{LR_SCOPE_KEY} has {str(len(between))} items, expecting 2 for a LR!")

            if not is_lr_scope_covering_split(between, split_segment):
                return None

        output = {}
        for key in x.keys():
            if key == LR_SCOPE_KEY:
                continue
            clean_sub_prop = apply_lr_scope(x[key], split_segment, f"{xpath}.{key}")
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

def get_split_segment_dict(original_segment_dict, split_segment, lr_columns_for_splitting):
    modified_segment_dict = original_segment_dict.copy()
    #debug_messages.append("type(start_lr)=" + str(type(split_segment.start_split_point.lr)))
    #debug_messages.append("type(geometry)=" + str(type(wkb.dumps(split_segment.geometry))))
    modified_segment_dict["start_lr"] = float(split_segment.start_split_point.lr)
    modified_segment_dict["end_lr"] = float(split_segment.end_split_point.lr)
    modified_segment_dict["geometry"] = split_segment.geometry
    modified_segment_dict["connector_ids"] = [split_segment.start_split_point.id, split_segment.end_split_point.id]
    for column in lr_columns_for_splitting:
        if column not in modified_segment_dict or modified_segment_dict[column] is None:
            continue
        modified_segment_dict[column] = apply_lr_scope(modified_segment_dict[column], split_segment)

    if SPLIT_AT_CONNECTORS and PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
        # remove turn restrictions that we're sure don't apply to this split and construct turn_restrictions field -
        # this is a flat list of all segment_id referenced in sequences;
        # used to resolve segment references after split - for each TR segment_id reference we need to identify which of the splits to retain as reference
        prohibited_transitions = modified_segment_dict.get(PROHIBITED_TRANSITIONS_COLUMN) or {}
        modified_segment_dict[PROHIBITED_TRANSITIONS_COLUMN], modified_segment_dict["turn_restrictions"] = get_trs(prohibited_transitions, modified_segment_dict["connector_ids"])

    return modified_segment_dict

def is_distinct_split_lr(lr1, lr2, segment_length):
    return abs(lr1 - lr2) * segment_length > LR_SPLIT_POINT_MIN_DIST_METERS

def add_lr_split_points(split_points, lrs, segment_id, original_segment_geometry):
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

def get_connector_split_points(connectors, original_segment_geometry):
    split_points = []
    if not connectors:
        return split_points

    # Get the length of the projected segment
    geod = pyproj.Geod(ellps="WGS84")
    original_segment_length = geod.geometry_length(original_segment_geometry)
    original_segment_coords = list(original_segment_geometry.coords)
    sorted_valid_connectors = sorted([c for c in connectors if c.connector_geometry], key=lambda p: p.connector_index)
    connectors_queue = deque(sorted_valid_connectors)
    coord_idx = 0 
    accumulated_segment_length = 0
    for (lon1, lat1), (lon2, lat2) in zip(original_segment_coords[:-1], original_segment_coords[1:]):
        sub_segment = LineString([(lon1, lat1), (lon2, lat2)])
        sub_segment_length = geod.geometry_length(sub_segment)
        
        while connectors_queue:
            next_connector = connectors_queue[0]
            next_connector_geometry = next_connector.connector_geometry               

            # Calculate the distances between points 1 and 2 of the sub-segment and the connector
            _, _, dist12 = geod.inv(lon1, lat1, lon2, lat2)
            _, _, dist1c = geod.inv(lon1, lat1, next_connector_geometry.x, next_connector_geometry.y)
            _, _, dist2c = geod.inv(lon2, lat2, next_connector_geometry.x, next_connector_geometry.y)            
                    

            dist_diff = abs(dist1c + dist2c - dist12)
            if dist_diff < IS_ON_SUB_SEGMENT_THRESHOLD_METERS:

                if len(connectors_queue) == 1 and not are_different_coords(next_connector_geometry.coords[0], original_segment_coords[-1]):
                    # edge case first - if this is the last connector, and it matches the last coordinate then LR should be 1,
                    # no matter if the same coordinate may also appear here, somewhere else in the middle of the segment
                    at_coord_idx = len(original_segment_coords) - 1
                    lr_meters = original_segment_length
                else:
                    offset_on_segment_meters = dist1c
                    at_coord_idx = coord_idx + 1 if offset_on_segment_meters == sub_segment_length else coord_idx
                    lr_meters = accumulated_segment_length + offset_on_segment_meters

                lr = lr_meters / original_segment_length
                split_points.append(SplitPoint(next_connector.connector_id, next_connector_geometry, lr, lr_meters, is_lr_added=False, at_coord_idx=at_coord_idx))
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
    StructField("end_lr", DoubleType(), True)
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

def split_joined_segments(df: DataFrame, lr_columns_for_splitting: list[str]) -> DataFrame:
    broadcast_lr_columns_for_splitting = sc.broadcast(lr_columns_for_splitting)    
    input_fields_to_drop_in_splits = ["joined_connectors"]
    transportation_feature_schema = StructType([field for field in df.schema.fields if field.name not in input_fields_to_drop_in_splits])
    split_segment_fields = [field for field in transportation_feature_schema] + additional_fields_in_split_segments
    if PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
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

            debug_messages.append(str(input_segment.geometry))

            split_points = get_connector_split_points(input_segment.joined_connectors, input_segment.geometry) if SPLIT_AT_CONNECTORS else []

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

            add_lr_split_points(split_points, lrs, original_segment_dict["id"], input_segment.geometry)

            sorted_split_points = sorted(split_points, key=lambda p: p.lr)
            debug_messages.append("sorted final split points:")
            for p in sorted_split_points:
                debug_messages.append(", ".join([p.id, str(p.geometry), str(p.lr)]))

            if len(sorted_split_points) < 2:
                raise Exception(f"Unexpected number of split points: {str(len(sorted_split_points))}; (expected at least 2)")

            #debug_messages.append("splitting into segments...")
            split_segments = split_line(input_segment.geometry, sorted_split_points)
            for split_segment in split_segments:
                debug_messages.append(f"{split_segment.start_split_point.lr}-{split_segment.end_split_point.lr}: " + str(split_segment.geometry))
                if not are_different_coords(list(split_segment.geometry.coords)[0], list(split_segment.geometry.coords)[-1]):
                    error_message += f"Wrong segment created: {split_segment.start_split_point.lr}-{split_segment.end_split_point.lr}: " + str(split_segment.geometry)
                modified_segment_dict = get_split_segment_dict(original_segment_dict, split_segment, lr_columns_for_splitting)
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
    if SPLIT_AT_CONNECTORS and PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
        final_segments_df = resolve_tr_references(final_segments_df)

        # if we're resolving TR refs we need to set the schema with start_lr, end_lr fields for connectors too, so that the union can work:
        all_connectors_df = all_connectors_df.drop(PROHIBITED_TRANSITIONS_COLUMN).withColumn(PROHIBITED_TRANSITIONS_COLUMN, lit(None).cast(resolved_prohibited_transitions_schema))

    extra_columns = [field.name for field in additional_fields_in_split_segments if field.name != "turn_restrictions"]    
    for extra_col in extra_columns:
        all_connectors_df = all_connectors_df.withColumn(extra_col, lit(None))

    final_df = final_segments_df.select(filtered_df.columns + extra_columns).unionByName(all_connectors_df)
    write_geoparquet(final_df, output_path)
    loaded_final_df = read_geoparquet(spark, output_path)
    loaded_final_df.groupBy("type").agg(count("*").alias("count")).show()
    
    return loaded_final_df

# TESTS

class Connector(NamedTuple):
    connector_id: str
    connector_geometry: Point
    connector_index: int

class TestSplitter(unittest.TestCase):

    def test_has_consecutive_dupe_coords(self):
        line1 = LineString([(0, 0), (1, 1), (1, 1), (2, 2)])
        line2 = LineString([(0, 0), (1, 1), (2, 2), (3, 3)])

        assert(has_consecutive_dupe_coords(line1))
        assert(not has_consecutive_dupe_coords(line2))

    def test_remove_consecutive_dupe_coords(self):
        line = LineString([(0, 0), (1, 1), (1, 1), (2, 2)])
        cleaned_line = get_split_line_geometry(line.coords)

        assert(has_consecutive_dupe_coords(line))
        assert(not has_consecutive_dupe_coords(cleaned_line))

    def test_dupe_coords_at_split(self):
        # input to split from may release, segment id 081b23ffffffffff0457be22b5dfa156:        
        input_segment_wkt = "LINESTRING (-69.247486 -25.388171, -69.2473756 -25.3882167, -69.247262 -25.3882807, -69.2471534 -25.3883515, -69.2470669 -25.3884155, -69.2469844 -25.3884839, -69.2469063 -25.3885563, -69.2468329 -25.3886326, -69.2467939 -25.3886713, -69.2466653 -25.388799, -69.2464922 -25.3889609, -69.2463312 -25.3890824, -69.2461661 -25.3891996, -69.2459974 -25.3893123, -69.2458006 -25.3894351, -69.2455994 -25.3895519, -69.2453939 -25.3896624, -69.2452905 -25.3897115, -69.2451922 -25.3897684, -69.2450995 -25.3898326, -69.2450132 -25.3899036, -69.2449339 -25.389981, -69.2448622 -25.3900641, -69.2447986 -25.3901524, -69.2447434 -25.3902452, -69.2446845 -25.3903625, -69.2446352 -25.3904835, -69.2445959 -25.3906074, -69.2443049 -25.391384, -69.2442218 -25.3916141, -69.2441322 -25.3918421, -69.244036 -25.392068, -69.2439334 -25.3922915, -69.2438139 -25.392533, -69.243687 -25.3927713, -69.2435525 -25.3930063, -69.242949 -25.3941742, -69.2426151 -25.3947885, -69.2425412 -25.3948905, -69.2424629 -25.3949899, -69.2423804 -25.3950865, -69.2422728 -25.3952016, -69.2421591 -25.3953118, -69.241907 -25.3955662, -69.2417047 -25.39578, -69.2415074 -25.3959975, -69.240873 -25.3966808, -69.2404519 -25.3971691, -69.2403365 -25.3973258, -69.2402279 -25.3974865, -69.2401141 -25.3976715, -69.2400093 -25.3978608, -69.2395896 -25.3985514, -69.2387326 -25.3999664, -69.2383638 -25.4006484, -69.2382419 -25.4008823, -69.2381117 -25.4011124, -69.238008 -25.4012836, -69.2378998 -25.4014524, -69.2377871 -25.4016188, -69.2372641 -25.4024002, -69.2368296 -25.4030641, -69.236756 -25.4031596, -69.2366772 -25.4032517, -69.2365935 -25.4033403, -69.2365359 -25.403396, -69.2365103 -25.4034202, -69.2364231 -25.4034966, -69.236332 -25.4035692, -69.2361303 -25.4037085, -69.2359257 -25.4038442, -69.2356769 -25.4040019, -69.2355493 -25.4040765, -69.2354241 -25.4041543, -69.2352497 -25.4042709, -69.2350806 -25.4043937, -69.2349172 -25.4045226, -69.2347525 -25.4046672, -69.2345819 -25.4048061, -69.2344023 -25.4049413, -69.2343126 -25.4050091, -69.2342171 -25.4050702, -69.2341334 -25.4051158, -69.2340466 -25.4051562, -69.2339569 -25.4051913, -69.2335113 -25.4053708, -69.2330691 -25.4055572, -69.232479 -25.4057922, -69.2321625 -25.4058867, -69.2320643 -25.4059149, -69.2319677 -25.4059472, -69.2318729 -25.4059836, -69.231744 -25.4060408, -69.2316196 -25.4061055, -69.2315 -25.4061774, -69.2314028 -25.4062284, -69.2313027 -25.4062745, -69.2312 -25.4063157, -69.231095 -25.4063518, -69.23077 -25.4064906, -69.2304419 -25.406623, -69.2301106 -25.4067492, -69.229778 -25.4068684, -69.2294426 -25.4069813, -69.2291046 -25.4070878, -69.2287642 -25.4071877, -69.2285706 -25.4072456, -69.2283744 -25.4072957, -69.228176 -25.4073378, -69.2279756 -25.4073718, -69.2278296 -25.4073915, -69.2276829 -25.4074068, -69.2276094 -25.4074137, -69.2275357 -25.4074179, -69.2273965 -25.4074185, -69.2272577 -25.4074096, -69.22712 -25.4073912, -69.2264655 -25.4073234, -69.2255455 -25.4072265, -69.2254415 -25.4072392, -69.2253392 -25.4072606, -69.2252395 -25.4072903, -69.2251432 -25.4073282, -69.225055 -25.4073719, -69.2249714 -25.4074224, -69.224893 -25.4074794, -69.2248205 -25.4075424, -69.2247544 -25.4076109, -69.2246953 -25.4076844, -69.2246214 -25.4077558, -69.2245536 -25.4078319, -69.2244923 -25.4079125, -69.2244378 -25.4079969, -69.2243854 -25.4080952, -69.2243423 -25.4081971, -69.2243087 -25.4083019, -69.2242849 -25.4084088, -69.2242525 -25.4084806, -69.2242131 -25.4085494, -69.2241669 -25.4086147, -69.2241184 -25.4086717, -69.2240648 -25.4087247, -69.2240064 -25.4087735, -69.2239438 -25.4088177, -69.2238772 -25.408857, -69.2234749 -25.4090532, -69.2234177 -25.4090725, -69.2233581 -25.4090841, -69.2232972 -25.4090875, -69.2232365 -25.4090829, -69.2231771 -25.4090702, -69.2231213 -25.4090502, -69.2230693 -25.409023, -69.2230222 -25.4089894, -69.2229809 -25.4089499, -69.2229465 -25.4089055, -69.2228538 -25.4087792, -69.2227544 -25.4086572, -69.2226487 -25.4085396, -69.2225261 -25.4084164, -69.2223965 -25.4082992, -69.2222602 -25.4081884, -69.2221177 -25.4080841, -69.2217341 -25.4078128, -69.2216052 -25.4077268, -69.2214717 -25.4076468, -69.2213339 -25.4075728, -69.2211923 -25.4075051, -69.2210555 -25.4074471, -69.2209158 -25.407395, -69.2207735 -25.4073489, -69.220629 -25.4073089, -69.220106 -25.4072652, -69.2196152 -25.4072289, -69.2195029 -25.4072092, -69.2193927 -25.4071817, -69.2192853 -25.4071465, -69.2191854 -25.4071059, -69.2190892 -25.4070586, -69.2189971 -25.407005, -69.2189097 -25.4069454, -69.2187785 -25.4068548, -69.218643 -25.4067696, -69.2185033 -25.4066899, -69.2183599 -25.4066159, -69.2182154 -25.4065489, -69.2180677 -25.4064876, -69.2179172 -25.4064323, -69.2177642 -25.4063829, -69.2176089 -25.4063397, -69.2168096 -25.4061459, -69.2167134 -25.4061193, -69.2166194 -25.406087, -69.2165279 -25.406049, -69.2164336 -25.4060024, -69.2163431 -25.4059498, -69.216257 -25.4058915, -69.2159969 -25.4057219, -69.2159475 -25.4056964, -69.2158946 -25.4056773, -69.2158394 -25.405665, -69.2157828 -25.4056596, -69.215726 -25.4056613, -69.2156643 -25.4056714, -69.2156051 -25.4056898, -69.2155497 -25.4057161, -69.2154992 -25.4057497, -69.2154551 -25.4057898, -69.2153903 -25.4058662, -69.2153195 -25.4059382, -69.2152432 -25.4060054, -69.2151538 -25.4060727, -69.2150589 -25.4061334, -69.2149588 -25.4061871, -69.2148763 -25.4062219, -69.2147978 -25.4062637, -69.2147241 -25.406312, -69.2146558 -25.4063664, -69.2145921 -25.4064278, -69.2145354 -25.4064946, -69.2144861 -25.406566, -69.2144447 -25.4066414, -69.2144117 -25.4067201, -69.2143889 -25.4068202, -69.2143581 -25.4069185, -69.2143192 -25.4070145, -69.2142726 -25.4071076, -69.2142186 -25.4071974, -69.2141521 -25.4072899, -69.2140778 -25.4073773, -69.2139959 -25.4074591, -69.2138565 -25.4075766, -69.2137224 -25.4076989, -69.2135853 -25.4078345, -69.2134547 -25.4079751, -69.2133307 -25.4081205, -69.2131611 -25.4083531, -69.2129955 -25.4085881, -69.2127576 -25.4089406, -69.2125288 -25.4092979, -69.2122176 -25.4097316, -69.2121552 -25.4098076, -69.2120852 -25.409878, -69.2120084 -25.4099424, -69.2119288 -25.4099979, -69.211844 -25.4100469, -69.2117548 -25.4100889, -69.2116619 -25.4101236, -69.2115659 -25.4101507, -69.2114556 -25.4101816, -69.2113434 -25.4102065, -69.2112299 -25.4102253, -69.2111152 -25.410238, -69.2109822 -25.4102449, -69.2108489 -25.4102436, -69.210716 -25.410234, -69.2105842 -25.4102162, -69.2103794 -25.4101705, -69.2101728 -25.4101325, -69.2099646 -25.4101023, -69.2097716 -25.4100814, -69.2095779 -25.4100671, -69.2093838 -25.4100595, -69.2091894 -25.4100587, -69.2080843 -25.4100441, -69.2078622 -25.4100401, -69.2076404 -25.4100288, -69.2074192 -25.4100102, -69.2072272 -25.4099881, -69.2070362 -25.4099606, -69.2068462 -25.4099275, -69.2066574 -25.4098891, -69.2063556 -25.4098177, -69.2060515 -25.4097546, -69.2057455 -25.4097001, -69.205496 -25.4096622, -69.2052456 -25.4096299, -69.2051204 -25.4096139, -69.2049944 -25.4096032, -69.2048532 -25.4095974, -69.2047118 -25.4095983, -69.2045707 -25.4096056, -69.204397 -25.4096011, -69.2042235 -25.4095922, -69.2040503 -25.409579, -69.2038681 -25.4095604, -69.2036865 -25.4095369, -69.2035058 -25.4095087, -69.203422 -25.4094949, -69.2033397 -25.4094753, -69.2032593 -25.4094499, -69.2031813 -25.4094191, -69.2030988 -25.409379, -69.2030205 -25.4093326, -69.2029467 -25.4092805, -69.2028782 -25.4092228, -69.2028339 -25.4091929, -69.2027861 -25.4091677, -69.2027353 -25.4091477, -69.2026824 -25.4091332, -69.2026221 -25.4091237, -69.2025704 -25.4091193, -69.2025302 -25.4091204, -69.2025 -25.4091259, -69.2024718 -25.4091377, -69.2024474 -25.4091551, -69.2024281 -25.4091771, -69.202415 -25.4092026, -69.2024088 -25.4092301, -69.2024034 -25.4092849, -69.2023899 -25.4093385, -69.2023686 -25.40939, -69.2023404 -25.4094376, -69.2023056 -25.4094814, -69.2022646 -25.4095207, -69.2022184 -25.4095548, -69.2018536 -25.4096977, -69.2017795 -25.4096979, -69.2017059 -25.4096906, -69.2016336 -25.4096759, -69.2015661 -25.4096548, -69.2015015 -25.4096273, -69.2014405 -25.4095935, -69.2013887 -25.4095606, -69.2013322 -25.4095348, -69.2012722 -25.4095165, -69.2012098 -25.4095063, -69.2011508 -25.4095041, -69.201092 -25.4095092, -69.2010344 -25.4095213, -69.2009792 -25.4095402, -69.2009339 -25.4095624, -69.200892 -25.4095894, -69.2008542 -25.409621, -69.2008209 -25.4096565, -69.2007602 -25.4096745, -69.2006971 -25.4096843, -69.2006332 -25.4096856, -69.2005718 -25.4096788, -69.2005121 -25.4096642, -69.2004553 -25.4096421, -69.2004025 -25.4096129, -69.1998017 -25.4093319, -69.1996203 -25.4092368, -69.1994352 -25.409148, -69.1992465 -25.4090654, -69.1990646 -25.4089929, -69.1988799 -25.4089263, -69.1986928 -25.4088657, -69.1985035 -25.408811, -69.1982331 -25.408756, -69.1979644 -25.4086947, -69.1977349 -25.408637, -69.1975069 -25.4085749, -69.1972804 -25.4085081, -69.1967972 -25.4083958, -69.1963121 -25.4082901, -69.1958582 -25.4081975, -69.1954029 -25.4081108, -69.1952764 -25.4080955, -69.1951509 -25.4080746, -69.1950266 -25.4080482, -69.194904 -25.4080163, -69.1947709 -25.4079749, -69.1946404 -25.4079271, -69.194513 -25.407873, -69.194389 -25.4078128, -69.1939437 -25.4075947, -69.1938928 -25.4075561, -69.1938472 -25.4075124, -69.1938115 -25.4074693, -69.1937809 -25.4074231, -69.193756 -25.4073743, -69.1937 -25.4072911, -69.1936364 -25.4072126, -69.1935656 -25.4071393, -69.193503 -25.4070839, -69.1934363 -25.4070325, -69.1933659 -25.4069855, -69.193292 -25.406943, -69.1932291 -25.4068997, -69.1931704 -25.4068519, -69.1931161 -25.4068, -69.1930667 -25.4067443, -69.1930171 -25.4066983, -69.1929632 -25.4066565, -69.1929054 -25.4066193, -69.192844 -25.4065869, -69.1927702 -25.4065277, -69.1926992 -25.4064657, -69.1926362 -25.4064062, -69.1926078 -25.4063739, -69.1925758 -25.4063446, -69.192523 -25.4063074, -69.1924644 -25.4062781, -69.1924015 -25.4062574, -69.1923253 -25.4062325, -69.192251 -25.4062034, -69.1921788 -25.4061701, -69.1921021 -25.4061287, -69.1920286 -25.4060826, -69.1919589 -25.406032, -69.1918713 -25.4059901, -69.1917792 -25.4059569, -69.1917065 -25.4059377, -69.1916323 -25.405924, -69.1915571 -25.4059159, -69.1914815 -25.4059133, -69.1913856 -25.4059024, -69.1912914 -25.4058829, -69.1911998 -25.4058552, -69.1911302 -25.40585, -69.1910604 -25.4058527, -69.1910013 -25.4058614, -69.1909437 -25.4058757, -69.190888 -25.4058955, -69.1908351 -25.4059206, -69.1907792 -25.4059485, -69.190721 -25.405972, -69.1906607 -25.4059908, -69.1905949 -25.4060058, -69.190528 -25.4060152, -69.1904603 -25.4060191, -69.1903925 -25.4060175, -69.1903012 -25.4060105, -69.1902106 -25.405998, -69.1901211 -25.4059801, -69.1900331 -25.4059569, -69.1899283 -25.4059216, -69.1898271 -25.4058787, -69.18973 -25.4058285, -69.1896761 -25.4057896, -69.1896178 -25.4057563, -69.1895556 -25.4057292, -69.1894812 -25.4057061, -69.1894042 -25.4056919, -69.1893257 -25.4056866, -69.1892472 -25.4056904, -69.1890313 -25.4057025, -69.188755 -25.4057183, -69.1886638 -25.4057436, -69.188574 -25.4057728, -69.1884465 -25.4058221, -69.1883232 -25.4058794, -69.1882337 -25.4059308, -69.1881406 -25.4059765, -69.1880442 -25.4060163, -69.1879373 -25.4060524, -69.1878277 -25.4060813, -69.1877161 -25.4061028, -69.187603 -25.4061168, -69.1870196 -25.4061713, -69.1863598 -25.4062513, -69.1862333 -25.406262, -69.1861063 -25.4062646, -69.1859862 -25.4062597, -69.1858666 -25.4062476, -69.1857483 -25.4062283, -69.1856483 -25.4062077, -69.1855468 -25.4061945, -69.1854444 -25.4061889, -69.1853419 -25.4061907, -69.1852401 -25.4062001, -69.1851394 -25.4062169, -69.1850406 -25.4062411, -69.1849442 -25.4062724, -69.1848511 -25.4063107, -69.1846955 -25.4063706, -69.1845381 -25.4064263, -69.184379 -25.4064778, -69.1842524 -25.4065217, -69.184128 -25.4065706, -69.1840062 -25.4066244, -69.1838755 -25.4066891, -69.1837484 -25.4067594, -69.1836253 -25.4068352, -69.1834948 -25.4069235, -69.1833691 -25.4070173, -69.1832484 -25.4071162, -69.1829628 -25.4073658, -69.1828967 -25.4074236, -69.1828268 -25.4074778, -69.1827536 -25.4075281, -69.1826681 -25.4075795, -69.182579 -25.4076256, -69.1824867 -25.4076662, -69.1821953 -25.4077896, -69.181906 -25.407917, -69.1817203 -25.4080055, -69.1815318 -25.408089, -69.1813977 -25.4081448, -69.1812623 -25.408198, -69.1811376 -25.4082508, -69.1810167 -25.4083101, -69.1808999 -25.4083758, -69.1807875 -25.4084476, -69.1806868 -25.4085201, -69.1805907 -25.4085975, -69.1804994 -25.4086796, -69.1804133 -25.4087662, -69.1801766 -25.4089739, -69.1799372 -25.4091792, -69.1795706 -25.4094849, -69.1794502 -25.4095886, -69.1793261 -25.4096887, -69.1791983 -25.4097849, -69.1790626 -25.4098803, -69.1789913 -25.4099238, -69.1789234 -25.4099715, -69.178842 -25.410038, -69.1787672 -25.4101104, -69.1786994 -25.4101883, -69.1785626 -25.4103494, -69.1784191 -25.4105057, -69.1782921 -25.4106346, -69.1781607 -25.4107598, -69.1780248 -25.4108812, -69.1778761 -25.4109917, -69.1777217 -25.4110956, -69.177575 -25.4111852, -69.177498 -25.4112248, -69.177424 -25.4112688, -69.1773437 -25.411324, -69.177268 -25.4113843, -69.1771974 -25.4114493, -69.1770788 -25.4115669, -69.1769666 -25.4116893, -69.1768608 -25.4118164, -69.17679 -25.4118926, -69.1767134 -25.4119641, -69.1766314 -25.4120305, -69.1765443 -25.4120913, -69.176449 -25.4121483, -69.1763493 -25.4121988, -69.1762457 -25.4122424, -69.1761389 -25.4122789, -69.1760293 -25.4123082, -69.1758177 -25.4123689, -69.175608 -25.4124347, -69.1754003 -25.4125056, -69.1749497 -25.4126534, -69.1740605 -25.4129708, -69.1737615 -25.4130786, -69.1737169 -25.4130907, -69.1736711 -25.4130984, -69.1736247 -25.4131016, -69.1735712 -25.4130997, -69.1735185 -25.413092, -69.1734672 -25.4130784, -69.1734181 -25.4130592, -69.1733372 -25.4130334, -69.1732542 -25.4130138, -69.1731697 -25.4130005, -69.1730842 -25.4129938, -69.1729522 -25.4129778, -69.1728213 -25.412955, -69.1725652 -25.4129078, -69.1724825 -25.412908, -69.1724002 -25.4129151, -69.1723319 -25.4129262, -69.1722647 -25.4129419, -69.1721991 -25.4129623, -69.1720636 -25.4129914, -69.1719255 -25.413018, -69.1717879 -25.4130465, -69.1716546 -25.4130871, -69.1715449 -25.4131257, -69.1714371 -25.4131685, -69.1713314 -25.4132155, -69.1711356 -25.4133221, -69.1710762 -25.4133451, -69.1710142 -25.4133617, -69.1709505 -25.4133717, -69.1708766 -25.4133749, -69.1708028 -25.4133692, -69.1707306 -25.4133548, -69.1705685 -25.4133185, -69.1704047 -25.4132894, -69.1702699 -25.4132708, -69.1701343 -25.4132571, -69.1699983 -25.4132482, -69.1696081 -25.4132227, -69.1695281 -25.4132222, -69.1694484 -25.4132275, -69.1693693 -25.4132385, -69.1692732 -25.41326, -69.1691799 -25.4132899, -69.1690904 -25.4133281, -69.1689735 -25.4133567, -69.1688544 -25.4133766, -69.1687501 -25.4133865, -69.1686453 -25.4133898, -69.1685405 -25.4133863, -69.1684326 -25.4133944, -69.1683252 -25.4134073, -69.1682187 -25.413425, -69.168095 -25.4134518, -69.1679733 -25.4134849, -69.1678539 -25.4135244, -69.1677459 -25.4135456, -69.1676366 -25.4135607, -69.1675099 -25.4135703, -69.1673827 -25.4135715, -69.1672558 -25.4135643, -69.1670904 -25.4135721, -69.1669258 -25.4135886, -69.1667776 -25.413611, -69.1666308 -25.4136405, -69.166486 -25.413677, -69.1663266 -25.4137093, -69.1661655 -25.413734, -69.1660032 -25.4137509, -69.1659317 -25.4137562, -69.1658599 -25.413755, -69.1657887 -25.4137474, -69.1657186 -25.4137335, -69.1656505 -25.4137133, -69.165574 -25.4136821, -69.165502 -25.4136431, -69.1654356 -25.4135967, -69.1653755 -25.4135437, -69.1652485 -25.4134039, -69.1651154 -25.4132688, -69.165002 -25.4131619, -69.1648849 -25.4130585, -69.164764 -25.4129587, -69.1646998 -25.4129175, -69.1646316 -25.4128819, -69.1645601 -25.4128521, -69.1644795 -25.4128266, -69.1644386 -25.4128155, -69.1643965 -25.4128085, -69.164342 -25.4128056, -69.1642875 -25.4128097, -69.1642343 -25.4128206, -69.1641627 -25.4128403, -69.1640932 -25.4128654, -69.1640264 -25.4128957, -69.1639538 -25.4129158, -69.163879 -25.4129279, -69.1638031 -25.4129317, -69.1637273 -25.4129272, -69.1636536 -25.4129146, -69.1635821 -25.4128942, -69.1635138 -25.4128661, -69.1634497 -25.4128309, -69.1633907 -25.4127891, -69.1633017 -25.4127189, -69.1632177 -25.4126437, -69.1629455 -25.4123494, -69.1628732 -25.412289, -69.1627952 -25.4122348, -69.1627121 -25.412187, -69.1626389 -25.4121523, -69.1625631 -25.4121227, -69.1624849 -25.4120982, -69.162405 -25.4120792, -69.1622646 -25.4120413, -69.162126 -25.4119981, -69.1619604 -25.4119433, -69.1617961 -25.4118854, -69.1616935 -25.4118587, -69.1615892 -25.4118381, -69.1614836 -25.4118236, -69.1613846 -25.4118158, -69.1612851 -25.4118134, -69.1611857 -25.4118164, -69.1610867 -25.4118248, -69.1609873 -25.4118382, -69.1608889 -25.4118564, -69.1607916 -25.4118794, -69.1606853 -25.4118982, -69.1605779 -25.4119112, -69.1604698 -25.4119181, -69.1603496 -25.4119188, -69.1602297 -25.4119121, -69.1601357 -25.411923, -69.1600433 -25.4119423, -69.1599582 -25.4119681, -69.1598762 -25.4120009, -69.1597979 -25.4120405, -69.1596766 -25.4120998, -69.1595517 -25.4121528, -69.1594237 -25.4121992, -69.1592835 -25.4122414, -69.1591408 -25.4122758, -69.1589959 -25.4123021, -69.1589118 -25.412312, -69.1588269 -25.4123135, -69.1587424 -25.4123066, -69.1586593 -25.4122912, -69.1585842 -25.4122696, -69.158512 -25.4122412, -69.1584433 -25.4122063, -69.158379 -25.4121652, -69.1582828 -25.4121029, -69.158182 -25.4120468, -69.1580771 -25.4119972, -69.1579686 -25.4119545, -69.1578668 -25.4119215, -69.1577628 -25.4118945, -69.1576571 -25.4118735, -69.1575502 -25.4118588, -69.157431 -25.4118511, -69.1573115 -25.4118527, -69.1571258 -25.4118522, -69.1569403 -25.4118474, -69.1567549 -25.4118382, -69.1565356 -25.4118216, -69.1563169 -25.411799, -69.1560991 -25.4117703, -69.1559224 -25.411742, -69.1557467 -25.4117093, -69.1555721 -25.4116722, -69.1554501 -25.4116521, -69.155327 -25.4116388, -69.1552033 -25.4116322, -69.1550595 -25.4116332, -69.1549162 -25.4116433, -69.1547741 -25.4116625, -69.1545957 -25.4116895, -69.1544164 -25.4117105, -69.1542363 -25.4117255, -69.1540384 -25.4117351, -69.1538403 -25.4117376, -69.1536422 -25.4117328, -69.1533223 -25.4117302, -69.1530025 -25.411734, -69.1528107 -25.4117287, -69.1526193 -25.4117158, -69.1524288 -25.4116954, -69.1522394 -25.4116674, -69.1520626 -25.4116342, -69.1518875 -25.4115944, -69.1517143 -25.4115481, -69.1515434 -25.4114954, -69.1514424 -25.4114746, -69.1513399 -25.4114607, -69.1512366 -25.4114536, -69.1511331 -25.4114535, -69.1510297 -25.4114602, -69.1509202 -25.4114751, -69.1508124 -25.4114977, -69.1507069 -25.4115279, -69.1506043 -25.4115656, -69.1505054 -25.4116104, -69.1503374 -25.4116799, -69.1501661 -25.4117425, -69.1499828 -25.4118006, -69.1497967 -25.4118507, -69.1496082 -25.4118927, -69.149439 -25.4119389, -69.1492726 -25.4119927, -69.1491093 -25.4120538, -69.14891 -25.4121289, -69.1487082 -25.4121984, -69.1485041 -25.4122622, -69.1482979 -25.4123203, -69.1481115 -25.4123674, -69.1479237 -25.4124098, -69.1477346 -25.4124475, -69.1475213 -25.4124796, -69.1473068 -25.4125044, -69.1471092 -25.4125207, -69.1469111 -25.4125308, -69.1467127 -25.4125347, -69.1458289 -25.4125819, -69.1456277 -25.4125808, -69.1454266 -25.4125771, -69.1452106 -25.4125701, -69.1449948 -25.4125601, -69.1448563 -25.4125533, -69.1447176 -25.4125517, -69.144579 -25.4125553, -69.1444346 -25.4125646, -69.1442907 -25.4125795, -69.1441748 -25.412587, -69.1440587 -25.4125904, -69.1438882 -25.4125893, -69.143718 -25.4125807, -69.1434996 -25.4125651, -69.1432808 -25.4125541, -69.1430603 -25.4125475, -69.1428396 -25.4125456, -69.1426516 -25.4125396, -69.1424641 -25.412525, -69.1423087 -25.4125062, -69.1421543 -25.4124814, -69.1420068 -25.412462, -69.1418129 -25.4124649, -69.1416191 -25.4124602, -69.1414256 -25.4124479, -69.141233 -25.4124281, -69.1410391 -25.4124004, -69.1408467 -25.4123651, -69.1407655 -25.412348, -69.1406831 -25.4123358, -69.1406 -25.4123288, -69.1405157 -25.4123268, -69.1404316 -25.41233, -69.1403478 -25.4123385, -69.1402015 -25.4123495, -69.1400548 -25.4123543, -69.139908 -25.412353, -69.1397507 -25.4123447, -69.1395941 -25.4123294, -69.1394386 -25.412307, -69.1392466 -25.4122816, -69.1390539 -25.4122618, -69.1388606 -25.4122476, -69.1386146 -25.4122376, -69.1383684 -25.4122367, -69.1382418 -25.412234, -69.1381156 -25.4122247, -69.1379902 -25.4122088, -69.1378529 -25.4121836, -69.1377177 -25.4121504, -69.1375852 -25.4121095, -69.1370983 -25.4119302, -69.1370171 -25.411903, -69.1369338 -25.4118815, -69.1368489 -25.411866, -69.1367558 -25.4118561, -69.1366622 -25.4118532, -69.1365686 -25.4118576, -69.1364473 -25.4118628, -69.1363264 -25.4118734, -69.1362063 -25.4118895, -69.1360872 -25.4119109, -69.135956 -25.4119409, -69.1358268 -25.4119775, -69.1357001 -25.4120204, -69.1355762 -25.4120695, -69.1354999 -25.4121086, -69.1354205 -25.4121424, -69.1353385 -25.4121706, -69.1352543 -25.4121931, -69.1351568 -25.4122116, -69.1350578 -25.4122225, -69.1349582 -25.4122256, -69.1348587 -25.412221, -69.134278 -25.4121749, -69.1328309 -25.411952, -69.1311854 -25.4116843, -69.1307714 -25.4116256, -69.1303588 -25.4115598, -69.1299476 -25.4114869, -69.1296079 -25.4114211, -69.1292695 -25.4113504, -69.1289324 -25.4112749, -69.1286431 -25.4112084, -69.128356 -25.4111345, -69.1280714 -25.4110532, -69.1278428 -25.410982, -69.1276161 -25.4109061, -69.1273914 -25.4108255, -69.1265398 -25.4105117, -69.126406 -25.4104722, -69.1262705 -25.4104379, -69.1261335 -25.4104088, -69.1260086 -25.410387, -69.1258829 -25.4103697, -69.1257566 -25.4103567, -69.1256493 -25.4103571, -69.1255424 -25.4103485, -69.1254369 -25.4103309, -69.1253336 -25.4103045, -69.1252336 -25.4102695, -69.1251544 -25.4102344, -69.1250784 -25.410194, -69.1250061 -25.4101485, -69.1249379 -25.410098, -69.1248742 -25.4100429, -69.1247856 -25.4099607, -69.1246902 -25.4098849, -69.1245885 -25.4098161, -69.1244812 -25.4097546, -69.1243686 -25.4097007, -69.1242517 -25.4096549, -69.1241312 -25.4096175, -69.1240078 -25.4095887, -69.1238574 -25.4095474, -69.1237101 -25.4094978, -69.1236416 -25.4094702, -69.1235665 -25.40944, -69.1234271 -25.4093743, -69.1233061 -25.4093087, -69.1231893 -25.4092372, -69.1230771 -25.4091599, -69.1229699 -25.4090771, -69.1228679 -25.408989, -69.1227463 -25.4088728, -69.1226211 -25.4087597, -69.1224924 -25.4086499, -69.1223547 -25.4085391, -69.1222134 -25.408432, -69.1220686 -25.4083288, -69.1211405 -25.4075838, -69.1210632 -25.407539, -69.1209811 -25.4075017, -69.1208951 -25.4074724, -69.1208088 -25.4074518, -69.1207207 -25.4074392, -69.1206316 -25.4074348, -69.1205424 -25.4074385, -69.1204002 -25.4074494, -69.1202576 -25.4074542, -69.1201149 -25.407453, -69.1199724 -25.4074457, -69.1197983 -25.4074285, -69.1196256 -25.4074023, -69.1194548 -25.407367, -69.1192421 -25.4073275, -69.1190283 -25.4072928, -69.1188137 -25.4072628, -69.1186354 -25.4072559, -69.118457 -25.4072568, -69.1182951 -25.4072644, -69.1181338 -25.4072786, -69.1180243 -25.407279, -69.117915 -25.4072725, -69.1178066 -25.4072592, -69.1176986 -25.4072389, -69.1175925 -25.4072118, -69.1174887 -25.407178, -69.1169831 -25.4070169, -69.1168725 -25.4069945, -69.1167605 -25.4069794, -69.1166547 -25.4069717, -69.1165486 -25.4069705, -69.1164427 -25.4069757, -69.1163153 -25.4069831, -69.1161876 -25.4069818, -69.1160604 -25.4069721, -69.115943 -25.4069554, -69.1158272 -25.4069314, -69.1157135 -25.4069002, -69.1156024 -25.406862, -69.1154945 -25.406817, -69.1153979 -25.4067613, -69.1152977 -25.406711, -69.1151943 -25.4066662, -69.1150881 -25.4066272, -69.1149795 -25.4065941, -69.1148604 -25.4065652, -69.1147394 -25.4065434, -69.1146171 -25.4065288, -69.114494 -25.4065214, -69.1143527 -25.4065177, -69.1142114 -25.4065189, -69.1140702 -25.4065251, -69.1138581 -25.406549, -69.113647 -25.4065797, -69.1134372 -25.4066171, -69.1132178 -25.4066639, -69.1130004 -25.406718, -69.1127855 -25.4067795, -69.1126768 -25.4068088, -69.1125659 -25.4068304, -69.1124535 -25.406844, -69.1123402 -25.4068497, -69.1122319 -25.4068477, -69.112124 -25.4068384, -69.1120172 -25.4068218, -69.111912 -25.4067981, -69.1118091 -25.4067674, -69.1116401 -25.4067114, -69.1114736 -25.4066495, -69.1113099 -25.4065817, -69.1111493 -25.4065081, -69.1099343 -25.4059485, -69.1096053 -25.4058114, -69.1092744 -25.4056783, -69.1087836 -25.4054893, -69.1086666 -25.4054379, -69.1085462 -25.4053934, -69.1084228 -25.4053561, -69.108316 -25.4053301, -69.1082078 -25.4053095, -69.1080984 -25.4052943, -69.1079883 -25.4052846, -69.1079175 -25.4052906, -69.107848 -25.4053044, -69.107781 -25.405326, -69.1077174 -25.4053549, -69.1076566 -25.4053919, -69.1076016 -25.4054356, -69.1075532 -25.4054853, -69.1075122 -25.4055402, -69.1074365 -25.405661, -69.1073553 -25.4057789, -69.1072668 -25.4058963, -69.1071728 -25.4060102, -69.1070737 -25.4061205, -69.1069596 -25.4062206, -69.1068403 -25.4063158, -69.1067161 -25.4064056, -69.1065873 -25.40649, -69.1064541 -25.4065687, -69.1062942 -25.4066529, -69.1061293 -25.4067288, -69.10596 -25.4067964, -69.1057868 -25.4068554, -69.1056102 -25.4069055, -69.1053852 -25.4069745, -69.1052255 -25.4070184, -69.1050647 -25.4070593, -69.1048722 -25.4071039, -69.1046785 -25.4071441, -69.1043939 -25.4071945, -69.1041112 -25.4072531, -69.1038583 -25.4073129, -69.1036075 -25.4073791, -69.1034823 -25.407413, -69.1033588 -25.4074518, -69.1032068 -25.4075071, -69.1030582 -25.4075698, -69.1029136 -25.4076396, -69.1024683 -25.4078612, -69.1023668 -25.4079052, -69.102262 -25.4079424, -69.1021545 -25.4079727, -69.1020288 -25.4079987, -69.1019011 -25.4080153, -69.1017723 -25.4080224, -69.1016061 -25.4080393, -69.1014406 -25.4080615, -69.1012761 -25.408089, -69.1010799 -25.4081288, -69.1008858 -25.4081761, -69.1006941 -25.4082307, -69.1004113 -25.4083022, -69.1001314 -25.4083822, -69.0998545 -25.4084706, -69.0996109 -25.4085563, -69.0993701 -25.4086485, -69.0991326 -25.4087472, -69.0988983 -25.4088522, -69.0976189 -25.4093864, -69.0954503 -25.410238, -69.0943801 -25.4106886, -69.0938263 -25.4109066, -69.0936334 -25.4109919, -69.0934441 -25.4110835, -69.0932738 -25.4111729, -69.093107 -25.4112674, -69.0929438 -25.411367, -69.0927857 -25.4114447, -69.0926237 -25.4115158, -69.0924583 -25.4115802, -69.0922872 -25.4116384, -69.0921132 -25.4116894, -69.0919368 -25.411733, -69.0917583 -25.4117691, -69.0915539 -25.4118175, -69.0913518 -25.4118728, -69.0911521 -25.4119351, -69.0902562 -25.4122028, -69.0893523 -25.4124741, -69.0885758 -25.4127115, -69.0884496 -25.4127646, -69.0883257 -25.412822, -69.0882044 -25.4128836, -69.0880805 -25.4129523, -69.0879597 -25.4130254, -69.087902 -25.4130653, -69.0878423 -25.4131028, -69.0877525 -25.4131499, -69.0876584 -25.4131893, -69.0875632 -25.413226, -69.0874666 -25.4132603, -69.0873689 -25.4132919, -69.0872952 -25.4133137, -69.0872369 -25.4133299, -69.0871871 -25.4133429, -69.0871565 -25.4133505, -69.087079 -25.4133685, -69.0870459 -25.4133757, -69.087012 -25.4133828, -69.0869316 -25.4133982, -69.0868385 -25.4134138, -69.0867831 -25.4134219, -69.0866371 -25.4134435, -69.0864896 -25.4134543, -69.0863416 -25.4134541, -69.0861877 -25.4134612, -69.0860344 -25.4134759, -69.0858121 -25.4134967, -69.0855892 -25.413511, -69.0854002 -25.4135181, -69.0852111 -25.4135205, -69.0850219 -25.4135183, -69.0848792 -25.4135103, -69.0847374 -25.4134945, -69.0845968 -25.4134711, -69.0844575 -25.4134398, -69.0843205 -25.413401, -69.0841864 -25.4133548, -69.0841121 -25.4133261, -69.0840356 -25.4133026, -69.0839574 -25.4132842, -69.083878 -25.4132712, -69.0837784 -25.4132625, -69.0836785 -25.4132621, -69.0835789 -25.41327, -69.0834937 -25.4132733, -69.0834085 -25.4132697, -69.0833241 -25.4132591, -69.0832471 -25.4132431, -69.0831719 -25.4132214, -69.083099 -25.4131941, -69.0830288 -25.4131614, -69.082962 -25.4131234, -69.0827649 -25.4130152, -69.0825631 -25.4129142, -69.0823571 -25.4128206, -69.0821831 -25.4127485, -69.0820064 -25.4126816, -69.0818275 -25.41262, -69.0817375 -25.4125904, -69.0816464 -25.4125638, -69.0815121 -25.4125303, -69.0813762 -25.4125031, -69.0812389 -25.4124824, -69.0811005 -25.4124681, -69.080931 -25.4124595, -69.0807612 -25.4124608, -69.0796494 -25.4124729, -69.0789655 -25.4125165, -69.0788687 -25.4125124, -69.0787724 -25.4125032, -69.0786534 -25.4124847, -69.0785363 -25.4124584, -69.0780039 -25.4123445, -69.0778098 -25.4123195, -69.0776148 -25.4123013, -69.0774192 -25.41229, -69.0771879 -25.4122856, -69.0769566 -25.4122909, -69.0767258 -25.4123057, -69.0766021 -25.4123132, -69.0764781 -25.4123136, -69.0763544 -25.412307, -69.0762211 -25.4122918, -69.0760894 -25.4122685, -69.0759597 -25.4122372, -69.0758327 -25.4121979, -69.0747799 -25.4118406, -69.0742019 -25.4116649, -69.0741013 -25.4116407, -69.073927 -25.4116141, -69.0737379 -25.4116007, -69.073585 -25.4115959, -69.0733995 -25.4115878, -69.0732143 -25.4115745, -69.0730298 -25.4115559, -69.0728618 -25.4115344, -69.0726945 -25.4115086, -69.0725282 -25.4114784, -69.0722508 -25.4114289, -69.071975 -25.4113732, -69.0717007 -25.4113112, -69.0713982 -25.4112352, -69.0710982 -25.4111516, -69.0708008 -25.4110605, -69.0705303 -25.4109609, -69.0702575 -25.4108667, -69.0699824 -25.4107779, -69.0697052 -25.4106947, -69.0694002 -25.41061, -69.069093 -25.4105321, -69.0687838 -25.4104609, -69.0686591 -25.4104391, -69.0685332 -25.410424, -69.0684066 -25.4104154, -69.0682796 -25.4104136, -69.0681532 -25.4104184, -69.0680274 -25.4104298, -69.0679025 -25.4104478, -69.067779 -25.4104723, -69.0676573 -25.4105033, -69.0669559 -25.4106825, -69.066883 -25.4107009, -69.0668083 -25.4107114, -69.0667326 -25.4107139, -69.0666572 -25.4107083, -69.0665831 -25.4106947, -69.0665211 -25.4106767, -69.0664615 -25.4106531, -69.0664049 -25.4106243, -69.0663518 -25.4105903, -69.0663028 -25.4105517, -69.0662397 -25.4104912, -69.0661716 -25.4104354, -69.0660989 -25.4103845, -69.0660144 -25.4103349, -69.0659255 -25.410292, -69.0658328 -25.4102561, -69.065737 -25.4102275, -69.0656389 -25.4102065, -69.06549 -25.4101654, -69.0653429 -25.4101193, -69.0651977 -25.4100684, -69.0650512 -25.4100111, -69.0649072 -25.4099489, -69.0647659 -25.4098818, -69.064665 -25.409848, -69.064562 -25.4098197, -69.0644574 -25.409797, -69.0643239 -25.4097766, -69.0641891 -25.4097652, -69.0641214 -25.4097656, -69.0640537 -25.4097631, -69.0639474 -25.4097533, -69.0638421 -25.4097363, -69.0637386 -25.4097122, -69.0634092 -25.4096415, -69.0630774 -25.4095802, -69.0628209 -25.4095394, -69.0625634 -25.4095043, -69.0623049 -25.4094748, -69.0620174 -25.4094631, -69.0617296 -25.4094554, -69.0614929 -25.4094522, -69.0612562 -25.4094518, -69.0611123 -25.4094456, -69.0609687 -25.4094347, -69.0608257 -25.4094191, -69.0606648 -25.4093959, -69.0605052 -25.4093668, -69.0603469 -25.4093319, -69.0599097 -25.4092592, -69.0597466 -25.409246, -69.0595842 -25.4092266, -69.0594229 -25.409201, -69.0592758 -25.4091722, -69.05913 -25.4091383, -69.0589857 -25.4090993, -69.058118 -25.4088655, -69.0579783 -25.4088282, -69.0578409 -25.4087844, -69.0577062 -25.4087344, -69.0575745 -25.4086782, -69.0574461 -25.4086159, -69.0573263 -25.4085506, -69.0572102 -25.4084801, -69.057098 -25.4084045, -69.0569901 -25.408324, -69.056928 -25.4082529, -69.0568593 -25.4081869, -69.0567846 -25.4081265, -69.0567045 -25.408072, -69.0566239 -25.4080262, -69.0565394 -25.4079865, -69.0564517 -25.4079531, -69.0563612 -25.4079262, -69.0562686 -25.4079061, -69.0561546 -25.4078865, -69.0560396 -25.4078723, -69.055924 -25.4078637, -69.0556234 -25.407856, -69.0553232 -25.4078419, -69.0550274 -25.4078215, -69.0547323 -25.4077949, -69.0545848 -25.4077809, -69.054438 -25.4077619, -69.0542651 -25.4077329, -69.0540938 -25.407697, -69.0539244 -25.4076541, -69.0529078 -25.407476, -69.052657 -25.4074354, -69.0524079 -25.4073869, -69.0521608 -25.4073307, -69.051925 -25.4072691, -69.0516915 -25.4072004, -69.0514608 -25.4071247, -69.0505824 -25.4068485, -69.0491514 -25.4064439, -69.0483078 -25.4062404, -69.0477687 -25.4061071, -69.0463646 -25.4056892, -69.0458535 -25.4055254, -69.04534 -25.4053682, -69.0448942 -25.4052376, -69.0444467 -25.4051121, -69.0439975 -25.4049914, -69.0434879 -25.4048352, -69.0430671 -25.4046886, -69.0430445 -25.404681, -69.0426444 -25.4045468, -69.0421634 -25.4043921, -69.0416802 -25.4042435, -69.0414364 -25.4041749, -69.0411946 -25.404101, -69.0409181 -25.4040094, -69.0406444 -25.4039108, -69.0403739 -25.4038055, -69.0400981 -25.4037083, -69.0398186 -25.4036201, -69.0393751 -25.4034782, -69.0389294 -25.4033421, -69.0384816 -25.4032119, -69.0380541 -25.4030935, -69.037625 -25.4029804, -69.0371941 -25.4028726, -69.0369338 -25.4028116, -69.0366713 -25.4027587, -69.0364069 -25.4027139, -69.0361393 -25.4026773, -69.0358706 -25.402649, -69.0356009 -25.4026291, -69.0353459 -25.4026075, -69.0350913 -25.4025831, -69.0338802 -25.4024462, -69.0328771 -25.4023239, -69.0325304 -25.4022811, -69.032183 -25.4022431, -69.0318351 -25.40221, -69.0314082 -25.4021761, -69.0309808 -25.4021494, -69.030589 -25.4021248, -69.0301976 -25.4020973, -69.0295998 -25.4020498, -69.0290026 -25.4019956, -69.028324 -25.4019386, -69.0264344 -25.401826, -69.0262163 -25.4018198, -69.0259981 -25.4018198, -69.02578 -25.401826, -69.0255355 -25.4018402, -69.0254139 -25.4018537, -69.0252918 -25.4018623, -69.0251609 -25.4018663, -69.0250299 -25.4018648, -69.0248991 -25.4018578, -69.0247688 -25.4018453, -69.0245231 -25.4018341, -69.0242772 -25.4018285, -69.0240312 -25.4018284, -69.0237785 -25.4018341, -69.0235261 -25.4018457, -69.0232741 -25.4018632, -69.0230227 -25.4018865, -69.022754 -25.4019166, -69.0224862 -25.401952, -69.0217003 -25.4020828, -69.0211661 -25.4021766, -69.0206328 -25.4022742, -69.0202832 -25.402317, -69.0199327 -25.4023542, -69.0195576 -25.4023877, -69.0191817 -25.4024147, -69.0189492 -25.4024352, -69.0187176 -25.4024627, -69.018487 -25.4024971, -69.0182409 -25.4025417, -69.0179968 -25.4025942, -69.0177548 -25.4026546, -69.0175058 -25.4027115, -69.0172554 -25.4027632, -69.0170038 -25.4028097, -69.0167179 -25.4028559, -69.0164309 -25.4028955, -69.0162872 -25.4029145, -69.0161428 -25.4029284, -69.0159458 -25.4029391, -69.0157485 -25.4029405, -69.0156059 -25.4029308, -69.0154629 -25.4029272, -69.0153199 -25.4029296, -69.0151772 -25.4029381, -69.0150292 -25.4029532, -69.0148822 -25.4029749, -69.0147365 -25.403003, -69.0145925 -25.4030374, -69.0138307 -25.4032482, -69.0131924 -25.4033911, -69.0130406 -25.4034005, -69.0128885 -25.4034046, -69.0127364 -25.4034033, -69.0125469 -25.4033941, -69.0123582 -25.4033766, -69.0122381 -25.4033925, -69.0121195 -25.4034154, -69.0120221 -25.4034397, -69.0119263 -25.4034688, -69.0118325 -25.4035026, -69.0114838 -25.4036552, -69.0105209 -25.4041253, -69.0101992 -25.4042943, -69.0098798 -25.4044669, -69.0093997 -25.4047358, -69.0091846 -25.4048401, -69.0089652 -25.4049369, -69.0087913 -25.4050071, -69.0086151 -25.4050725, -69.0084368 -25.4051332, -69.0081939 -25.4052252, -69.0079489 -25.4053124, -69.0077019 -25.4053948, -69.0074154 -25.4054837, -69.0071265 -25.4055661, -69.0068355 -25.405642, -69.0053281 -25.406049, -69.0042311 -25.4063446, -69.0033647 -25.4065723, -69.002611 -25.4067637, -69.0023998 -25.4068147, -69.0021869 -25.40686, -69.0019727 -25.4068994, -69.00172 -25.4069382, -69.001466 -25.4069689, -69.0012109 -25.4069915, -69.0010266 -25.4070125, -69.0008434 -25.4070401, -69.0006616 -25.4070743, -69.0004814 -25.407115, -69.0002781 -25.4071694, -69.0000777 -25.407232, -68.9998807 -25.4073028, -68.9996874 -25.4073815, -68.9993942 -25.4075046, -68.9992499 -25.4075705, -68.9991027 -25.4076311, -68.9989306 -25.4076938, -68.9987554 -25.4077491, -68.9985775 -25.4077969, -68.9983973 -25.407837, -68.998192 -25.4078914, -68.9979898 -25.4079545, -68.9978911 -25.4079917, -68.9977911 -25.408026, -68.9976549 -25.4080672, -68.9975168 -25.4081027, -68.997377 -25.4081326, -68.9972359 -25.4081568, -68.9967451 -25.408261, -68.9963035 -25.4083578, -68.9958626 -25.4084572, -68.9951384 -25.4086026, -68.9950382 -25.4086177, -68.9949369 -25.408625, -68.9948353 -25.4086244, -68.994731 -25.4086157, -68.9946279 -25.4085987, -68.9945269 -25.4085735, -68.9943876 -25.4085479, -68.9942471 -25.4085285, -68.9941058 -25.4085154, -68.9939723 -25.4085088, -68.9938387 -25.4085078, -68.9937052 -25.4085125, -68.993572 -25.4085227, -68.9930996 -25.4085274, -68.9926275 -25.4085412, -68.9921558 -25.4085638, -68.9917519 -25.4085904, -68.9913487 -25.4086235, -68.9909461 -25.4086632, -68.9906904 -25.4086861, -68.9904338 -25.4086995, -68.9902371 -25.4087034, -68.9900403 -25.4087018, -68.9899421 -25.4086969, -68.9898437 -25.4086947, -68.9896707 -25.4086975, -68.9894981 -25.4087089, -68.9893265 -25.4087289, -68.9891563 -25.4087573, -68.9889881 -25.408794, -68.9887622 -25.4088373, -68.9885348 -25.408874, -68.9880581 -25.4089549, -68.9875826 -25.4090411, -68.9872393 -25.409103, -68.9868951 -25.4091603, -68.98655 -25.4092131, -68.9860451 -25.4092822, -68.9855388 -25.4093416, -68.9849817 -25.4093998, -68.9844238 -25.4094507, -68.9838651 -25.4094942, -68.9835308 -25.4095314, -68.9831954 -25.4095596, -68.9828593 -25.409579, -68.9825268 -25.4095893, -68.9821941 -25.4095909, -68.9818615 -25.4095838, -68.9815234 -25.4095826, -68.9811854 -25.4095875, -68.9808476 -25.4095984, -68.9804441 -25.4096049, -68.9800408 -25.4096186, -68.9796379 -25.4096396, -68.9792514 -25.4096665, -68.9788654 -25.4097001, -68.9780366 -25.4097898, -68.9768967 -25.4098964, -68.9767508 -25.4099127, -68.9766041 -25.4099208, -68.9764571 -25.4099208, -68.9763104 -25.4099127, -68.9761645 -25.4098964, -68.9760407 -25.409876, -68.9759183 -25.4098498, -68.9757976 -25.4098178, -68.975679 -25.4097801, -68.9755413 -25.4097281, -68.9754018 -25.4096804, -68.9752606 -25.4096371, -68.9751026 -25.4095944, -68.9749429 -25.4095571, -68.9747818 -25.4095253, -68.9746195 -25.409499, -68.97378 -25.4093512, -68.9736139 -25.409336, -68.9734489 -25.4093124, -68.9732857 -25.4092804, -68.9731248 -25.4092402, -68.9729666 -25.4091919, -68.9728117 -25.4091356, -68.9726702 -25.409076, -68.9725324 -25.4090097, -68.9723986 -25.408937, -68.9723039 -25.4088862, -68.9722054 -25.4088417, -68.9721036 -25.4088037, -68.9720101 -25.4087754, -68.9719147 -25.4087525, -68.9718179 -25.4087353, -68.97172 -25.4087238, -68.9716025 -25.4087143, -68.9714858 -25.408699, -68.9713702 -25.4086779, -68.971256 -25.4086511, -68.9711402 -25.4086175, -68.9710267 -25.4085781, -68.9709158 -25.4085329, -68.970808 -25.408482, -68.9707035 -25.4084258, -68.970209 -25.4082144, -68.9697164 -25.4079993, -68.9693188 -25.4078225, -68.9689225 -25.4076432, -68.9680991 -25.4073113, -68.9676806 -25.4071943, -68.9672598 -25.4070842, -68.9668369 -25.406981, -68.966412 -25.4068849, -68.9659659 -25.4067919, -68.9655179 -25.4067066, -68.9650682 -25.406629, -68.9646169 -25.4065592, -68.9641643 -25.4064972, -68.9635438 -25.4064046, -68.9629251 -25.4063034, -68.9626363 -25.4062697, -68.9623486 -25.4062294, -68.9620621 -25.4061825, -68.9617771 -25.4061289, -68.9614757 -25.4060648, -68.9613267 -25.4060266, -68.9611763 -25.4059933, -68.9609775 -25.4059572, -68.9607771 -25.4059297, -68.9605755 -25.4059109, -68.9594865 -25.4058285, -68.9581937 -25.4057268, -68.9566165 -25.4054699, -68.9562378 -25.4054121, -68.9558605 -25.4053475, -68.9554846 -25.4052761, -68.9551073 -25.4051973, -68.9547317 -25.4051117, -68.9545454 -25.4050637, -68.9543581 -25.4050193, -68.9540909 -25.4049624, -68.953822 -25.4049127, -68.9535516 -25.4048703, -68.9532799 -25.4048352, -68.9524504 -25.4047227, -68.9516223 -25.4046026, -68.9488757 -25.404181, -68.9474943 -25.4039872, -68.9472016 -25.4039667, -68.9469096 -25.4039387, -68.9467256 -25.4039172, -68.9465421 -25.4038927, -68.9463078 -25.4038579, -68.9460726 -25.403828, -68.9458367 -25.403803, -68.9455585 -25.40378, -68.9452796 -25.4037638, -68.9450003 -25.4037546, -68.9447209 -25.4037521, -68.9444334 -25.4037712, -68.9441454 -25.4037849, -68.9438573 -25.4037933, -68.9435448 -25.4037965, -68.9432323 -25.4037933, -68.9426878 -25.4038369, -68.9423918 -25.4038641, -68.942095 -25.403883, -68.9418359 -25.4038926, -68.9415765 -25.4038958, -68.9413172 -25.4038927, -68.9405742 -25.4038854, -68.9401894 -25.4038696, -68.9398044 -25.4038588, -68.9392841 -25.4038519, -68.9387637 -25.4038539, -68.9385002 -25.4038697, -68.9382365 -25.403881, -68.9379725 -25.4038878, -68.9377043 -25.4038901, -68.937436 -25.4038878, -68.9372774 -25.4039002, -68.9371195 -25.4039193, -68.936932 -25.4039509, -68.9367467 -25.403992, -68.9365887 -25.4040196, -68.9364294 -25.4040406, -68.9362693 -25.404055, -68.9360047 -25.4040754, -68.9357409 -25.4041035, -68.9355172 -25.4041334, -68.9352945 -25.404169, -68.935073 -25.4042101, -68.9342791 -25.4044063, -68.9336595 -25.4045614, -68.933497 -25.4045955, -68.9333333 -25.4046246, -68.9331687 -25.4046486, -68.9329718 -25.4046704, -68.9327741 -25.404685, -68.9325759 -25.4046922, -68.9323929 -25.404681, -68.9322095 -25.4046788, -68.9320262 -25.4046858, -68.9318436 -25.4047019, -68.9316886 -25.4047228, -68.9315349 -25.4047503, -68.9313827 -25.4047843, -68.9312325 -25.4048247, -68.9310846 -25.4048715, -68.9308436 -25.4049373, -68.9306056 -25.4050117, -68.9303711 -25.4050944, -68.9301775 -25.4051701, -68.9299868 -25.4052515, -68.9297991 -25.4053386, -68.9296147 -25.4054312, -68.9294742 -25.4054822, -68.9293376 -25.4055415, -68.9292056 -25.4056086, -68.9290788 -25.4056834, -68.9289576 -25.4057655, -68.9288546 -25.4058447, -68.928757 -25.4059292, -68.9286649 -25.4060186, -68.9285788 -25.4061128, -68.9284989 -25.4062113, -68.9282173 -25.4066087, -68.9281609 -25.4066653, -68.928099 -25.4067172, -68.9280322 -25.4067637, -68.9279615 -25.4068044, -68.927887 -25.4068391, -68.9278093 -25.4068676, -68.9277291 -25.4068897, -68.9275893 -25.4069515, -68.9274524 -25.4070186, -68.9273188 -25.4070908, -68.9271733 -25.4071776, -68.9270325 -25.4072704, -68.9268967 -25.4073692, -68.9267662 -25.4074736, -68.9266171 -25.4076073, -68.9264632 -25.4077365, -68.9263049 -25.4078612, -68.9261367 -25.4079851, -68.9259641 -25.4081039, -68.9258779 -25.4081635, -68.9257872 -25.4082174, -68.9256864 -25.4082681, -68.9255816 -25.4083118, -68.9254734 -25.4083482, -68.9253645 -25.4083715, -68.9252572 -25.4083998, -68.9251515 -25.408433, -68.9250349 -25.4084763, -68.9249212 -25.4085256, -68.9248109 -25.4085808, -68.9247299 -25.4086643, -68.9246423 -25.4087421, -68.9245484 -25.4088137, -68.9244488 -25.4088788, -68.9243416 -25.4089382, -68.9242296 -25.40899, -68.9241136 -25.4090339, -68.9239942 -25.4090696, -68.9238721 -25.4090969, -68.9234718 -25.409179, -68.9230728 -25.4092664, -68.9227276 -25.4093468, -68.9223835 -25.4094312, -68.9221681 -25.4095192, -68.9219569 -25.4096154, -68.9217505 -25.4097195, -68.92157 -25.4098193, -68.9213938 -25.4099251, -68.9213332 -25.409958, -68.9212758 -25.4099954, -68.9212221 -25.4100369, -68.9211694 -25.4100852, -68.9211217 -25.4101375, -68.9210792 -25.4101934, -68.9210424 -25.4102525, -68.9209901 -25.4103253, -68.9209322 -25.4103947, -68.9208691 -25.4104601, -68.920801 -25.4105214, -68.9204094 -25.4108388, -68.9202135 -25.4109709, -68.9200221 -25.4111082, -68.9198354 -25.4112507, -68.9196587 -25.4113937, -68.9195713 -25.4114663, -68.9194867 -25.4115414, -68.9193526 -25.4116714, -68.9192915 -25.4117411, -68.9192265 -25.4118079, -68.9191508 -25.4118777, -68.9190709 -25.4119435, -68.918987 -25.4120051, -68.9188993 -25.4120623, -68.9182985 -25.4124499, -68.9173222 -25.4130386, -68.9162976 -25.4136588, -68.9162064 -25.4137165, -68.9161212 -25.4137813, -68.9160427 -25.4138526, -68.9159817 -25.413918, -68.9159263 -25.4139872, -68.9158768 -25.4140601, -68.9158335 -25.4141361, -68.9157523 -25.4142578, -68.9156646 -25.4143759, -68.9155728 -25.4144875, -68.9154753 -25.4145949, -68.9153722 -25.4146981, -68.9148223 -25.4152723, -68.9146913 -25.415395, -68.9145652 -25.4155219, -68.9144442 -25.4156526, -68.9143328 -25.4157818, -68.9142264 -25.4159143, -68.914125 -25.41605, -68.9138943 -25.416394, -68.9138372 -25.4164591, -68.9137735 -25.416519, -68.9137039 -25.4165732, -68.9136312 -25.4166199, -68.9135541 -25.4166604, -68.9134732 -25.4166944, -68.913346 -25.416749, -68.9132216 -25.4168088, -68.9131004 -25.4168736, -68.9129668 -25.4169532, -68.9128379 -25.4170389, -68.9127141 -25.4171304, -68.9125616 -25.4172689, -68.912411 -25.417409, -68.9122216 -25.4175906, -68.9120355 -25.4177748, -68.9119607 -25.4178476, -68.911881 -25.417916, -68.9117967 -25.4179799, -68.9117083 -25.4180389, -68.9116141 -25.4180939, -68.9115163 -25.4181435, -68.9114153 -25.4181874, -68.9113113 -25.4182254, -68.9112173 -25.418248, -68.9111263 -25.4182792, -68.9110393 -25.4183186, -68.9109573 -25.4183659, -68.9108864 -25.4184163, -68.9108212 -25.4184726, -68.9107622 -25.4185342, -68.9107099 -25.4186006, -68.9106649 -25.4186712, -68.9103457 -25.419146, -68.9097852 -25.4199769, -68.9097209 -25.4200862, -68.9096637 -25.4201986, -68.9096135 -25.4203137, -68.9095693 -25.4204351, -68.9095331 -25.4205586, -68.9095048 -25.4206838, -68.9094847 -25.4208103, -68.9094595 -25.4209191, -68.9094269 -25.4210263, -68.909387 -25.4211315, -68.9093399 -25.4212342, -68.909284 -25.4213373, -68.909221 -25.4214369, -68.909151 -25.4215327, -68.9090744 -25.4216243, -68.9089429 -25.4217878, -68.9088159 -25.4219541, -68.9086935 -25.4221233, -68.9083368 -25.4226538, -68.9080793 -25.4229857, -68.9080274 -25.423043, -68.90797 -25.4230959, -68.9079075 -25.423144, -68.9078406 -25.4231868, -68.9077671 -25.4232251, -68.9076899 -25.4232571, -68.9076099 -25.4232826, -68.9075275 -25.4233011, -68.9074436 -25.4233127, -68.9073428 -25.4233361, -68.9072441 -25.4233658, -68.9071479 -25.4234017, -68.9070547 -25.4234435, -68.9069491 -25.4235003, -68.9068491 -25.4235648, -68.9067554 -25.4236365, -68.9066684 -25.4237149, -68.9064471 -25.4239367, -68.9062312 -25.424163, -68.906034 -25.424379, -68.9058417 -25.4245987, -68.9056546 -25.4248219, -68.9054704 -25.4249898, -68.9052914 -25.4251621, -68.9051176 -25.4253388, -68.9049491 -25.4255196, -68.9047742 -25.4257184, -68.9046058 -25.4259217, -68.9044207 -25.4261581, -68.9042276 -25.4263892, -68.9040276 -25.4266137, -68.9038199 -25.4268325, -68.903754 -25.4268926, -68.9036925 -25.4269564, -68.9036357 -25.4270237, -68.9035839 -25.4270941, -68.9035343 -25.4271725, -68.9034908 -25.4272538, -68.9034536 -25.4273376, -68.903423 -25.4274236, -68.9033794 -25.4275184, -68.9033311 -25.4276113, -68.9032781 -25.4277022, -68.9032073 -25.4278099, -68.9031298 -25.4279138, -68.9030461 -25.4280137, -68.9029563 -25.4281091, -68.9028801 -25.428211, -68.9027973 -25.4283086, -68.9027081 -25.4284015, -68.9026129 -25.4284894, -68.9024996 -25.4285815, -68.9023796 -25.4286664, -68.9022535 -25.4287438, -68.9018206 -25.4289576, -68.9013898 -25.4291749, -68.9007354 -25.4295141, -68.9002419 -25.4298266, -68.8999458 -25.4300191, -68.8998012 -25.4301195, -68.8996518 -25.4302141, -68.8995069 -25.4302977, -68.8993583 -25.4303757, -68.8992062 -25.430448, -68.899051 -25.4305145, -68.8987755 -25.4306258, -68.8984967 -25.4307299, -68.8982147 -25.4308269, -68.8979298 -25.4309166, -68.8975627 -25.4310398, -68.8971988 -25.4311705, -68.8968383 -25.4313087, -68.8964814 -25.4314544, -68.8961929 -25.4315789, -68.8959071 -25.4317085, -68.8956242 -25.431843, -68.8953441 -25.4319824, -68.8938797 -25.4327043, -68.8925922 -25.4333437, -68.8922985 -25.4334587, -68.8920088 -25.4335814, -68.8917232 -25.4337119, -68.8914597 -25.433841, -68.8912004 -25.4339767, -68.8909453 -25.4341189, -68.8907279 -25.4342228, -68.890514 -25.4343326, -68.8903039 -25.4344482, -68.8900977 -25.4345694, -68.8898838 -25.4347038, -68.8896747 -25.4348444, -68.8894706 -25.4349908, -68.8892718 -25.435143, -68.8890785 -25.4353009, -68.8883489 -25.4359065, -68.8882436 -25.4360048, -68.8881323 -25.4360975, -68.8880153 -25.4361843, -68.887893 -25.4362649, -68.8877514 -25.436347, -68.8876044 -25.4364207, -68.8874525 -25.4364859, -68.8872964 -25.4365422, -68.8871366 -25.4365895, -68.8869511 -25.4366366, -68.8867639 -25.4366782, -68.8865753 -25.4367142, -68.8863856 -25.4367445, -68.8861488 -25.4367743, -68.8859108 -25.4367953, -68.8857913 -25.4367991, -68.8856721 -25.4368075, -68.8855404 -25.4368222, -68.8854095 -25.4368424, -68.8852799 -25.4368683, -68.8851517 -25.4368996, -68.8850676 -25.4369423, -68.8849789 -25.4369768, -68.8848867 -25.4370026, -68.8847919 -25.4370194, -68.8846958 -25.4370271, -68.8845992 -25.4370255, -68.884506 -25.4370151, -68.8844144 -25.4369961, -68.8843255 -25.4369687, -68.8842401 -25.4369332, -68.8841593 -25.4368899, -68.8840965 -25.4368467, -68.8840302 -25.4368078, -68.8839608 -25.4367736, -68.8838812 -25.4367413, -68.8837988 -25.4367151, -68.8837143 -25.4366952, -68.8836283 -25.4366816, -68.8835056 -25.4366587, -68.8833855 -25.4366267, -68.8832688 -25.4365858, -68.8831562 -25.4365362, -68.8830642 -25.4364875, -68.8829762 -25.436433, -68.8828927 -25.436373, -68.8828142 -25.4363078, -68.8827409 -25.4362378, -68.8826734 -25.4361632, -68.8825421 -25.4360516, -68.8824043 -25.4359465, -68.8822603 -25.4358483, -68.8821159 -25.4357603, -68.8819665 -25.4356793, -68.8818127 -25.4356055, -68.8816548 -25.435539, -68.8814932 -25.4354802, -68.8812526 -25.4353773, -68.8810076 -25.4352832, -68.8807586 -25.4351979, -68.8805062 -25.4351217, -68.8802699 -25.4350593, -68.8800314 -25.4350048, -68.8797907 -25.4349582, -68.8795483 -25.4349196, -68.8793045 -25.4348891, -68.8786662 -25.4348407, -68.8767886 -25.4347486, -68.8756782 -25.434676, -68.8754046 -25.4346178, -68.8752901 -25.4345944, -68.8751739 -25.4345791, -68.8750746 -25.4345726, -68.8749751 -25.434572, -68.8748757 -25.4345774, -68.874777 -25.4345888, -68.874648 -25.4345975, -68.8745188 -25.4345995, -68.8743896 -25.4345946, -68.8742609 -25.4345829, -68.8741332 -25.4345645, -68.8736612 -25.4345258, -68.8735327 -25.4345164, -68.8734039 -25.4345116, -68.8732749 -25.4345113, -68.8731043 -25.4345179, -68.8729343 -25.4345324, -68.8727653 -25.4345549, -68.8725229 -25.4346019, -68.8722793 -25.4346434, -68.8720346 -25.4346794, -68.871789 -25.4347099, -68.8715104 -25.4347377, -68.8713703 -25.4347433, -68.8712311 -25.4347583, -68.871097 -25.4347818, -68.8709651 -25.434814, -68.8708361 -25.4348547, -68.8707107 -25.4349037, -68.8705181 -25.4350024, -68.8703224 -25.4350959, -68.8701237 -25.4351841, -68.8699222 -25.435267, -68.8697364 -25.4353378, -68.8695486 -25.435404, -68.8693589 -25.4354656, -68.8687903 -25.4355238, -68.8682914 -25.4355722, -68.8680764 -25.4356145, -68.8678635 -25.4356646, -68.867653 -25.4357224, -68.8674766 -25.4357773, -68.8673023 -25.4358377, -68.8671303 -25.4359034, -68.866961 -25.4359743, -68.8666921 -25.4361039, -68.8664184 -25.436225, -68.8661403 -25.4363376, -68.8658936 -25.4364289, -68.8656441 -25.4365135, -68.8653918 -25.4365913, -68.8651371 -25.4366622, -68.8640964 -25.4369432, -68.8639124 -25.4370076, -68.8637257 -25.4370655, -68.8635366 -25.4371167, -68.8633454 -25.4371612, -68.8631608 -25.4371973, -68.8629748 -25.4372271, -68.8627877 -25.4372506, -68.8625997 -25.4372677, -68.8617307 -25.437258, -68.861649 -25.437262, -68.8615678 -25.4372714, -68.8614876 -25.4372863, -68.8614088 -25.4373065, -68.861314 -25.4373388, -68.8612229 -25.4373789, -68.8611362 -25.4374264, -68.8610548 -25.4374809, -68.8608386 -25.4376212, -68.8606256 -25.4377655, -68.8604779 -25.4378693, -68.8603319 -25.437975, -68.8602697 -25.4380108, -68.8602032 -25.4380396, -68.8601334 -25.438061, -68.8600609 -25.4380748, -68.8599871 -25.4380807, -68.859913 -25.4380785, -68.8598397 -25.4380683, -68.8597182 -25.4380587, -68.8595963 -25.4380581, -68.8594747 -25.4380666, -68.8593543 -25.438084, -68.8589922 -25.4381337, -68.8580936 -25.4382705, -68.8579667 -25.438293, -68.8578415 -25.438322, -68.8577182 -25.4383573, -68.8575974 -25.4383989, -68.8574848 -25.4384442, -68.8573751 -25.4384949, -68.8572686 -25.4385509, -68.8571656 -25.438612, -68.8569496 -25.4387417, -68.8567284 -25.4388639, -68.8565558 -25.4389524, -68.8563805 -25.4390364, -68.8562027 -25.4391159, -68.8560421 -25.4391785, -68.8558855 -25.4392488, -68.8557333 -25.4393266, -68.8555999 -25.439403, -68.8554708 -25.4394851, -68.8553461 -25.4395727, -68.8552263 -25.4396657, -68.8548642 -25.4399781, -68.8547886 -25.4400336, -68.8547079 -25.4400829, -68.8546228 -25.4401259, -68.854526 -25.440165, -68.8544256 -25.4401957, -68.8543224 -25.4402179, -68.8540398 -25.4402645, -68.8537592 -25.4403197, -68.8535128 -25.4403757, -68.8532683 -25.4404383, -68.8531936 -25.4404549, -68.8531173 -25.4404647, -68.8530404 -25.4404677, -68.8529635 -25.4404637, -68.8528875 -25.4404529, -68.8528141 -25.4404356, -68.8527429 -25.4404119, -68.8526747 -25.4403821, -68.85261 -25.4403464, -68.8525495 -25.4403051, -68.8524482 -25.4402308, -68.8523506 -25.4401524, -68.8522571 -25.4400702, -68.852177 -25.4399808, -68.8520915 -25.4398956, -68.852001 -25.4398147, -68.8519058 -25.4397383, -68.8517944 -25.439659, -68.8516778 -25.439586, -68.8515565 -25.4395195, -68.851431 -25.4394598, -68.85113 -25.4393314, -68.8508302 -25.4392006, -68.8502857 -25.438956, -68.8501123 -25.4389224, -68.849937 -25.4388979, -68.849814 -25.4388861, -68.8496906 -25.4388789, -68.8495669 -25.4388761, -68.8493225 -25.4388761, -68.8490783 -25.4388842, -68.8488346 -25.4389003, -68.8485766 -25.4389261, -68.8483199 -25.4389608, -68.8480648 -25.4390044, -68.8478537 -25.4390745, -68.8476437 -25.4391473, -68.8472892 -25.4392778, -68.8469383 -25.4394162, -68.8468058 -25.4394789, -68.8466697 -25.439535, -68.8465305 -25.4395844, -68.8463885 -25.4396269, -68.8462447 -25.4396624, -68.8460991 -25.4396908, -68.8459519 -25.4397121, -68.8458037 -25.4397262, -68.8456716 -25.439734, -68.8455392 -25.4397372, -68.8454068 -25.4397359, -68.8452186 -25.4397264, -68.8450312 -25.4397078, -68.8448452 -25.4396803, -68.8446611 -25.4396439, -68.8445436 -25.4396192, -68.8444245 -25.439602, -68.8443043 -25.4395925, -68.8441837 -25.4395906, -68.8440635 -25.4395964, -68.8439441 -25.4396098, -68.8438261 -25.4396307, -68.8437099 -25.4396591, -68.8435963 -25.4396947, -68.8434874 -25.4397507, -68.8433821 -25.4398118, -68.8432806 -25.439878, -68.8431832 -25.4399491, -68.8430828 -25.4400312, -68.8429878 -25.4401186, -68.8427575 -25.4401938, -68.8425239 -25.44026, -68.8422874 -25.4403172, -68.8420835 -25.4403588, -68.8418782 -25.4403937, -68.8416716 -25.4404218, -68.8415679 -25.440434, -68.8414639 -25.4404432, -68.8413031 -25.4404513, -68.841142 -25.4404521, -68.8409811 -25.4404456, -68.8407745 -25.4404457, -68.8406712 -25.4404492, -68.8405681 -25.4404553, -68.8404062 -25.44047, -68.8402452 -25.440491, -68.8401658 -25.4405075, -68.8400853 -25.4405183, -68.8400007 -25.4405232, -68.839916 -25.4405218, -68.8398318 -25.440514, -68.8397485 -25.4404998, -68.8396668 -25.4404795, -68.8391787 -25.4403633, -68.8380683 -25.4400435, -68.837908 -25.4399975, -68.8377505 -25.4399441, -68.8375962 -25.4398837, -68.8374596 -25.4398229, -68.8373263 -25.4397565, -68.8371965 -25.4396847, -68.8370705 -25.4396075, -68.8368286 -25.4394681, -68.8365812 -25.4393368, -68.8363288 -25.4392135, -68.8360715 -25.4390987, -68.8358098 -25.4389923, -68.833943 -25.4380913, -68.8338057 -25.4380212, -68.8336718 -25.4379459, -68.8335418 -25.4378653, -68.8334157 -25.4377797, -68.8332939 -25.4376892, -68.8331767 -25.4375941, -68.8330642 -25.4374944, -68.8329565 -25.4373904, -68.832854 -25.4372823, -68.8326562 -25.4370833, -68.8324517 -25.4368899, -68.8322392 -25.4366966, -68.8321296 -25.4366031, -68.8320226 -25.4365072, -68.8318635 -25.4363552, -68.8317112 -25.4361978, -68.8315656 -25.4360352, -68.8314937 -25.4359533, -68.8314271 -25.4358677, -68.8313513 -25.4357556, -68.8312847 -25.4356389, -68.8312275 -25.4355181, -68.8311802 -25.4353939, -68.8311428 -25.435267, -68.8310428 -25.4349371, -68.8309389 -25.4346081, -68.8301826 -25.4326437, -68.8301097 -25.4325042, -68.8300429 -25.4323623, -68.8299822 -25.4322182, -68.8299277 -25.432072, -68.8298793 -25.4319234, -68.8298372 -25.4317731, -68.8298017 -25.4316215, -68.8297145 -25.4311289, -68.8296327 -25.4306356, -68.829562 -25.4301805, -68.8295266 -25.4299529, -68.8294959 -25.4297248, -68.8294658 -25.4294562, -68.8294579 -25.4293214, -68.8294423 -25.4291871, -68.8294152 -25.4290362, -68.8293784 -25.4288871, -68.8293319 -25.4287402, -68.829302 -25.4286687, -68.829276 -25.428596, -68.8292454 -25.4284898, -68.829223 -25.4283818, -68.829209 -25.4282728, -68.8292034 -25.4281631, -68.8292062 -25.4280534, -68.8292222 -25.4279279, -68.8292293 -25.4278018, -68.8292276 -25.4276755, -68.829217 -25.4275495, -68.8291983 -25.4274286, -68.8291714 -25.427309, -68.8291365 -25.427191, -68.829075 -25.4270131, -68.8290238 -25.4268325, -68.8289916 -25.4266922, -68.8289657 -25.4265507, -68.828946 -25.4264085, -68.8289326 -25.4262657, -68.828918 -25.4260405, -68.8289184 -25.4259277, -68.8289112 -25.4258151, -68.8288959 -25.4256992, -68.8288726 -25.4255844, -68.8288545 -25.4255283, -68.8288415 -25.4254711, -68.8288321 -25.4253952, -68.8288316 -25.4253189, -68.8288399 -25.4252429, -68.828857 -25.4251681, -68.8288826 -25.4250953, -68.8289166 -25.4250254, -68.8289735 -25.4248899, -68.8290203 -25.4247512, -68.829057 -25.4246101, -68.8290832 -25.4244671, -68.8290989 -25.4243229, -68.8291278 -25.424146, -68.8291493 -25.4239683, -68.8291633 -25.4237899, -68.8291702 -25.4235783, -68.8291667 -25.4233666, -68.8291526 -25.4231553, -68.8291338 -25.4230315, -68.8291213 -25.422907, -68.829115 -25.4227822, -68.8291156 -25.4226381, -68.8291246 -25.4224942, -68.8291419 -25.422351, -68.8291843 -25.422056, -68.829217 -25.4217599, -68.8292363 -25.4215178, -68.8292406 -25.4213965, -68.8292491 -25.4212754, -68.8292636 -25.4211424, -68.8292833 -25.4210099, -68.8293082 -25.4208781, -68.8293245 -25.4207158, -68.8293478 -25.4205543, -68.8293779 -25.4203936, -68.8294222 -25.4202057, -68.8294759 -25.4200199, -68.8295388 -25.4198364, -68.8295817 -25.4196922, -68.8296193 -25.4195468, -68.8296373 -25.4194739, -68.8296515 -25.4194004, -68.8296651 -25.419293, -68.8296706 -25.4191849, -68.8296678 -25.4190768, -68.8296568 -25.4189692, -68.8296495 -25.418819, -68.829652 -25.4187439, -68.8296515 -25.4186688, -68.8296461 -25.4185666, -68.8296354 -25.4184648, -68.8296193 -25.4183635, -68.8295987 -25.4182796, -68.8295719 -25.4181971, -68.8295388 -25.4181164, -68.8294107 -25.4177197, -68.8292867 -25.4173218, -68.8291414 -25.4168356, -68.8290024 -25.4163479, -68.8288296 -25.4157165, -68.8287667 -25.4155071, -68.8287116 -25.415296, -68.8286644 -25.4150833, -68.8286387 -25.4149479, -68.8286295 -25.4148797, -68.8286161 -25.414812, -68.8285896 -25.4147164, -68.8285546 -25.4146231, -68.8285113 -25.4145327, -68.82846 -25.4144457, -68.8284012 -25.4143628, -68.828335 -25.4142844, -68.8282621 -25.4142112, -68.8281184 -25.4140821, -68.8279804 -25.413948, -68.8278482 -25.4138092, -68.8277221 -25.4136659, -68.8276023 -25.4135183, -68.8274862 -25.4133628, -68.827377 -25.4132033, -68.8272155 -25.4129759, -68.8270492 -25.4127514, -68.8268781 -25.4125298, -68.8266755 -25.4122722, -68.8265742 -25.4121434, -68.8264704 -25.4120162, -68.8263095 -25.4118278, -68.8261432 -25.4116431, -68.8260924 -25.4115847, -68.8260467 -25.411523, -68.8260063 -25.4114584, -68.8259715 -25.4113912, -68.8259424 -25.411322, -68.8259193 -25.411251, -68.8259021 -25.4111786, -68.825891 -25.4111053, -68.8258618 -25.4107358, -68.8258404 -25.4103659, -68.8258267 -25.4099957, -68.8258211 -25.4096824, -68.8258211 -25.409369, -68.8258267 -25.4090557, -68.8258279 -25.4089471, -68.8258212 -25.4088387, -68.8258064 -25.408731, -68.8257837 -25.4086244, -68.8257529 -25.4085185, -68.8257142 -25.4084146, -68.8256679 -25.4083134, -68.8256141 -25.4082152, -68.8255531 -25.4081205, -68.8253974 -25.4078845, -68.8252473 -25.4076456, -68.8250826 -25.4073688, -68.8249254 -25.4070884, -68.824871 -25.4069997, -68.8248107 -25.4069142, -68.8247447 -25.4068322, -68.8246733 -25.406754, -68.8245767 -25.406662, -68.8244725 -25.4065768, -68.8243615 -25.4064991, -68.8242441 -25.4064294, -68.8240399 -25.4062803, -68.8238325 -25.4061349, -68.8236219 -25.4059933, -68.823357 -25.4058233, -68.8230877 -25.4056592, -68.8228139 -25.4055011, -68.822536 -25.4053492, -68.8222539 -25.4052034, -68.821578 -25.4047964, -68.8213653 -25.4047148, -68.8211489 -25.4046413, -68.8209249 -25.4045749, -68.8206979 -25.4045171, -68.8204685 -25.4044682, -68.8202369 -25.4044281, -68.8199898 -25.4043848, -68.8197411 -25.4043493, -68.8194913 -25.4043215, -68.8192518 -25.4043023, -68.8190118 -25.4042902, -68.8187714 -25.4042853, -68.818531 -25.4042876, -68.8180455 -25.4042781, -68.8175601 -25.4042634, -68.8169713 -25.4042386, -68.816383 -25.4042063, -68.8157952 -25.4041665, -68.8151094 -25.404112, -68.8144245 -25.404049, -68.8140822 -25.4040159, -68.8137406 -25.4039775, -68.8133701 -25.4039298, -68.8132437 -25.4039114, -68.8130007 -25.4038759, -68.8126324 -25.4038159, -68.8125698 -25.4038046, -68.8122654 -25.4037497, -68.8118189 -25.4036644, -68.8113736 -25.4035739, -68.8109297 -25.4034784, -68.8104931 -25.4033838, -68.8100553 -25.4032942, -68.8095731 -25.4032016, -68.8090897 -25.4031149, -68.8088996 -25.4030834, -68.8087116 -25.4030429, -68.8085263 -25.4029936, -68.808344 -25.4029356, -68.8081664 -25.4028695, -68.8079928 -25.4027952, -68.8078237 -25.4027127, -68.8072121 -25.4024317, -68.8070941 -25.4023842, -68.8069722 -25.4023453, -68.8068473 -25.4023154, -68.8067391 -25.4022971, -68.8066298 -25.4022854, -68.8065199 -25.4022806)"
        split_points_wkts = [
            "POINT (-69.247486 -25.388171)",
            "POINT (-69.2467939 -25.3886713)",
            "POINT (-69.2365359 -25.403396)",
            "POINT (-68.8429878 -25.4401186)",
            "POINT (-68.8264704 -25.4120162)",
            "POINT (-68.8132437 -25.4039114)",
            "POINT (-68.8125698 -25.4038046)",
            "POINT (-68.8065199 -25.4022806)",
        ]

        input_segment_geometry = wkt.loads(input_segment_wkt)
        joined_connectors = [Connector(str(index), wkt.loads(point_wkt), index) for index, point_wkt in enumerate(split_points_wkts)]
        
        split_points = get_connector_split_points(joined_connectors, input_segment_geometry)
        sorted_split_points = sorted(split_points, key=lambda p: p.lr)
        split_segments = split_line(input_segment_geometry, sorted_split_points)

        for ss in split_segments:
            assert(not has_consecutive_dupe_coords(ss.geometry))

    make_split_point_params = [
        (0.5, "POINT (0 0)", "LINESTRING (0 -1, 0 0, 0 1)"),

        # Segment 0861f8e8b7ffffff0472f50156177cbd
        (0, "POINT (9.0801344 47.6186536)", "LINESTRING (9.0801344 47.6186536, 9.0800997 47.6186326, 9.0800791 47.6185924, 9.0800843 47.6185427, 9.0801188 47.6184615, 9.0801668 47.6183788, 9.0804543 47.6180565, 9.0805653 47.6179441, 9.0810953 47.6173648, 9.0814542 47.616938, 9.0816872 47.6165648, 9.0822655 47.6154759, 9.0824197 47.6151925, 9.0828786 47.6143488, 9.0830582 47.6140789, 9.08328 47.6138545, 9.0835056 47.6136879, 9.0837723 47.6135666, 9.0840884 47.6134675, 9.0843454 47.613421, 9.0845588 47.6134024, 9.0848577 47.6134032, 9.0856258 47.6134536, 9.0887196 47.6137318, 9.0890434 47.6137309, 9.0893306 47.6137106, 9.0896078 47.6136567, 9.0898093 47.6135883, 9.0902317 47.6134075, 9.0902962 47.6133758)"),
        (0.065383196, "POINT (9.0804543 47.6180565)", "LINESTRING (9.0801344 47.6186536, 9.0800997 47.6186326, 9.0800791 47.6185924, 9.0800843 47.6185427, 9.0801188 47.6184615, 9.0801668 47.6183788, 9.0804543 47.6180565, 9.0805653 47.6179441, 9.0810953 47.6173648, 9.0814542 47.616938, 9.0816872 47.6165648, 9.0822655 47.6154759, 9.0824197 47.6151925, 9.0828786 47.6143488, 9.0830582 47.6140789, 9.08328 47.6138545, 9.0835056 47.6136879, 9.0837723 47.6135666, 9.0840884 47.6134675, 9.0843454 47.613421, 9.0845588 47.6134024, 9.0848577 47.6134032, 9.0856258 47.6134536, 9.0887196 47.6137318, 9.0890434 47.6137309, 9.0893306 47.6137106, 9.0896078 47.6136567, 9.0898093 47.6135883, 9.0902317 47.6134075, 9.0902962 47.6133758)"), # tunnel start
        (1, "POINT (9.0902962 47.6133758)", "LINESTRING (9.0801344 47.6186536, 9.0800997 47.6186326, 9.0800791 47.6185924, 9.0800843 47.6185427, 9.0801188 47.6184615, 9.0801668 47.6183788, 9.0804543 47.6180565, 9.0805653 47.6179441, 9.0810953 47.6173648, 9.0814542 47.616938, 9.0816872 47.6165648, 9.0822655 47.6154759, 9.0824197 47.6151925, 9.0828786 47.6143488, 9.0830582 47.6140789, 9.08328 47.6138545, 9.0835056 47.6136879, 9.0837723 47.6135666, 9.0840884 47.6134675, 9.0843454 47.613421, 9.0845588 47.6134024, 9.0848577 47.6134032, 9.0856258 47.6134536, 9.0887196 47.6137318, 9.0890434 47.6137309, 9.0893306 47.6137106, 9.0896078 47.6136567, 9.0898093 47.6135883, 9.0902317 47.6134075, 9.0902962 47.6133758)"),

        # Segment 08811942715fffff045effc98960a032
        (0, "POINT (36.1369686 51.7212314)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"),
        (0.4511375, "POINT (36.1364841 51.7211159)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"),
        (0.6320235, "POINT (36.1357653 51.7209887)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"),
        (0.6474039, "POINT (36.1357133 51.7210215)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"),
        (0.7476722, "POINT (36.1356138 51.7213004)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"),
        (0.7621897, "POINT (36.135643 51.7213398)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"),
        # edge case if multiple matches for same point LR should be 1 if it matches last coord
        (1.0, "POINT (36.1366374 51.7214502)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)")
    ]
    def test_make_split_point(self):
        for expected_lr, connector_geometry, segment_geometry in self.make_split_point_params:
            with self.subTest(expected_lr=expected_lr, connector_geometry=connector_geometry, segment_geometry=segment_geometry):
                connector = Connector(
                    "0",
                    wkt.loads(connector_geometry),
                    0
                )
                segment_geometry = wkt.loads(segment_geometry)

                split_points = get_connector_split_points([connector], segment_geometry)
                self.assertEqual(len(split_points), 1, f"Expected 1 split point to be added")
                split_point = split_points[0]
                self.assertEqual(split_point.id, connector.connector_id)
                self.assertEqual(split_point.geometry, connector.connector_geometry)
                diff = abs(split_point.lr - expected_lr)
                max_diff = 0.0037 # using old relative threshold that worked for these test cases
                self.assertLess(diff, max_diff, f"Expected LR {expected_lr} but got {split_point.lr}, diff={diff} > {max_diff} for point {connector_geometry} on {segment_geometry}")

    all_lr_split_points_params = [
        (["POINT (0 -1)", "POINT (0 0)", "POINT (0 1)"], [0, 0.5, 1], "LINESTRING (0 -1, 0 0, 0 1)"),
        (
            ["POINT (9.0801344 47.6186536)", "POINT (9.0804543 47.6180565)", "POINT (9.0902962 47.6133758)"],
            [0, 0.065383196, 1],
            "LINESTRING (9.0801344 47.6186536, 9.0800997 47.6186326, 9.0800791 47.6185924, 9.0800843 47.6185427, 9.0801188 47.6184615, 9.0801668 47.6183788, 9.0804543 47.6180565, 9.0805653 47.6179441, 9.0810953 47.6173648, 9.0814542 47.616938, 9.0816872 47.6165648, 9.0822655 47.6154759, 9.0824197 47.6151925, 9.0828786 47.6143488, 9.0830582 47.6140789, 9.08328 47.6138545, 9.0835056 47.6136879, 9.0837723 47.6135666, 9.0840884 47.6134675, 9.0843454 47.613421, 9.0845588 47.6134024, 9.0848577 47.6134032, 9.0856258 47.6134536, 9.0887196 47.6137318, 9.0890434 47.6137309, 9.0893306 47.6137106, 9.0896078 47.6136567, 9.0898093 47.6135883, 9.0902317 47.6134075, 9.0902962 47.6133758)"
        ),
        # this is the new bug test case
        (["POINT(-75.5559947 6.3343023)", "POINT(-75.555979  6.3343055)", "POINT (-75.5559789 6.3343055)", "POINT(-75.5559623 6.3343089)"],
         [0, 0.4834686262557952, 0.4875426572351761, 1],
         "LINESTRING (-75.5559947 6.3343023,  -75.5559789 6.3343055, -75.5559623 6.3343089)"),        
    ]

    def test_add_lr_split_points(self):
        for expected_points, lrs, segment_geometry in self.all_lr_split_points_params:
            with self.subTest(expected_points=expected_points, lrs=lrs, segment_geometry=segment_geometry):
                split_points = add_lr_split_points([], lrs, "1", wkt.loads(segment_geometry))
                self.assertEqual(len(split_points), len(expected_points))
                for split_point, expected_point, lr in zip(split_points, expected_points, lrs):
                    self.assertEqual(split_point.id, f"1@{lr}")
                    self.assertEqual(split_point.lr, lr)
                    self.assertTrue(equals(split_point.geometry, wkt.loads(expected_point)), f"Expected {expected_point} but got {split_point.geometry}")
                    
    def test_split_line_simple(self):
        segment_wkt = "LINESTRING (0 -1, 0 0, 0 1)"
        split_point_wkts = [
            "POINT (0 -1)",
            "POINT (0 0)",
            "POINT (0 1)",            
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts)
        self.assert_expected_splits(split_segments, [
            "LINESTRING (0 -1, 0 0)",
            "LINESTRING (0 0, 0 1)"
        ])

    def test_split_line_no_mid_split_points(self):
        segment_wkt ="LINESTRING (0 -1, 0 0, 0 1)"
        split_point_wkts = [
            "POINT (0 -1)",
            "POINT (0 1)",            
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts)
        self.assert_expected_splits(split_segments, ["LINESTRING (0 -1, 0 0, 0 1)"])

    def test_split_line_middle(self):
        segment_wkt ="LINESTRING (18.3863377 47.760202, 18.3828519 47.7581581, 18.3824642 47.7580508, 18.3748238 47.7543556, 18.3778371 47.7498419, 18.3773911 47.7496924)"
        lrs = [0, 0.207325479, 0.22614114, 1]
        split_segments = self.split_line(segment_wkt, [], lrs)
        self.assert_expected_splits(split_segments, [
            "LINESTRING (18.3863377 47.760202, 18.3828519 47.7581581)",
            "LINESTRING (18.3828519 47.7581581, 18.3824642 47.7580508, 18.3824641 47.7580508)",
            "LINESTRING (18.3824641 47.7580508, 18.3748238 47.7543556, 18.3778371 47.7498419, 18.3773911 47.7496924)"
            ])        

    def test_split_line(self):
        # Segment 08811942715fffff045effc98960a032
        segment_wkt = "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"
        
        split_point_wkts = [
            "POINT (36.1369686 51.7212314)",
            "POINT (36.135643 51.7213398)",            
            "POINT (36.1366374 51.7214502)",
        ]

        split_segments = self.split_line(segment_wkt, split_point_wkts)
        self.assert_expected_splits(split_segments, [
            "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398)",
            "LINESTRING (36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)",            
        ])

    def test_split_line_close_connectors(self):
        segment_wkt ="LINESTRING (-75.5559947 6.3343023, -75.5559789 6.3343055, -75.5559623 6.3343089)"
        split_point_wkts = [
            "POINT (-75.5559947 6.3343023)",
            "POINT (-75.555979  6.3343055)",
            "POINT (-75.5559789 6.3343055)",
            "POINT (-75.5559623 6.3343089)",
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts)

        expected_splits = [
            "LINESTRING (-75.5559947 6.3343023, -75.555979  6.3343055)",
            "LINESTRING (-75.555979  6.3343055, -75.5559789 6.3343055)",
            "LINESTRING (-75.5559789 6.3343055, -75.5559623 6.3343089)",           
        ]
        self.assert_expected_splits(split_segments, expected_splits)


    def test_split_line_close_lrs(self):
        segment_wkt ="LINESTRING (-75.5559947 6.3343023, -75.5559789 6.3343055, -75.5559623 6.3343089)"
        split_point_wkts = [
            "POINT (-75.5559947 6.3343023)",
            "POINT (-75.5559623 6.3343089)",
        ]
        lrs = [
            0.4834686262557952, #POINT (-75.555979 6.3343055)
            0.4875426572351761, #POINT (-75.5559789 6.3343055)
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts, lrs)        

        expected_splits = [
            "LINESTRING (-75.5559947 6.3343023, -75.555979  6.3343055)",
            "LINESTRING (-75.555979  6.3343055, -75.5559789 6.3343055)",
            "LINESTRING (-75.5559789 6.3343055, -75.5559623 6.3343089)",           
        ]

        self.assert_expected_splits(split_segments, expected_splits)

    def test_self_intersecting(self): 
        segment_wkt ="LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"
        split_point_wkts = [
            "POINT (36.1369686 51.7212314)",
            "POINT (36.1366374 51.7214502)",
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts)

        expected_splits = [
            segment_wkt
        ]

        self.assert_expected_splits(split_segments, expected_splits)        

    def test_self_intersecting2(self): 
        segment_wkt ="LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"
        split_point_wkts = [
            "POINT (36.1369686 51.7212314)",
            "POINT (36.1366374 51.7214502)",
            "POINT (36.1366374 51.7214502)",
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts)

        expected_splits = [
            "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502)",
            "LINESTRING (36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"
        ]

        self.assert_expected_splits(split_segments, expected_splits)        

    def test_self_simple_case(self): 
        segment_wkt ="LINESTRING (-122.1861714 47.6170546, -122.1862553 47.617057, -122.1863795 47.6169457, -122.1865169 47.6168246, -122.1865565 47.616773, -122.1865631 47.6167195, -122.1864931 47.6166421, -122.1864337 47.6166269, -122.186071 47.6166306)"
        split_point_wkts = [
            "POINT (-122.1861714 47.6170546)",
            "POINT (-122.186071 47.6166306)",
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts)

        expected_splits = [
            "LINESTRING (-122.1861714 47.6170546, -122.1862553 47.617057, -122.1863795 47.6169457, -122.1865169 47.6168246, -122.1865565 47.616773, -122.1865631 47.6167195, -122.1864931 47.6166421, -122.1864337 47.6166269, -122.186071 47.6166306)",
        ]

        self.assert_expected_splits(split_segments, expected_splits)

    def test_self_close_lrs(self): 
        segment_wkt ="LINESTRING (-52.2980902 -27.3932688, -52.2981714 -27.3933322, -52.2983511 -27.3934847, -52.298498 -27.393567)"
        split_point_wkts = [
            "POINT (-52.2980902 -27.3932688)",
            "POINT (-52.298498 -27.393567)",
        ]
        # even though 0.672404689 is "different" LR than previous one 0.672192 
        # according to LR_SPLIT_POINT_MIN_DIST_METERS (they are >1cm apart), 
        # it results after rounding the two corresponding split points in the
        # same coordinates, which would produce invalid 1 coordinate lines, 
        # so we expect the code to "snap" these two LRs into one and handle this 
        # case gracefuly
        lrs = [
            0,
            0.203822671,
            0.672192,
            0.672404689, 
            1
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts, lrs)

        expected_splits = [
            "LINESTRING (-52.2980902 -27.3932688, -52.2981714 -27.3933322)",
            "LINESTRING (-52.2981714 -27.3933322, -52.2983511 -27.3934847)",
            "LINESTRING (-52.2983511 -27.3934847, -52.298498 -27.393567)",
        ]

        self.assert_expected_splits(split_segments, expected_splits)


    def test_self_close_lrs2(self): 
        segment_wkt ="LINESTRING (-42.4236725 -22.1629146, -42.4235532 -22.1628487, -42.4234941 -22.1628052, -42.4230324 -22.1624829, -42.4229095 -22.162408, -42.4228656 -22.1623602)"
        split_point_wkts = [
            "POINT (-42.4236725 -22.1629146)",
            "POINT (-42.4228656 -22.1623602)",
        ]
        lrs = [
            0,
            0.212825,
            0.212921497,
            0.786432, 
            0.786787361,
            1
        ]
        split_segments = self.split_line(segment_wkt, split_point_wkts, lrs)

        expected_splits = [
            "LINESTRING (-42.4236725 -22.1629146, -42.4235532 -22.1628487, -42.4234941 -22.1628052)",
            "LINESTRING (-42.4234941 -22.1628052, -42.4230327 -22.1624831)",
            "LINESTRING (-42.4230327 -22.1624831, -42.4230324 -22.1624829)",
            "LINESTRING (-42.4230324 -22.1624829, -42.4229095 -22.162408, -42.4228656 -22.1623602)",
        ]

        self.assert_expected_splits(split_segments, expected_splits)



    def split_line(self, segment_wkt:str, split_point_wkts: list[str], lrs: list[float]=[]) -> list[SplitSegment]:
        segment_geometry = wkt.loads(segment_wkt)
        joined_connectors = [Connector(str(i), wkt.loads(point_wkt), i) for i, point_wkt in enumerate(split_point_wkts)]
        split_points: list[SplitPoint] = get_connector_split_points(joined_connectors, segment_geometry)        
        add_lr_split_points(split_points, lrs, "", segment_geometry)
        sorted_split_points = sorted(split_points, key=lambda p: p.lr)
        return split_line(segment_geometry, sorted_split_points)
    
    def assert_expected_splits(self, split_segments, expected_splits):
        self.assertEqual(len(split_segments), len(expected_splits))
        for index, (expected, actual) in enumerate(zip(expected_splits, split_segments)):
            assert(not has_consecutive_dupe_coords(actual.geometry))
            self.assertEqual(wkt.loads(expected), actual.geometry, f"Incorrect split number #{index}:\nexpected:\n{expected}\nbut got\n{str(actual.geometry)}")


# COMMAND ----------

# Run the tests before starting splitter
unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

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
