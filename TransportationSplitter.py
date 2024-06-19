# Databricks notebook source
# MAGIC %md
# MAGIC Please see instructions and details [here](https://github.com/OvertureMaps/tf-data-platform/blob/dev/splitrefs/datapipelines/examples/TransportationSplitter.md).
# MAGIC
# MAGIC # AWS Glue notebook - see instructions for magic commands

# COMMAND ----------

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

PROHIBITED_TRANSITIONS_COLUMN = "prohibited_transitions"

# List of columns that are considered for identifying the LRs values to split at is constructed at runtime out of input parquet's schema columns that have LR_SCOPE_KEY = "between" anywhere in their structure. The include/exclude constants below are applied to that list.
# Explicit list of column names to use for finding LR "between" values to split at. Set to None or empty list to use all columns that have "between" fields.
LR_COLUMNS_TO_INCLUDE = []
# list columns to exclude from finding the LR values to split at. Set to None or empty list to use all columns that have "between" fields.
LR_COLUMNS_TO_EXCLUDE = [] 

LR_SCOPE_KEY = "between"
POINT_PRECISION = 7
SPLIT_POINT_THRESHOLD = 0.0037 # New split points are needed for linear references. How far from existing overture connectors do these LRs need to be for us to create a new connector for them instead of using the existing connector.
class SplitPoint:
    """POCO to represent a segment split point."""
    def __init__(self, id=None, geometry=None, lr=None, is_lr_added=False):
        self.id = id
        self.geometry = geometry
        self.lr = lr
        self.is_lr_added = is_lr_added

    def __repr__(self):
        return f"SplitPoint(id={self.id!r}, geometry={self.geometry!r}, lr={self.lr!r}), is_lr_added={self.is_lr_added!r}"

class SplitSegment:
    """POCO to represent a split segment."""
    def __init__(self, id=None, geometry=None, start_split_point=None, end_split_point=None):
        self.id = id
        self.geometry = geometry
        self.start_split_point = start_split_point
        self.end_split_point = end_split_point

    def __repr__(self):
        return f"SplitSegment(id={self.id!r}, geometry={self.geometry!r}, start_split_point={self.start_split_point!r}, end_split_point={self.end_split_point!r})"


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
    connectors_df = connector_df = input_df.filter(col("type") == "connector")\
        .withColumnRenamed("id", "connector_id")\
        .withColumnRenamed("geometry", "connector_geometry")\

    segments_connectors_exploded = segments_df.select(
        col("segment_id"),
        explode("connector_ids").alias("connector_id")
    )

    # Step 2: Join with connectors_df to get connector geometry
    joined_df = segments_connectors_exploded.join(
        connectors_df,
        segments_connectors_exploded.connector_id == connectors_df.connector_id,
        "left"
    ).select(
        segments_connectors_exploded.segment_id,
        segments_connectors_exploded.connector_id,
        connectors_df.connector_geometry
    )

    # Step 3: Group by segment_id and aggregate connector information
    aggregated_connectors = joined_df.groupBy("segment_id").agg(
        collect_list(
            struct(
                col("connector_id"),
                col("connector_geometry")
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

def are_different_coords(coords1, coords2):
    return coords1[0] != coords2[0] or coords1[1] != coords2[1]

def round_point(point, precision=POINT_PRECISION):
    return Point(round(point.x, precision), round(point.y, precision))

def split_line(original_line_geometry: LineString, split_points: [SplitPoint] ) -> [SplitSegment]:
    """Split the LineString into segments at the given points"""

    # Special case to avoid processing when there are only start/end split points
    if len(split_points) == 2 and split_points[0].lr == 0 and split_points[1].lr == 1:
        return [SplitSegment(0, original_line_geometry, split_points[0], split_points[1])]

    original_coordinates = list(original_line_geometry.coords)
    split_segments = []
    last_coordinate_index = 0
    last_coordinate_dist = 0.0
    last_segment_coordinates = [split_points[0].geometry.coords[0]] # initialize first split segment with first split point

    remaining_line_geometry = original_line_geometry

    for i in range(len(split_points) - 1):
        next_split_geometry = split_points[i + 1].geometry
        split_line = ops.split(ops.snap(remaining_line_geometry, next_split_geometry, tolerance=0.0000001), next_split_geometry)
        split_segment_geometry = LineString(split_line.geoms[0])
        if len(split_line.geoms) > 1:
            # Only use remainder of line for snapping to minimize chance of snapping to the wrong location when segment is self-intersecting
            remaining_line_geometry = split_line.geoms[1]
        split_segments.append(SplitSegment(i, split_segment_geometry, split_points[i], split_points[i + 1]))

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
    return (between[0] if between[0] else 0) <= split_segment.start_split_point.lr and (between[1] if between[1] else 1) >= split_segment.end_split_point.lr

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

    if PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
        # remove turn restrictions that we're sure don't apply to this split and construct turn_restrictions field -
        # this is a flat list of all segment_id referenced in sequences;
        # used to resolve segment references after split - for each TR segment_id reference we need to identify which of the splits to retain as reference
        prohibited_transitions = modified_segment_dict.get(PROHIBITED_TRANSITIONS_COLUMN) or {}
        modified_segment_dict[PROHIBITED_TRANSITIONS_COLUMN], modified_segment_dict["turn_restrictions"] = get_trs(prohibited_transitions, modified_segment_dict["connector_ids"])

    return modified_segment_dict

def is_distinct_split_lr(lr1, lr2):
    return abs(lr1 - lr2) > SPLIT_POINT_THRESHOLD

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
    for lr in lrs:
        add_split_point = False
        if len(split_points) == 0:
            add_split_point = True
        else:
            closest_existing_split_point = min(split_points, key=lambda existing: abs(lr - existing.lr))
            if is_distinct_split_lr(closest_existing_split_point.lr, lr):
                add_split_point = True

        if add_split_point:
            target_length = lr * line_length

            coords = list(original_segment_geometry.coords)
            for (lon1, lat1), (lon2, lat2) in zip(coords[:-1], coords[1:]):
                (azimuth, _, subsegment_length) = geod.inv(lon1, lat1, lon2, lat2, return_back_azimuth=False)
                if round(target_length - subsegment_length, 6) <= 0:
                    # Compute final point on this subsegment
                    break
                target_length -= subsegment_length

            # target_length is the length along this subsegment where the point is located. Use geod.fwd()
            # with the azimuth to get the final point
            split_lon, split_lat, _ = geod.fwd(lon1, lat1, azimuth, target_length, return_back_azimuth=False)

            point_geometry = round_point(Point(split_lon, split_lat))
            split_points.append(SplitPoint(f"{segment_id}@{str(lr)}", point_geometry, lr, is_lr_added=True))
    return split_points


def make_split_point(connector, segment_geometry):
    """
    Determine the linear-referenced length along the segment for the given connector
    """
    # Start point special case because split will provide the full line
    if not are_different_coords([connector.connector_geometry.x, connector.connector_geometry.y], list(segment_geometry.coords)[0]):
        return SplitPoint(connector.connector_id, connector.connector_geometry, 0)

    # Snap segment to connector geometry which adds it as a vertex, then split at the new line
    split_line = ops.split(ops.snap(segment_geometry, connector.connector_geometry, tolerance=0.01), connector.connector_geometry)

    # Compute length for split segment (first in collection) and compare to full segment
    geod = pyproj.Geod(ellps="WGS84")
    target_geo_length = geod.geometry_length(split_line.geoms[0])
    total_geo_length = geod.geometry_length(segment_geometry)
    lr = target_geo_length / total_geo_length
    return SplitPoint(connector.connector_id, connector.connector_geometry, lr)

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

            split_points = [make_split_point(connector, input_segment.geometry) for connector in input_segment.joined_connectors if connector.connector_geometry is not None]

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
    if PROHIBITED_TRANSITIONS_COLUMN in lr_columns_for_splitting:
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

class TestSplitter(unittest.TestCase):

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
        (1.0, "POINT (36.1366374 51.7214502)", "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)")
    ]
    def test_make_split_point(self):
        for expected_lr, connector_geometry, segment_geometry in self.make_split_point_params:
            with self.subTest(expected_lr=expected_lr, connector_geometry=connector_geometry, segment_geometry=segment_geometry):
                connector = Connector(
                    "1",
                    wkt.loads(connector_geometry)
                )
                segment_geometry = wkt.loads(segment_geometry)

                split_point = make_split_point(connector, segment_geometry)
                self.assertEqual(split_point.id, connector.connector_id)
                self.assertEqual(split_point.geometry, connector.connector_geometry)
                self.assertTrue(not is_distinct_split_lr(split_point.lr, expected_lr), f"Expected {expected_lr} but got {split_point.lr}")

    all_lr_split_points_params = [
        (["POINT (0 -1)", "POINT (0 0)", "POINT (0 1)"], [0, 0.5, 1], "LINESTRING (0 -1, 0 0, 0 1)"),

        # Segment 0861f8e8b7ffffff0472f50156177cbd
        (
            ["POINT (9.0801344 47.6186536)", "POINT (9.0804543 47.6180565)", "POINT (9.0902962 47.6133758)"],
            [0, 0.065383196, 1],
            "LINESTRING (9.0801344 47.6186536, 9.0800997 47.6186326, 9.0800791 47.6185924, 9.0800843 47.6185427, 9.0801188 47.6184615, 9.0801668 47.6183788, 9.0804543 47.6180565, 9.0805653 47.6179441, 9.0810953 47.6173648, 9.0814542 47.616938, 9.0816872 47.6165648, 9.0822655 47.6154759, 9.0824197 47.6151925, 9.0828786 47.6143488, 9.0830582 47.6140789, 9.08328 47.6138545, 9.0835056 47.6136879, 9.0837723 47.6135666, 9.0840884 47.6134675, 9.0843454 47.613421, 9.0845588 47.6134024, 9.0848577 47.6134032, 9.0856258 47.6134536, 9.0887196 47.6137318, 9.0890434 47.6137309, 9.0893306 47.6137106, 9.0896078 47.6136567, 9.0898093 47.6135883, 9.0902317 47.6134075, 9.0902962 47.6133758)"
        )
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
        segment_geometry = wkt.loads("LINESTRING (0 -1, 0 0, 0 1)")
        split_points = [
            make_split_point(Connector("1", wkt.loads("POINT (0 -1)")), segment_geometry),
            make_split_point(Connector("2", wkt.loads("POINT (0 0)")), segment_geometry),
            make_split_point(Connector("3", wkt.loads("POINT (0 1)")), segment_geometry),
        ]
        split_segments = split_line(segment_geometry, split_points)

        self.assertEqual(len(split_segments), 2)
        self.assertTrue(equals(split_segments[0].geometry, wkt.loads("LINESTRING (0 -1, 0 0)")), f"Unexpected line split value {wkt.dumps(split_segments[0].geometry)} for first segment split")
        self.assertTrue(equals(split_segments[1].geometry, wkt.loads("LINESTRING (0 0, 0 1)")), f"Unexpected line split value {wkt.dumps(split_segments[1].geometry)} for second segment split")

    def test_split_line_no_mid_split_points(self):
        segment_geometry = wkt.loads("LINESTRING (0 -1, 0 0, 0 1)")
        split_points = [
            make_split_point(Connector("1", wkt.loads("POINT (0 -1)")), segment_geometry),
            make_split_point(Connector("2", wkt.loads("POINT (0 1)")), segment_geometry),
        ]
        split_segments = split_line(segment_geometry, split_points)

        self.assertEqual(len(split_segments), 1, "Only one segment expected")
        self.assertTrue(equals(split_segments[0].geometry, wkt.loads("LINESTRING (0 -1, 0 0, 0 1)")), f"Unexpected line split value {wkt.dumps(split_segments[0].geometry)} for segment")

    def test_split_line_middle(self):
        segment_geometry = wkt.loads("LINESTRING (18.3863377 47.760202, 18.3828519 47.7581581, 18.3824642 47.7580508, 18.3748238 47.7543556, 18.3778371 47.7498419, 18.3773911 47.7496924))")
        split_points = add_lr_split_points([], [0, 0.207325479, 0.22614114, 1], "1", segment_geometry)
        split_segments = split_line(segment_geometry, split_points)

        self.assertEqual(len(split_segments), 3)
        self.assertTrue(equals(split_segments[0].geometry,
            wkt.loads("LINESTRING (18.3863377 47.760202, 18.3828519 47.7581581)")),
            f"Unexpected line split value {wkt.dumps(split_segments[0].geometry)} for first segment split"
        )
        self.assertTrue(equals(split_segments[1].geometry,
            wkt.loads("LINESTRING (18.3828519 47.7581581, 18.3824642 47.7580508, 18.3824641 47.7580508)")),
            f"Unexpected line split value {split_segments[1].geometry} for second segment split"
        )
        self.assertTrue(equals(split_segments[2].geometry,
            wkt.loads("LINESTRING (18.3824641 47.7580508, 18.3748238 47.7543556, 18.3778371 47.7498419, 18.3773911 47.7496924))")),
            f"Unexpected line split value {wkt.dumps(split_segments[2].geometry)} for third segment split"
        )



    def test_split_line(self):
        # Segment 08811942715fffff045effc98960a032
        segment_geometry = wkt.loads(
            "LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)"
        )
        split_points = [
            make_split_point(Connector("1", wkt.loads("POINT (36.1369686 51.7212314)")), segment_geometry),
            make_split_point(Connector("2", wkt.loads("POINT (36.135643 51.7213398)")), segment_geometry),
            make_split_point(Connector("3", wkt.loads("POINT (36.1366374 51.7214502)")), segment_geometry),
        ]
        split_segments = split_line(segment_geometry, split_points)

        self.assertEqual(len(split_segments), 2)
        self.assertTrue(equals(split_segments[0].geometry,
            wkt.loads("LINESTRING (36.1369686 51.7212314, 36.136962 51.7218083, 36.1366374 51.7214502, 36.1366057 51.7214161, 36.136594 51.7213954, 36.136579 51.7213789, 36.1365232 51.7213392, 36.1365077 51.7213249, 36.1365 51.72131, 36.1364969 51.7212912, 36.1365011 51.7212699, 36.1365117 51.7211947, 36.1364841 51.7211159, 36.1364818 51.7211093, 36.1364116 51.7210335, 36.1363076 51.7209744, 36.1361797 51.7209376, 36.1360698 51.7209266, 36.1359589 51.7209322, 36.1358534 51.7209541, 36.1357653 51.7209887, 36.1357133 51.7210215, 36.1356825 51.7210408, 36.1356271 51.7211005, 36.1355964 51.7211668, 36.1355922 51.7212356, 36.1356138 51.7213004, 36.1356147 51.7213031, 36.135643 51.7213398)")),
            f"Unexpected line split value {wkt.dumps(split_segments[0].geometry)} for first segment split"
        )
        self.assertTrue(equals(split_segments[1].geometry,
            wkt.loads("LINESTRING (36.135643 51.7213398, 36.1356627 51.7213653, 36.1357334 51.7214186, 36.1358226 51.7214598, 36.1359543 51.7214911, 36.1360951 51.7214962, 36.1362319 51.7214747, 36.1363667 51.7214374, 36.1364183 51.7214191, 36.1364665 51.7214049, 36.1365165 51.7214048, 36.136563 51.7214132, 36.1366058 51.7214295, 36.1366374 51.7214502)")),
            f"Unexpected line split value {wkt.dumps(split_segments[1].geometry)} for second segment split"
        )

# COMMAND ----------

# Run the tests before starting splitter
unittest.main(argv=[''], verbosity=2, exit=False)
overture_release_version = "2024-06-13-beta.0"
overture_release_path = "wasbs://release@overturemapswestus2.blob.core.windows.net" #  "s3://overturemaps-us-west-2/release"
base_output_path = "wasbs://test@ovtpipelinedev.blob.core.windows.net/transportation-splits-jun" # "s3://<bucket>/transportation-split"

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
