"""Spark UDF schemas and helper functions for the transportation splitter."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Additional fields added to split segments
additional_fields_in_split_segments = [
    StructField("start_lr", DoubleType(), True),
    StructField("end_lr", DoubleType(), True),
    StructField("metrics", MapType(StringType(), StringType()), True),
]

# Schema for flattened turn restriction info
flattened_tr_info_schema = ArrayType(
    StructType(
        [
            StructField("tr_index", IntegerType(), True),
            StructField("sequence_index", IntegerType(), True),
            StructField("segment_id", StringType(), True),
            StructField("connector_id", StringType(), True),
            StructField("next_connector_id", StringType(), True),
            StructField("final_heading", StringType(), True),
        ]
    )
)

# Schema for resolved prohibited transitions after reference resolution
resolved_prohibited_transitions_schema = ArrayType(
    StructType(
        [
            StructField(
                "sequence",
                ArrayType(
                    StructType(
                        [
                            StructField("connector_id", StringType(), True),
                            StructField("segment_id", StringType(), True),
                            StructField("start_lr", DoubleType(), True),
                            StructField("end_lr", DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField("final_heading", StringType(), True),
            StructField(
                "when",
                StructType(
                    [
                        StructField("heading", StringType(), True),
                        StructField("during", StringType(), True),
                        StructField("using", ArrayType(StringType()), True),
                        StructField("recognized", ArrayType(StringType()), True),
                        StructField("mode", ArrayType(StringType()), True),
                        StructField(
                            "vehicle",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("dimension", StringType(), True),
                                        StructField("comparison", StringType(), True),
                                        StructField("value", DoubleType(), True),
                                        StructField("unit", StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField("between", ArrayType(DoubleType()), True),
        ]
    )
)

# Schema for resolved destinations after reference resolution
resolved_destinations_schema = ArrayType(
    StructType(
        [
            StructField(
                "labels",
                ArrayType(
                    StructType(
                        [
                            StructField("value", StringType(), True),
                            StructField("type", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("symbols", ArrayType(StringType(), True), True),
            StructField("from_connector_id", StringType(), True),
            StructField("to_segment_id", StringType(), True),
            StructField("to_segment_start_lr", DoubleType(), True),
            StructField("to_segment_end_lr", DoubleType(), True),
            StructField("to_connector_id", StringType(), True),
            StructField("when", StructType([StructField("heading", StringType(), True)]), True),
            StructField("final_heading", StringType(), True),
        ]
    ),
    False,
)


def contains_field_name(data_type, field_name: str) -> bool:
    """Check if a Spark data type contains a field with the given name."""
    if isinstance(data_type, StructType):
        for fld in data_type.fields:
            if fld.name == field_name or contains_field_name(fld.dataType, field_name):
                return True
    elif isinstance(data_type, ArrayType):
        return contains_field_name(data_type.elementType, field_name)
    return False


def get_columns_with_struct_field_name(df, field_name: str) -> list[str]:
    """Get list of column names that contain a nested field with the given name."""
    return [field.name for field in df.schema.fields if contains_field_name(field.dataType, field_name)]


def get_filtered_columns(
    existing_columns: list[str],
    columns_to_include: list[str],
    columns_to_exclude: list[str],
) -> list[str]:
    """Filter columns based on include and exclude lists."""
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
