# Transportation Splitter

Pyspark app for splitting [Overture](https://github.com/OvertureMaps/data) transportation segments to simplify consuming the data.

Note: This repository and project are experimental. Things are likely change until a stable release, but we will keep the documentation here up-to-date.

## Description

Working with multiple connectors and linearly referenced properties can be difficult. The purpose of this notebook is to offer one option in how Overture transportation features can be consumed, by first splitting them into simpler sub-segments.

There are multiple ways this can be done, current behavior is to output for each input segment all its corresponding sub-segments "split" at all its connectors and all `between` length-relative location references (LRs).

The result is a transportation data set where segments have exactly two connectors, one for each end, and no linear references.

New "artificial" connector features are added for all LRs that don't already have connectors.

Result is using same Overture schema as the input, except for segments two columns are added: `start_lr` and `end_lr`.

For `sequence` in property `prohibited_transitions` it also adds fields `start_lr` and `end_lr` to identify to which of the splits the `segment_id` refers to.

For details on the process see [here](/ProcessDetails.md).

If you also have access to other open or proprietary data feeds that map Overture segment ids to other properties, with `between` LR fields or not, these can be consolidated into a single parquet via trivial join by `id`, then processed one time by this splitter to produce easy to consume split segments.

## Getting Started

Any Spark environment should work, but for reference this is tested on:

1. Databricks on Azure (Runtime: 15.4 LTS - Apache Spark 3.5.0, Scala 2.12, Python 3.11)
2. AWS Glue (Version: Glue 4.0 - Spark 3.3.0, Scala 2.12, Python 3.10)

### Dependencies

See [pyproject.toml](/pyproject.toml) for required packages.

**Requirements:**

- Python 3.10+
- Java 17+ (recommended for Apache Sedona 1.8.x)
- PySpark 3.5.x
- Apache Sedona 1.8.x

### Installing

**Local install with uv (recommended)**

```bash
uv sync
```

**Local install with pip**

```bash
pip install -e .
```

**Running tests**

```bash
uv sync --group test
uv run pytest tests/
```

**AWS Glue Notebook example config**

```
%idle_timeout 60
%worker_type G.2X
# Will need more workers to process planet, recommended 100
%number_of_workers 5
%glue_version 4.0

%additional_python_modules apache-sedona==1.7.0,shapely,pyproj
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.3_2.12/1.7.0/sedona-spark-shaded-3.3_2.12-1.7.0.jar,https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.7.0-28.5/geotools-wrapper-1.7.0-28.5.jar
```

**Validating Spark setup**
To ensure environment is properly configured for Sedona, run this test code before attempting to use the notebook:

```
from sedona.spark import *

sedona = SedonaContext.create(spark)
sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

### Executing program

Use the `OvertureTransportationSplitter` class:

```python
from transportation_splitter import (
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

# Initialize the splitter
splitter = OvertureTransportationSplitter(
    spark=spark_session,
    wrangler=SplitterDataWrangler(
        input_path="path/to/data/*.parquet",
        output_path="output/"
    ),
    cfg=SplitConfig()  # Uses defaults: split_at_connectors=False
)

# Run the splitting pipeline
result_df = splitter.split()

# Or with a spatial filter (uses bbox predicate pushdown for performance)
# The filter causes intermediate files to use _filtered suffix for cache isolation
splitter_filtered = OvertureTransportationSplitter(
    spark=spark_session,
    wrangler=SplitterDataWrangler(
        input_path="path/to/data/*.parquet",
        output_path="output/",
        filter_wkt="POLYGON(...)"
    ),
    cfg=SplitConfig(split_at_connectors=True)
)
result_df = splitter_filtered.split()
```

#### Configuration Options

The `SplitConfig` dataclass controls the splitting behavior:

```python
SplitConfig(
    split_at_connectors=False,             # Split at all connectors along the segment (default: False)
    lr_columns_to_include=[],              # Only consider these columns for LR splitting
    lr_columns_to_exclude=[],              # Exclude these columns from LR splitting
    point_precision=7,                     # Decimal places for split point coordinates
    lr_split_point_min_dist_meters=0.01,   # Minimum distance between split points (1cm)
    skip_debug_output=True,                # Skip expensive count()/show() operations (default: True)
)
```

The `SplitterDataWrangler` handles I/O configuration and state management:

```python
from transportation_splitter import SplitterDataWrangler

SplitterDataWrangler(
    input_path="path/to/data/*.parquet",   # Input data path (glob patterns supported)
    output_path="output/",                  # Output directory for results (None = in-memory only)
    filter_wkt="POLYGON(...)",              # Optional spatial filter (WKT polygon)
    write_geoparquet=True,                  # Output format: True=GeoParquet, False=Parquet+WKB
    geometry_column="geometry",             # Geometry column name (for non-standard schemas)
    write_intermediate_files=True,          # Write intermediate files for caching
    reuse_existing_intermediate_outputs=True,  # Reuse cached intermediate files
    compression="zstd",                     # Parquet compression codec
    block_size=16 * 1024 * 1024,           # Parquet row group size (16MB default)
)
```

**Note:** Input format is always auto-detected by checking for GeoParquet metadata (the `geo` key in Parquet file metadata). You don't need to specify it.

The wrangler acts as the "keeper of state" for the pipeline, managing:

- In-memory DataFrame caching via `store()` and `get()` methods
- Optional disk persistence of intermediate results
- Geometry type conversion (WKB ↔ GeometryUDT)
- Cache isolation via path hashing for different filter configurations
- Support for both GeoParquet and Parquet+WKB formats

**Pipeline Steps (SplitterStep enum):**

The wrangler tracks these pipeline steps for caching:

- `read_input`: Raw input data (read-only, not stored)
- `spatial_filter`: Data after applying WKT spatial filter
- `joined`: Segments joined with connectors
- `raw_split`: UDF output with split results
- `segment_splits_exploded`: Exploded split segments
- `debug`: Debug output with length comparison per segment
- `final_output`: Final combined output

**Example: Split only at LR values from specific columns, not at connectors:**

```python
splitter = OvertureTransportationSplitter(
    spark=spark_session,
    wrangler=SplitterDataWrangler(
        input_path="path/to/data/*.parquet",
        output_path="output/"
    ),
    cfg=SplitConfig(
        split_at_connectors=False,  # Default behavior
        lr_columns_to_include=["road_flags"]
    )
)
result_df = splitter.split()
```

**Example: Enable debug output:**

```python
splitter = OvertureTransportationSplitter(
    spark=spark_session,
    wrangler=SplitterDataWrangler(
        input_path="path/to/data/*.parquet",
        output_path="output/"
    ),
    cfg=SplitConfig(skip_debug_output=False)
)
result_df = splitter.split()

# Access debug DataFrame with length comparison info
debug_df = splitter.debug_df
```

If you are using databricks you can also add this repo as a git folder, see instructions [here](https://docs.databricks.com/en/repos/repos-setup.html).

## Planet-Scale Performance Optimizations

This splitter includes several optimizations for processing planet-scale Overture data:

### 1. Bbox Predicate Pushdown

When using `filter_wkt`, the splitter uses the pre-computed `bbox` struct column for efficient filtering:

```python
# The filter_df function uses bbox fields for Parquet predicate pushdown
# This allows Spark to skip entire row groups and partition files
bbox_filter = (
    (col("bbox.xmin") <= filter_xmax) &
    (col("bbox.xmax") >= filter_xmin) &
    (col("bbox.ymin") <= filter_ymax) &
    (col("bbox.ymax") >= filter_ymin)
)
```

This is much faster than computing `ST_Envelope(geometry)` on every row.

### 2. Sedona ST_LengthSpheroid

Segment length is pre-computed using Sedona's native `ST_LengthSpheroid` function before the split UDF runs. This avoids calling Python's pyproj library inside the UDF for every row.

### 3. Shuffle Hash Joins

Joins use `shuffle_hash` hints to prevent broadcast join failures on large datasets:

```python
segments_df.hint("shuffle_hash").join(connectors_df, ...)
```

### 4. Cache Isolation via Path Suffixes

When `filter_wkt` is set on the wrangler, the output paths automatically get isolated suffixes based on an MD5 hash of the WKT. This ensures complete cache isolation between different filter polygons:

```
# Unfiltered run (output_path="output/"):
output/_intermediate/1_spatially_filtered/
output/_debug/
output/split/

# Filtered run with polygon A (hash: a1b2c3d4):
output_filtered/_intermediate_a1b2c3d4/1_spatially_filtered/
output_filtered/_debug_a1b2c3d4/
output_filtered/split_a1b2c3d4/

# Filtered run with polygon B (hash: e5f6g7h8):
output_filtered/_intermediate_e5f6g7h8/1_spatially_filtered/
output_filtered/_debug_e5f6g7h8/
output_filtered/split_e5f6g7h8/
```

## Version History

- 0.2
  - Refactored to `OvertureTransportationSplitter` class with `SplitterDataWrangler` for I/O
  - Simplified API: removed `InputFormat`/`OutputFormat` enums, replaced with `write_geoparquet` boolean
  - Input format is always auto-detected by checking for GeoParquet metadata
  - Added Sedona `ST_LengthSpheroid` optimization
  - Added bbox predicate pushdown for spatial filtering
  - Added shuffle_hash join hints for planet-scale processing
  - Added cache isolation with hash-based path suffixes
  - Changed `split_at_connectors` default to `False`
  - Added debug DataFrame output via `splitter.debug_df`
- 0.1
  - Initial Release
