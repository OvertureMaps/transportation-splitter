# Transportation Splitter - LLM Context

## Overview

This is a PySpark application for splitting [Overture Maps](https://github.com/OvertureMaps/data) transportation segments into simpler sub-segments. It processes GeoParquet files containing road segment and connector data.

## Tech Stack

- **Python**: 3.10+
- **PySpark**: 3.5.x (Spark SQL and DataFrames)
- **Apache Sedona**: 1.8.x (geospatial extension for Spark)
- **Shapely**: Geometry operations
- **Package Manager**: uv (migrated from Poetry)
- **Testing**: pytest
- **Linting**: ruff

## Critical Requirements

### Java Version

**Sedona 1.8.x requires Java 11+**. Tests will fail with cryptic `NoClassDefFoundError` if using Java 8.

```bash
# macOS with Homebrew
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Linux
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

### Scala Version

PySpark from PyPI only ships with **Scala 2.12**, regardless of PySpark version. Always use `_2.12` suffix for Sedona JARs:

```python
f"org.apache.sedona:sedona-spark-shaded-{spark_version}_2.12:{sedona_version}"
```

## Project Structure

```
transportation-splitter/
├── pyproject.toml                   # Package config (uv/hatch)
├── uv.lock                          # Lockfile
├── transportation_splitter/         # Main package (modular structure)
│   ├── __init__.py                  # Public API exports
│   ├── config.py                    # SplitConfig dataclass and constants
│   ├── models.py                    # Domain models (JoinedConnector, SplitPoint, SplitSegment)
│   ├── wrangler.py                  # SplitterDataWrangler and SplitterStep enum
│   ├── geometry.py                  # Pure geometry functions (get_length, split_line, etc.)
│   ├── linear_reference.py          # LR logic (get_lrs, apply_lr_on_split, etc.)
│   ├── properties.py                # Turn restrictions & destinations (get_trs, get_destinations)
│   ├── _spark_udfs.py               # Spark UDF helpers and schema definitions
│   ├── _pipeline_helpers.py         # Pipeline helper functions (filter_df, join_segments_with_connectors, add_segment_length_column)
│   ├── _resolve_refs.py             # Reference resolution (resolve_tr_references, resolve_destinations_references)
│   └── pipeline.py                  # Main pipeline (OvertureTransportationSplitter class)
├── tests/
│   ├── conftest.py                  # Shared pytest fixtures (spark_session, test data paths)
│   ├── data/                        # Test parquet files (segment, connector, boulder_*)
│   ├── unit/                        # Pure Python tests (no Spark)
│   │   ├── test_split.py            # Segment splitting logic
│   │   ├── test_apply_lr.py         # Linear reference application
│   │   └── test_wrangler.py         # SplitterDataWrangler
│   └── integration/                 # Tests requiring Spark/Sedona
│       ├── test_e2e.py              # End-to-end pipeline tests
│       └── test_profiling.py        # Performance benchmarks and Sedona comparisons
└── .github/workflows/tests.yaml     # CI configuration
```

## Module Breakdown

### Public API (`__init__.py`)

Exports all user-facing classes and functions:

- `OvertureTransportationSplitter` - Main class
- `SplitConfig`, `DEFAULT_CFG` - Configuration
- `DESTINATIONS_COLUMN`, `PROHIBITED_TRANSITIONS_COLUMN` - Column name constants
- `SplitterDataWrangler`, `SplitterStep` - I/O handling
- `JoinedConnector`, `SplitPoint`, `SplitSegment` - Domain models
- Geometry and linear reference functions for testing/profiling

### Private Modules (prefix `_`)

- `_spark_udfs.py` - Spark schema definitions and UDF helpers
- `_pipeline_helpers.py` - Internal pipeline functions (filter_df, join_segments_with_connectors, add_segment_length_column)
- `_resolve_refs.py` - Turn restriction and destination reference resolution

## Key Concepts

### Linear References (LRs)

Positions along a road segment expressed as fractions (0.0 to 1.0). Properties like speed limits or road flags can apply to portions of a segment using `between` fields with start/end LRs.

### Connectors

Points where road segments connect to each other. Segments can be split at connectors to create simpler sub-segments with exactly 2 connectors (one at each end).

### Split Points

Points where a segment should be divided. Can come from:

1. Existing connectors
2. LR values from `between` properties
3. New "artificial" connectors created at LR positions

### Input Data Requirements

The input Overture data must have:

- `geometry` column: LineString geometries for segments, Point for connectors
- `bbox` struct column: `struct<xmin:float, xmax:float, ymin:float, ymax:float>` for predicate pushdown
- `type` column: "segment" or "connector"
- `connectors` array column on segments: List of connector references with `connector_id` and `at` fields

## Common Commands

```bash
# Install dependencies
uv sync

# Install with all dev and test dependencies
uv sync --group dev --group test

# Run unit tests (fast, no Spark)
uv run pytest tests/unit -v

# Run all tests (requires Java 11+)
JAVA_HOME=/opt/homebrew/opt/openjdk@11 uv run pytest tests/ -v

# Run specific test
uv run pytest tests/unit/test_split.py::TestSplitter::test_split_line_all_cases -v

# Lint and format (ALWAYS run after making changes!)
uv run ruff check --fix .     # Auto-fix lint issues
uv run ruff format .          # Apply formatting
```

## Main Entry Points

### OvertureTransportationSplitter Class

```python
from transportation_splitter import (
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

splitter = OvertureTransportationSplitter(
    spark=spark_session,
    wrangler=SplitterDataWrangler(
        input_path="path/to/data/*.parquet",
        output_path="output/"
    ),
    cfg=SplitConfig(split_at_connectors=True)
)

# Run the pipeline
result_df = splitter.split()

# With spatial filter (uses bbox predicate pushdown)
result_df = splitter.split(filter_wkt="POLYGON(...)")
```

## Planet-Scale Performance Optimizations

### 1. Bbox Predicate Pushdown (`filter_df`)

Uses the pre-computed `bbox` struct column for efficient spatial filtering:

```python
bbox_filter = (
    (col("bbox.xmin") <= filter_xmax) &
    (col("bbox.xmax") >= filter_xmin) &
    (col("bbox.ymin") <= filter_ymax) &
    (col("bbox.ymax") >= filter_ymin)
)
```

This enables Parquet row group skipping on spatially partitioned data.

### 2. Sedona ST_LengthSpheroid (`add_segment_length_column`)

Pre-computes segment length using Sedona's native JVM implementation:

```python
df.withColumn("segment_length_meters", expr("ST_LengthSpheroid(geometry)"))
```

This replaces calling pyproj inside Python UDFs, which is much slower.

### 3. Shuffle Hash Joins (`join_segments_with_connectors`)

Uses `shuffle_hash` hints to prevent broadcast join failures on large datasets:

```python
segments_df.hint("shuffle_hash").join(connectors_df, ...)
```

### 4. Cache Invalidation Tracking

The pipeline tracks `upstream_recomputed` flags. If a spatial filter changes, all downstream cached intermediate files are automatically invalidated.

## Test Data

Test parquet files in `tests/data/` contain:

- `segment.parquet`: Road segment features with geometry and properties
- `connector.parquet`: Connector point features
- `boulder_segments.parquet`: Boulder, CO test data
- `boulder_connectors.parquet`: Boulder, CO connector data

These are small subsets from real Overture data for testing.

## Gotchas

1. **Spark Session Scope**: Integration tests use session-scoped fixtures. Don't create multiple SparkContexts.

2. **Coordinate Precision**: Split point coordinates are rounded to 7 decimal places (~1cm precision) to avoid duplicate coordinates.

3. **Self-intersecting Lines**: Some road segments self-intersect (e.g., loops). The code handles these by tracking LR position rather than geometric intersection.

4. **Long WKT Strings**: Test files contain very long WKT strings for real-world geometries. Don't try to edit these by hand.

5. **Import Structure**: Use absolute imports from `transportation_splitter` package (e.g., `from transportation_splitter import OvertureTransportationSplitter`).

6. **Always Run Formatting**: After ANY code changes, always run `uv run ruff check --fix . && uv run ruff format .` to fix linting and formatting issues.

7. **Intermediate File Caching**: The pipeline caches intermediate results. If you change the filter_wkt, ensure `reuse_existing_intermediate_outputs` handles cache invalidation properly (it does via `upstream_recomputed` tracking).

8. **Broadcast Join Failures**: For planet-scale data, broadcast joins will fail. The code uses `shuffle_hash` hints to prevent this.
