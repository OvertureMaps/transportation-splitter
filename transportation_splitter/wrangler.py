"""Data wrangling and I/O management for the transportation splitter.

The SplitterDataWrangler is the central "keeper of state" for the pipeline.
It manages:
- In-memory DataFrame caching
- Optional disk persistence of intermediate results
- Geometry type conversion (WKB ↔ GeometryUDT)
- Spatial filtering when filter_wkt is configured
- Cache isolation via path hashing for different filter configurations

All pipeline interaction with data should go through:
- wrangler.get(step) - retrieve data (from memory or disk)
- wrangler.store(step, df) - persist data (to memory and optionally disk)
"""

import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class SplitterStep(Enum):
    """Enumeration of pipeline steps for intermediate file naming.

    Each step represents a stage in the splitting pipeline:
    - read_input: Raw input data (read-only, not stored)
    - spatial_filter: Data after applying WKT spatial filter (expensive)
    - joined: Segments joined with connectors (expensive shuffle join)
    - raw_split: UDF output with split results (most expensive - Python UDF)
    - segment_splits_exploded: Exploded split segments (convenience cache)
    - final_output: Final combined output (always written if output_path set)
    """

    read_input = "input"
    spatial_filter = "1_spatially_filtered"
    joined = "2_joined"
    raw_split = "3_raw_split"  # Note: no geometry column, uses vanilla parquet
    segment_splits_exploded = "4_segments_splits"
    final_output = "final"


class InputFormat(Enum):
    """Input data format options."""

    GEOPARQUET = "geoparquet"  # Native GeoParquet with GeometryType
    PARQUET_WKB = "parquet_wkb"  # Standard Parquet with WKB-encoded geometry
    AUTO = "auto"  # Auto-detect format (default)


class OutputFormat(Enum):
    """Output data format options."""

    GEOPARQUET = "geoparquet"  # Native GeoParquet with GeometryType
    PARQUET_WKB = "parquet_wkb"  # Standard Parquet with WKB-encoded geometry
    DATAFRAME = "dataframe"  # Just return the DF at the end of the split() function


@dataclass
class SplitterDataWrangler:
    """
    Configuration, state management, and I/O operations for the transportation splitter.

    This class is the "keeper of state" for the pipeline. It handles:
    - In-memory DataFrame caching via store() and get()
    - Optional disk persistence of intermediate results
    - Geometry type conversion (WKB ↔ GeometryUDT)
    - Spatial filtering when filter_wkt is configured
    - Cache isolation via path hashing for different filter configurations

    Example:
        >>> wrangler = SplitterDataWrangler(
        ...     input_path="data/input/*.parquet",
        ...     output_path="data/output/",
        ... )
        >>>
        >>> # Get input (reads from disk, caches in memory)
        >>> df = wrangler.get(spark, SplitterStep.read_input)
        >>>
        >>> # Store result (caches in memory, optionally writes to disk)
        >>> wrangler.store(SplitterStep.joined, joined_df)
        >>>
        >>> # Get cached result (from memory or disk)
        >>> joined = wrangler.get(spark, SplitterStep.joined)

    To customize I/O behavior (e.g., for cloud storage), subclass and override
    the read() and write() methods.
    """

    # Required paths
    input_path: str
    output_path: str | None = None  # None = skip final output write

    # Optional spatial filter WKT. When set, only features intersecting this
    # polygon will be processed. Creates isolated cache paths with _filtered suffix.
    filter_wkt: str | None = None

    # Format options
    input_format: InputFormat = InputFormat.AUTO
    output_format: OutputFormat = OutputFormat.GEOPARQUET

    # Geometry column name (for non-standard column names)
    geometry_column: str = "geometry"

    # Parquet write options
    compression: str = "zstd"
    block_size: int = 16 * 1024 * 1024  # 16MB row groups

    # Whether to write intermediate files during pipeline execution.
    # Set to False to skip writing intermediate files (faster, but no caching).
    write_intermediate_files: bool = True

    # Whether to reuse existing intermediate outputs from disk.
    # Set to False to always force reprocessing for all sub-steps.
    reuse_existing_intermediate_outputs: bool = True

    # Private: computed paths (set in __post_init__)
    _intermediate_path: str = field(init=False, repr=False)
    _final_output_path: str = field(init=False, repr=False)

    # Private: in-memory state cache for DataFrames
    _state: dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        """Normalize paths and compute derived paths."""
        self.input_path = self.input_path.rstrip("/")
        if self.output_path is not None:
            self.output_path = self.output_path.rstrip("/")
            # When filter_wkt is set, add _filtered_{hash} suffix to output path
            # to isolate cached data for different filters
            if self.filter_wkt:
                filter_hash = self._compute_filter_hash(self.filter_wkt)
                base_output = f"{self.output_path}_filtered_{filter_hash}"
            else:
                base_output = self.output_path
            self._intermediate_path = f"{base_output}/_intermediate"
            self._final_output_path = f"{base_output}/split"
        else:
            self._intermediate_path = None
            self._final_output_path = None
            self.output_format = OutputFormat.DATAFRAME

    def _compute_filter_hash(self, wkt: str) -> str:
        """Compute a short hash of the filter WKT for path isolation.

        Returns an 8-character hex hash that uniquely identifies the filter.
        """
        return hashlib.md5(wkt.encode()).hexdigest()[:8]

    # =========================================================================
    # Primary API - store() and get()
    # =========================================================================

    def store(self, step: SplitterStep, df: DataFrame) -> DataFrame:
        """Store a DataFrame for a pipeline step.

        This is the primary method for persisting data. It:
        1. Caches the DataFrame in memory for future get() calls
        2. Optionally writes to disk based on write_intermediate_files setting

        Geometry conversion is handled by the read/write methods:
        - GeoParquet: native geometry, no conversion needed
        - Parquet+WKB: GeometryUDT → WKB on write, WKB → GeometryUDT on read

        Args:
            step: Pipeline step to store data for
            df: DataFrame to store

        Returns:
            The stored DataFrame (same as input, cached in memory)
        """
        # Cache in memory
        self._state[step] = df

        # Determine if we should write to disk
        should_write = False
        if step == SplitterStep.final_output:
            # Always write final output if output_path is configured
            should_write = self.output_path is not None
        elif step == SplitterStep.read_input:
            # Never write input (it's read-only)
            should_write = False
        else:
            # Intermediate steps: write if configured
            should_write = (
                self.write_intermediate_files and self.output_path is not None
            )

        if should_write:
            self.write(df, step)

        return df

    def get(self, spark: SparkSession, step: SplitterStep) -> DataFrame | None:
        """Get a DataFrame for a pipeline step.

        This is the primary method for retrieving data. It checks:
        1. In-memory cache first (fastest)
        2. Disk cache if reuse_existing_intermediate_outputs is True
        3. Returns None if data is not available anywhere

        Geometry conversion is handled by the read methods:
        - GeoParquet: returns GeometryUDT natively
        - Parquet+WKB: converts WKB → GeometryUDT on read

        Special handling for read_input step:
        - If filter_wkt is set, returns filtered data (from cache or computed)
        - If no filter, returns raw input data

        Args:
            spark: Active SparkSession
            step: Pipeline step to get data for

        Returns:
            DataFrame if available, None otherwise
        """
        # Special case: read_input handles spatial filtering
        if step == SplitterStep.read_input:
            return self._get_input(spark)

        # Check in-memory cache first
        if step in self._state:
            return self._state[step]

        # Check disk if configured to reuse
        if self.reuse_existing_intermediate_outputs and self.check_exists(spark, step):
            logger.info(f"Reusing existing {step.name} from disk")
            df = self.read(spark, step)
            # read() handles geometry conversion for GeoParquet and Parquet+WKB
            self._state[step] = df
            return df

        return None

    def _get_input(self, spark: SparkSession) -> DataFrame:
        """Get input data, applying spatial filter if configured.

        This method handles the read_input step with special logic for filtering:
        1. If no filter_wkt: just read and return raw input (cached as read_input)
        2. If filter_wkt is set and reuse is enabled and cache exists: return cached filtered data
        3. If filter_wkt is set but no cache: read, filter, optionally cache, and return

        The spatial_filter cache is only used when filter_wkt is set.

        Args:
            spark: Active SparkSession

        Returns:
            DataFrame with input data (filtered if filter_wkt is set)
        """
        # No filter: just read raw input
        if self.filter_wkt is None:
            # Check in-memory cache first
            if SplitterStep.read_input in self._state:
                return self._state[SplitterStep.read_input]
            df = self.read(spark, SplitterStep.read_input)
            self._state[SplitterStep.read_input] = df
            return df

        # Filter is set - check in-memory cache for filtered data
        if SplitterStep.spatial_filter in self._state:
            return self._state[SplitterStep.spatial_filter]

        # Check disk cache for filtered data
        if self.reuse_existing_intermediate_outputs and self.check_exists(
            spark, SplitterStep.spatial_filter
        ):
            logger.info("Reusing existing spatially filtered data from disk")
            df = self.read(spark, SplitterStep.spatial_filter)
            self._state[SplitterStep.spatial_filter] = df
            return df

        # Need to compute: read raw input and apply filter
        logger.info(f"Applying spatial filter: {self.filter_wkt[:50]}...")
        raw_df = self.read(spark, SplitterStep.read_input)
        filtered_df = self._apply_spatial_filter(raw_df, self.filter_wkt)

        # Store in state as spatial_filter (not read_input)
        self._state[SplitterStep.spatial_filter] = filtered_df

        # Optionally write filtered data to cache
        if self.write_intermediate_files and self.output_path is not None:
            self.write(filtered_df, SplitterStep.spatial_filter)

        return filtered_df

    def _apply_spatial_filter(self, df: DataFrame, filter_wkt: str) -> DataFrame:
        """Apply a spatial filter to a DataFrame using bbox predicate pushdown.

        Uses the pre-computed bbox struct column for efficient Parquet row group
        skipping, then applies a precise geometry intersection filter.

        Args:
            df: DataFrame with geometry and bbox columns
            filter_wkt: WKT string for the filter polygon

        Returns:
            DataFrame containing only features that intersect the filter polygon
        """
        from pyspark.sql.functions import col

        # Parse filter geometry to get bbox for predicate pushdown
        from shapely import wkt

        filter_geom = wkt.loads(filter_wkt)
        filter_minx, filter_miny, filter_maxx, filter_maxy = filter_geom.bounds

        # Apply bbox filter first (enables Parquet predicate pushdown)
        bbox_filtered = df.filter(
            (col("bbox.xmin") <= filter_maxx)
            & (col("bbox.xmax") >= filter_minx)
            & (col("bbox.ymin") <= filter_maxy)
            & (col("bbox.ymax") >= filter_miny)
        )

        # Apply precise geometry intersection filter
        # Use Sedona's ST_Intersects for accurate filtering
        return bbox_filtered.filter(
            expr(f"ST_Intersects(geometry, ST_GeomFromWKT('{filter_wkt}'))")
        )

    def has(self, spark: SparkSession, step: SplitterStep) -> bool:
        """Check if a DataFrame is available for a pipeline step.

        Checks both in-memory cache and disk (if reuse is enabled).

        Args:
            spark: Active SparkSession
            step: Pipeline step to check

        Returns:
            True if data is available (in memory or on disk)
        """
        if step in self._state:
            return True
        if self.reuse_existing_intermediate_outputs and self.check_exists(spark, step):
            return True
        return False

    def clear_state(self, step: SplitterStep | None = None) -> None:
        """Clear in-memory state for a step or all steps.

        Does not delete files from disk.

        Args:
            step: Specific step to clear, or None to clear all
        """
        if step is None:
            self._state.clear()
        elif step in self._state:
            del self._state[step]

    # =========================================================================
    # Low-level I/O - Override these in subclasses for custom storage
    # =========================================================================

    def read(self, spark: SparkSession, step: SplitterStep) -> DataFrame:
        """
        Read a DataFrame for the given pipeline step from disk.

        Override this method to customize read behavior (e.g., for cloud storage).

        Args:
            spark: Active SparkSession
            step: Pipeline step to read data for

        Returns:
            DataFrame with geometry column parsed as Sedona GeometryType
        """
        read_path = self._path_for_step(step)
        logger.info(f"Reading {step.name} from {read_path}")

        if step == SplitterStep.read_input:
            return self._read_input(spark, read_path)
        elif step == SplitterStep.raw_split:
            # raw_split has no geometry column
            return self._read_parquet(spark, read_path)
        else:
            # Intermediate files use GeoParquet or Parquet+WKB based on output_format
            if self.output_format == OutputFormat.GEOPARQUET:
                return self._read_geoparquet(spark, read_path)
            else:
                return self._read_parquet_wkb(spark, read_path)

    def write(self, df: DataFrame, step: SplitterStep) -> None:
        """
        Write a DataFrame for the given pipeline step to disk.

        Override this method to customize write behavior (e.g., for cloud storage).
        If output_path is None, this method does nothing.

        Args:
            df: DataFrame to write
            step: Pipeline step being written
        """
        if self.output_path is None:
            logger.debug(f"Skipping write for {step.name} (no output_path configured)")
            return

        write_path = self._path_for_step(step)
        logger.info(f"Writing {step.name} to {write_path}")

        if step == SplitterStep.raw_split:
            # raw_split has no geometry, must use vanilla Parquet
            self._write_parquet(df, write_path)
        elif self.output_format == OutputFormat.GEOPARQUET:
            # Use GeoParquet for both intermediate and final output
            self._write_geoparquet(df, write_path)
        else:
            # Use Parquet+WKB for both intermediate and final output
            # (assumes no GeoParquet support in environment)
            self._write_parquet_wkb(df, write_path)

    def check_exists(self, spark: SparkSession, step: SplitterStep) -> bool:
        """
        Check if a pipeline step has already been materialized on disk.

        Override this method for custom existence checks (e.g., cloud storage).

        Args:
            spark: Active SparkSession
            step: Pipeline step to check

        Returns:
            True if the step's output exists and can be read
        """
        if self.output_path is None:
            return False
        read_path = self._path_for_step(step)
        return self._parquet_exists(spark, read_path)

    # =========================================================================
    # Path computation
    # =========================================================================

    def _path_for_step(self, step: SplitterStep) -> str:
        """Get the file path for a given pipeline step.

        When filter_wkt is set, the base output path already has the _filtered
        suffix applied in __post_init__.
        """
        if step == SplitterStep.read_input:
            return self.input_path
        elif step == SplitterStep.final_output:
            return self._final_output_path
        else:
            return f"{self._intermediate_path}/{step.value}"

    # =========================================================================
    # Format detection and reading
    # =========================================================================

    def _read_input(self, spark: SparkSession, path: str) -> DataFrame:
        """Read input data based on configured input_format."""
        if self.input_format == InputFormat.AUTO:
            return self._read_auto_detect(spark, path)
        elif self.input_format == InputFormat.GEOPARQUET:
            return self._read_geoparquet(spark, path)
        else:  # PARQUET_WKB
            return self._read_parquet_wkb(spark, path)

    def _read_auto_detect(self, spark: SparkSession, path: str) -> DataFrame:
        """Auto-detect format and read appropriately."""
        if self._is_native_geoparquet(spark, path):
            return self._read_geoparquet(spark, path)
        else:
            return self._read_parquet_wkb(spark, path)

    def _is_native_geoparquet(self, spark: SparkSession, path: str) -> bool:
        """Check if a parquet file is in native GeoParquet format."""
        try:
            sample_df = spark.read.format("geoparquet").load(path).limit(1)
            col_type = sample_df.schema[self.geometry_column].dataType
            return str(col_type) == "GeometryType()"
        except Exception:
            return False

    def _read_geoparquet(self, spark: SparkSession, path: str) -> DataFrame:
        """Read a native GeoParquet file."""
        return spark.read.format("geoparquet").option("mergeSchema", "true").load(path)

    def _read_parquet_wkb(self, spark: SparkSession, path: str) -> DataFrame:
        """Read a Parquet file with WKB-encoded geometry."""
        geom_col = self.geometry_column
        return (
            spark.read.option("mergeSchema", "true")
            .parquet(path)
            .withColumn(geom_col, expr(f"ST_GeomFromWKB({geom_col})"))
        )

    def _read_parquet(self, spark: SparkSession, path: str) -> DataFrame:
        """Read a plain Parquet file (no geometry parsing)."""
        return spark.read.option("mergeSchema", "true").parquet(path)

    # =========================================================================
    # Writing
    # =========================================================================

    def _write_geoparquet(self, df: DataFrame, path: str) -> None:
        """Write a DataFrame as native GeoParquet."""
        (
            df.write.format("geoparquet")
            .option("compression", self.compression)
            .option("parquet.block.size", self.block_size)
            .mode("overwrite")
            .save(path)
        )

    def _write_parquet_wkb(self, df: DataFrame, path: str) -> None:
        """Write a DataFrame as Parquet with WKB-encoded geometry."""
        geom_col = self.geometry_column
        # Convert geometry to WKB binary
        df_wkb = df.withColumn(geom_col, expr(f"ST_AsBinary({geom_col})"))
        (
            df_wkb.write.format("parquet")
            .option("compression", self.compression)
            .option("parquet.block.size", self.block_size)
            .mode("overwrite")
            .save(path)
        )

    def _write_parquet(self, df: DataFrame, path: str) -> None:
        """Write a DataFrame as plain Parquet (no geometry conversion)."""
        (
            df.write.format("parquet")
            .option("compression", self.compression)
            .option("parquet.block.size", self.block_size)
            .mode("overwrite")
            .save(path)
        )

    # =========================================================================
    # Utilities
    # =========================================================================

    def _parquet_exists(self, spark: SparkSession, path: str) -> bool:
        """Check if a parquet file exists at the given path."""
        try:
            spark.read.format("parquet").load(path).limit(0)
            return True
        except AnalysisException:
            return False

    def __str__(self) -> str:
        if self.output_path:
            if self.filter_wkt:
                filter_hash = self._compute_filter_hash(self.filter_wkt)
                filter_str = f"enabled (hash: {filter_hash})"
            else:
                filter_str = "none"
            intermediate_str = self._intermediate_path
            output_str = self._final_output_path
        else:
            filter_str = "none"
            intermediate_str = "(none - in-memory only)"
            output_str = "(none - in-memory only)"

        lines = [
            "SplitterDataWrangler:",
            f"  Input:              {self.input_path}",
            f"  Intermediate:       {intermediate_str}",
            f"  Output:             {output_str}",
            f"  Filter:             {filter_str}",
            f"  Input Format:       {self.input_format.value}",
            f"  Output Format:      {self.output_format.value}",
            f"  Write Intermediate: {self.write_intermediate_files}",
            f"  Reuse Existing:     {self.reuse_existing_intermediate_outputs}",
            f"  Compression:        {self.compression}",
            f"  Block Size:         {self.block_size // (1024 * 1024)}MB",
        ]
        return "\n".join(lines)
