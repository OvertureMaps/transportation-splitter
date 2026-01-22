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
    - debug: Debug output with length comparison per segment
    - final_output: Final combined output (always written if output_path set)
    """

    read_input = "input"
    spatial_filter = "1_spatially_filtered"
    joined = "2_joined"
    raw_split = "3_raw_split"  # Note: no geometry column, uses vanilla parquet
    segment_splits_exploded = "4_segments_splits"
    debug = "_debug"
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
    _base_output_path: str = field(init=False, repr=False)
    _intermediate_path: str = field(init=False, repr=False)
    _final_output_path: str = field(init=False, repr=False)

    # Private: filter hash for path isolation (set in __post_init__)
    _filter_hash: str | None = field(init=False, repr=False)

    # Private: in-memory state cache for DataFrames
    _state: dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        """Normalize paths and compute derived paths."""
        self.input_path = self.input_path.rstrip("/")
        if self.output_path is not None:
            self.output_path = self.output_path.rstrip("/")
            # When filter_wkt is set, add _filtered suffix and compute hash
            # The base output path gets _filtered, and each folder gets the hash
            if self.filter_wkt:
                self._filter_hash = self._compute_filter_hash(self.filter_wkt)
                self._base_output_path = f"{self.output_path}_filtered"
            else:
                self._filter_hash = None
                self._base_output_path = self.output_path
            # Apply hash suffix to folder names when filtered
            if self._filter_hash:
                self._intermediate_path = f"{self._base_output_path}/_intermediate_{self._filter_hash}"
                self._final_output_path = f"{self._base_output_path}/split_{self._filter_hash}"
            else:
                self._intermediate_path = f"{self.output_path}/_intermediate"
                self._final_output_path = f"{self.output_path}/split"
        else:
            self._intermediate_path = None
            self._final_output_path = None
            self._filter_hash = None
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
            should_write = self.write_intermediate_files and self.output_path is not None

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

        # Skip disk cache for joined step when output_format is PARQUET_WKB
        # because the nested connector_geometry column cannot be converted
        # from BinaryType to GeometryUDT when reading from Parquet+WKB files
        if step == SplitterStep.joined and self.output_format == OutputFormat.PARQUET_WKB:
            logger.debug("Skipping disk cache for joined step (nested geometry incompatibility with PARQUET_WKB)")
            return None

        # Skip disk cache for spatial_filter step when output_format is PARQUET_WKB
        # The lazy geometry conversion can cause schema conflicts during downstream
        # operations when DataFrames are combined
        if step == SplitterStep.spatial_filter and self.output_format == OutputFormat.PARQUET_WKB:
            logger.debug(
                "Skipping disk cache for spatial_filter step (geometry conversion compatibility with PARQUET_WKB)"
            )
            return None

        # Skip disk cache for segment_splits_exploded and raw_split for ALL output formats.
        # These steps contain data that gets combined with other DataFrames
        # (via unionByName) which causes schema ordering issues during write.
        # - segment_splits_exploded: segments that get combined with connectors
        # - raw_split: contains added_connectors_rows that get combined with segments
        if step in (SplitterStep.segment_splits_exploded, SplitterStep.raw_split):
            logger.debug(
                f"Skipping disk cache for {step.name} step (schema ordering issues when combined with other data)"
            )
            return None

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

        # Skip disk cache for spatial_filter step when output_format is PARQUET_WKB
        # The lazy geometry conversion can cause schema conflicts during downstream
        # operations when DataFrames are combined
        skip_disk_cache = self.output_format == OutputFormat.PARQUET_WKB
        if skip_disk_cache:
            logger.debug(
                "Skipping disk cache for spatial_filter step (geometry conversion compatibility with PARQUET_WKB)"
            )

        # Check disk cache for filtered data (unless skipping)
        if (
            not skip_disk_cache
            and self.reuse_existing_intermediate_outputs
            and self.check_exists(spark, SplitterStep.spatial_filter)
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
        from shapely import wkt

        from transportation_splitter.geometry import sanitize_wkt

        # Sanitize the WKT to handle invalid characters and escape quotes
        sanitized_wkt = sanitize_wkt(filter_wkt)

        # Parse filter geometry to get bbox for predicate pushdown
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
        return bbox_filtered.filter(expr(f"ST_Intersects(geometry, ST_GeomFromWKT('{sanitized_wkt}'))"))

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
            # Intermediate files: auto-detect format since they may have been
            # written by a different wrangler instance with a different output_format
            return self._read_auto_detect(spark, read_path)

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

        if step == SplitterStep.raw_split or step == SplitterStep.debug:
            # raw_split and debug have no geometry, must use vanilla Parquet
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

        Filter hash is applied to all folders (intermediate, debug, split)
        when filter_wkt is set. The base output path also gets _filtered suffix.
        """
        if step == SplitterStep.read_input:
            return self.input_path
        elif step == SplitterStep.final_output:
            return self._final_output_path
        elif step == SplitterStep.debug:
            # Debug goes at same level as _intermediate with hash suffix if filtered
            if self._filter_hash:
                return f"{self._base_output_path}/_debug_{self._filter_hash}"
            else:
                return f"{self._base_output_path}/_debug"
        else:
            return f"{self._intermediate_path}/{step.value}"

    # =========================================================================
    # Format detection and reading
    # =========================================================================

    def _read_input(self, spark: SparkSession, path: str) -> DataFrame:
        """Read input data based on configured input_format.

        Input files (unlike intermediate files) may contain segments and connectors
        with different schemas, so we always use mergeSchema=true when reading.
        """
        if self.input_format == InputFormat.AUTO:
            # Auto-detect: check if GeoParquet or Parquet+WKB
            if self._is_native_geoparquet(spark, path):
                logger.debug(f"Detected native GeoParquet input at {path}")
                return self._read_geoparquet(spark, path)
            else:
                # Parquet+WKB - need mergeSchema for segments/connectors with different columns
                logger.debug(f"Reading input as Parquet+WKB with mergeSchema at {path}")
                return self._read_parquet_wkb(spark, path)
        elif self.input_format == InputFormat.GEOPARQUET:
            return self._read_geoparquet(spark, path)
        else:  # PARQUET_WKB
            return self._read_parquet_wkb(spark, path)

    def _read_auto_detect(self, spark: SparkSession, path: str) -> DataFrame:
        """Auto-detect format and read appropriately.

        For cached intermediate files, we need to be careful because:
        - GeoParquet files have native GeometryUDT
        - Parquet+WKB files have BinaryType that needs conversion

        We probe a single file first to determine the actual schema, then read
        all files with explicit format handling.

        NOTE: We do NOT use mergeSchema for intermediate files because:
        1. All files within an intermediate step have the same schema
        2. mergeSchema can cause column ordering issues when combining with other DataFrames
        """
        # First, check if this is native GeoParquet by probing the actual column type
        if self._is_native_geoparquet(spark, path):
            logger.debug(f"Detected native GeoParquet at {path}")
            # For intermediate files, read without mergeSchema to avoid column ordering issues
            df = spark.read.format("geoparquet").load(path)
            return self._ensure_geometry_udt(df)

        # Not GeoParquet - read as Parquet+WKB
        # For intermediate files, we don't use mergeSchema because all files
        # have the same schema.
        logger.debug(f"Reading as Parquet+WKB (no mergeSchema) at {path}")
        geom_col = self.geometry_column
        df = spark.read.parquet(path)
        return df.withColumn(geom_col, expr(f"ST_GeomFromWKB({geom_col})"))

    def _is_native_geoparquet(self, spark: SparkSession, path: str) -> bool:
        """Check if a parquet file is in native GeoParquet format.

        GeoParquet files contain special metadata ("geo" key) that identifies
        which columns contain geometry and how to parse them. We check for this
        metadata to distinguish from plain Parquet+WKB files.
        """
        try:
            # Try to read a single parquet file to check for GeoParquet metadata
            import glob as glob_module

            from pyarrow import parquet as pq

            # Handle glob patterns
            if "*" in path:
                files = glob_module.glob(path)
                if not files:
                    return False
                sample_file = files[0]
            else:
                # Directory: find first parquet file
                files = glob_module.glob(f"{path}/*.parquet")
                if not files:
                    return False
                sample_file = files[0]

            # Check for GeoParquet metadata
            pq_file = pq.ParquetFile(sample_file)
            metadata = pq_file.schema_arrow.metadata
            if metadata is not None and b"geo" in metadata:
                return True
            return False
        except Exception:
            return False

    def _read_geoparquet(self, spark: SparkSession, path: str) -> DataFrame:
        """Read a native GeoParquet file.

        Ensures the geometry column is returned as GeometryUDT. If the file
        was written without proper GeoParquet metadata, we fall back to parsing
        the geometry column as WKB.
        """
        df = spark.read.format("geoparquet").option("mergeSchema", "true").load(path)
        return self._ensure_geometry_udt(df)

    def _read_parquet_wkb(self, spark: SparkSession, path: str) -> DataFrame:
        """Read a Parquet file with WKB-encoded geometry.

        The geometry column is read as BinaryType and then converted to GeometryUDT.
        Uses mergeSchema to handle files with different schemas (e.g., segments
        and connectors in the same glob pattern).
        """
        geom_col = self.geometry_column
        df = spark.read.option("mergeSchema", "true").parquet(path)
        # Convert WKB to GeometryUDT
        return df.withColumn(geom_col, expr(f"ST_GeomFromWKB({geom_col})"))

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

    def _ensure_geometry_udt(self, df: DataFrame) -> DataFrame:
        """Ensure the geometry column is GeometryUDT, not BinaryType.

        When reading GeoParquet files that may have been written without proper
        GeoParquet metadata (or when Spark reads geometry as binary), this
        converts WKB binary to GeometryUDT.

        Args:
            df: DataFrame that may have geometry as BinaryType or GeometryUDT

        Returns:
            DataFrame with geometry column guaranteed to be GeometryUDT
        """
        from pyspark.sql.types import BinaryType

        geom_col = self.geometry_column
        if geom_col not in df.columns:
            return df

        geom_type = df.schema[geom_col].dataType
        if isinstance(geom_type, BinaryType):
            logger.debug(f"Converting {geom_col} from BinaryType to GeometryUDT")
            return df.withColumn(geom_col, expr(f"ST_GeomFromWKB({geom_col})"))
        return df

    def __str__(self) -> str:
        if self.output_path:
            if self._filter_hash:
                filter_str = f"enabled (hash: {self._filter_hash})"
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
