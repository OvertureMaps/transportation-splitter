"""Data wrangling and I/O management for the transportation splitter."""

import logging
from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class SplitterStep(Enum):
    """Enumeration of pipeline steps for intermediate file naming."""

    read_input = "input"
    spatial_filter = "1_spatially_filtered"
    joined = "2_joined"
    raw_split = (
        "3_raw_split"  # Note: this step has no geometry, must use vanilla parquet
    )
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
    MEMORY = "in-memory"  # When there is no output path, DF is returned.


@dataclass
class SplitterDataWrangler:
    """
    Configuration and I/O operations for the transportation splitter.

    This class handles reading input data, writing intermediate files,
    and writing final output in various formats.

    Example:
        >>> wrangler = SplitterDataWrangler(
        ...     input_path="data/input/*.parquet",
        ...     output_path="data/output/",
        ...     input_format=InputFormat.AUTO,
        ...     output_format=OutputFormat.GEOPARQUET,
        ... )
        >>>
        >>> # With spatial filter (creates isolated cache at _filtered paths):
        >>> wrangler = SplitterDataWrangler(
        ...     input_path="data/input/*.parquet",
        ...     output_path="data/output/",
        ...     filter_wkt="POLYGON((...)))",
        ... )

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

    # Private: computed paths (set in __post_init__)
    _intermediate_path: str = field(init=False, repr=False)
    _final_output_path: str = field(init=False, repr=False)

    def __post_init__(self):
        """Normalize paths and compute derived paths."""
        self.input_path = self.input_path.rstrip("/")
        if self.output_path is not None:
            self.output_path = self.output_path.rstrip("/")
            # When filter_wkt is set, use _filtered suffix on output path
            # to isolate all cached data from unfiltered runs
            base_output = (
                f"{self.output_path}_filtered" if self.filter_wkt else self.output_path
            )
            self._intermediate_path = f"{base_output}/_intermediate"
            self._final_output_path = f"{base_output}/_output"
        else:
            self._intermediate_path = None
            self._final_output_path = None
            self.output_format = OutputFormat.MEMORY

    # =========================================================================
    # Public API - Override these methods in subclasses for custom I/O
    # =========================================================================

    def read(self, spark: SparkSession, step: SplitterStep) -> DataFrame:
        """
        Read a DataFrame for the given pipeline step.

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
        Write a DataFrame for the given pipeline step.

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
        Check if a pipeline step has already been materialized.

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

        When filter_wkt is set, the parent directories (_intermediate, _output)
        already have the _filtered suffix applied in __post_init__.
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
        output_str = self.output_path if self.output_path else "(none - in-memory only)"
        lines = [
            "SplitterDataWrangler:",
            f"  Input:        {self.input_path}",
            f"  Output:       {output_str}",
            f"  Filter:       {'enabled (paths use _filtered suffix)' if self.filter_wkt else 'none'}",
            f"  Input Format: {self.input_format.value}",
            f"  Output Format:{self.output_format.value}",
            f"  Compression:  {self.compression}",
            f"  Block Size:   {self.block_size // (1024 * 1024)}MB",
        ]
        return "\n".join(lines)
