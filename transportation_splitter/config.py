"""Configuration and constants for the transportation splitter."""

from dataclasses import dataclass, field

# Column name constants
PROHIBITED_TRANSITIONS_COLUMN = "prohibited_transitions"
DESTINATIONS_COLUMN = "destinations"
LR_SCOPE_KEY = "between"

# IS_ON_SUB_SEGMENT_THRESHOLD_METERS 1mm
# This is the max distance from a connector point to a sub-segment for the connector to be
# considered "on" that sub-segment. It is used for the fallback logic to find connector's
# place in the segment's coordinates, for dealing with edge cases where the exact connector
# latlong coordinates are not found on the geometry because of rounding.
IS_ON_SUB_SEGMENT_THRESHOLD_METERS = 0.001


@dataclass
class SplitConfig:
    """Configuration for the segment splitting process."""

    # Controls whether or not to introduce a split on every connector along the segment
    split_at_connectors: bool = False

    # Which columns to explicitly include when looking for 'between' LR values to split at.
    # If left empty all then columns in the input parquet are considered.
    # If non-empty list is provided, then only those columns are considered.
    lr_columns_to_include: list[str] = field(default_factory=list)

    # Which columns to explicitly exclude when looking for 'between' LR values to split at.
    # Behaves like the include counterpart but negative.
    lr_columns_to_exclude: list[str] = field(default_factory=list)

    # How many digits to round the lat and long for split points.
    point_precision: int = 7

    # New split points are needed for linear references. This controls how far in meters from
    # other existing splits (from either connectors or other LRs) do these LRs need to be
    # for us to create a new connector for them instead of using the existing connector.
    lr_split_point_min_dist_meters: float = 0.01  # 1cm

    # Skips steps for which intermediate streams are found, default True,
    # set to False to always force reprocess all sub-steps
    reuse_existing_intermediate_outputs: bool = True

    # Skips expensive debug operations like count() and show() calls, default False.
    # Set to True to improve performance by skipping debug output.
    skip_debug_output: bool = True

    def __str__(self) -> str:
        lines = [
            "SplitConfig:",
            f"  split_at_connectors:       {self.split_at_connectors}",
            f"  reuse_existing_outputs:    {self.reuse_existing_intermediate_outputs}",
            f"  skip_debug_output:         {self.skip_debug_output}",
            f"  point_precision:           {self.point_precision} decimals",
            f"  lr_min_dist:               {self.lr_split_point_min_dist_meters * 100:.1f}cm",
        ]
        if self.lr_columns_to_include:
            lines.append(f"  lr_columns_include:        {self.lr_columns_to_include}")
        if self.lr_columns_to_exclude:
            lines.append(f"  lr_columns_exclude:        {self.lr_columns_to_exclude}")
        return "\n".join(lines)


DEFAULT_CFG = SplitConfig()
