"""
Transportation Splitter - Split Overture transportation segments into simpler sub-segments.

This package processes GeoParquet files containing road segments and connectors,
splitting segments at connector points and linear reference boundaries.
"""

from transportation_splitter.config import (
    DEFAULT_CFG,
    DESTINATIONS_COLUMN,
    PROHIBITED_TRANSITIONS_COLUMN,
    SplitConfig,
)
from transportation_splitter.geometry import (
    get_length,
    has_consecutive_dupe_coords,
    remove_consecutive_dupes,
    split_line,
)
from transportation_splitter.linear_reference import (
    add_lr_split_points,
    apply_lr_on_split,
    get_connector_split_points,
    get_lrs,
)
from transportation_splitter.models import JoinedConnector, SplitPoint, SplitSegment
from transportation_splitter.pipeline import OvertureTransportationSplitter
from transportation_splitter.wrangler import (
    InputFormat,
    OutputFormat,
    SplitterDataWrangler,
    SplitterStep,
)

__all__ = [
    # Main entry point
    "OvertureTransportationSplitter",
    # Configuration
    "SplitConfig",
    "DEFAULT_CFG",
    "DESTINATIONS_COLUMN",
    "PROHIBITED_TRANSITIONS_COLUMN",
    # I/O
    "SplitterDataWrangler",
    "SplitterStep",
    "InputFormat",
    "OutputFormat",
    # Models
    "JoinedConnector",
    "SplitPoint",
    "SplitSegment",
    # Geometry functions (for testing/profiling)
    "get_length",
    "split_line",
    "has_consecutive_dupe_coords",
    "remove_consecutive_dupes",
    # Linear reference functions (for testing/profiling)
    "get_lrs",
    "apply_lr_on_split",
    "add_lr_split_points",
    "get_connector_split_points",
]

__version__ = "0.1.0"
