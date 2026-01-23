"""Geometry operations for the transportation splitter."""

import pyproj
from shapely import wkt
from shapely.geometry import LineString, Point

from transportation_splitter.models import SplitPoint, SplitSegment


def has_consecutive_dupe_coords(line: LineString) -> bool:
    """Check if a LineString has consecutive duplicate coordinates."""
    coordinates = list(line.coords)
    return any(coordinates[i] == coordinates[i - 1] for i in range(1, len(coordinates)))


def remove_consecutive_dupes(coordinates):
    """Remove consecutive duplicate coordinates from a list."""
    return [coordinates[i] for i in range(len(coordinates)) if i == 0 or coordinates[i] != coordinates[i - 1]]


def are_different_coords(coords1, coords2):
    """Check if two coordinate tuples are different."""
    return coords1[0] != coords2[0] or coords1[1] != coords2[1]


def round_point(point: Point, precision: int) -> Point:
    """Round a point's coordinates to the specified precision."""
    return Point(round(point.x, precision), round(point.y, precision))


def get_length(line_geometry: LineString) -> float:
    """Calculate the geodesic length of a LineString in meters using WGS84."""
    geod = pyproj.Geod(ellps="WGS84")
    return geod.geometry_length(line_geometry)


def split_line(original_line_geometry: LineString, split_points: list[SplitPoint]) -> list[SplitSegment]:
    """Split a LineString into segments at the given points."""
    # Special case to avoid processing when there are only start/end split points
    if len(split_points) == 2 and split_points[0].lr == 0 and split_points[1].lr == 1:
        return [SplitSegment(0, original_line_geometry, split_points[0], split_points[1])]

    split_segments: list[SplitSegment] = []
    for split_point_start, split_point_end in zip(split_points[:-1], split_points[1:]):
        idx_start = split_point_start.at_coord_idx + 1
        idx_end = split_point_end.at_coord_idx + 1
        coords = (
            list(split_point_start.geometry.coords)
            + original_line_geometry.coords[idx_start:idx_end]
            + list(split_point_end.geometry.coords)
        )

        deduped_coords = remove_consecutive_dupes(coords)
        if len(deduped_coords) > 1:
            # edge case - only one point after removing dupes is just ignored
            geom = LineString(deduped_coords)
            split_segments.append(SplitSegment(len(split_segments), geom, split_point_start, split_point_end))
    return split_segments


def sanitize_wkt(wkt_str: str) -> str:
    """Sanitize a WKT string by parsing and escaping quotes."""
    geom = wkt.loads(wkt_str)  # This will throw an error if the WKT is invalid.
    # Escape single quotes by replacing them with two single quotes
    return str(geom).replace("'", "''")


def get_length_bucket(length: float) -> str:
    """Categorize a length into a human-readable bucket."""
    if length <= 0.01:
        return "A. <=1cm"
    if length <= 0.1:
        return "B. 1cm-10cm"
    if length <= 1:
        return "C. 10cm-1m"
    if length <= 100:
        return "D. 1m-100m"
    if length <= 1000:
        return "E. 100m-1km"
    if length <= 10000:
        return "F. 1km-10km"
    return "G. >10km"
