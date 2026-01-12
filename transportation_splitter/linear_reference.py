"""Linear reference operations for the transportation splitter."""

from collections import deque
from typing import Any, Optional

import pyproj
from shapely.geometry import LineString, Point

from transportation_splitter.config import (
    DEFAULT_CFG,
    IS_ON_SUB_SEGMENT_THRESHOLD_METERS,
    LR_SCOPE_KEY,
)
from transportation_splitter.geometry import are_different_coords, round_point
from transportation_splitter.models import JoinedConnector, SplitPoint, SplitSegment


def get_lrs(x) -> set[float]:
    """
    Extract all linear reference values from a nested structure.

    Uses a set internally for O(1) membership checks instead of O(N) list checks.
    Returns a set containing 0.0, 1.0, and all LR values found.
    """
    output: set[float] = {0.0, 1.0}
    _extract_lrs_recursive(x, output)
    return output


def _extract_lrs_recursive(x, output: set[float]) -> None:
    """Helper: recursively extract LR values into the output set."""
    if x is None:
        return

    if isinstance(x, list):
        for sub_item in x:
            _extract_lrs_recursive(sub_item, output)
    elif isinstance(x, dict):
        if LR_SCOPE_KEY in x and x[LR_SCOPE_KEY] is not None:
            for lr in x[LR_SCOPE_KEY]:
                if lr:  # Skip None/0 values
                    output.add(lr)
        for key, value in x.items():
            if key != LR_SCOPE_KEY and value is not None:
                _extract_lrs_recursive(value, output)


def apply_lr_on_split(
    original_lr: list[float],
    split_segment: SplitSegment,
    original_segment_length: float,
    min_overlapping_length_meters: float,
) -> tuple[bool, Optional[list[float]]]:
    """
    Apply a linear reference to a split segment.

    Returns a tuple of (is_applicable, new_lr) where:
    - is_applicable: True if the LR applies to this split
    - new_lr: The new LR relative to the split, or None if it covers the whole split
    """
    split_length = split_segment.length
    lr_start_meters = (original_lr[0] if original_lr[0] else 0) * original_segment_length
    lr_end_meters = (original_lr[1] if original_lr[1] else 1) * original_segment_length

    # make LRs relative to this split
    lr_start_within_split_meters = lr_start_meters - split_segment.start_split_point.lr_meters
    lr_end_within_split_meters = lr_end_meters - split_segment.start_split_point.lr_meters

    # [--|---(--|---)----]
    # 0  10  25 30  45  60
    # between = [25m to 45m]
    # split = |10m to 30m|
    # new between = [-15m to 35]

    # part overlapping with the split:
    overlapping_start_meters = max(lr_start_within_split_meters, 0)
    overlapping_end_meters = min(lr_end_within_split_meters, split_length)

    overlapping_length_meters = overlapping_end_meters - overlapping_start_meters
    if overlapping_length_meters < min_overlapping_length_meters:
        return (False, None)

    new_lr = (
        None
        if split_length - overlapping_length_meters
        < min_overlapping_length_meters  # set new LR to null if it covers the whole split within tolerance
        else [
            overlapping_start_meters / split_length,
            overlapping_end_meters / split_length,
        ]  # new LR relative to split length
    )

    return (True, new_lr)


def apply_lr_scope(
    x: Any,
    split_segment: SplitSegment,
    original_segment_length: float,
    min_overlapping_length_meters: float,
    xpath: str = "$",
):
    """
    Recursively apply linear reference scope to a nested structure.

    Filters out items whose LR scope doesn't apply to the given split,
    and adjusts LR values to be relative to the split.
    """
    if x is None:
        return None

    if isinstance(x, list):
        output_list = []
        for index, sub_item in enumerate(x):
            cleaned_sub_item = apply_lr_scope(
                sub_item,
                split_segment,
                original_segment_length,
                min_overlapping_length_meters,
                f"{xpath}[{index}]",
            )
            if cleaned_sub_item is not None:
                output_list.append(cleaned_sub_item)
        return output_list if output_list else None
    elif isinstance(x, dict):
        output_dict = {}
        if LR_SCOPE_KEY in x and x[LR_SCOPE_KEY] is not None:
            original_lr = x[LR_SCOPE_KEY]
            if not isinstance(original_lr, list):
                raise Exception(f"{xpath}.{LR_SCOPE_KEY} is of type {str(type(original_lr))}, expecting list!")
            if len(original_lr) != 2:
                raise Exception(f"{xpath}.{LR_SCOPE_KEY} has {str(len(original_lr))} items, expecting 2 for a LR!")

            is_applicable, new_lr = apply_lr_on_split(
                original_lr,
                split_segment,
                original_segment_length,
                min_overlapping_length_meters,
            )
            if not is_applicable:
                return None
            if new_lr:
                output_dict[LR_SCOPE_KEY] = new_lr

        for key in x.keys():
            if key == LR_SCOPE_KEY:
                continue
            clean_sub_prop = apply_lr_scope(
                x[key],
                split_segment,
                original_segment_length,
                min_overlapping_length_meters,
                f"{xpath}.{key}",
            )
            if clean_sub_prop is not None:
                output_dict[key] = clean_sub_prop

        return output_dict if output_dict else None
    else:
        return x


def add_lr_split_points(
    split_points: list[SplitPoint],
    lrs: list[float],
    segment_id: str,
    original_segment_geometry: LineString,
    line_length: float,
    split_point_precision: int = DEFAULT_CFG.point_precision,
    min_dist_meters: float = DEFAULT_CFG.lr_split_point_min_dist_meters,
) -> list[SplitPoint]:
    """
    Add split points at linear reference positions along a line.

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
    coords = list(original_segment_geometry.coords)

    for lr in lrs:
        add_split_point = False
        if len(split_points) == 0:
            add_split_point = True
        else:
            closest_existing_split_point = min(split_points, key=lambda existing: abs(lr - existing.lr))
            if abs(closest_existing_split_point.lr - lr) * line_length > min_dist_meters:
                add_split_point = True

        if add_split_point:
            target_length = lr * line_length
            coord_idx = 0  # remember the index of the original geometry's coordinate
            for (lon1, lat1), (lon2, lat2) in zip(coords[:-1], coords[1:]):
                (azimuth, _, subsegment_length) = geod.inv(lon1, lat1, lon2, lat2, return_back_azimuth=False)
                if round(target_length - subsegment_length, 6) <= 0:
                    # Compute final point on this subsegment
                    break
                target_length -= subsegment_length
                coord_idx += 1

            # target_length is the length along this subsegment where the point is located. Use geod.fwd()
            # with the azimuth to get the final point
            split_lon, split_lat, _ = geod.fwd(lon1, lat1, azimuth, target_length, return_back_azimuth=False)

            point_geometry = round_point(Point(split_lon, split_lat), split_point_precision)
            # see if after rounding we have a point identical to last one;
            # if yes, then don't add a new split point; if we did, we would end up with invalid lines
            # that start and end in the same point, or, more accurately, because we have a step that
            # removes consecutive identical coordinates from splits, it would result in a line with a
            # single point, which would be invalid.
            # this effectively is an additional tolerance on top of LR_SPLIT_POINT_MIN_DIST_METERS,
            # which is controlled by the rounding parameter split_point_precision, default value=7, which is ~centimeter size
            if not split_points or are_different_coords(split_points[-1].geometry.coords[0], point_geometry.coords[0]):
                split_points.append(
                    SplitPoint(
                        f"{segment_id}@{str(lr)}",
                        point_geometry,
                        lr,
                        lr_meters=lr * line_length,
                        is_lr_added=True,
                        at_coord_idx=coord_idx,
                    )
                )
    return split_points


def get_connector_split_points(
    connectors: list[JoinedConnector],
    original_segment_geometry: LineString,
    original_segment_length: float,
) -> list[SplitPoint]:
    """
    Get split points from connectors along a segment.

    Finds the position of each connector on the segment geometry and creates
    corresponding split points.
    """
    split_points = []
    if not connectors:
        return split_points

    original_segment_coords = list(original_segment_geometry.coords)
    sorted_valid_connectors = sorted([c for c in connectors if c.connector_geometry], key=lambda p: p.connector_index)
    connectors_queue = deque(sorted_valid_connectors)
    if not connectors_queue:
        return split_points

    # Get the length of the projected segment
    geod = pyproj.Geod(ellps="WGS84")
    coord_count = len(original_segment_coords)

    # Connector coordinates have exact match in the segment's coordinates.
    # Edge case first - if last connector matches the last coordinate then LR should be 1,
    # no matter if the same coordinate may also appear somewhere else in the middle of the segment
    last_connector = connectors_queue[-1]
    if not are_different_coords(last_connector.connector_geometry.coords[0], original_segment_coords[-1]):
        split_points.append(
            SplitPoint(
                last_connector.connector_id,
                last_connector.connector_geometry,
                lr=1,
                lr_meters=original_segment_length,
                is_lr_added=False,
                at_coord_idx=coord_count - 1,
            )
        )
        connectors_queue.pop()

    lr_meters = 0
    for coord_idx in range(0, coord_count):
        for connector in list(connectors_queue):
            connector_geometry = connector.connector_geometry
            if not are_different_coords(connector_geometry.coords[0], original_segment_coords[coord_idx]):
                lr = lr_meters / original_segment_length
                split_points.append(
                    SplitPoint(
                        connector.connector_id,
                        connector_geometry,
                        lr,
                        lr_meters,
                        is_lr_added=False,
                        at_coord_idx=coord_idx,
                    )
                )
                connectors_queue.remove(connector)

        if coord_idx < coord_count - 1:
            sub_segment = LineString(
                [
                    original_segment_coords[coord_idx],
                    original_segment_coords[coord_idx + 1],
                ]
            )
            sub_segment_length = geod.geometry_length(sub_segment)
            lr_meters += sub_segment_length

    if not connectors_queue:
        return split_points

    # Pass 2 - fallback if some connectors don't match exactly the coordinates,
    # find which consecutive coord pair sub-segment it lies on
    coord_idx = 0
    accumulated_segment_length = 0
    for (lon1, lat1), (lon2, lat2) in zip(original_segment_coords[:-1], original_segment_coords[1:]):
        sub_segment = LineString([(lon1, lat1), (lon2, lat2)])
        sub_segment_length = geod.geometry_length(sub_segment)

        while connectors_queue:
            connector = connectors_queue[0]
            connector_geometry = connector.connector_geometry

            # Calculate the distances between points 1 and 2 of the sub-segment and the connector
            _, _, dist12 = geod.inv(lon1, lat1, lon2, lat2)
            _, _, dist1c = geod.inv(lon1, lat1, connector_geometry.x, connector_geometry.y)
            _, _, dist2c = geod.inv(lon2, lat2, connector_geometry.x, connector_geometry.y)

            dist_diff = abs(dist1c + dist2c - dist12)
            if dist_diff < IS_ON_SUB_SEGMENT_THRESHOLD_METERS:
                if len(connectors_queue) == 1 and not are_different_coords(
                    connector_geometry.coords[0], original_segment_coords[-1]
                ):
                    # edge case first - if this is the last connector, and it matches the last coordinate
                    # then LR should be 1, no matter if the same coordinate may also appear here,
                    # somewhere else in the middle of the segment
                    at_coord_idx = len(original_segment_coords) - 1
                    lr_meters = original_segment_length
                else:
                    offset_on_segment_meters = dist1c
                    at_coord_idx = coord_idx + 1 if offset_on_segment_meters == sub_segment_length else coord_idx
                    lr_meters = accumulated_segment_length + offset_on_segment_meters

                lr = lr_meters / original_segment_length
                split_points.append(
                    SplitPoint(
                        connector.connector_id,
                        connector_geometry,
                        lr,
                        lr_meters,
                        is_lr_added=False,
                        at_coord_idx=at_coord_idx,
                    )
                )
                connectors_queue.popleft()
            else:
                # next connector is not on this segment, move to next segment
                break
        coord_idx += 1
        accumulated_segment_length += sub_segment_length

    if connectors_queue:
        raise Exception("Could not find coordinates of connectors in segment's geometry")
    return split_points
