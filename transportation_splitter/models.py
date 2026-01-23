"""Domain models for the transportation splitter."""

from dataclasses import dataclass

from shapely.geometry import LineString, Point


@dataclass
class JoinedConnector:
    """Represents a connector joined with its geometry from the connectors table."""

    connector_id: str
    connector_geometry: Point
    connector_index: int
    connector_at: float


class SplitPoint:
    """Represents a segment split point."""

    def __init__(
        self,
        id=None,
        geometry=None,
        lr=None,
        lr_meters=None,
        is_lr_added=False,
        at_coord_idx=None,
    ):
        self.id = id
        self.geometry = geometry
        self.lr = lr
        self.lr_meters = lr_meters
        self.is_lr_added = is_lr_added
        self.at_coord_idx = at_coord_idx

    def __repr__(self):
        return (
            f"SplitPoint(at_coord_idx={str(self.at_coord_idx).rjust(3)}, "
            f"geometry={str(self.geometry).ljust(40)}, "
            f"lr={str(self.lr).ljust(22)}) ({str(self.lr_meters).ljust(22)}m), "
            f"is_lr_added={self.is_lr_added}"
        )


class SplitSegment:
    """Represents a split segment."""

    def __init__(
        self,
        id=None,
        geometry: LineString | None = None,
        start_split_point: SplitPoint | None = None,
        end_split_point: SplitPoint | None = None,
    ):
        self.id = id
        self.geometry = geometry
        self.start_split_point = start_split_point
        self.end_split_point = end_split_point

    def __repr__(self):
        return (
            f"SplitSegment(id={self.id}, "
            f"@{str(self.start_split_point.lr).ljust(20)} -{str(self.end_split_point.lr).rjust(20)} "
            f"length={str(self.length).rjust(22)}, geometry={str(self.geometry)})"
        )

    @property
    def length(self) -> float:
        """Length of the segment in meters."""
        return self.end_split_point.lr_meters - self.start_split_point.lr_meters
