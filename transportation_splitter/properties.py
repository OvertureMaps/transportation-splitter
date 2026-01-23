"""Property handling for turn restrictions and destinations."""


def get_trs(turn_restrictions, connectors: list[dict]) -> tuple:
    """
    Extract and filter turn restrictions for a split segment.

    Returns a tuple of (trs_to_keep, flattened_tr_seq_items) where:
    - trs_to_keep: Turn restrictions that apply to this split
    - flattened_tr_seq_items: Flattened list of all segment_id references in sequences,
      used to resolve segment references after split
    """
    if turn_restrictions is None:
        return None, None

    flattened_tr_seq_items = []
    trs_to_keep: list[dict] = []
    for tr in turn_restrictions:
        tr_heading = (tr.get("when") or {}).get("heading")
        tr_sequence = tr.get("sequence")
        if not tr_sequence or len(tr_sequence) == 0:
            continue

        if not connectors or len(connectors) != 2:
            # at this point modified segments are expected to have exactly two connector ids, skip edge cases
            continue

        first_connector_id_ref = tr_sequence[0].get("connector_id")

        if tr_heading == "forward" and connectors[1]["connector_id"] != first_connector_id_ref:
            # the second connector id on this segment split needs to match the first connector id in the sequence
            # because heading scope applies only to forward
            continue

        if tr_heading == "backward" and connectors[0]["connector_id"] != first_connector_id_ref:
            # the first connector id on this segment split needs to match the first connector id in the sequence
            # because heading scope applies only to backward
            continue

        if not any(first_connector_id_ref == c["connector_id"] for c in connectors):
            # the first connector id in the sequence needs to be in this split's connectors no matter what
            continue

        tr_idx = len(trs_to_keep)
        trs_to_keep.append(tr)

        for seq_idx, seq in enumerate(tr_sequence):
            flattened_tr_seq_items.append(
                {
                    "tr_index": tr_idx,
                    "sequence_index": seq_idx,
                    "segment_id": seq.get("segment_id"),
                    "connector_id": seq.get("connector_id"),
                    "next_connector_id": (
                        tr_sequence[seq_idx + 1]["connector_id"] if seq_idx + 1 < len(tr_sequence) else None
                    ),
                    "final_heading": tr.get("final_heading"),  # required, should always be not null
                }
            )

    if len(trs_to_keep) == 0:
        return None, None

    return trs_to_keep, flattened_tr_seq_items


def get_destinations(destinations, connectors: list[dict]) -> list[dict] | None:
    """Filter destinations that apply to the given connectors."""
    if destinations is None:
        return None
    if not connectors or len(connectors) != 2:
        return None

    # Pre-compute connector IDs as a set for O(1) lookup
    connector_ids = {c["connector_id"] for c in connectors}
    forward_connector_id = connectors[1]["connector_id"]
    backward_connector_id = connectors[0]["connector_id"]

    destinations_to_keep = [
        d for d in destinations if _destination_applies(d, connector_ids, forward_connector_id, backward_connector_id)
    ]
    return destinations_to_keep if destinations_to_keep else None


def _destination_applies(
    d: dict,
    connector_ids: set[str],
    forward_connector_id: str,
    backward_connector_id: str,
) -> bool:
    """Check if a destination applies to the given split connectors (optimized)."""
    when = d.get("when") or {}
    when_heading = when.get("heading")
    from_connector_id = d.get("from_connector_id")

    if not from_connector_id or not when_heading:
        # these are required properties and need them to exist to resolve the split reference
        return False

    # O(1) set lookup instead of O(N) list iteration
    if from_connector_id not in connector_ids:
        return False

    if when_heading == "forward" and forward_connector_id != from_connector_id:
        # the second connector id on this segment split needs to match the from_connector_id
        # because heading scope applies only to forward
        return False

    if when_heading == "backward" and backward_connector_id != from_connector_id:
        # the first connector id on this segment split needs to match the from_connector_id
        # because heading scope applies only to backward
        return False

    return True
