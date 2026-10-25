"""Property handling for turn restrictions, destinations, and names."""


def normalize_names(names: dict | None) -> dict | None:
    """
    Promote names.rules entries that cover an entire split segment to top-level name fields.

    After applying LR scope filtering to a split segment, rules whose 'between' is None
    (meaning they now cover the whole segment) are promoted:
    - Rules with variant='common' and language=None → promoted to names.primary
    - Rules with variant='common' and language=X   → promoted to names.common[X]
      only when names.common does not already contain X

    Rules that are promoted are removed from names.rules. All other rules are kept as-is.
    Returns the updated names dict, or the original dict if no changes are needed.
    """
    if names is None:
        return None

    rules = names.get("rules")
    if not rules:
        return names

    existing_common: dict[str, str] = names.get("common") or {}
    remaining_rules = []
    new_primary = None
    new_common_additions: dict[str, str] = {}

    for rule in rules:
        variant = rule.get("variant")
        language = rule.get("language")
        between = rule.get("between")
        value = rule.get("value")

        # Only promote rules that cover the whole segment (between is None),
        # have variant='common', and have a non-null value.
        if variant == "common" and between is None and value is not None:
            if language is None:
                if new_primary is None:
                    new_primary = value
                    continue
            elif language not in existing_common and language not in new_common_additions:
                new_common_additions[language] = value
                continue

        remaining_rules.append(rule)

    if new_primary is None and not new_common_additions:
        return names

    result = dict(names)

    if new_primary is not None:
        result["primary"] = new_primary

    if new_common_additions:
        merged = dict(existing_common)
        merged.update(new_common_additions)
        result["common"] = merged

    result["rules"] = remaining_rules if remaining_rules else None

    return result


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
