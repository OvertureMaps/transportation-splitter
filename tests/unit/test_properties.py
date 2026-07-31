"""Unit tests for names normalization logic."""

from transportation_splitter.properties import normalize_names


def test_normalize_names_none():
    assert normalize_names(None) is None


def test_normalize_names_no_rules():
    names = {"primary": "Main St", "common": None, "rules": None}
    assert normalize_names(names) is names


def test_normalize_names_empty_rules():
    names = {"primary": "Main St", "common": None, "rules": []}
    assert normalize_names(names) is names


def test_normalize_names_no_promotable_rules():
    """Rules with a between value are not promoted."""
    names = {
        "primary": "Main St",
        "common": None,
        "rules": [
            {"variant": "common", "language": None, "value": "Other St", "between": [0.0, 0.5]},
        ],
    }
    result = normalize_names(names)
    assert result is names


def test_normalize_names_promotes_primary():
    """A rule with variant=common, language=None, between=None is promoted to primary."""
    names = {
        "primary": "Falconberg Mews",
        "common": None,
        "rules": [
            {"variant": "common", "language": None, "value": "Sutton Row"},
            {"variant": "common", "language": "ja", "value": "サットン・ロウ", "between": [0.0, 0.5]},
        ],
    }
    result = normalize_names(names)
    assert result["primary"] == "Sutton Row"
    # Remaining rules keep the one with a between value
    assert result["rules"] == [
        {"variant": "common", "language": "ja", "value": "サットン・ロウ", "between": [0.0, 0.5]},
    ]


def test_normalize_names_promotes_common_language():
    """A rule with variant=common, language=X, between=None is promoted to common[X]."""
    names = {
        "primary": "Falconberg Mews",
        "common": None,
        "rules": [
            {"variant": "common", "language": "ja", "value": "サットン・ロウ"},
            {"variant": "common", "language": None, "value": "Sutton Row"},
        ],
    }
    result = normalize_names(names)
    assert result["primary"] == "Sutton Row"
    assert result["common"] == {"ja": "サットン・ロウ"}
    assert result["rules"] is None


def test_normalize_names_full_issue_example_segment1():
    """
    Reproduces the first split segment from issue #35.

    Original segment: primary="Falconberg Mews", rules covering [0, 0.489] with
    "Sutton Row" and "サットン・ロウ", and [0.489, 1] with "Falconberg Mews".

    After apply_lr_scope for the first split [0, 0.489], the two rules for that range
    cover the whole split so their between is removed. normalize_names should then
    promote "Sutton Row" to primary and "サットン・ロウ" to common["ja"].
    """
    names_after_lr_scope = {
        "primary": "Falconberg Mews",
        "common": None,
        # between has been removed by apply_lr_scope since both rules now cover the whole split
        "rules": [
            {"variant": "common", "language": "ja", "value": "サットン・ロウ", "between": None},
            {"variant": "common", "language": None, "value": "Sutton Row", "between": None},
        ],
    }
    result = normalize_names(names_after_lr_scope)
    assert result["primary"] == "Sutton Row"
    assert result["common"] == {"ja": "サットン・ロウ"}
    assert result["rules"] is None


def test_normalize_names_full_issue_example_segment2():
    """
    Reproduces the second split segment from issue #35.

    After apply_lr_scope for the second split [0.489, 1], only the "Falconberg Mews"
    common rule remains (between removed). normalize_names should set primary to
    "Falconberg Mews" (same value, no functional change) and remove the rule.
    """
    names_after_lr_scope = {
        "primary": "Falconberg Mews",
        "common": None,
        "rules": [
            {"variant": "common", "language": None, "value": "Falconberg Mews"},
        ],
    }
    result = normalize_names(names_after_lr_scope)
    assert result["primary"] == "Falconberg Mews"
    assert result["rules"] is None


def test_normalize_names_does_not_overwrite_existing_common():
    """Existing common[language] entries are preserved; new ones are added."""
    names = {
        "primary": "Main St",
        "common": {"fr": "Rue Principale"},
        "rules": [
            {"variant": "common", "language": "fr", "value": "Rue Conflicting"},
            {"variant": "common", "language": "de", "value": "Hauptstraße"},
        ],
    }
    result = normalize_names(names)
    # fr already exists in common, so the rule is kept as-is but the existing value is preserved
    assert result["common"]["fr"] == "Rue Principale"
    assert result["common"]["de"] == "Hauptstraße"
    # The fr rule is kept because fr was already in common
    assert any(r.get("language") == "fr" for r in (result["rules"] or []))


def test_normalize_names_non_common_variant_not_promoted():
    """Rules with variant != 'common' are never promoted."""
    names = {
        "primary": "Main St",
        "common": None,
        "rules": [
            {"variant": "official", "language": None, "value": "Official Main St"},
            {"variant": "alternate", "language": None, "value": "Alt St"},
        ],
    }
    result = normalize_names(names)
    assert result is names  # unchanged


def test_normalize_names_only_first_common_null_language_is_promoted():
    """When multiple common/null-language rules exist, only the first is promoted."""
    names = {
        "primary": "Old Name",
        "common": None,
        "rules": [
            {"variant": "common", "language": None, "value": "First Name"},
            {"variant": "common", "language": None, "value": "Second Name"},
        ],
    }
    result = normalize_names(names)
    assert result["primary"] == "First Name"
    # The second duplicate rule is kept in rules
    assert result["rules"] == [{"variant": "common", "language": None, "value": "Second Name"}]
