import pytest
from transportation_splitter import SplitConfig


def test_split_config_skip_debug_default():
    """Test that skip_debug_output defaults to False"""
    cfg = SplitConfig()
    assert cfg.skip_debug_output == False


def test_split_config_skip_debug_enabled():
    """Test that skip_debug_output can be set to True"""
    cfg = SplitConfig(skip_debug_output=True)
    assert cfg.skip_debug_output == True


def test_split_config_skip_debug_with_other_params():
    """Test that skip_debug_output works with other configuration parameters"""
    cfg = SplitConfig(
        split_at_connectors=False,
        skip_debug_output=True,
        point_precision=5
    )
    assert cfg.skip_debug_output == True
    assert cfg.split_at_connectors == False
    assert cfg.point_precision == 5
