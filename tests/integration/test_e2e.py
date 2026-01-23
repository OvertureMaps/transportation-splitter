"""Integration tests for end-to-end transportation splitting."""

import logging

from pyspark.sql import Row

from tests.conftest import get_test_data_path
from transportation_splitter import (
    DESTINATIONS_COLUMN,
    PROHIBITED_TRANSITIONS_COLUMN,
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

logger: logging.Logger = logging.getLogger(__name__)


def test_split_no_connector_split(spark_session):
    input_path = get_test_data_path()
    output_path = "tests/out/preserve_connectors"

    test_config = SplitConfig(
        split_at_connectors=False,
        lr_columns_to_exclude=[PROHIBITED_TRANSITIONS_COLUMN, DESTINATIONS_COLUMN],
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            reuse_existing_intermediate_outputs=False,
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    actual_df = result_df.filter("id = '08728d5430ffffff04767d5cdb906cb5'").sort("start_lr").collect()
    assert len(actual_df) == 3

    # Check attributes
    assert all(
        [
            split["class"] == "motorway"
            and split.subclass == "link"
            and split.subclass_rules == [Row(value="link", between=None)]
            for split in actual_df
        ]
    )
    assert actual_df[0].road_flags == [Row(values=["is_bridge", "is_link"], between=None)]
    assert actual_df[1].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[2].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[0].road_surface is None
    assert actual_df[1].road_surface is None
    assert actual_df[2].road_surface == [Row(value="paved", between=None)]

    # Interior connector is preserved on one split segment
    assert actual_df[0].connectors == [
        Row(connector_id="08f28d5430ce156b042ffd6280a10651", at=0.0),
        Row(connector_id="08f28d5430ce146b0432776290e587d8", at=0.582983938),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=1.0),
    ]
    assert actual_df[1].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=0.0),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=1.0),
    ]
    assert actual_df[2].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=0.0),
        Row(connector_id="08f28d543056a114043a7d54efe962ca", at=1.0),
    ]


def test_split_all_connectors(spark_session):
    input_path = get_test_data_path()
    output_path = "tests/out/all_connectors"

    test_config = SplitConfig(
        split_at_connectors=True,
    )

    splitter = OvertureTransportationSplitter(
        spark=spark_session,
        wrangler=SplitterDataWrangler(
            input_path=input_path,
            output_path=output_path,
            reuse_existing_intermediate_outputs=False,
        ),
        cfg=test_config,
    )
    result_df = splitter.split()

    validate_all_connector_split(result_df)


def validate_all_connector_split(result_df):
    actual_df = result_df.filter("id = '08728d5430ffffff04767d5cdb906cb5'").sort("start_lr").collect()
    assert len(actual_df) == 4

    # Check attributes
    assert all(
        [
            split["class"] == "motorway"
            and split.subclass == "link"
            and split.subclass_rules == [Row(value="link", between=None)]
            for split in actual_df
        ]
    )
    assert actual_df[0].road_flags == [Row(values=["is_bridge", "is_link"], between=None)]
    assert actual_df[1].road_flags == [Row(values=["is_bridge", "is_link"], between=None)]
    assert actual_df[2].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[3].road_flags == [Row(values=["is_link"], between=None)]
    assert actual_df[0].road_surface is None
    assert actual_df[1].road_surface is None
    assert actual_df[2].road_surface is None
    assert actual_df[3].road_surface == [Row(value="paved", between=None)]

    assert actual_df[0].connectors == [
        Row(connector_id="08f28d5430ce156b042ffd6280a10651", at=0.0),
        Row(connector_id="08f28d5430ce146b0432776290e587d8", at=1.0),
    ]
    assert actual_df[1].connectors == [
        Row(connector_id="08f28d5430ce146b0432776290e587d8", at=0.0),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=1.0),
    ]
    assert actual_df[2].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.02877969", at=0.0),
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=1.0),
    ]
    assert actual_df[3].connectors == [
        Row(connector_id="08728d5430ffffff04767d5cdb906cb5@0.268194787", at=0.0),
        Row(connector_id="08f28d543056a114043a7d54efe962ca", at=1.0),
    ]
