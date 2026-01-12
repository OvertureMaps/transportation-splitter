"""Reference resolution functions for turn restrictions and destinations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, collect_list, struct, udf
from pyspark.sql.types import Row

from transportation_splitter._spark_udfs import (
    resolved_prohibited_transitions_schema,
)
from transportation_splitter.config import (
    DESTINATIONS_COLUMN,
    PROHIBITED_TRANSITIONS_COLUMN,
)


def resolve_tr_references(result_df: DataFrame) -> DataFrame:
    """Resolve turn restriction references after splitting."""
    splits_w_trs_df = (
        result_df.filter("turn_restrictions is not null and size(turn_restrictions)>0")
        .select("id", "start_lr", "end_lr", "turn_restrictions")
        .withColumn("tr", F.explode("turn_restrictions"))
        .drop("turn_restrictions")
        .select("*", "tr.*")
        .drop("tr")
    )

    referenced_segment_ids_df = splits_w_trs_df.select(col("segment_id").alias("referenced_segment_id")).distinct()

    referenced_splits_info_df = referenced_segment_ids_df.join(
        result_df,
        result_df.id == referenced_segment_ids_df.referenced_segment_id,
        "inner",
    ).select(
        col("id").alias("ref_id"),
        col("start_lr").alias("ref_start_lr"),
        col("end_lr").alias("ref_end_lr"),
        col("connectors").alias("ref_connectors"),
    )

    ref_joined_df = (
        splits_w_trs_df.join(
            referenced_splits_info_df,
            splits_w_trs_df.segment_id == referenced_splits_info_df.ref_id,
            "inner",
        )
        .select(splits_w_trs_df["*"], referenced_splits_info_df["*"])
        .drop("segment_id")
    )

    referenced_split_condition = F.when(
        F.col("next_connector_id").isNull(),
        F.when(
            F.col("final_heading") == "forward",
            F.expr("ref_connectors[0].connector_id=connector_id"),
        ).otherwise(F.expr("ref_connectors[1].connector_id=connector_id")),
    ).otherwise(
        F.expr(
            "exists(ref_connectors, x -> x.connector_id = connector_id) and "
            "exists(ref_connectors, x -> x.connector_id = next_connector_id)"
        )
    )

    trs_refs_resolved_df = ref_joined_df.filter(referenced_split_condition).select(
        col("id").alias("trs_id"),
        col("start_lr").alias("trs_start_lr"),
        col("end_lr").alias("trs_end_lr"),
        struct("tr_index", "sequence_index", "ref_id", "ref_start_lr", "ref_end_lr").alias("tr_seq"),
    )

    trs_refs_resolved_agg_df = trs_refs_resolved_df.groupBy("trs_id", "trs_start_lr", "trs_end_lr").agg(
        collect_list("tr_seq").alias("turn_restrictions")
    )

    result_w_trs_refs_df = (
        result_df.drop("turn_restrictions")
        .join(
            trs_refs_resolved_agg_df,
            (result_df.id == trs_refs_resolved_agg_df.trs_id)
            & (result_df.start_lr == trs_refs_resolved_agg_df.trs_start_lr)
            & (result_df.end_lr == trs_refs_resolved_agg_df.trs_end_lr),
            "left",
        )
        .drop("trs_id", "trs_start_lr", "trs_end_lr")
    )

    def apply_tr_split_refs(prohibited_transitions, turn_restrictions):
        if prohibited_transitions is None:
            return None
        new_prohibited_transitions = [pt.asDict(recursive=True) for pt in prohibited_transitions]
        if turn_restrictions:
            for tr in turn_restrictions:
                seq_item = new_prohibited_transitions[tr["tr_index"]]["sequence"][tr["sequence_index"]]
                seq_item["start_lr"] = tr["ref_start_lr"]
                seq_item["end_lr"] = tr["ref_end_lr"]
        resolved = [
            pt
            for pt in new_prohibited_transitions
            if "sequence" in pt and all(("start_lr" in s and "end_lr" in s) for s in pt["sequence"])
        ]
        return None if len(resolved) == 0 else [Row(**pt) for pt in resolved]

    apply_tr_split_refs_udf = udf(apply_tr_split_refs, resolved_prohibited_transitions_schema)

    result_trs_resolved_df = result_w_trs_refs_df.withColumn(
        PROHIBITED_TRANSITIONS_COLUMN,
        apply_tr_split_refs_udf(col(PROHIBITED_TRANSITIONS_COLUMN), col("turn_restrictions")),
    )
    return result_trs_resolved_df


def resolve_destinations_references(result_df: DataFrame) -> DataFrame:
    """Resolve destination references after splitting."""
    splits_w_destinations_df = (
        result_df.filter(f"{DESTINATIONS_COLUMN} is not null and size({DESTINATIONS_COLUMN})>0")
        .select("id", "start_lr", "end_lr", DESTINATIONS_COLUMN)
        .withColumn("dr", F.explode(DESTINATIONS_COLUMN))
        .select("*", "dr.*")
        .drop("dr")
    )

    referenced_segment_ids_df = splits_w_destinations_df.select(
        col("to_segment_id").alias("referenced_segment_id")
    ).distinct()

    referenced_splits_info_df = referenced_segment_ids_df.join(
        result_df,
        result_df.id == referenced_segment_ids_df.referenced_segment_id,
        "inner",
    ).select(
        col("id").alias("ref_id"),
        col("start_lr").alias("to_segment_start_lr"),
        col("end_lr").alias("to_segment_end_lr"),
        col("connectors").alias("ref_connectors"),
    )

    ref_joined_df = (
        splits_w_destinations_df.join(
            referenced_splits_info_df,
            splits_w_destinations_df.to_segment_id == referenced_splits_info_df.ref_id,
            "inner",
        )
        .select(splits_w_destinations_df["*"], referenced_splits_info_df["*"])
        .drop("segment_id")
    )

    referenced_split_condition = F.when(
        F.col("final_heading") == "forward",
        F.expr("ref_connectors[0].connector_id=to_connector_id"),
    ).otherwise(F.expr("ref_connectors[1].connector_id=to_connector_id"))

    destination_refs_resolved_df = ref_joined_df.filter(referenced_split_condition).select(
        col("id").alias("from_id"),
        col("start_lr").alias("from_start_lr"),
        col("end_lr").alias("from_end_lr"),
        struct(
            "labels",
            "symbols",
            "from_connector_id",
            "to_segment_id",
            "to_segment_start_lr",
            "to_segment_end_lr",
            "to_connector_id",
            "when",
            "final_heading",
        ).alias("d"),
    )

    destination_refs_resolved_agg_df = destination_refs_resolved_df.groupBy(
        "from_id", "from_start_lr", "from_end_lr"
    ).agg(collect_list("d").alias(f"{DESTINATIONS_COLUMN}_resolved"))

    result_w_destinations_resolved_df = (
        result_df.drop("destinations")
        .join(
            destination_refs_resolved_agg_df,
            (result_df.id == destination_refs_resolved_agg_df.from_id)
            & (result_df.start_lr == destination_refs_resolved_agg_df.from_start_lr)
            & (result_df.end_lr == destination_refs_resolved_agg_df.from_end_lr),
            "left",
        )
        .drop("from_id", "from_start_lr", "from_end_lr")
        .withColumnRenamed(f"{DESTINATIONS_COLUMN}_resolved", DESTINATIONS_COLUMN)
    )

    return result_w_destinations_resolved_df
