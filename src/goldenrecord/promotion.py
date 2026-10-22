"""Promotion to Gold: golden records with history, cross-reference and audit log.

* ``gld_customer`` is a type 2 slowly changing dimension: a changed golden record closes its
  current version (``valid_to``, ``is_current = false``) and opens a new one.
* ``gld_customer_xref`` maps every source record to its master key.
* ``gld_dq_audit`` has one row per attribute value that changed, with the rule that chose
  it and the winning source record, so any golden value traces back to its source.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import Settings
from goldenrecord.schema import GOLDEN_ATTRIBUTES

GOLD_COLUMNS: tuple[str, ...] = (
    "master_key",
    *GOLDEN_ATTRIBUTES,
    "source_record_count",
    "source_systems",
    "confidence_band",
    "attr_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "run_id",
)


def attr_hash(columns: tuple[str, ...] = GOLDEN_ATTRIBUTES) -> Column:
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns])), 256)


def apply_scd2(
    snapshot: DataFrame, current_gold: DataFrame | None, retired_keys: DataFrame, now: datetime, run_id: str
) -> DataFrame:
    """Merge a new golden snapshot into the existing SCD2 table (returns the full new table)."""
    ts = F.lit(now).cast("timestamp")
    new_rows = (
        snapshot.withColumn("attr_hash", attr_hash())
        .withColumn("valid_from", ts)
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("run_id", F.lit(run_id))
        .select(*GOLD_COLUMNS)
    )
    if current_gold is None:
        return new_rows

    history = current_gold.filter(~F.col("is_current"))
    current = current_gold.filter(F.col("is_current"))
    unchanged_keys = (
        current.select("master_key", "attr_hash")
        .join(new_rows.select("master_key", "attr_hash"), ["master_key", "attr_hash"])
        .select("master_key")
    )

    kept_current = current.join(unchanged_keys, "master_key", "left_semi")
    closed = (
        current.join(unchanged_keys, "master_key", "left_anti")
        .withColumn("valid_to", ts)
        .withColumn("is_current", F.lit(False))
    )
    opened = new_rows.join(unchanged_keys, "master_key", "left_anti").join(
        retired_keys.select("master_key"), "master_key", "left_anti"
    )
    return (
        history.select(*GOLD_COLUMNS)
        .unionByName(kept_current.select(*GOLD_COLUMNS))
        .unionByName(closed.select(*GOLD_COLUMNS))
        .unionByName(opened.select(*GOLD_COLUMNS))
    )


def audit_changes(
    snapshot: DataFrame, current_gold: DataFrame | None, settings: Settings, now: datetime, run_id: str
) -> DataFrame:
    """One row per (master_key, attribute) whose value changed (or was first set)."""
    strategies = {r.attribute: r.strategy for r in settings.survivorship}
    ts = F.lit(now).cast("timestamp")
    new = snapshot.select(
        "master_key",
        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(a).alias("attribute"),
                        F.col(a).cast("string").alias("new_value"),
                        F.col(f"{a}__winner").alias("winning_record_id"),
                        F.lit(strategies[a]).alias("rule"),
                    )
                    for a in GOLDEN_ATTRIBUTES
                ]
            )
        ).alias("x"),
    ).select("master_key", "x.*")
    if current_gold is None:
        old = new.sparkSession.createDataFrame([], "master_key string, attribute string, old_value string")
    else:
        old = (
            current_gold.filter("is_current")
            .select(
                "master_key",
                F.explode(
                    F.array(
                        *[
                            F.struct(F.lit(a).alias("attribute"), F.col(a).cast("string").alias("old_value"))
                            for a in GOLDEN_ATTRIBUTES
                        ]
                    )
                ).alias("x"),
            )
            .select("master_key", "x.*")
        )
    joined = new.join(old, ["master_key", "attribute"], "left")
    changed = joined.filter(~F.col("new_value").eqNullSafe(F.col("old_value")))
    return changed.select(
        "master_key",
        "attribute",
        "old_value",
        "new_value",
        "rule",
        "winning_record_id",
        ts.alias("changed_at"),
        F.lit(run_id).alias("run_id"),
    )


def build_xref(members: DataFrame, links: DataFrame, now: datetime, previous: DataFrame | None) -> DataFrame:
    """Source record -> master key, with how the record was linked and since when."""
    linked_by = (
        links.select(F.col("left_id").alias("record_id"), "decided_by")
        .unionByName(links.select(F.col("right_id").alias("record_id"), "decided_by"))
        .groupBy("record_id")
        .agg(F.min("decided_by").alias("linked_by"))
    )
    xref = (
        members.select("record_id", "source_system", "source_record_id", "master_key")
        .join(linked_by, "record_id", "left")
        .withColumn("linked_by", F.coalesce(F.col("linked_by"), F.lit("singleton")))
    )
    ts = F.lit(now).cast("timestamp")
    if previous is None:
        return xref.withColumn("linked_at", ts)
    prev = previous.select(
        "record_id", F.col("master_key").alias("_prev_key"), F.col("linked_at").alias("_prev_at")
    )
    return (
        xref.join(prev, "record_id", "left")
        .withColumn(
            "linked_at", F.when(F.col("_prev_key") == F.col("master_key"), F.col("_prev_at")).otherwise(ts)
        )
        .drop("_prev_key", "_prev_at")
    )


def confidence_band_per_key(members: DataFrame, links: DataFrame) -> DataFrame:
    """Weakest band among the links that formed each golden record (SINGLE for singletons).

    A steward-approved link counts as HIGH: a person verified it.
    """
    rank = (
        F.when(F.col("decided_by") == "steward", 3)
        .when(F.col("band") == "LOW", 1)
        .when(F.col("band") == "MEDIUM", 2)
        .otherwise(3)
    )
    by_record = links.select(F.col("left_id").alias("record_id"), rank.alias("r")).unionByName(
        links.select(F.col("right_id").alias("record_id"), rank.alias("r"))
    )
    agg = (
        members.select("record_id", "master_key")
        .join(by_record, "record_id", "left")
        .groupBy("master_key")
        .agg(F.min("r").alias("r"))
    )
    return agg.select(
        "master_key",
        F.when(F.col("r").isNull(), F.lit("SINGLE"))
        .when(F.col("r") == 1, F.lit("LOW"))
        .when(F.col("r") == 2, F.lit("MEDIUM"))
        .otherwise(F.lit("HIGH"))
        .alias("confidence_band"),
    )
