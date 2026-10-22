"""Survivorship: choose one winning value per attribute for each golden record.

Strategies are configured per attribute in config/survivorship.yaml. Each strategy is a
deterministic ordering over the cluster's non-null values, so the same inputs always
produce the same golden record. The winning record id is kept per attribute for the audit
trail ("which value won and why").
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import Settings, SurvivorshipRule
from goldenrecord.schema import CONSENT_NO, CONSENT_YES, GOLDEN_ATTRIBUTES

EPOCH_TEXT = "1970-01-01 00:00:00"


def _order_key(rule: SurvivorshipRule, attr: str) -> Column:
    """Sortable struct: larger = better. Ties always end on record_id for determinism."""
    updated = F.coalesce(F.col("source_updated_at"), F.lit(EPOCH_TEXT).cast("timestamp"))
    trust = F.col("source_trust")
    rid = F.col("record_id")
    strategy = rule.strategy
    if strategy == "source_of_record":
        strategy = rule.fallback or "most_recent"
    if strategy == "most_trusted":
        return F.struct(trust, updated, rid)
    if strategy == "most_recent":
        return F.struct(updated, trust, rid)
    if strategy == "longest_complete":
        return F.struct(trust, F.length(F.col(attr)), updated, rid)
    if strategy == "most_frequent":
        return F.struct(F.col(f"_{attr}_freq"), trust, updated, rid)
    raise ValueError(f"No ordering for strategy {rule.strategy!r}")


def most_restrictive(values: Column) -> Column:
    """Any explicit 'no' wins; 'yes' only if no 'no'; no explicit value -> 'no'."""
    return (
        F.when(F.array_contains(values, CONSENT_NO), F.lit(CONSENT_NO))
        .when(F.array_contains(values, CONSENT_YES), F.lit(CONSENT_YES))
        .otherwise(F.lit(CONSENT_NO))
    )


def build_golden(records: DataFrame, settings: Settings) -> DataFrame:
    """``records`` = Silver rows with ``master_key``. Returns one row per master_key."""
    rules = {r.attribute: r for r in settings.survivorship}
    prepared = records
    for attr, rule in rules.items():
        if rule.strategy == "most_frequent":
            freq = (
                records.filter(F.col(attr).isNotNull())
                .groupBy("master_key", attr)
                .agg(F.count(F.lit(1)).alias(f"_{attr}_freq"))
            )
            prepared = prepared.join(freq, ["master_key", attr], "left")

    aggs: list[Column] = [
        F.count(F.lit(1)).alias("source_record_count"),
        F.sort_array(F.collect_set("source_system")).alias("source_systems"),
    ]
    for attr in GOLDEN_ATTRIBUTES:
        rule = rules[attr]
        if rule.strategy == "most_restrictive":
            aggs.append(most_restrictive(F.collect_list(F.col(attr))).alias(attr))
            aggs.append(F.lit(None).cast("string").alias(f"{attr}__winner"))
            continue
        present = F.col(attr).isNotNull()
        if rule.strategy == "source_of_record" and rule.source:
            preferred = present & (F.col("source_system") == rule.source)
            key = F.struct(preferred.cast("int"), _order_key(rule, attr))
        else:
            key = _order_key(rule, attr)
        value_key = F.when(
            present, F.struct(key.alias("k"), F.col(attr).alias("v"), F.col("record_id").alias("r"))
        )
        aggs.append(F.max(value_key).alias(f"_{attr}_best"))
    grouped = prepared.groupBy("master_key").agg(*aggs)
    out = grouped
    for attr in GOLDEN_ATTRIBUTES:
        if rules[attr].strategy == "most_restrictive":
            continue
        out = (
            out.withColumn(attr, F.col(f"_{attr}_best.v"))
            .withColumn(f"{attr}__winner", F.col(f"_{attr}_best.r"))
            .drop(f"_{attr}_best")
        )
    return out


def winners_map(golden: DataFrame) -> Column:
    """Map attribute -> winning record id (for the audit log)."""
    items: list[Column] = []
    for attr in GOLDEN_ATTRIBUTES:
        items.extend([F.lit(attr), F.col(f"{attr}__winner")])
    return F.create_map(*items)
