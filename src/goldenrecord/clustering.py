"""Group linked records into one customer each (connected components).

Approved links form a graph: A-B and B-C put A, B and C in one group. Components are
found with iterative minimum-label propagation in plain PySpark (no GraphFrames jar needed).

Chaining guards: a component is *held* (not promoted) when it is larger than
``clustering.max_cluster_size`` or contains two different tax IDs. Held components are
reported in ``slv_cluster_issue`` for a steward.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.logging_utils import get_logger, kv

log = get_logger("clustering")


def connected_components(vertices: DataFrame, edges: DataFrame, max_iterations: int = 20) -> DataFrame:
    """Return (record_id, cluster_id) where cluster_id is the smallest record_id in the component.

    ``vertices`` needs a ``record_id`` column; ``edges`` needs ``left_id`` and ``right_id``.
    """
    undirected = (
        edges.select(F.col("left_id").alias("src"), F.col("right_id").alias("dst"))
        .unionByName(edges.select(F.col("right_id").alias("src"), F.col("left_id").alias("dst")))
        .distinct()
        .cache()
    )
    labels = vertices.select("record_id").distinct().withColumn("cluster_id", F.col("record_id")).cache()
    for iteration in range(1, max_iterations + 1):
        neighbour_min = (
            undirected.join(labels.withColumnRenamed("record_id", "src"), "src")
            .groupBy("dst")
            .agg(F.min("cluster_id").alias("neighbour_min"))
            .withColumnRenamed("dst", "record_id")
        )
        updated = (
            labels.join(neighbour_min, "record_id", "left")
            .select(
                "record_id",
                F.least(F.col("cluster_id"), F.coalesce(F.col("neighbour_min"), F.col("cluster_id"))).alias(
                    "cluster_id"
                ),
            )
            .localCheckpoint()
        )
        changed = (
            updated.join(labels.withColumnRenamed("cluster_id", "old"), "record_id")
            .filter(F.col("cluster_id") != F.col("old"))
            .count()
        )
        labels.unpersist()
        labels = updated
        log.info(kv("cc_iteration", iteration=iteration, changed=changed))
        if changed == 0:
            break
    else:
        log.warning(kv("cc_not_converged", max_iterations=max_iterations))
    undirected.unpersist()
    return labels


def cluster_guards(clusters: DataFrame, silver: DataFrame, max_cluster_size: int) -> DataFrame:
    """Return (cluster_id, cluster_size, distinct_tax_ids, held, hold_reason)."""
    stats = (
        clusters.join(silver.select("record_id", "tax_id"), "record_id", "left")
        .groupBy("cluster_id")
        .agg(F.count(F.lit(1)).alias("cluster_size"), F.countDistinct("tax_id").alias("distinct_tax_ids"))
    )
    reason = F.when(F.col("cluster_size") > max_cluster_size, F.lit("cluster_too_large")).when(
        F.col("distinct_tax_ids") > 1, F.lit("conflicting_tax_ids")
    )
    return stats.withColumn("hold_reason", reason).withColumn("held", reason.isNotNull())
