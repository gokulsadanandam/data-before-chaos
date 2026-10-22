"""Stable master keys: issued once, never changed, never reused.

Rules (docs/adr/0004-master-key-lifecycle.md):

* A cluster whose records already carry one key keeps it.
* A cluster that joins two or more existing keys keeps the **oldest** (lowest sequence);
  the others are retired with ``replaced_by`` pointing at the survivor.
* If a key is claimed by two clusters (a steward split a wrong merge), the cluster holding
  most of that key's records keeps it; the other cluster gets a new key.
* A new cluster gets the next sequence number. Retired keys are never issued again,
  because the sequence only grows.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812

REGISTRY_COLUMNS = ("master_key", "seq", "status", "issued_at", "retired_at", "replaced_by")


def format_key(seq_col: Column, prefix: str, digits: int) -> Column:
    return F.concat(F.lit(prefix), F.lpad(seq_col.cast("string"), digits, "0"))


def empty_registry(spark_df: DataFrame) -> DataFrame:
    return spark_df.sparkSession.createDataFrame(
        [],
        "master_key string, seq long, status string, issued_at timestamp, retired_at timestamp, "
        "replaced_by string",
    )


def assign_master_keys(
    clusters: DataFrame,
    previous_xref: DataFrame | None,
    registry: DataFrame | None,
    prefix: str,
    digits: int,
    now: datetime,
) -> tuple[DataFrame, DataFrame]:
    """Return (assignments[cluster_id, master_key], new_registry)."""
    reg = registry if registry is not None else empty_registry(clusters)
    active = reg.filter(F.col("status") == "active").select("master_key", "seq")

    if previous_xref is not None:
        claims = (
            clusters.join(previous_xref.select("record_id", "master_key"), "record_id")
            .join(active, "master_key")
            .groupBy("cluster_id", "master_key", "seq")
            .agg(F.count(F.lit(1)).alias("n_records"))
        )
    else:
        claims = clusters.sparkSession.createDataFrame(
            [], "cluster_id string, master_key string, seq long, n_records long"
        )

    # A key claimed by several clusters (split) goes to the cluster holding most of its records.
    key_winner = Window.partitionBy("master_key").orderBy(F.col("n_records").desc(), F.col("cluster_id"))
    valid = claims.withColumn("_rk", F.row_number().over(key_winner)).filter("_rk = 1").drop("_rk")

    # A cluster holding several keys (merge) keeps the oldest; the rest are retired.
    oldest = Window.partitionBy("cluster_id").orderBy(F.col("seq"))
    ranked = valid.withColumn("_rk", F.row_number().over(oldest)).cache()
    kept = ranked.filter("_rk = 1").select("cluster_id", "master_key", "seq")
    merged_away = (
        ranked.filter("_rk > 1")
        .select(F.col("master_key").alias("retired_key"), F.col("cluster_id"))
        .join(kept.select("cluster_id", F.col("master_key").alias("survivor")), "cluster_id")
    )

    max_seq_row = reg.agg(F.max("seq").alias("m")).collect()[0]
    max_seq = int(max_seq_row["m"]) if max_seq_row["m"] is not None else 0
    needing = (
        clusters.select("cluster_id").distinct().join(kept.select("cluster_id"), "cluster_id", "left_anti")
    )
    new_keys = needing.withColumn(
        "seq", F.row_number().over(Window.orderBy("cluster_id")).cast("long") + F.lit(max_seq)
    ).withColumn("master_key", format_key(F.col("seq"), prefix, digits))

    assignments = kept.select("cluster_id", "master_key").unionByName(
        new_keys.select("cluster_id", "master_key")
    )

    ts = F.lit(now).cast("timestamp")
    assigned_keys = assignments.select("master_key").distinct()
    # Registry: keep history, add new keys, retire merged-away keys and keys with no records left.
    still_active = reg.filter(F.col("status") == "active").join(assigned_keys, "master_key", "left_semi")
    to_retire = (
        reg.filter(F.col("status") == "active")
        .join(assigned_keys, "master_key", "left_anti")
        .join(merged_away.select(F.col("retired_key").alias("master_key"), "survivor"), "master_key", "left")
        .select(
            "master_key",
            "seq",
            F.lit("retired").alias("status"),
            "issued_at",
            ts.alias("retired_at"),
            F.col("survivor").alias("replaced_by"),
        )
    )
    already_retired = reg.filter(F.col("status") != "active")
    issued = new_keys.select(
        "master_key",
        "seq",
        F.lit("active").alias("status"),
        ts.alias("issued_at"),
        F.lit(None).cast("timestamp").alias("retired_at"),
        F.lit(None).cast("string").alias("replaced_by"),
    )
    new_registry = (
        still_active.select(*REGISTRY_COLUMNS)
        .unionByName(to_retire.select(*REGISTRY_COLUMNS))
        .unionByName(already_retired.select(*REGISTRY_COLUMNS))
        .unionByName(issued.select(*REGISTRY_COLUMNS))
    )
    return assignments, new_registry
