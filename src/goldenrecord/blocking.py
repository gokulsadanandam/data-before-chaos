"""Blocking: decide which record pairs are worth comparing.

Comparing every record with every other is O(n^2) (1.25 billion pairs at 50,000 records).
Records are compared only when they share at least one blocking key. Several keys are
used so a typo in one field does not hide a true match. Oversized blocks (placeholder
values, store default numbers) are skipped and reported, which also stops chaining.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.profiling import SHARED

KEY_BUILDERS: dict[str, tuple[str, ...]] = {
    "email_token": ("email_token",),
    "phone_token": ("phone_token",),
    "loyalty_id": ("loyalty_id",),
    "tax_id": ("tax_id",),
    "surname_soundex_postcode": ("last_name", "postcode"),
    "email_domain_first_initial_city": ("email_domain", "first_name", "city"),
}


def _key_expr(name: str) -> Column:
    if name in {"email_token", "phone_token", "loyalty_id"}:
        label = F.col(f"{name}_label")
        return F.when(F.col(name).isNotNull() & ((label != SHARED) | label.isNull()), F.col(name))
    if name == "tax_id":
        return F.col("tax_id")
    if name == "surname_soundex_postcode":
        return F.when(
            F.col("last_name").isNotNull() & F.col("postcode").isNotNull(),
            F.concat_ws("|", F.soundex(F.col("last_name")), F.col("postcode")),
        )
    if name == "email_domain_first_initial_city":
        return F.when(
            F.col("email_domain").isNotNull() & F.col("first_name").isNotNull() & F.col("city").isNotNull(),
            F.concat_ws(
                "|",
                F.col("email_domain"),
                F.lower(F.substring(F.col("first_name"), 1, 1)),
                F.lower(F.col("city")),
            ),
        )
    raise ValueError(f"Unknown blocking key '{name}'")


def block_keys(records: DataFrame, key_names: list[str]) -> DataFrame:
    """Return (record_id, block_key) rows; block_key is prefixed with the key type."""
    parts = [
        records.select(
            F.col("record_id"),
            F.concat_ws(":", F.lit(name), _key_expr(name)).alias("block_key"),
            _key_expr(name).alias("_v"),
        ).filter(F.col("_v").isNotNull())
        for name in key_names
    ]
    return reduce(lambda a, b: a.unionByName(b), parts).drop("_v").distinct()


def candidate_pairs(
    records: DataFrame, key_names: list[str], max_block_size: int, do_not_match: DataFrame | None = None
) -> tuple[DataFrame, DataFrame]:
    """Return (pairs, skipped_blocks).

    ``pairs`` has one row per unordered pair (left_id < right_id) with the keys that
    brought it together; ``skipped_blocks`` lists oversized blocks for the run report.
    """
    keys = block_keys(records, key_names)
    sizes = keys.groupBy("block_key").agg(F.count(F.lit(1)).alias("block_size"))
    usable = sizes.filter((F.col("block_size") > 1) & (F.col("block_size") <= max_block_size)).select(
        "block_key"
    )
    skipped = sizes.filter(F.col("block_size") > max_block_size)
    kept = keys.join(usable, "block_key")
    left = kept.select(F.col("block_key"), F.col("record_id").alias("left_id"))
    right = kept.select(F.col("block_key"), F.col("record_id").alias("right_id"))
    pairs = (
        left.join(right, "block_key")
        .filter(F.col("left_id") < F.col("right_id"))
        .groupBy("left_id", "right_id")
        # Store only the key *types* (e.g. "phone_token"), never key values, which can hold identifiers.
        .agg(F.sort_array(F.collect_set(F.substring_index("block_key", ":", 1))).alias("block_keys"))
    )
    if do_not_match is not None:
        pairs = pairs.join(do_not_match.select("left_id", "right_id"), ["left_id", "right_id"], "left_anti")
    pairs = pairs.withColumn(
        "pair_id", F.substring(F.sha2(F.concat_ws("||", "left_id", "right_id"), 256), 1, 24)
    )
    return pairs, skipped
