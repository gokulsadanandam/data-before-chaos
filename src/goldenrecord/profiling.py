"""Identifier profiling: is an email / phone / loyalty id really unique to one person?

Uniqueness is measured, not assumed. Each identifier value is labelled by how many
different people (distinct first names, compared phonetically) carry it:

* ``unique``     one person            -> strong match evidence
* ``household``  2..N first names      -> weak evidence, never merges on its own
* ``shared``     more than N           -> ignored for matching (store default, info@...)

The labels feed blocking (shared values are not used as blocking keys) and the match
features (household agreement is down-weighted, see features.py).
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import Settings
from goldenrecord.features import canonical_first_name

IDENTIFIERS: tuple[str, ...] = ("email_token", "phone_token", "loyalty_id", "tax_id")

UNIQUE = "unique"
HOUSEHOLD = "household"
SHARED = "shared"


def profile_identifiers(silver: DataFrame, settings: Settings) -> DataFrame:
    section = settings.section("identifier_profile")
    household_max = int(section.get("household_max_first_names", 4))
    shared_min = int(section.get("shared_min_first_names", household_max + 1))
    # Soundex of the nickname-normalised first name, so "Priya" and "Priyaa" count as one person.
    first = F.soundex(canonical_first_name(F.col("first_name"), settings.reference.nicknames))

    parts: list[DataFrame] = []
    for ident in IDENTIFIERS:
        parts.append(
            silver.filter(F.col(ident).isNotNull())
            .groupBy(F.col(ident).alias("id_value"))
            .agg(
                F.count(F.lit(1)).alias("records"),
                F.countDistinct(first).alias("distinct_first_names"),
                F.countDistinct(F.lower(F.col("last_name"))).alias("distinct_last_names"),
                F.countDistinct("source_system").alias("distinct_sources"),
            )
            .withColumn("id_type", F.lit(ident))
        )
    profile = reduce(lambda a, b: a.unionByName(b), parts)
    names = F.col("distinct_first_names")
    label: Column = (
        F.when(names >= shared_min, F.lit(SHARED))
        .when((names >= 2) & (names <= household_max), F.lit(HOUSEHOLD))
        .when(names > household_max, F.lit(SHARED))
        .otherwise(F.lit(UNIQUE))
    )
    return profile.withColumn("label", label).select(
        "id_type",
        "id_value",
        "records",
        "distinct_first_names",
        "distinct_last_names",
        "distinct_sources",
        "label",
    )


def attach_identifier_labels(silver: DataFrame, profile: DataFrame) -> DataFrame:
    """Add ``<identifier>_label`` columns to Silver records."""
    out = silver
    for ident in IDENTIFIERS:
        labels = profile.filter(F.col("id_type") == ident).select(
            F.col("id_value").alias(ident), F.col("label").alias(f"{ident}_label")
        )
        out = out.join(labels, on=ident, how="left")
    return out
