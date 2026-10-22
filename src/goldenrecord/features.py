"""Pairwise comparison features (Tier 2).

Each candidate pair is compared field by field with the algorithm suited to the field:

==================  =======================================================
email, phone        exact match on tokens, weighted by identifier label;
                    both present and different = conflict
loyalty id, tax id  exact match; a tax id mismatch is a hard conflict
first name          Jaro-Winkler on nickname-normalised names (initials: first letter)
last name           Jaro-Winkler, plus Soundex phonetic equality
address             token Jaccard after abbreviation normalisation
postcode, city      equality
email local part    1 - Levenshtein / max length
==================  =======================================================

Similarity features are centred to [-1, 1] (agreement positive, disagreement negative,
missing = 0 = no evidence) so the logistic weights in config read naturally.
"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import DoubleType

from goldenrecord.config import Settings

ADDRESS_ABBREVIATIONS: dict[str, str] = {
    "road": "rd",
    "street": "st",
    "avenue": "ave",
    "nagar": "ngr",
    "main": "mn",
    "cross": "crs",
    "layout": "lyt",
    "apartment": "apt",
    "apartments": "apt",
    "flat": "apt",
    "number": "no",
    "near": "nr",
    "opposite": "opp",
    "sector": "sec",
    "phase": "ph",
}

PAIR_ATTRIBUTES: tuple[str, ...] = (
    "source_system",
    "first_name",
    "last_name",
    "email",
    "email_domain",
    "email_token",
    "phone_token",
    "address_line",
    "city",
    "postcode",
    "tax_id",
    "loyalty_id",
    "email_token_label",
    "phone_token_label",
    "loyalty_id_label",
)


# ----------------------------------------------------------------------------- Jaro-Winkler
def jaro_winkler(s1: str | None, s2: str | None, prefix_scale: float = 0.1) -> float | None:
    """Jaro-Winkler similarity in [0, 1]; ``None`` when either side is missing."""
    if not s1 or not s2:
        return None
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    match_distance = max(max(len1, len2) // 2 - 1, 0)
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    for i, ch in enumerate(s1):
        start, end = max(0, i - match_distance), min(i + match_distance + 1, len2)
        for j in range(start, end):
            if not s2_matches[j] and s2[j] == ch:
                s1_matches[i] = s2_matches[j] = True
                matches += 1
                break
    if matches == 0:
        return 0.0
    transpositions = 0
    k = 0
    for i in range(len1):
        if s1_matches[i]:
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1
    m = float(matches)
    jaro = (m / len1 + m / len2 + (m - transpositions / 2) / m) / 3
    prefix = 0
    for a, b in zip(s1[:4], s2[:4], strict=False):
        if a != b:
            break
        prefix += 1
    return jaro + prefix * prefix_scale * (1 - jaro)


def _jaro_winkler_series(left: pd.Series, right: pd.Series) -> pd.Series:
    return pd.Series(
        [
            jaro_winkler(a.lower(), b.lower()) if isinstance(a, str) and isinstance(b, str) else None
            for a, b in zip(left, right, strict=True)
        ],
        dtype="float64",
    )


# Vectorised (Arrow) UDF: Jaro-Winkler is not a Spark built-in. Called as jaro_winkler_udf(col_a, col_b).
jaro_winkler_udf = pandas_udf(_jaro_winkler_series, DoubleType())  # type: ignore[call-overload]


# ----------------------------------------------------------------------------- normalisers
def canonical_first_name(col: Column, nicknames: Mapping[str, str]) -> Column:
    lowered = F.lower(F.trim(col))
    if not nicknames:
        return lowered
    items: list[Column] = []
    for key, value in sorted(nicknames.items()):
        items.extend([F.lit(key), F.lit(value)])
    return F.coalesce(F.try_element_at(F.create_map(*items), lowered), lowered)


def address_tokens(col: Column) -> Column:
    cleaned = F.regexp_replace(F.lower(col), r"[^a-z0-9\s]", " ")
    tokens = F.split(F.trim(F.regexp_replace(cleaned, r"\s+", " ")), " ")
    mapped = F.transform(
        tokens,
        lambda t: F.coalesce(
            F.try_element_at(
                F.create_map(*[F.lit(x) for kv in sorted(ADDRESS_ABBREVIATIONS.items()) for x in kv]), t
            ),
            t,
        ),
    )
    return F.when(col.isNotNull(), F.array_distinct(F.filter(mapped, lambda t: F.length(t) > 0)))


NAME_PIVOT = 0.75


def centred(sim: Column) -> Column:
    """Map a [0, 1] similarity to [-1, 1]; missing stays 0 (no evidence)."""
    return F.coalesce(sim * 2 - 1, F.lit(0.0))


def centred_name(jw: Column, pivot: float = NAME_PIVOT) -> Column:
    """Map a Jaro-Winkler name similarity to [-1, 1] around ``pivot``.

    Unrelated names still score 0.4-0.6 on Jaro-Winkler, so centring at 0.5 would treat
    "Riya" vs "Amit" as neutral. Centring at 0.75 makes clearly different names count
    against a match, while typos and nicknames (0.9+) still count for it.
    """
    scaled = (jw - F.lit(pivot)) / F.lit(1.0 - pivot)
    return F.coalesce(F.greatest(F.lit(-1.0), F.least(F.lit(1.0), scaled)), F.lit(0.0))


def tri_state_equal(left: Column, right: Column) -> Column:
    """1 when equal, -1 when both present and different, 0 when either is missing."""
    return (
        F.when(left.isNull() | right.isNull(), F.lit(0.0))
        .when(left == right, F.lit(1.0))
        .otherwise(F.lit(-1.0))
    )


def identifier_agreement(left: Column, right: Column, label: Column) -> Column:
    """Exact identifier match scaled by how unique the identifier is."""
    weight = (
        F.when(label == "unique", F.lit(1.0)).when(label == "household", F.lit(0.5)).otherwise(F.lit(0.0))
    )
    return F.when(left.isNotNull() & (left == right), weight).otherwise(F.lit(0.0))


# ----------------------------------------------------------------------------- pair features
def attach_pair_attributes(pairs: DataFrame, records: DataFrame) -> DataFrame:
    """Join left (``l_*``) and right (``r_*``) record attributes onto (left_id, right_id) pairs."""
    cols = [c for c in PAIR_ATTRIBUTES if c in records.columns]
    left = records.select(F.col("record_id").alias("left_id"), *[F.col(c).alias(f"l_{c}") for c in cols])
    right = records.select(F.col("record_id").alias("right_id"), *[F.col(c).alias(f"r_{c}") for c in cols])
    return pairs.join(left, "left_id").join(right, "right_id")


def compute_features(pairs: DataFrame, settings: Settings) -> DataFrame:
    """Add feature columns to a pairs DataFrame produced by :func:`attach_pair_attributes`."""
    nick = settings.reference.nicknames
    lf, rf = (
        canonical_first_name(F.col("l_first_name"), nick),
        canonical_first_name(F.col("r_first_name"), nick),
    )
    # An initial ("K") only tells us the first letter: consistent initial = 0.9, otherwise 0.
    initial_only = (F.length(lf) == 1) | (F.length(rf) == 1)
    first_sim = F.when(
        initial_only,
        F.when(F.lower(F.substring(lf, 1, 1)) == F.lower(F.substring(rf, 1, 1)), 0.9).otherwise(0.0),
    ).otherwise(jaro_winkler_udf(lf, rf))
    last_sim = jaro_winkler_udf(F.lower(F.col("l_last_name")), F.lower(F.col("r_last_name")))
    la, ra = address_tokens(F.col("l_address_line")), address_tokens(F.col("r_address_line"))
    jaccard = F.when(
        la.isNotNull() & ra.isNotNull() & (F.size(F.array_union(la, ra)) > 0),
        F.size(F.array_intersect(la, ra)) / F.size(F.array_union(la, ra)),
    )
    l_local = F.substring_index(F.col("l_email"), "@", 1)
    r_local = F.substring_index(F.col("r_email"), "@", 1)
    local_sim = F.when(
        l_local.isNotNull() & r_local.isNotNull(),
        1 - F.levenshtein(l_local, r_local) / F.greatest(F.length(l_local), F.length(r_local)),
    )
    email_agree = identifier_agreement(
        F.col("l_email_token"), F.col("r_email_token"), F.col("l_email_token_label")
    )
    phone_agree = identifier_agreement(
        F.col("l_phone_token"), F.col("r_phone_token"), F.col("l_phone_token_label")
    )

    with_sims = (
        pairs.withColumn("first_name_jw", first_sim)
        .withColumn("last_name_jw", last_sim)
        .withColumn("address_jaccard", jaccard)
        .withColumn("email_local_ratio", local_sim)
    )
    strong_shared = (email_agree > 0) | (phone_agree > 0)

    def conflict(left: str, right: str) -> Column:
        """Both records carry the identifier and the values differ."""
        return F.when(
            F.col(left).isNotNull() & F.col(right).isNotNull() & (F.col(left) != F.col(right)), 1.0
        ).otherwise(0.0)

    return (
        with_sims.withColumn("email_exact", email_agree)
        .withColumn("phone_exact", phone_agree)
        .withColumn(
            "loyalty_exact",
            identifier_agreement(F.col("l_loyalty_id"), F.col("r_loyalty_id"), F.col("l_loyalty_id_label")),
        )
        .withColumn(
            "tax_exact",
            F.when(F.col("l_tax_id").isNotNull() & (F.col("l_tax_id") == F.col("r_tax_id")), 1.0).otherwise(
                0.0
            ),
        )
        .withColumn(
            "tax_conflict",
            F.when(
                F.col("l_tax_id").isNotNull()
                & F.col("r_tax_id").isNotNull()
                & (F.col("l_tax_id") != F.col("r_tax_id")),
                1.0,
            ).otherwise(0.0),
        )
        .withColumn("first_name_sim", centred_name(F.col("first_name_jw")))
        .withColumn("last_name_sim", centred_name(F.col("last_name_jw")))
        .withColumn(
            "surname_phonetic_eq",
            tri_state_equal(F.soundex(F.col("l_last_name")), F.soundex(F.col("r_last_name"))),
        )
        .withColumn("address_sim", centred(F.col("address_jaccard")))
        .withColumn("postcode_eq", tri_state_equal(F.col("l_postcode"), F.col("r_postcode")))
        .withColumn("city_eq", tri_state_equal(F.lower(F.col("l_city")), F.lower(F.col("r_city"))))
        .withColumn("email_local_sim", centred(F.col("email_local_ratio")))
        .withColumn("email_conflict", conflict("l_email_token", "r_email_token"))
        .withColumn("phone_conflict", conflict("l_phone_token", "r_phone_token"))
        .withColumn(
            "household_conflict",
            F.when(strong_shared & (F.col("first_name_jw") < HOUSEHOLD_FIRST_NAME_JW), 1.0).otherwise(0.0),
        )
    )


# Below this first-name similarity, a shared email / phone points to a household, not one person.
# Jaro-Winkler is generous to short names that share a first letter (Sneha / Suresh = 0.73).
HOUSEHOLD_FIRST_NAME_JW = 0.85


FEATURE_COLUMNS: tuple[str, ...] = (
    "email_exact",
    "phone_exact",
    "loyalty_exact",
    "tax_exact",
    "first_name_sim",
    "last_name_sim",
    "surname_phonetic_eq",
    "address_sim",
    "postcode_eq",
    "city_eq",
    "email_local_sim",
    "email_conflict",
    "phone_conflict",
    "household_conflict",
    "tax_conflict",
)

# Features that count as a shared identifier (auto-merge needs at least one, see scoring.py).
IDENTIFIER_FEATURES: tuple[str, ...] = ("email_exact", "phone_exact", "loyalty_exact", "tax_exact")
