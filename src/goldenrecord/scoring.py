"""Match scoring, confidence bands and plain-language explanations.

``score = sigmoid(intercept + sum(weight_i * feature_i))``. Day 1 weights are expert-set
in config/pipeline.yaml; :mod:`goldenrecord.training` refits them on steward labels so the
score is calibrated (a HIGH band of 0.90+ should mean about 95% precision).

Confidence is never the language model's own word: when AI functions run (Tier 3), their
signal is blended into this score with a configured weight.
"""

from __future__ import annotations

import math
from collections.abc import Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import MatchingConfig
from goldenrecord.features import FEATURE_COLUMNS, IDENTIFIER_FEATURES
from goldenrecord.schema import (
    BAND_HIGH,
    BAND_LOW,
    BAND_MEDIUM,
    DECISION_MATCH,
    DECISION_REVIEW,
)

FEATURE_LABELS: dict[str, tuple[str, str]] = {
    # feature: (text when it supports a match, text when it argues against)
    "email_exact": ("same email", ""),
    "phone_exact": ("same phone", ""),
    "loyalty_exact": ("same loyalty card", ""),
    "tax_exact": ("same tax ID", ""),
    "first_name_sim": ("first names match", "first names differ"),
    "last_name_sim": ("last names match", "last names differ"),
    "surname_phonetic_eq": ("surnames sound alike", "surnames sound different"),
    "address_sim": ("addresses match", "addresses differ"),
    "postcode_eq": ("same postcode", "different postcode"),
    "city_eq": ("same city", "different city"),
    "email_local_sim": ("similar email names", "different email names"),
    "email_conflict": ("", "different emails"),
    "phone_conflict": ("", "different phones"),
    "household_conflict": ("", "shared contact but different first name (possible household)"),
    "tax_conflict": ("", "different tax IDs"),
}


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def linear_predictor(intercept: float, weights: Mapping[str, float]) -> Column:
    expr: Column = F.lit(float(intercept))
    for feature in FEATURE_COLUMNS:
        weight = float(weights.get(feature, 0.0))
        if weight:
            expr = expr + F.lit(weight) * F.coalesce(F.col(feature), F.lit(0.0))
    return expr


def score_pairs(features: DataFrame, cfg: MatchingConfig) -> DataFrame:
    lp = linear_predictor(cfg.intercept, cfg.weights)
    return features.withColumn("model_score", 1 / (1 + F.exp(-lp)))


def band(score: Column, cfg: MatchingConfig) -> Column:
    return (
        F.when(score >= cfg.band_high, F.lit(BAND_HIGH))
        .when(score >= cfg.band_medium, F.lit(BAND_MEDIUM))
        .otherwise(F.lit(BAND_LOW))
    )


def decision(band_col: Column) -> Column:
    return F.when(band_col.isin(BAND_HIGH, BAND_MEDIUM), F.lit(DECISION_MATCH)).otherwise(
        F.lit(DECISION_REVIEW)
    )


def spot_check(pair_id: Column, band_col: Column, rate: float) -> Column:
    """Deterministic sample of MEDIUM-band auto-merges for steward spot checks."""
    bucket = F.abs(F.xxhash64(pair_id)) % 1000
    return (band_col == BAND_MEDIUM) & (bucket < int(rate * 1000))


def explanation(weights: Mapping[str, float], top_n: int = 4) -> Column:
    """Rule-based explanation listing the strongest evidence for and against a match."""
    contributions = [
        F.struct(
            (F.lit(float(weights.get(f, 0.0))) * F.coalesce(F.col(f), F.lit(0.0))).alias("w"),
            F.lit(f).alias("feature"),
        )
        for f in FEATURE_COLUMNS
        if weights.get(f, 0.0)
    ]
    arr = F.array(*contributions)
    positives = F.slice(F.sort_array(F.filter(arr, lambda c: c["w"] > 0.3), asc=False), 1, top_n)
    negatives = F.slice(F.sort_array(F.filter(arr, lambda c: c["w"] < -0.3), asc=True), 1, top_n)

    def to_text(items: Column, idx: int) -> Column:
        labels = F.create_map(*[F.lit(x) for f, pair in FEATURE_LABELS.items() for x in (f, pair[idx])])
        return F.concat_ws(
            ", ",
            F.filter(F.transform(items, lambda c: F.try_element_at(labels, c["feature"])), lambda t: t != ""),
        )

    pos_text, neg_text = to_text(positives, 0), to_text(negatives, 1)
    return F.concat_ws(
        "; ",
        F.when(F.length(pos_text) > 0, F.concat(F.lit("For: "), pos_text)),
        F.when(F.length(neg_text) > 0, F.concat(F.lit("Against: "), neg_text)),
    )


NO_IDENTIFIER_NOTE = "Review: no shared email, phone, loyalty card or tax ID"


def has_shared_identifier() -> Column:
    return F.greatest(*[F.coalesce(F.col(f), F.lit(0.0)) for f in IDENTIFIER_FEATURES]) > 0


def apply_bands(scored: DataFrame, cfg: MatchingConfig, score_col: str = "final_score") -> DataFrame:
    """Assign bands and decisions.

    With ``auto_merge_requires_identifier`` (the default) a pair that agrees on name and
    location only is never merged automatically: it goes to review (LOW band), because
    two different people can share a common name and a postcode.
    """
    b = band(F.col(score_col), cfg)
    if cfg.auto_merge_requires_identifier:
        capped = (b != BAND_LOW) & ~has_shared_identifier()
        b = F.when(capped, F.lit(BAND_LOW)).otherwise(b)
        scored = scored.withColumn(
            "rule_explanation",
            F.when(capped, F.concat_ws("; ", F.col("rule_explanation"), F.lit(NO_IDENTIFIER_NOTE))).otherwise(
                F.col("rule_explanation")
            ),
        )
    return (
        scored.withColumn("band", b)
        .withColumn("decision", decision(F.col("band")))
        .withColumn("spot_check", spot_check(F.col("pair_id"), F.col("band"), cfg.spot_check_rate))
    )


__all__ = [
    "BAND_LOW",
    "apply_bands",
    "band",
    "explanation",
    "has_shared_identifier",
    "linear_predictor",
    "score_pairs",
    "sigmoid",
]
