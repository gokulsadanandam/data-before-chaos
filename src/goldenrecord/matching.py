"""Matching orchestration: blocking -> features -> model score -> AI tier -> bands.

Output is one row per candidate pair in ``slv_match_candidate`` with every feature, the
model score, the AI signal (when used), the final calibrated score, the band and a
plain-language explanation. Nothing is merged here; merging happens in resolution.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.ai import AIAdapter, ai_signal, blend
from goldenrecord.blocking import candidate_pairs
from goldenrecord.config import Settings
from goldenrecord.features import FEATURE_COLUMNS, attach_pair_attributes, compute_features
from goldenrecord.profiling import attach_identifier_labels
from goldenrecord.schema import BAND_LOW
from goldenrecord.scoring import apply_bands, explanation, score_pairs

CANDIDATE_COLUMNS: tuple[str, ...] = (
    "pair_id",
    "left_id",
    "right_id",
    "l_source_system",
    "r_source_system",
    "block_keys",
    *FEATURE_COLUMNS,
    "first_name_jw",
    "last_name_jw",
    "address_jaccard",
    "model_score",
    "ai_similarity",
    "ai_verdict",
    "ai_error",
    "final_score",
    "band",
    "decision",
    "spot_check",
    "rule_explanation",
    "ai_explanation",
)


def _with_null_ai(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ai_similarity", F.lit(None).cast("double"))
        .withColumn("ai_verdict", F.lit(None).cast("string"))
        .withColumn("ai_error", F.lit(None).cast("string"))
        .withColumn("final_score", F.col("model_score"))
    )


def match_records(
    silver: DataFrame,
    profile: DataFrame,
    settings: Settings,
    ai: AIAdapter,
    do_not_match: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Return (candidates, skipped_blocks)."""
    cfg = settings.matching
    blocking_cfg = settings.section("blocking")
    records = attach_identifier_labels(silver, profile)
    pairs, skipped = candidate_pairs(
        records,
        list(blocking_cfg.get("keys") or []),
        int(blocking_cfg.get("max_block_size", 200)),
        do_not_match,
    )
    features = compute_features(attach_pair_attributes(pairs, records), settings)
    scored = (
        score_pairs(features, cfg)
        .withColumn("rule_explanation", explanation(cfg.weights))
        .filter(F.col("model_score") >= cfg.min_candidate_score)
    )

    base_cols = [c for c in scored.columns]
    if cfg.ai_enabled:
        in_range = (F.col("model_score") >= cfg.ai_lower) & (F.col("model_score") < cfg.ai_upper)
        assessed = ai.assess(scored.filter(in_range), cfg)
        assessed = assessed.withColumn(
            "final_score", blend(F.col("model_score"), ai_signal(cfg), cfg.ai_blend_weight)
        ).select(*base_cols, "ai_similarity", "ai_verdict", "ai_error", "final_score")
        untouched = _with_null_ai(scored.filter(~in_range))
        combined = assessed.unionByName(untouched)
    else:
        combined = _with_null_ai(scored)

    banded = apply_bands(combined, cfg).cache()
    low = banded.filter(F.col("band") == BAND_LOW)
    explained = ai.explain(low).select(*banded.columns, "ai_explanation")
    others = banded.filter(F.col("band") != BAND_LOW).withColumn("ai_explanation", F.lit(None).cast("string"))
    candidates = explained.unionByName(others).select(*CANDIDATE_COLUMNS)
    return candidates, skipped
