"""Tier 3: Fabric AI functions for the ambiguous middle, behind a small adapter.

Only pairs whose model score falls in ``[ai.lower, ai.upper)`` reach AI. The AI never sees
raw contact details: each side is described with the minimum fields the decision needs
(name, city, postcode, email domain) plus *comparison facts* ("same phone: yes") instead
of identifier values.

Two adapters share one interface:

* :class:`FabricAIAdapter` calls ``df.ai.similarity`` / ``df.ai.classify`` /
  ``df.ai.generate_response`` (available on DataFrames inside Fabric notebooks).
* :class:`HeuristicAIAdapter` is a deterministic stand-in for local runs and tests.

If an AI call fails for a row, its error is recorded and the pair keeps its model score
(graceful degradation): AI can refine a decision, never block the pipeline.
"""

from __future__ import annotations

import contextlib
from typing import Any, Protocol, cast

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import MatchingConfig
from goldenrecord.features import jaro_winkler_udf

EXPLAIN_PROMPT = (
    "You help a data steward decide whether two customer records describe the same person. "
    "Record A: {l_desc}. Record B: {r_desc}. Evidence: {evidence}. "
    "In one sentence of at most 30 words, say whether they look like the same person and why. "
    "Do not invent details that are not given."
)


def side_description(prefix: str) -> Column:
    """Minimal, non-identifying description of one side of a pair."""
    p = prefix
    return F.concat_ws(
        "; ",
        F.concat(F.lit("first name: "), F.coalesce(F.col(f"{p}first_name"), F.lit("unknown"))),
        F.concat(F.lit("last name: "), F.coalesce(F.col(f"{p}last_name"), F.lit("unknown"))),
        F.concat(F.lit("city: "), F.coalesce(F.col(f"{p}city"), F.lit("unknown"))),
        F.concat(F.lit("postcode: "), F.coalesce(F.col(f"{p}postcode"), F.lit("unknown"))),
        F.concat(F.lit("email domain: "), F.coalesce(F.col(f"{p}email_domain"), F.lit("unknown"))),
        F.concat(F.lit("source: "), F.col(f"{p}source_system")),
    )


def comparison_facts() -> Column:
    def yes_no(cond: Column) -> Column:
        return F.when(cond, F.lit("yes")).otherwise(F.lit("no"))

    return F.concat_ws(
        "; ",
        F.concat(F.lit("same email: "), yes_no(F.col("email_exact") > 0)),
        F.concat(F.lit("same phone: "), yes_no(F.col("phone_exact") > 0)),
        F.concat(F.lit("same loyalty card: "), yes_no(F.col("loyalty_exact") > 0)),
    )


def prepare_ai_inputs(pairs: DataFrame) -> DataFrame:
    return (
        pairs.withColumn("l_desc", side_description("l_"))
        .withColumn("r_desc", side_description("r_"))
        .withColumn("evidence", comparison_facts())
        .withColumn(
            "pair_text",
            F.concat(
                F.lit("Record A: "),
                F.col("l_desc"),
                F.lit(". Record B: "),
                F.col("r_desc"),
                F.lit(". Facts: "),
                F.col("evidence"),
            ),
        )
    )


class AIAdapter(Protocol):
    def assess(self, pairs: DataFrame, cfg: MatchingConfig) -> DataFrame:
        """Add ``ai_similarity`` (-1..1), ``ai_verdict`` (label or NULL) and ``ai_error``."""
        ...

    def explain(self, pairs: DataFrame) -> DataFrame:
        """Add ``ai_explanation`` (one sentence) for the steward."""
        ...


def _ai(df: DataFrame) -> Any:
    """The ``df.ai`` accessor that Fabric adds to DataFrames (untyped, Fabric-only)."""
    return df.ai


class FabricAIAdapter:
    """Fabric AI functions (PySpark). Requires a Fabric notebook on F2+ capacity."""

    def __init__(self) -> None:
        # Current Fabric runtimes register ``df.ai`` on PySpark DataFrames automatically.
        # Older runtimes needed this explicit import; it is harmless where it is not needed.
        with contextlib.suppress(ImportError):
            import synapse.ml.spark.aifunc.DataFrameExtensions  # noqa: F401, PLC0415

    def assess(self, pairs: DataFrame, cfg: MatchingConfig) -> DataFrame:
        prepared = prepare_ai_inputs(pairs)
        with_sim = _ai(prepared).similarity(
            input_col="l_desc", other_col="r_desc", output_col="ai_similarity", error_col="ai_sim_error"
        )
        classified = _ai(with_sim).classify(
            labels=list(cfg.ai_labels),
            input_col="pair_text",
            output_col="ai_verdict",
            error_col="ai_cls_error",
        )
        return (
            cast(DataFrame, classified)
            .withColumn(
                "ai_error",
                F.coalesce(F.col("ai_sim_error").cast("string"), F.col("ai_cls_error").cast("string")),
            )
            .drop("ai_sim_error", "ai_cls_error")
        )

    def explain(self, pairs: DataFrame) -> DataFrame:
        prepared = prepare_ai_inputs(pairs)
        out = _ai(prepared).generate_response(
            prompt=EXPLAIN_PROMPT,
            is_prompt_template=True,
            output_col="ai_explanation",
            error_col="ai_explain_error",
        )
        return cast(DataFrame, out).drop("ai_explain_error")


class HeuristicAIAdapter:
    """Deterministic local stand-in that mimics the AI tier's behaviour."""

    def assess(self, pairs: DataFrame, cfg: MatchingConfig) -> DataFrame:
        first = jaro_winkler_udf(F.col("l_first_name"), F.col("r_first_name"))
        last = jaro_winkler_udf(F.col("l_last_name"), F.col("r_last_name"))
        name_sim = F.coalesce((first + last) / 2, last, F.lit(0.5))
        same_place = (F.col("postcode_eq") > 0) | (F.col("city_eq") > 0)
        similarity = name_sim * 2 - 1
        verdict = F.when((name_sim >= 0.88) & same_place, F.lit(cfg.ai_labels[0])).when(
            name_sim < 0.75, F.lit(cfg.ai_labels[1])
        )
        return (
            prepare_ai_inputs(pairs)
            .withColumn("ai_similarity", similarity)
            .withColumn("ai_verdict", verdict)
            .withColumn("ai_error", F.lit(None).cast("string"))
        )

    def explain(self, pairs: DataFrame) -> DataFrame:
        return pairs.withColumn(
            "ai_explanation",
            F.concat(
                F.lit("Heuristic: "), F.coalesce(F.col("rule_explanation"), F.lit("no strong evidence"))
            ),
        )


def ai_signal(cfg: MatchingConfig) -> Column:
    """Map AI outputs to a 0..1 signal: half semantic similarity, half the classifier verdict."""
    sim01 = (F.col("ai_similarity").cast("double") + 1) / 2
    verdict = F.when(F.col("ai_verdict") == cfg.ai_labels[0], F.lit(1.0)).when(
        F.col("ai_verdict") == cfg.ai_labels[1], F.lit(0.0)
    )
    return F.when(verdict.isNull(), sim01).otherwise(F.coalesce(0.5 * sim01 + 0.5 * verdict, verdict))


def blend(model_score: Column, signal: Column, weight: float) -> Column:
    return F.when(signal.isNull(), model_score).otherwise((1 - weight) * model_score + weight * signal)
