"""Human review: the outbox of pairs for stewards and the decisions coming back.

Flow (see docs/architecture.md, "write-back"):

1. ``build_review_outbox`` writes LOW-band pairs (and MEDIUM spot checks) to a Lakehouse
   outbox table. The pipeline copies it into the Fabric SQL database ``review.review_queue``.
2. A steward decides on the Power BI page; the user data function writes
   ``review.decision_log`` and ``review.approval`` in one transaction.
3. The SQL database mirrors into OneLake; a Lakehouse shortcut exposes the decision log
   to Spark as ``rvw_decision_log``.
4. ``resolve_links`` turns auto-matches plus approvals into match links, and rejects into
   the do-not-match list. HOLD keeps a pair out of Gold until it is decided.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.schema import (
    BAND_LOW,
    BAND_MEDIUM,
    DECIDED_BY_MODEL,
    DECIDED_BY_STEWARD,
    DECISION_MATCH,
    STEWARD_APPROVE,
    STEWARD_HOLD,
    STEWARD_REJECT,
)

DECISION_LOG_COLUMNS = ("pair_id", "decision", "reason", "decided_by", "decided_at")


def mask_email(col: Column) -> Column:
    """``priya.sharma@gmail.com`` -> ``p***@gmail.com``."""
    return F.when(
        col.isNotNull(), F.concat(F.substring(col, 1, 1), F.lit("***@"), F.substring_index(col, "@", -1))
    )


def mask_phone(col: Column) -> Column:
    """``+919876543210`` -> ``+91******3210``."""
    return F.when(
        col.isNotNull(),
        F.concat(F.substring(col, 1, 3), F.lit("******"), F.substring(col, -4, 4)),
    )


def latest_decisions(decision_log: DataFrame) -> DataFrame:
    window = Window.partitionBy("pair_id").orderBy(F.col("decided_at").desc())
    return (
        decision_log.select(*DECISION_LOG_COLUMNS)
        .withColumn("_rn", F.row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("decision", F.upper(F.col("decision")))
    )


def empty_decisions(spark_df: DataFrame) -> DataFrame:
    """An empty decision log with the right schema (first run, before any steward input)."""
    return spark_df.sparkSession.createDataFrame(
        [], "pair_id string, decision string, reason string, decided_by string, decided_at timestamp"
    )


def build_review_outbox(candidates: DataFrame, silver: DataFrame, decisions: DataFrame) -> DataFrame:
    """Pairs that need a steward: LOW band and MEDIUM spot checks not yet finally decided."""
    final = latest_decisions(decisions).filter(F.col("decision").isin(STEWARD_APPROVE, STEWARD_REJECT))
    needs_review = candidates.filter(
        (F.col("band") == BAND_LOW) | ((F.col("band") == BAND_MEDIUM) & F.col("spot_check"))
    ).join(final.select("pair_id"), "pair_id", "left_anti")

    def side(prefix: str, key: str) -> DataFrame:
        return silver.select(
            F.col("record_id").alias(key),
            F.col("source_system").alias(f"{prefix}_source"),
            F.concat_ws(" ", "first_name", "last_name").alias(f"{prefix}_name"),
            mask_email(F.col("email")).alias(f"{prefix}_email_masked"),
            mask_phone(F.col("phone_e164")).alias(f"{prefix}_phone_masked"),
            F.col("city").alias(f"{prefix}_city"),
            F.col("postcode").alias(f"{prefix}_postcode"),
        )

    return (
        needs_review.join(side("left", "left_id"), "left_id")
        .join(side("right", "right_id"), "right_id")
        .select(
            "pair_id",
            F.when(F.col("band") == BAND_LOW, F.lit("MATCH_REVIEW"))
            .otherwise(F.lit("SPOT_CHECK"))
            .alias("review_type"),
            "left_id",
            "right_id",
            "left_source",
            "left_name",
            "left_email_masked",
            "left_phone_masked",
            "left_city",
            "left_postcode",
            "right_source",
            "right_name",
            "right_email_masked",
            "right_phone_masked",
            "right_city",
            "right_postcode",
            F.round("final_score", 3).alias("confidence"),
            "band",
            F.coalesce("ai_explanation", "rule_explanation").alias("explanation"),
            F.col("rule_explanation").alias("evidence"),
            F.round(F.col("final_score") * 100).cast("int").alias("priority"),
            F.current_timestamp().alias("created_at"),
        )
    )


def resolve_links(candidates: DataFrame, decisions: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (links, new_do_not_match) from model decisions plus steward decisions."""
    latest = latest_decisions(decisions)
    blocked = latest.filter(F.col("decision").isin(STEWARD_REJECT, STEWARD_HOLD)).select("pair_id")
    auto = (
        candidates.filter(F.col("decision") == DECISION_MATCH)
        .join(blocked, "pair_id", "left_anti")
        .select("pair_id", "left_id", "right_id", "band", F.lit(DECIDED_BY_MODEL).alias("decided_by"))
    )
    approved_ids = latest.filter(F.col("decision") == STEWARD_APPROVE).select("pair_id")
    approved = (
        candidates.join(approved_ids, "pair_id")
        .filter(F.col("decision") != DECISION_MATCH)  # auto matches are already in `auto`
        .select("pair_id", "left_id", "right_id", "band", F.lit(DECIDED_BY_STEWARD).alias("decided_by"))
    )
    links = auto.unionByName(approved).withColumn("linked_at", F.current_timestamp())
    rejects = (
        latest.filter(F.col("decision") == STEWARD_REJECT)
        .join(candidates.select("pair_id", "left_id", "right_id"), "pair_id")
        .select(
            "left_id",
            "right_id",
            "pair_id",
            F.col("decided_by").alias("rejected_by"),
            F.col("decided_at").alias("rejected_at"),
        )
    )
    return links, rejects


def merge_do_not_match(existing: DataFrame | None, new: DataFrame) -> DataFrame:
    if existing is None:
        return new.dropDuplicates(["left_id", "right_id"])
    return existing.unionByName(new).dropDuplicates(["left_id", "right_id"])
