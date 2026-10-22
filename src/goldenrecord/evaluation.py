"""Measure matching quality against a labelled sample of pairs.

Labelled pairs come from steward decisions (APPROVE = match, REJECT = non-match) and,
for synthetic data, from generator ground truth. A pair counts as *predicted match* when
both records ended up under the same master key, so the metric reflects what reports and
the agent actually see.

Metrics are optionally logged to MLflow (available in Fabric notebooks).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.logging_utils import get_logger, kv

log = get_logger("evaluation")


@dataclass(frozen=True)
class EvalResult:
    labelled_pairs: int
    true_matches: int
    true_positives: int
    false_positives: int
    false_negatives: int

    @property
    def precision(self) -> float:
        denom = self.true_positives + self.false_positives
        return 1.0 if denom == 0 else self.true_positives / denom

    @property
    def recall(self) -> float:
        denom = self.true_positives + self.false_negatives
        return 1.0 if denom == 0 else self.true_positives / denom

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 0.0 if p + r == 0 else 2 * p * r / (p + r)

    def as_dict(self) -> dict[str, float]:
        return {
            **{k: float(v) for k, v in asdict(self).items()},
            "precision": self.precision,
            "recall": self.recall,
            "f1": self.f1,
        }


def evaluate_pairs(labelled: DataFrame, xref: DataFrame) -> EvalResult:
    """``labelled`` has left_id, right_id, is_match (boolean)."""
    keys = xref.select("record_id", "master_key")
    joined = (
        labelled.join(
            keys.withColumnRenamed("record_id", "left_id").withColumnRenamed("master_key", "lk"),
            "left_id",
            "left",
        )
        .join(
            keys.withColumnRenamed("record_id", "right_id").withColumnRenamed("master_key", "rk"),
            "right_id",
            "left",
        )
        .withColumn("predicted", F.col("lk").isNotNull() & (F.col("lk") == F.col("rk")))
    )
    row = joined.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("is_match").cast("int")).alias("tm"),
        F.sum((F.col("is_match") & F.col("predicted")).cast("int")).alias("tp"),
        F.sum((~F.col("is_match") & F.col("predicted")).cast("int")).alias("fp"),
        F.sum((F.col("is_match") & ~F.col("predicted")).cast("int")).alias("fn"),
    ).collect()[0]
    return EvalResult(
        labelled_pairs=int(row["n"] or 0),
        true_matches=int(row["tm"] or 0),
        true_positives=int(row["tp"] or 0),
        false_positives=int(row["fp"] or 0),
        false_negatives=int(row["fn"] or 0),
    )


def labels_from_decisions(latest_decisions: DataFrame, candidates: DataFrame) -> DataFrame:
    return (
        latest_decisions.filter(F.col("decision").isin("APPROVE", "REJECT"))
        .join(candidates.select("pair_id", "left_id", "right_id"), "pair_id")
        .select(
            "left_id",
            "right_id",
            (F.col("decision") == "APPROVE").alias("is_match"),
            F.lit("steward").alias("label_source"),
        )
    )


def log_to_mlflow(result: EvalResult, params: dict[str, float]) -> bool:
    """Log metrics to MLflow when it is available; returns True if logged."""
    try:
        import mlflow  # noqa: PLC0415 - optional dependency, only present in Fabric / data science envs
    except ImportError:
        log.info(kv("mlflow_unavailable"))
        return False
    mlflow.set_experiment("goldenrecord-matching")
    with mlflow.start_run(run_name="match-evaluation"):
        mlflow.log_params(params)
        mlflow.log_metrics(result.as_dict())
    return True
