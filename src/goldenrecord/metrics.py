"""Run metrics and anomaly flags (feeds the scorecard and Fabric Activator).

Every stage appends metrics to ``ops_run_metrics`` as (run_id, run_ts, stage, metric,
value, baseline, alert). The *baseline* is the value from the first recorded run (the
day 1 measurement on raw data). Alerts use relative thresholds because half of the raw
data is expected to be garbage: a fixed threshold would fire on every run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.store import TableStore, append_or_create

METRICS_SCHEMA = (
    "run_id string, run_ts timestamp, stage string, metric string, value double, "
    "baseline double, alert boolean, alert_reason string"
)


@dataclass(frozen=True)
class Metric:
    stage: str
    name: str
    value: float
    alert_delta_points: float | None = None  # alert if value is this many points above baseline (rates in %)


def baseline_values(store: TableStore, table: str) -> dict[tuple[str, str], float]:
    if not store.exists(table):
        return {}
    df = store.read(table)
    first = df.groupBy("stage", "metric").agg(F.min_by("value", "run_ts").alias("baseline")).collect()
    return {(r["stage"], r["metric"]): float(r["baseline"]) for r in first if r["baseline"] is not None}


def record_metrics(
    spark: SparkSession, store: TableStore, table: str, run_id: str, run_ts: datetime, metrics: list[Metric]
) -> DataFrame:
    baselines = baseline_values(store, table)
    rows = []
    for m in metrics:
        base = baselines.get((m.stage, m.name))
        alert = (
            m.alert_delta_points is not None and base is not None and (m.value - base) >= m.alert_delta_points
        )
        reason = (
            f"{m.name} is {m.value - base:.1f} points above baseline" if alert and base is not None else None
        )
        rows.append((run_id, run_ts, m.stage, m.name, float(m.value), base, bool(alert), reason))
    df = spark.createDataFrame(rows, METRICS_SCHEMA)
    append_or_create(store, table, df)
    return df


def pct(part: int, whole: int) -> float:
    return 0.0 if whole == 0 else round(100.0 * part / whole, 3)
