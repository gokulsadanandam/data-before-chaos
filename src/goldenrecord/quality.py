"""Data quality gate: the six rules evaluated on the Gold candidate before it is published.

The same rules are configured in Purview for the published scorecard; evaluating them in
the pipeline too means a breach *holds promotion* instead of only showing up on a report.
Results are appended to ``ops_dq_results`` (one row per rule per run) for trend reporting
and Activator alerts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import DQRule, ReferenceData


@dataclass(frozen=True)
class RuleResult:
    rule_id: str
    name: str
    passed: int
    total: int
    threshold: float
    blocking: bool

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total == 0 else self.passed / self.total

    @property
    def ok(self) -> bool:
        return self.pass_rate >= self.threshold


def _row_check(rule: DQRule, ref: ReferenceData) -> Column:
    """Boolean column: does this row pass the rule? (``unique`` is handled separately.)"""
    if rule.check == "regex":
        assert rule.column and rule.pattern
        col = F.col(rule.column)
        ok = col.rlike(rule.pattern)
        return F.when(col.isNull(), F.lit(rule.allow_null)).otherwise(ok)
    if rule.check == "not_null":
        cols = rule.columns or ((rule.column,) if rule.column else ())
        cond: Column = F.lit(True)
        for c in cols:
            cond = cond & F.col(c).isNotNull()
        return cond
    if rule.check == "mandatory":
        cond = F.lit(True)
        for c in rule.columns:
            cond = cond & F.col(c).isNotNull()
        if rule.any_of:
            any_cond: Column = F.lit(False)
            for c in rule.any_of:
                any_cond = any_cond | F.col(c).isNotNull()
            cond = cond & any_cond
        return cond
    if rule.check == "in_set":
        assert rule.column
        col = F.col(rule.column)
        return F.when(col.isNull(), F.lit(rule.allow_null)).otherwise(col.isin(*sorted(ref.valid_countries)))
    raise ValueError(f"Unsupported row check {rule.check!r}")


def evaluate_rules(df: DataFrame, rules: tuple[DQRule, ...], ref: ReferenceData) -> list[RuleResult]:
    """Evaluate all rules with a single pass over ``df`` (plus one for uniqueness rules)."""
    row_rules = [r for r in rules if r.check != "unique"]
    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.when(_row_check(r, ref), 1).otherwise(0)).alias(r.id) for r in row_rules
    ]
    row = df.agg(*aggs).collect()[0]
    total = int(row["_total"])
    results = [
        RuleResult(r.id, r.name, int(row[r.id] or 0), total, r.threshold, r.blocking) for r in row_rules
    ]
    for r in (r for r in rules if r.check == "unique"):
        assert r.column
        dupes = (
            df.groupBy(r.column).count().filter("count > 1").agg(F.sum("count").alias("d")).collect()[0]["d"]
        )
        results.append(RuleResult(r.id, r.name, total - int(dupes or 0), total, r.threshold, r.blocking))
    return sorted(results, key=lambda x: x.rule_id)


def gate_passed(results: list[RuleResult]) -> bool:
    return all(r.ok for r in results if r.blocking)


def results_frame(
    df_for_session: DataFrame, results: list[RuleResult], run_id: str, run_ts: datetime, table: str
) -> DataFrame:
    rows = [
        (
            run_id,
            run_ts,
            table,
            r.rule_id,
            r.name,
            r.passed,
            r.total,
            float(r.pass_rate),
            r.threshold,
            r.ok,
            r.blocking,
        )
        for r in results
    ]
    return df_for_session.sparkSession.createDataFrame(
        rows,
        "run_id string, run_ts timestamp, table_name string, rule_id string, rule_name string, passed long, "
        "total long, pass_rate double, threshold double, ok boolean, blocking boolean",
    )
