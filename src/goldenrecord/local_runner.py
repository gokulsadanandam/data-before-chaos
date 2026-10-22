"""Run the whole pipeline on a laptop (no Fabric needed).

Uses a local Spark session, parquet folders instead of Lakehouse Delta tables and the
heuristic AI adapter instead of Fabric AI functions. Steward decisions can be simulated
from ground truth so the review loop can be exercised end to end.

    python -m goldenrecord.local_runner --landing local_data/landing --warehouse local_data/warehouse
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.ai import HeuristicAIAdapter
from goldenrecord.config import load_settings
from goldenrecord.jobs import (
    RunContext,
    resolve_token_key,
    run_all,
    run_evaluate,
    run_publish_review,
    run_resolve,
)
from goldenrecord.logging_utils import configure_logging, get_logger, kv
from goldenrecord.store import PathTableStore, append_or_create

log = get_logger("local_runner")


def local_spark(app_name: str = "goldenrecord-local") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.ansi.enabled", "true")  # same default as Spark 4 / Fabric Runtime 2.0
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def simulate_steward(ctx: RunContext, truth: DataFrame, limit: int) -> int:
    """Answer up to ``limit`` open review items from ground truth (local demo only)."""
    outbox = ctx.store.read(ctx.table("review_outbox"))
    answers = (
        outbox.select("pair_id", "left_id", "right_id")
        .join(truth.select(F.col("record_id").alias("left_id"), F.col("person_id").alias("lp")), "left_id")
        .join(truth.select(F.col("record_id").alias("right_id"), F.col("person_id").alias("rp")), "right_id")
        .limit(limit)
        .select(
            "pair_id",
            F.when(F.col("lp") == F.col("rp"), F.lit("APPROVE")).otherwise(F.lit("REJECT")).alias("decision"),
            F.lit("simulated from ground truth").alias("reason"),
            F.lit("local.steward@example.com").alias("decided_by"),
            F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp").alias("decided_at"),
        )
    )
    table = ctx.table("review_decisions")
    n = answers.count()
    append_or_create(ctx.store, table, answers)
    return n


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--landing",
        required=True,
        help="Folder with the landing files (see tools/generate_synthetic_data.py)",
    )
    p.add_argument("--warehouse", required=True, help="Folder for the local tables")
    p.add_argument("--config", default="config", help="Config folder (default: config)")
    p.add_argument(
        "--simulate-review",
        type=int,
        default=0,
        metavar="N",
        help="Answer N review items from ground truth, then re-run resolution",
    )
    p.add_argument("--report", default=None, help="Write a JSON run report to this path")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging()
    settings = load_settings(args.config)
    spark = local_spark()
    spark.sparkContext.setCheckpointDir(str(Path(args.warehouse) / "_checkpoints"))
    ctx = RunContext(
        spark=spark,
        store=PathTableStore(spark, args.warehouse),
        settings=settings,
        landing_root=str(Path(args.landing).resolve()),
        token_key=resolve_token_key(settings, use_key_vault=False),
        ai=HeuristicAIAdapter(),
    )
    report: dict[str, object] = {"run_id": ctx.run_id, "stages": run_all(ctx)}

    landing = Path(args.landing)
    truth_path = landing / "_ground_truth" / "truth.csv"
    labelled_path = landing / "_ground_truth" / "labelled_pairs.csv"
    labelled = None
    if labelled_path.exists():
        labelled = (
            spark.read.option("header", True)
            .csv(str(labelled_path))
            .select("left_id", "right_id", (F.col("is_match") == "1").alias("is_match"))
            .cache()
        )
        report["evaluation_before_review"] = run_evaluate(ctx, labelled)
    if args.simulate_review and truth_path.exists():
        truth = spark.read.option("header", True).csv(str(truth_path))
        report["simulated_decisions"] = simulate_steward(ctx, truth, args.simulate_review)
        report["after_review"] = {"review": run_publish_review(ctx), "resolve": run_resolve(ctx)}
        if labelled is not None:
            report["evaluation_after_review"] = run_evaluate(ctx, labelled)

    text = json.dumps(report, indent=2, default=str)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    sys.stdout.write(text + "\n")
    log.info(kv("local_run_done", run_id=ctx.run_id))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
