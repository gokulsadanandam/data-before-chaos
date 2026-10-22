"""End-to-end: synthetic multi-source data -> Bronze -> Silver -> matching -> review -> Gold."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.ai import HeuristicAIAdapter
from goldenrecord.jobs import (
    RunContext,
    resolve_token_key,
    run_all,
    run_evaluate,
    run_ingest,
    run_publish_review,
    run_resolve,
)
from goldenrecord.local_runner import simulate_steward
from goldenrecord.store import PathTableStore
from tests.conftest import ROOT

pytestmark = [pytest.mark.spark, pytest.mark.e2e]


def _load_generator():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("gen", ROOT / "tools" / "generate_synthetic_data.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen"] = module  # dataclasses need the module registered
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pipeline(spark, settings, tmp_path_factory):  # type: ignore[no-untyped-def]
    base: Path = tmp_path_factory.mktemp("e2e")
    landing = base / "landing"
    stats = _load_generator().generate(400, landing, seed=7)
    ctx = RunContext(
        spark=spark,
        store=PathTableStore(spark, base / "warehouse"),
        settings=settings,
        landing_root=str(landing),
        token_key=resolve_token_key(settings, use_key_vault=False),
        ai=HeuristicAIAdapter(),
    )
    first = run_all(ctx)
    labelled = (
        spark.read.option("header", True)
        .csv(str(landing / "_ground_truth" / "labelled_pairs.csv"))
        .select("left_id", "right_id", (F.col("is_match") == "1").alias("is_match"))
        .cache()
    )
    return {"ctx": ctx, "first": first, "stats": stats, "landing": landing, "labelled": labelled}


def test_all_stages_complete_and_gate_passes(pipeline) -> None:  # type: ignore[no-untyped-def]
    first = pipeline["first"]
    assert first["resolve"]["gate_passed"] is True
    assert first["cleanse"]["records_quarantined"] > 0  # placeholder records were planted
    assert first["match"]["candidates"] > 0 and first["review"]["review_items"] > 0
    assert first["resolve"]["golden_records"] < first["resolve"]["source_records"]


def test_matching_quality_on_ground_truth(pipeline) -> None:  # type: ignore[no-untyped-def]
    metrics = run_evaluate(pipeline["ctx"], pipeline["labelled"])
    assert metrics["precision"] >= 0.97
    assert metrics["recall"] >= 0.85


def test_gold_tables_are_consistent(pipeline) -> None:  # type: ignore[no-untyped-def]
    ctx = pipeline["ctx"]
    gold = ctx.store.read(ctx.table("gold_customer")).filter("is_current")
    xref = ctx.store.read(ctx.table("gold_xref"))
    registry = ctx.store.read(ctx.table("gold_key_registry"))
    assert gold.groupBy("master_key").count().filter("count > 1").count() == 0
    assert xref.groupBy("record_id").count().filter("count > 1").count() == 0
    assert xref.join(gold, "master_key", "left_anti").count() == 0  # every record points at a golden record
    active = registry.filter("status = 'active'").select("master_key")
    assert active.join(gold.select("master_key"), "master_key", "left_anti").count() == 0
    assert ctx.store.exists(ctx.table("gold_audit")) and ctx.store.exists(ctx.table("ops_dq_results"))


def test_review_outbox_has_no_raw_contacts(pipeline) -> None:  # type: ignore[no-untyped-def]
    ctx = pipeline["ctx"]
    outbox = ctx.store.read(ctx.table("review_outbox"))
    assert "left_email" not in outbox.columns
    assert (
        outbox.filter(
            F.col("left_email_masked").isNotNull() & ~F.col("left_email_masked").contains("***")
        ).count()
        == 0
    )


def test_engagement_flags_duplicates_and_missing_consent(pipeline) -> None:  # type: ignore[no-untyped-def]
    ctx = pipeline["ctx"]
    sends = ctx.store.read(ctx.table("gold_engagement"))
    assert sends.filter("duplicate_send").count() > 0
    assert sends.filter("sent_without_consent").count() > 0


def test_ingest_is_idempotent(pipeline) -> None:  # type: ignore[no-untyped-def]
    again = run_ingest(pipeline["ctx"])
    assert all(v == 0 for v in again.values())


def test_review_loop_keeps_keys_stable_and_improves(pipeline) -> None:  # type: ignore[no-untyped-def]
    ctx = pipeline["ctx"]
    before = {r.record_id: r.master_key for r in ctx.store.read(ctx.table("gold_xref")).collect()}
    truth = ctx.spark.read.option("header", True).csv(
        str(pipeline["landing"] / "_ground_truth" / "truth.csv")
    )
    answered = simulate_steward(ctx, truth, limit=10_000)
    assert answered > 0
    assert run_publish_review(ctx)["review_items"] == 0  # every open item was decided
    result = run_resolve(ctx)
    assert result["gate_passed"] is True
    after = {r.record_id: r.master_key for r in ctx.store.read(ctx.table("gold_xref")).collect()}
    # Records that were already together stay on the same key unless a steward split them.
    unchanged = sum(1 for rid, key in before.items() if after.get(rid) == key)
    assert unchanged / len(before) > 0.9
    metrics = run_evaluate(ctx, pipeline["labelled"])
    assert metrics["precision"] >= 0.99 and metrics["recall"] >= 0.9
