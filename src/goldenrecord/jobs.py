"""Stage entry points. Fabric notebooks and the local runner call these functions.

Stages (each is idempotent and can be re-run):

====================  =========================================================
``run_ingest``        landing files -> Bronze (append-only, hash de-duplicated)
``run_cleanse``       Bronze -> Silver clean + quarantine (Tier 1 rules, tokens)
``run_match``         Silver -> identifier profile -> candidate pairs with scores
``run_publish_review``  candidates -> review outbox for the Fabric SQL database
``run_resolve``       decisions + candidates -> links -> clusters -> master keys
                      -> survivorship -> DQ gate -> Gold (SCD2), xref, audit, facts
``run_evaluate``      precision / recall on labelled pairs (+ MLflow when available)
====================  =========================================================
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.ai import AIAdapter, FabricAIAdapter, HeuristicAIAdapter
from goldenrecord.cleanse import build_silver
from goldenrecord.clustering import cluster_guards, connected_components
from goldenrecord.config import Settings
from goldenrecord.evaluation import evaluate_pairs, labels_from_decisions, log_to_mlflow
from goldenrecord.facts import build_engagement, build_orders
from goldenrecord.ingest import ingest_customers, ingest_engagement, ingest_orders
from goldenrecord.logging_utils import get_logger, kv
from goldenrecord.master_key import assign_master_keys
from goldenrecord.matching import match_records
from goldenrecord.metrics import Metric, pct, record_metrics
from goldenrecord.profiling import profile_identifiers
from goldenrecord.promotion import (
    apply_scd2,
    audit_changes,
    build_xref,
    confidence_band_per_key,
)
from goldenrecord.quality import evaluate_rules, gate_passed, results_frame
from goldenrecord.review import (
    build_review_outbox,
    empty_decisions,
    latest_decisions,
    merge_do_not_match,
    resolve_links,
)
from goldenrecord.schema import BAND_HIGH, BAND_LOW, BAND_MEDIUM
from goldenrecord.store import TableStore, append_or_create
from goldenrecord.survivorship import build_golden

log = get_logger("jobs")


@dataclass
class RunContext:
    spark: SparkSession
    store: TableStore
    settings: Settings
    landing_root: str
    token_key: bytes
    ai: AIAdapter
    run_id: str = field(
        default_factory=lambda: (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        )
    )
    run_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def table(self, key: str) -> str:
        return self.settings.table(key)

    def read_optional(self, key: str) -> DataFrame | None:
        name = self.table(key)
        return self.store.read(name) if self.store.exists(name) else None

    def snapshot_optional(self, key: str) -> DataFrame | None:
        """Previous state of a table this stage will overwrite (stable across the overwrite)."""
        name = self.table(key)
        return self.store.snapshot(name) if self.store.exists(name) else None

    def metrics(self, metrics: list[Metric]) -> None:
        record_metrics(
            self.spark, self.store, self.table("ops_run_metrics"), self.run_id, self.run_ts, metrics
        )


def resolve_token_key(settings: Settings, use_key_vault: bool) -> bytes:
    """Token key from Azure Key Vault in Fabric, or the dev key locally."""
    tok = settings.section("tokenisation")
    if use_key_vault:
        import notebookutils  # noqa: PLC0415 - only exists in Fabric

        secret = notebookutils.credentials.getSecret(tok["key_vault_uri"], tok["key_secret_name"])
        return str(secret).encode("utf-8")
    return str(tok.get("local_dev_key", "local-dev-only")).encode("utf-8")


def default_ai(fabric: bool) -> AIAdapter:
    return FabricAIAdapter() if fabric else HeuristicAIAdapter()


# ----------------------------------------------------------------------------- stages
def run_ingest(ctx: RunContext) -> dict[str, Any]:
    s, store = ctx.settings, ctx.store
    counts: dict[str, int] = {}
    for spec in s.sources:
        counts[spec.name] = ingest_customers(
            ctx.spark, store, ctx.landing_root, spec, f"{s.table('bronze_prefix')}{spec.name}", ctx.run_id
        )
        if spec.orders is not None:
            counts[f"orders_{spec.name}"] = ingest_orders(
                ctx.spark,
                store,
                ctx.landing_root,
                spec.name,
                spec.format,
                spec.options,
                spec.orders,
                f"{s.table('bronze_orders_prefix')}{spec.name}",
                ctx.run_id,
            )
    if s.engagement is not None:
        counts["engagement"] = ingest_engagement(
            ctx.spark, store, ctx.landing_root, s.engagement, s.table("bronze_engagement"), ctx.run_id
        )
    log.info(kv("ingest_done", run_id=ctx.run_id, **counts))
    ctx.metrics([Metric("ingest", f"rows_new_{k}", float(v)) for k, v in counts.items()])
    return counts


def run_cleanse(ctx: RunContext) -> dict[str, Any]:
    clean, quarantine = build_silver(ctx.store, ctx.settings, ctx.token_key)
    ctx.store.overwrite(ctx.table("silver_clean"), clean)
    ctx.store.overwrite(ctx.table("silver_quarantine"), quarantine)
    silver = ctx.store.read(ctx.table("silver_clean"))
    n_clean = silver.count()
    n_quarantine = ctx.store.read(ctx.table("silver_quarantine")).count()
    total = n_clean + n_quarantine
    by_status = {r["dq_status"]: int(r["count"]) for r in silver.groupBy("dq_status").count().collect()}
    alerts = ctx.settings.section("alerts")
    ctx.metrics(
        [
            Metric("cleanse", "records_total", float(total)),
            Metric("cleanse", "records_clean", float(n_clean)),
            Metric("cleanse", "records_quarantined", float(n_quarantine)),
            Metric(
                "cleanse",
                "quarantine_rate_pct",
                pct(n_quarantine, total),
                float(alerts.get("quarantine_rate_delta_points", 5.0)),
            ),
            *[Metric("cleanse", f"status_{k}", float(v)) for k, v in by_status.items()],
        ]
    )
    result = {
        "records_clean": n_clean,
        "records_quarantined": n_quarantine,
        **{f"status_{k}": v for k, v in by_status.items()},
    }
    log.info(kv("cleanse_done", run_id=ctx.run_id, **result))
    return result


def run_match(ctx: RunContext) -> dict[str, Any]:
    silver = ctx.store.read(ctx.table("silver_clean"))
    profile = profile_identifiers(silver, ctx.settings)
    ctx.store.overwrite(ctx.table("silver_identifier_profile"), profile)
    profile = ctx.store.read(ctx.table("silver_identifier_profile"))
    dnm = ctx.read_optional("silver_do_not_match")
    candidates, skipped = match_records(silver, profile, ctx.settings, ctx.ai, dnm)
    ctx.store.overwrite(ctx.table("silver_candidates"), candidates.withColumn("run_id", F.lit(ctx.run_id)))
    stored = ctx.store.read(ctx.table("silver_candidates"))
    bands = {r["band"]: int(r["count"]) for r in stored.groupBy("band").count().collect()}
    total = sum(bands.values())
    n_skipped = skipped.count()
    ai_used = stored.filter(F.col("ai_similarity").isNotNull() | F.col("ai_verdict").isNotNull()).count()
    alerts = ctx.settings.section("alerts")
    ctx.metrics(
        [
            Metric("match", "candidate_pairs", float(total)),
            Metric("match", "pairs_high", float(bands.get(BAND_HIGH, 0))),
            Metric("match", "pairs_medium", float(bands.get(BAND_MEDIUM, 0))),
            Metric("match", "pairs_low", float(bands.get(BAND_LOW, 0))),
            Metric(
                "match",
                "review_rate_pct",
                pct(bands.get(BAND_LOW, 0), total),
                float(alerts.get("review_rate_delta_points", 5.0)),
            ),
            Metric("match", "pairs_sent_to_ai", float(ai_used)),
            Metric("match", "blocks_skipped_oversized", float(n_skipped)),
        ]
    )
    result = {"candidates": total, "skipped_blocks": n_skipped, "ai_pairs": ai_used, **bands}
    log.info(kv("match_done", run_id=ctx.run_id, **result))
    return result


def _decisions(ctx: RunContext, like: DataFrame) -> DataFrame:
    existing = ctx.read_optional("review_decisions")
    return existing if existing is not None else empty_decisions(like)


def run_publish_review(ctx: RunContext) -> dict[str, Any]:
    candidates = ctx.store.read(ctx.table("silver_candidates"))
    silver = ctx.store.read(ctx.table("silver_clean"))
    outbox = build_review_outbox(candidates, silver, _decisions(ctx, candidates))
    ctx.store.overwrite(ctx.table("review_outbox"), outbox.withColumn("run_id", F.lit(ctx.run_id)))
    n = ctx.store.read(ctx.table("review_outbox")).count()
    ctx.metrics([Metric("review", "review_queue_items", float(n))])
    log.info(kv("review_outbox_done", run_id=ctx.run_id, items=n))
    return {"review_items": n}


def run_resolve(ctx: RunContext) -> dict[str, Any]:
    s, store = ctx.settings, ctx.store
    silver = store.read(ctx.table("silver_clean"))
    candidates = store.read(ctx.table("silver_candidates"))
    decisions = _decisions(ctx, candidates)

    # 1. Links and do-not-match list.
    links, rejects = resolve_links(candidates, decisions)
    store.overwrite(ctx.table("silver_links"), links)
    links = store.read(ctx.table("silver_links"))
    store.overwrite(
        ctx.table("silver_do_not_match"),
        merge_do_not_match(ctx.snapshot_optional("silver_do_not_match"), rejects),
    )

    # 2. Clusters with chaining guards.
    ccfg = s.section("clustering")
    clusters = connected_components(silver.select("record_id"), links, int(ccfg.get("max_iterations", 20)))
    guards = cluster_guards(clusters, silver, int(ccfg.get("max_cluster_size", 25)))
    store.overwrite(ctx.table("silver_cluster_issue"), guards.filter("held"))
    held = guards.filter("held").select("cluster_id")
    previous_xref = ctx.snapshot_optional("gold_xref")
    # Held clusters merge nothing new: each record goes back to its previous golden record
    # (if it had one) or stays on its own until a steward resolves the issue.
    held_members = clusters.join(held, "cluster_id")
    if previous_xref is not None:
        held_members = held_members.join(
            previous_xref.select("record_id", F.col("master_key").alias("_prev_key")), "record_id", "left"
        )
    else:
        held_members = held_members.withColumn("_prev_key", F.lit(None).cast("string"))
    held_members = held_members.select(
        "record_id",
        F.coalesce(
            F.concat(F.lit("KEY:"), F.col("_prev_key")), F.concat(F.lit("REC:"), F.col("record_id"))
        ).alias("cluster_id"),
    )
    effective = clusters.join(held, "cluster_id", "left_anti").unionByName(held_members).cache()

    # 3. Stable master keys.
    assignments, registry = assign_master_keys(
        effective,
        previous_xref,
        ctx.snapshot_optional("gold_key_registry"),
        s.master_key_prefix,
        s.master_key_digits,
        ctx.run_ts,
    )
    members = effective.join(assignments, "cluster_id").join(silver, "record_id").cache()

    # 4. Survivorship -> golden snapshot.
    snapshot = (
        build_golden(members, s).join(confidence_band_per_key(members, links), "master_key", "left").cache()
    )

    # 5. Data quality gate on the candidate golden records.
    results = evaluate_rules(snapshot, s.dq_rules, s.reference)
    passed = gate_passed(results)
    dq_df = results_frame(snapshot, results, ctx.run_id, ctx.run_ts, ctx.table("gold_customer"))
    append_or_create(store, ctx.table("ops_dq_results"), dq_df)
    gate_metrics = [
        Metric("dq_gate", f"{r.rule_id}_pass_rate_pct", round(100 * r.pass_rate, 3)) for r in results
    ]
    if not passed:
        failed = [r.rule_id for r in results if r.blocking and not r.ok]
        ctx.metrics([*gate_metrics, Metric("dq_gate", "gate_passed", 0.0)])
        log.error(kv("dq_gate_failed", run_id=ctx.run_id, failed_rules=failed))
        for cached in (effective, members, snapshot):
            cached.unpersist()
        return {"gate_passed": False, "failed_rules": failed}

    # 6. Publish Gold: SCD2 golden records, registry, xref, audit, facts.
    current_gold = ctx.snapshot_optional("gold_customer")
    retired = registry.filter(F.col("status") == "retired").select("master_key")
    gold = apply_scd2(
        snapshot.drop(*[c for c in snapshot.columns if c.endswith("__winner")]),
        current_gold,
        retired,
        ctx.run_ts,
        ctx.run_id,
    )
    audit = audit_changes(snapshot, current_gold, s, ctx.run_ts, ctx.run_id)
    xref = build_xref(members, links, ctx.run_ts, previous_xref)

    store.overwrite(ctx.table("gold_customer"), gold)
    store.overwrite(ctx.table("gold_key_registry"), registry)
    store.overwrite(ctx.table("gold_xref"), xref)
    append_or_create(store, ctx.table("gold_audit"), audit)
    gold = store.read(ctx.table("gold_customer"))
    store.overwrite(ctx.table("gold_customer_current"), gold.filter("is_current"))
    xref = store.read(ctx.table("gold_xref"))
    orders = build_orders(store, s, xref)
    if orders is not None:
        store.overwrite(ctx.table("gold_orders"), orders)
    engagement = build_engagement(store, s, xref, gold)
    if engagement is not None:
        store.overwrite(ctx.table("gold_engagement"), engagement)

    n_golden = gold.filter("is_current").count()
    n_records = xref.count()
    result = {
        "gate_passed": True,
        "golden_records": n_golden,
        "source_records": n_records,
        "links": links.count(),
        "held_clusters": guards.filter("held").count(),
        "duplicate_reduction_pct": pct(n_records - n_golden, n_records),
    }
    ctx.metrics(
        [
            *gate_metrics,
            Metric("dq_gate", "gate_passed", 1.0),
            Metric("resolve", "golden_records", float(n_golden)),
            Metric("resolve", "source_records", float(n_records)),
            Metric("resolve", "records_merged_pct", result["duplicate_reduction_pct"]),
            Metric("resolve", "held_clusters", float(result["held_clusters"])),
        ]
    )
    for e in (effective, members, snapshot):
        e.unpersist()
    log.info(kv("resolve_done", run_id=ctx.run_id, **result))
    return result


def run_evaluate(ctx: RunContext, labelled: DataFrame | None = None) -> dict[str, Any]:
    """Evaluate against ``labelled`` (left_id, right_id, is_match) or steward decisions."""
    xref = ctx.store.read(ctx.table("gold_xref"))
    if labelled is None:
        stored = ctx.read_optional("ops_labelled_pairs")
        if stored is not None:
            labelled = stored
        else:
            candidates = ctx.store.read(ctx.table("silver_candidates"))
            labelled = labels_from_decisions(latest_decisions(_decisions(ctx, candidates)), candidates)
    result = evaluate_pairs(labelled, xref)
    m = ctx.settings.matching
    log_to_mlflow(result, {"band_high": m.band_high, "band_medium": m.band_medium, "intercept": m.intercept})
    metrics = result.as_dict()
    ctx.metrics([Metric("evaluate", k, round(v, 5)) for k, v in metrics.items()])
    row = ctx.spark.createDataFrame(
        [(ctx.run_id, ctx.run_ts, *[float(metrics[k]) for k in sorted(metrics)])],
        "run_id string, run_ts timestamp, " + ", ".join(f"{k} double" for k in sorted(metrics)),
    )
    table = ctx.table("ops_eval_results")
    append_or_create(ctx.store, table, row)
    log.info(kv("evaluate_done", run_id=ctx.run_id, **metrics))
    return metrics


def run_all(ctx: RunContext) -> dict[str, Any]:
    return {
        "ingest": run_ingest(ctx),
        "cleanse": run_cleanse(ctx),
        "match": run_match(ctx),
        "review": run_publish_review(ctx),
        "resolve": run_resolve(ctx),
    }


def run_retrain(ctx: RunContext, labelled: DataFrame | None = None, l2: float = 1.0) -> dict[str, Any]:
    """Propose new match weights from steward labels (never applied automatically).

    The proposal is returned and logged; a data owner reviews it and copies it into
    config/pipeline.yaml through a pull request, so every weight change is versioned.
    """
    from goldenrecord.features import FEATURE_COLUMNS  # noqa: PLC0415
    from goldenrecord.training import fit_weights  # noqa: PLC0415

    candidates = ctx.store.read(ctx.table("silver_candidates"))
    if labelled is None:
        labelled = labels_from_decisions(latest_decisions(_decisions(ctx, candidates)), candidates)
    rows = [
        r.asDict()
        for r in candidates.join(labelled.select("left_id", "right_id", "is_match"), ["left_id", "right_id"])
        .select(*FEATURE_COLUMNS, "is_match")
        .collect()
    ]
    fitted = fit_weights(rows, l2=l2)
    proposal = {
        "labelled_pairs": len(rows),
        "intercept": fitted.intercept,
        "weights": fitted.weights,
        "log_loss": fitted.log_loss,
        "current_intercept": ctx.settings.matching.intercept,
        "current_weights": ctx.settings.matching.weights,
    }
    log.info(kv("retrain_proposal", run_id=ctx.run_id, pairs=len(rows), log_loss=fitted.log_loss))
    return proposal
