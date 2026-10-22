"""Clustering, master keys, survivorship, promotion (SCD2), DQ gate and the review loop."""

from __future__ import annotations

import itertools
from datetime import datetime

import pytest
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.clustering import cluster_guards, connected_components
from goldenrecord.master_key import assign_master_keys
from goldenrecord.promotion import apply_scd2, audit_changes, build_xref, confidence_band_per_key
from goldenrecord.quality import evaluate_rules, gate_passed
from goldenrecord.review import build_review_outbox, mask_email, mask_phone, merge_do_not_match, resolve_links
from goldenrecord.survivorship import build_golden
from tests.helpers import silver_frame, silver_row

pytestmark = pytest.mark.spark

T0 = datetime(2026, 1, 1)
T1 = datetime(2026, 2, 1)
EDGE_SCHEMA = "left_id string, right_id string"


def _clusters(spark, vertices, edges):  # type: ignore[no-untyped-def]
    v = spark.createDataFrame([(x,) for x in vertices], "record_id string")
    e = spark.createDataFrame(edges, EDGE_SCHEMA)
    return {r.record_id: r.cluster_id for r in connected_components(v, e).collect()}


# ----------------------------------------------------------------------------- clustering
def test_connected_components_transitive(spark) -> None:  # type: ignore[no-untyped-def]
    out = _clusters(spark, ["a", "b", "c", "d", "e"], [("a", "b"), ("b", "c"), ("d", "e")])
    assert out == {"a": "a", "b": "a", "c": "a", "d": "d", "e": "d"}


def test_connected_components_long_chain(spark) -> None:  # type: ignore[no-untyped-def]
    ids = [f"r{i:02d}" for i in range(12)]
    out = _clusters(spark, ids, list(itertools.pairwise(ids)))
    assert set(out.values()) == {"r00"}


def test_guards_hold_conflicting_tax_ids_and_large_clusters(spark) -> None:  # type: ignore[no-untyped-def]
    silver = silver_frame(
        spark,
        [
            silver_row("crm|1", tax_id="AAA"),
            silver_row("erp_a|1", tax_id="BBB"),
            silver_row("erp_b|1"),
            silver_row("web|1"),
            silver_row("web|2"),
        ],
    )
    clusters = spark.createDataFrame(
        [("crm|1", "c1"), ("erp_a|1", "c1"), ("erp_b|1", "c2"), ("web|1", "c2"), ("web|2", "c2")],
        "record_id string, cluster_id string",
    )
    guards = {r.cluster_id: r for r in cluster_guards(clusters, silver, max_cluster_size=2).collect()}
    assert guards["c1"].held and guards["c1"].hold_reason == "conflicting_tax_ids"
    assert guards["c2"].held and guards["c2"].hold_reason == "cluster_too_large"


# ----------------------------------------------------------------------------- master keys
def _assign(spark, clusters, previous=None, registry=None):  # type: ignore[no-untyped-def]
    c = spark.createDataFrame(clusters, "record_id string, cluster_id string")
    prev = spark.createDataFrame(previous, "record_id string, master_key string") if previous else None
    assignments, reg = assign_master_keys(c, prev, registry, "C-", 6, T1)
    keys = {r.cluster_id: r.master_key for r in assignments.collect()}
    by_record = {rid: keys[cid] for rid, cid in clusters}
    return by_record, reg.cache()


def test_new_clusters_get_sequential_keys(spark) -> None:  # type: ignore[no-untyped-def]
    by_record, reg = _assign(spark, [("a", "a"), ("b", "a"), ("c", "c")])
    assert by_record == {"a": "C-000001", "b": "C-000001", "c": "C-000002"}
    assert reg.filter("status = 'active'").count() == 2


def test_keys_are_stable_and_merge_keeps_oldest(spark) -> None:  # type: ignore[no-untyped-def]
    first, reg = _assign(spark, [("a", "a"), ("b", "b"), ("c", "c")])
    # Next run: a steward links a and b (merge); c is unchanged.
    second, reg2 = _assign(
        spark, [("a", "a"), ("b", "a"), ("c", "c")], previous=list(first.items()), registry=reg
    )
    assert second["a"] == second["b"] == first["a"] == "C-000001"
    assert second["c"] == first["c"]
    retired = {r.master_key: r for r in reg2.filter("status = 'retired'").collect()}
    assert retired[first["b"]].replaced_by == "C-000001"


def test_split_keeps_key_with_majority_and_never_reuses(spark) -> None:  # type: ignore[no-untyped-def]
    first, reg = _assign(spark, [("a", "a"), ("b", "a"), ("c", "a")])
    # A wrong merge is split: c leaves the cluster.
    second, reg2 = _assign(
        spark, [("a", "a"), ("b", "a"), ("c", "c")], previous=list(first.items()), registry=reg
    )
    assert second["a"] == second["b"] == "C-000001"
    assert second["c"] == "C-000002"
    # A key retired earlier is never issued again, even when the sequence is reused elsewhere.
    third, reg3 = _assign(
        spark, [("a", "a"), ("b", "a"), ("c", "a"), ("d", "d")], previous=list(second.items()), registry=reg2
    )
    assert third["c"] == "C-000001" and third["d"] == "C-000003"
    assert reg3.filter("master_key = 'C-000002'").first().status == "retired"
    assert reg3.groupBy("master_key").count().filter("count > 1").count() == 0


# ----------------------------------------------------------------------------- survivorship
@pytest.fixture(scope="module")
def members(spark):  # type: ignore[no-untyped-def]
    rows = [
        silver_row(
            "crm|1",
            first_name="Priya",
            last_name="Sharma",
            email="priya@gmail.com",
            phone_e164="+919876143210",
            city="Bengaluru",
            consent_email="yes",
            consent_sms="no",
            source_updated_at=datetime(2025, 1, 1),
        ),
        silver_row(
            "web|1",
            first_name="Priyanka",
            last_name="Sharma",
            email="p.sharma@yahoo.com",
            city="Mumbai",
            consent_email="no",
            source_updated_at=datetime(2025, 6, 1),
        ),
        silver_row(
            "pos_loyalty|1",
            first_name="Priya",
            last_name="Sharma",
            loyalty_id="LC9",
            consent_whatsapp="unknown",
            source_updated_at=datetime(2024, 1, 1),
        ),
        silver_row(
            "mobile|1",
            first_name="Priya",
            last_name="Sharma",
            loyalty_id="LC1",
            consent_sms="yes",
            source_updated_at=datetime(2025, 9, 1),
        ),
    ]
    df = silver_frame(spark, rows).withColumn("master_key", F.lit("C-000001"))
    return df


def test_survivorship_rules(members, settings) -> None:  # type: ignore[no-untyped-def]
    g = build_golden(members, settings).first()
    assert g.source_record_count == 4
    assert g.email == "priya@gmail.com" and g.email__winner == "crm|1"  # most trusted
    assert g.city == "Mumbai"  # most recent
    assert g.first_name == "Priya"  # longest complete within the most trusted source (crm)
    assert g.loyalty_id == "LC9"  # source of record: pos_loyalty, although mobile is newer
    assert g.consent_email == "no"  # one explicit no wins
    assert g.consent_sms == "no"  # explicit no beats a later yes
    assert g.consent_whatsapp == "no"  # nothing explicit -> no


def test_survivorship_is_deterministic(members, settings) -> None:  # type: ignore[no-untyped-def]
    a = build_golden(members, settings).first().asDict()
    b = build_golden(members.orderBy(F.rand(1)), settings).first().asDict()
    assert a == b


# ----------------------------------------------------------------------------- promotion
def _snapshot(members, settings, **overrides):  # type: ignore[no-untyped-def]
    snap = build_golden(members, settings).withColumn("confidence_band", F.lit("HIGH"))
    for col, value in overrides.items():
        snap = snap.withColumn(col, F.lit(value))
    return snap


def test_scd2_opens_closes_and_keeps_unchanged(spark, members, settings) -> None:  # type: ignore[no-untyped-def]
    retired = spark.createDataFrame([], "master_key string")
    v1 = apply_scd2(_snapshot(members, settings), None, retired, T0, "run1").cache()
    assert v1.count() == 1 and v1.first().is_current
    same = apply_scd2(_snapshot(members, settings), v1, retired, T1, "run2")
    assert same.count() == 1 and same.first().run_id == "run1"  # unchanged -> no new version
    changed = apply_scd2(_snapshot(members, settings, city="Pune"), v1, retired, T1, "run2").cache()
    assert changed.count() == 2
    current = changed.filter("is_current").first()
    old = changed.filter("NOT is_current").first()
    assert current.city == "Pune" and current.valid_to is None
    assert old.valid_to == T1


def test_audit_records_changed_attributes(members, settings) -> None:  # type: ignore[no-untyped-def]
    first = audit_changes(_snapshot(members, settings), None, settings, T0, "run1")
    assert first.filter("attribute = 'email'").first().winning_record_id == "crm|1"
    v1 = apply_scd2(
        _snapshot(members, settings),
        None,
        members.sparkSession.createDataFrame([], "master_key string"),
        T0,
        "run1",
    )
    second = audit_changes(_snapshot(members, settings, city="Pune"), v1, settings, T1, "run2").collect()
    assert [(r.attribute, r.old_value, r.new_value) for r in second] == [("city", "Mumbai", "Pune")]


def test_xref_and_confidence_band(spark, members) -> None:  # type: ignore[no-untyped-def]
    links = spark.createDataFrame(
        [("p1", "crm|1", "web|1", "MEDIUM", "model"), ("p2", "mobile|1", "pos_loyalty|1", "LOW", "steward")],
        "pair_id string, left_id string, right_id string, band string, decided_by string",
    )
    xref = {r.record_id: r for r in build_xref(members, links, T0, None).collect()}
    assert xref["crm|1"].linked_by == "model" and xref["mobile|1"].linked_by == "steward"
    band = confidence_band_per_key(members, links).first().confidence_band
    assert band == "MEDIUM"  # weakest link; steward-approved LOW counts as HIGH


# ----------------------------------------------------------------------------- DQ gate
def test_dq_gate_blocks_duplicate_master_keys(spark, members, settings) -> None:  # type: ignore[no-untyped-def]
    good = _snapshot(members, settings)
    assert gate_passed(evaluate_rules(good, settings.dq_rules, settings.reference))
    duplicated = good.unionByName(good)
    results = {r.rule_id: r for r in evaluate_rules(duplicated, settings.dq_rules, settings.reference)}
    assert not results["DQ03"].ok
    assert not gate_passed(list(results.values()))


def test_dq_rules_detect_bad_formats(spark, members, settings) -> None:  # type: ignore[no-untyped-def]
    bad = _snapshot(members, settings, email="not-an-email", phone_e164="0123", country="ZZ")
    results = {r.rule_id: r for r in evaluate_rules(bad, settings.dq_rules, settings.reference)}
    assert not results["DQ01"].ok and not results["DQ02"].ok and not results["DQ06"].ok


# ----------------------------------------------------------------------------- review loop
def test_masking(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([("priya.sharma@gmail.com", "+919876143210")], "e string, p string")
    row = df.select(mask_email(F.col("e")), mask_phone(F.col("p"))).first()
    assert row[0] == "p***@gmail.com" and row[1] == "+91******3210"


@pytest.fixture
def candidates(spark):  # type: ignore[no-untyped-def]
    return (
        spark.createDataFrame(
            [
                ("p1", "crm|1", "web|1", "HIGH", "match", False, 0.97),
                ("p2", "crm|1", "mobile|1", "LOW", "review", False, 0.6),
                ("p3", "web|1", "pos_loyalty|1", "LOW", "review", False, 0.55),
                ("p4", "erp_a|1", "erp_b|1", "MEDIUM", "match", True, 0.8),
            ],
            "pair_id string, left_id string, right_id string, band string, decision string, "
            "spot_check boolean, final_score double",
        )
        .withColumn("rule_explanation", F.lit("For: same email"))
        .withColumn("ai_explanation", F.lit(None).cast("string"))
    )


def _decisions(spark, rows):  # type: ignore[no-untyped-def]
    return spark.createDataFrame(
        [(p, d, "test", "steward@example.com", t) for p, d, t in rows],
        "pair_id string, decision string, reason string, decided_by string, decided_at timestamp",
    )


def test_resolve_links_applies_steward_decisions(spark, candidates) -> None:  # type: ignore[no-untyped-def]
    decisions = _decisions(
        spark, [("p2", "approve", T0), ("p3", "REJECT", T0), ("p4", "REJECT", T0), ("p4", "APPROVE", T1)]
    )  # latest decision wins
    links, rejects = resolve_links(candidates, decisions)
    linked = {r.pair_id: r.decided_by for r in links.collect()}
    assert linked == {"p1": "model", "p2": "steward", "p4": "model"}
    assert [r.pair_id for r in rejects.collect()] == ["p3"]


def test_rejected_auto_match_is_unlinked(spark, candidates) -> None:  # type: ignore[no-untyped-def]
    links, _ = resolve_links(candidates, _decisions(spark, [("p1", "REJECT", T0)]))
    assert "p1" not in {r.pair_id for r in links.collect()}


def test_review_outbox_masks_and_skips_decided(spark, candidates) -> None:  # type: ignore[no-untyped-def]
    silver = silver_frame(
        spark,
        [
            silver_row(
                rid,
                first_name="Priya",
                last_name="Sharma",
                email="priya@gmail.com",
                phone_e164="+919876143210",
            )
            for rid in ["crm|1", "web|1", "mobile|1", "pos_loyalty|1", "erp_a|1", "erp_b|1"]
        ],
    )
    outbox = build_review_outbox(candidates, silver, _decisions(spark, [("p3", "REJECT", T0)])).collect()
    assert {r.pair_id: r.review_type for r in outbox} == {"p2": "MATCH_REVIEW", "p4": "SPOT_CHECK"}
    text = " ".join(str(v) for r in outbox for v in r.asDict().values())
    assert "priya@gmail.com" not in text and "+919876143210" not in text


def test_do_not_match_list_deduplicates(spark) -> None:  # type: ignore[no-untyped-def]
    a = spark.createDataFrame(
        [("x", "y", "p", "s", T0)],
        "left_id string, right_id string, pair_id string, rejected_by string, rejected_at timestamp",
    )
    assert merge_do_not_match(a, a).count() == 1
