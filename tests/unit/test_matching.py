"""Identifier profiling, blocking, features, scoring and the AI tier."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.ai import HeuristicAIAdapter, ai_signal, blend
from goldenrecord.blocking import candidate_pairs
from goldenrecord.matching import CANDIDATE_COLUMNS, match_records
from goldenrecord.profiling import attach_identifier_labels, profile_identifiers
from goldenrecord.schema import BAND_HIGH, BAND_LOW
from tests.helpers import silver_frame, silver_row

pytestmark = pytest.mark.spark

PRIYA = {
    "first_name": "Priya",
    "last_name": "Sharma",
    "city": "Bengaluru",
    "postcode": "560001",
    "address_line": "12 MG Road",
}


@pytest.fixture(scope="module")
def people(spark):  # type: ignore[no-untyped-def]
    return silver_frame(
        spark,
        [
            # One person in three systems: same email / phone, small variations.
            silver_row("crm|1", **PRIYA, email="priya.sharma@gmail.com", phone_e164="+919876143210"),
            silver_row(
                "erp_a|1", **{**PRIYA, "address_line": "12, M.G. Road"}, email="priya.sharma@gmail.com"
            ),
            silver_row(
                "pos_loyalty|1",
                **{**PRIYA, "first_name": "Priyaa"},
                phone_e164="+919876143210",
                loyalty_id="LC1",
            ),
            # Household: shares the phone, different first name.
            silver_row(
                "web|1",
                first_name="Rohan",
                last_name="Sharma",
                city="Bengaluru",
                postcode="560001",
                phone_e164="+919876143210",
            ),
            # Same common name, different person, different city and contacts.
            silver_row(
                "web|2",
                first_name="Priya",
                last_name="Sharma",
                city="Mumbai",
                postcode="400001",
                email="psharma77@yahoo.com",
                phone_e164="+919811122233",
            ),
            # Same name and postcode but no shared identifier.
            silver_row(
                "mobile|1", first_name="Priya", last_name="Sharma", city="Bengaluru", postcode="560001"
            ),
        ],
    )


def test_profile_labels_household_identifier(people, settings) -> None:  # type: ignore[no-untyped-def]
    profile = profile_identifiers(people, settings)
    labels = {(r.id_type, r.distinct_first_names): r.label for r in profile.collect()}
    assert labels[("phone_token", 2)] == "household"  # Priya + Rohan on one phone
    assert labels[("email_token", 1)] == "unique"


def test_shared_identifier_is_labelled_shared(spark, settings) -> None:  # type: ignore[no-untyped-def]
    names = ["Asha", "Bina", "Chitra", "Divya", "Esha", "Farah"]
    df = silver_frame(
        spark,
        [
            silver_row(f"pos_loyalty|{i}", first_name=n, last_name="X", phone_e164="+911111122222")
            for i, n in enumerate(names)
        ],
    )
    profile = profile_identifiers(df, settings)
    assert profile.filter("id_type = 'phone_token'").first().label == "shared"


def test_blocking_pairs_are_unordered_and_skip_large_blocks(people, settings) -> None:  # type: ignore[no-untyped-def]
    records = attach_identifier_labels(people, profile_identifiers(people, settings))
    pairs, skipped = candidate_pairs(records, ["email_token", "phone_token", "surname_soundex_postcode"], 200)
    rows = pairs.collect()
    assert all(r.left_id < r.right_id for r in rows)
    assert {("crm|1", "erp_a|1")} <= {(r.left_id, r.right_id) for r in rows}
    assert all(":" not in k for r in rows for k in r.block_keys)  # key types only, never values
    assert skipped.count() == 0
    _, skipped_small = candidate_pairs(records, ["surname_soundex_postcode"], max_block_size=2)
    assert skipped_small.count() == 1


def test_do_not_match_pairs_are_excluded(people, settings, spark) -> None:  # type: ignore[no-untyped-def]
    records = attach_identifier_labels(people, profile_identifiers(people, settings))
    dnm = spark.createDataFrame([("crm|1", "erp_a|1")], "left_id string, right_id string")
    pairs, _ = candidate_pairs(records, ["email_token"], 200, dnm)
    assert pairs.filter("left_id = 'crm|1' AND right_id = 'erp_a|1'").count() == 0


@pytest.fixture(scope="module")
def matched(people, settings):  # type: ignore[no-untyped-def]
    profile = profile_identifiers(people, settings)
    candidates, _ = match_records(people, profile, settings, HeuristicAIAdapter())
    candidates = candidates.cache()
    return {(r.left_id, r.right_id): r for r in candidates.collect()}, candidates


def test_match_records_contract(matched) -> None:  # type: ignore[no-untyped-def]
    _, df = matched
    assert tuple(df.columns) == CANDIDATE_COLUMNS
    assert (
        df.filter(
            F.col("final_score").isNull() | (F.col("final_score") < 0) | (F.col("final_score") > 1)
        ).count()
        == 0
    )


def test_same_person_auto_matches(matched) -> None:  # type: ignore[no-untyped-def]
    by_pair, _ = matched
    same = by_pair[("crm|1", "erp_a|1")]
    assert same.band == BAND_HIGH and same.decision == "match"
    assert "same email" in same.rule_explanation


def test_household_is_not_auto_merged(matched) -> None:  # type: ignore[no-untyped-def]
    by_pair, _ = matched
    pair = by_pair.get(("crm|1", "web|1"))
    assert pair is None or pair.decision != "match"


def test_name_and_postcode_alone_go_to_review(matched) -> None:  # type: ignore[no-untyped-def]
    by_pair, _ = matched
    pair = by_pair.get(("crm|1", "mobile|1"))
    assert pair is not None
    assert pair.band == BAND_LOW and pair.decision == "review"
    assert "no shared email" in pair.rule_explanation


def test_ai_signal_and_blend(spark, settings) -> None:  # type: ignore[no-untyped-def]
    cfg = settings.matching
    df = spark.createDataFrame(
        [(0.6, 1.0, cfg.ai_labels[0]), (0.6, -1.0, cfg.ai_labels[1]), (0.6, None, None)],
        "model_score double, ai_similarity double, ai_verdict string",
    )
    out = [r[0] for r in df.select(blend(F.col("model_score"), ai_signal(cfg), 0.3)).collect()]
    assert out[0] == pytest.approx(0.7 * 0.6 + 0.3 * 1.0)
    assert out[1] == pytest.approx(0.7 * 0.6)
    assert out[2] == pytest.approx(0.6)  # AI missing -> model score kept (graceful degradation)
