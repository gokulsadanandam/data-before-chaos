"""Tier 1 standardisation rules (Bronze -> Silver)."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord import standardise as st
from goldenrecord.ingest import to_bronze

pytestmark = pytest.mark.spark


def _one(spark, values, fn):  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([(v,) for v in values], "raw string")
    return [r[0] for r in df.select(fn(F.col("raw"))).collect()]


def test_email_typos_case_and_placeholders(spark, settings) -> None:  # type: ignore[no-untyped-def]
    ref = settings.reference
    out = _one(
        spark,
        [" Priya.Sharma@GMIAL.com ", "test@test.com", "not-an-email", None, "a.b@outlook.com"],
        lambda c: st.standardise_email(c, ref)["email"],
    )
    assert out == ["priya.sharma@gmail.com", None, None, None, "a.b@outlook.com"]


def test_email_flags(spark, settings) -> None:  # type: ignore[no-untyped-def]
    ref = settings.reference
    df = spark.createDataFrame([("x@gmial.com",), ("bad@",), ("noemail@x.com",)], "raw string")
    e = st.standardise_email(F.col("raw"), ref)
    rows = df.select(
        e["flag_email_typo_fixed"].alias("typo"),
        e["flag_email_invalid"].alias("inv"),
        e["flag_email_placeholder"].alias("ph"),
    ).collect()
    assert rows[0]["typo"] == "email_typo_fixed"
    assert rows[1]["inv"] == "email_invalid"
    assert rows[2]["ph"] == "email_placeholder"


# Note: 9876543210 itself is a placeholder in the reference data, so tests use 9876143210.
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("98761 43210", "+919876143210"),
        ("+91 98761 43210", "+919876143210"),
        ("09876143210", "+919876143210"),
        ("0091-98761-43210", "+919876143210"),
        ("919876143210", "+919876143210"),
        ("(987) 614-3210", "+919876143210"),
        ("9999999999", None),  # placeholder
        ("9876543210", None),  # listed placeholder
        ("12345", None),  # too short
        (None, None),
    ],
)
def test_phone_to_e164(spark, settings, raw, expected) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([(raw, "IN")], "raw string, country string")
    phone = st.standardise_phone(F.col("raw"), F.col("country"), settings.reference, "91", 8, 15)
    assert df.select(phone["phone_e164"]).collect()[0][0] == expected


def test_phone_uses_country_calling_code(spark, settings) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([("07700 900123", "GB")], "raw string, country string")
    phone = st.standardise_phone(F.col("raw"), F.col("country"), settings.reference, "91", 8, 15)
    assert df.select(phone["phone_e164"]).collect()[0][0] == "+447700900123"


def test_country_aliases(spark, settings) -> None:  # type: ignore[no-untyped-def]
    out = _one(
        spark,
        ["India", "IND", "in", "Bharat", "UK", "Atlantis", None],
        lambda c: st.standardise_country(c, settings.reference)["country"],
    )
    assert out == ["IN", "IN", "IN", "IN", "GB", None, None]


def test_consent_values(spark) -> None:  # type: ignore[no-untyped-def]
    out = _one(spark, ["Y", "true", "Opt-In", "N", "0", "maybe", "", None], st.standardise_consent)
    assert out == ["yes", "yes", "yes", "no", "no", "unknown", "unknown", "unknown"]


def test_split_full_name(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame(
        [("Sharma, Priya",), ("Mr. Rahul K Verma",), ("Iyer",), ("  priya   sharma ",)], "raw string"
    )
    first, last = st.split_full_name(F.col("raw"))
    rows = [(r[0], r[1]) for r in df.select(first, last).collect()]
    assert rows == [("priya", "sharma"), ("rahul", "verma"), (None, "iyer"), ("priya", "sharma")]


def test_parse_timestamp_formats(spark) -> None:  # type: ignore[no-untyped-def]
    out = _one(
        spark,
        [
            "20250319",
            "19/03/2025",
            "19-03-2025",
            "2025-03-19T10:00:00",
            "2025-03-19 10:00:00",
            "garbage",
            None,
        ],
        st.parse_timestamp,
    )
    assert [o.date().isoformat() if o else None for o in out] == ["2025-03-19"] * 5 + [None, None]


def test_standardise_source_end_to_end(spark, settings) -> None:  # type: ignore[no-untyped-def]
    spec = settings.source("crm")
    raw = spark.createDataFrame(
        [
            (
                "CRM-1",
                "Priya",
                "SHARMA",
                "priya@gmial.com",
                "+91 98761 43210",
                "12 MG Road",
                "bengaluru",
                "560001",
                "India",
                "true",
                "N",
                "",
                "2025-03-19T10:00:00",
            ),
            ("CRM-2", "Test", "Customer", "test@test.com", "9999999999", "", "", "", "", "", "", "", ""),
        ],
        "ContactId string, FirstName string, LastName string, Email string, MobilePhone string, "
        "Street string, City string, PostalCode string, Country string, EmailOptIn string, "
        "SmsOptIn string, WhatsAppOptIn string, ModifiedOn string",
    )
    out = st.standardise_source(to_bronze(raw, "crm", "ContactId", "b1"), spec, settings)
    df = out.withColumn("reason", st.quarantine_reason(out)).withColumn(
        "status", st.dq_status(F.col("dq_flags"))
    )
    rows = {r["source_record_id"]: r for r in df.collect()}
    good = rows["CRM-1"]
    assert good["record_id"] == "crm|CRM-1"
    assert (good["first_name"], good["last_name"]) == ("Priya", "Sharma")
    assert good["email"] == "priya@gmail.com" and good["email_domain"] == "gmail.com"
    assert good["phone_e164"] == "+919876143210"
    assert good["city"] == "Bengaluru" and good["country"] == "IN"
    assert (good["consent_email"], good["consent_sms"], good["consent_whatsapp"]) == ("yes", "no", "unknown")
    assert good["source_trust"] == spec.trust and good["source_updated_at"] is not None
    assert "email_typo_fixed" in good["dq_flags"] and good["reason"] is None and good["status"] == "fixed"
    junk = rows["CRM-2"]
    assert junk["reason"] == "placeholder_record"
    assert junk["email"] is None and junk["phone_e164"] is None


def test_to_bronze_hash_is_stable(spark) -> None:  # type: ignore[no-untyped-def]
    raw = spark.createDataFrame([("1", "a"), ("1", "a"), ("2", "b")], "id string, v string")
    bronze = to_bronze(raw, "src", "id", "batch")
    assert bronze.count() == 2  # exact re-delivered row de-duplicated by hash
    again = to_bronze(raw, "src", "id", "batch2")
    assert sorted(r.row_hash for r in bronze.collect()) == sorted(r.row_hash for r in again.collect())
