"""Builders for small Silver-shaped DataFrames used across tests."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from goldenrecord.tokenise import hmac_token

KEY = b"unit-test-key"

SILVER_SCHEMA = (
    "record_id string, source_system string, source_record_id string, source_trust int, "
    "source_updated_at timestamp, first_name string, last_name string, email string, email_domain string, "
    "email_token string, phone_e164 string, phone_token string, address_line string, city string, "
    "postcode string, country string, tax_id string, loyalty_id string, consent_email string, "
    "consent_sms string, consent_whatsapp string"
)

TRUST = {"crm": 90, "erp_a": 80, "erp_b": 75, "pos_loyalty": 60, "mobile": 55, "web": 50}


def silver_row(record_id: str, **values: Any) -> tuple[Any, ...]:
    source, source_id = record_id.split("|", 1)
    email = values.get("email")
    phone = values.get("phone_e164")
    row = {
        "record_id": record_id,
        "source_system": source,
        "source_record_id": source_id,
        "source_trust": values.get("source_trust", TRUST.get(source, 50)),
        "source_updated_at": values.get("source_updated_at", datetime(2025, 1, 1)),
        "first_name": values.get("first_name"),
        "last_name": values.get("last_name"),
        "email": email,
        "email_domain": email.split("@")[1] if email else None,
        "email_token": hmac_token(KEY, email),
        "phone_e164": phone,
        "phone_token": hmac_token(KEY, phone),
        "address_line": values.get("address_line"),
        "city": values.get("city"),
        "postcode": values.get("postcode"),
        "country": values.get("country", "IN"),
        "tax_id": values.get("tax_id"),
        "loyalty_id": values.get("loyalty_id"),
        "consent_email": values.get("consent_email", "unknown"),
        "consent_sms": values.get("consent_sms", "unknown"),
        "consent_whatsapp": values.get("consent_whatsapp", "unknown"),
    }
    return tuple(row.values())


def silver_frame(spark: Any, rows: list[tuple[Any, ...]]) -> Any:
    return spark.createDataFrame(rows, SILVER_SCHEMA)
