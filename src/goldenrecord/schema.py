"""Canonical column names shared by every stage.

Keeping names in one place avoids typos across modules and documents the data contract
between workstreams (see docs/data-contracts.md).
"""

from __future__ import annotations

# Canonical attributes a source can map onto (config/sources.yaml -> mapping).
SOURCE_ATTRIBUTES: tuple[str, ...] = (
    "full_name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "address_line",
    "city",
    "postcode",
    "country",
    "tax_id",
    "loyalty_id",
    "consent_email",
    "consent_sms",
    "consent_whatsapp",
)

CONSENT_COLUMNS: tuple[str, ...] = ("consent_email", "consent_sms", "consent_whatsapp")

# Metadata added at ingestion.
SOURCE_SYSTEM = "source_system"
SOURCE_RECORD_ID = "source_record_id"
RECORD_ID = "record_id"  # f"{source_system}|{source_record_id}"
SOURCE_UPDATED_AT = "source_updated_at"
BATCH_ID = "batch_id"
INGESTED_AT = "ingested_at"
ROW_HASH = "row_hash"
PAYLOAD = "payload"

# Silver standardised attributes (the golden record is built from these).
SILVER_ATTRIBUTES: tuple[str, ...] = (
    "first_name",
    "last_name",
    "email",
    "email_domain",
    "phone_e164",
    "address_line",
    "city",
    "postcode",
    "country",
    "tax_id",
    "loyalty_id",
    *CONSENT_COLUMNS,
)

# Attributes that survive into the golden record.
GOLDEN_ATTRIBUTES: tuple[str, ...] = (
    "first_name",
    "last_name",
    "email",
    "phone_e164",
    "address_line",
    "city",
    "postcode",
    "country",
    "tax_id",
    "loyalty_id",
    *CONSENT_COLUMNS,
)

# Consent values after standardisation.
CONSENT_YES = "yes"
CONSENT_NO = "no"
CONSENT_UNKNOWN = "unknown"

# Match decisions and bands.
BAND_HIGH = "HIGH"
BAND_MEDIUM = "MEDIUM"
BAND_LOW = "LOW"

DECISION_MATCH = "match"
DECISION_NO_MATCH = "no_match"
DECISION_REVIEW = "review"

DECIDED_BY_RULE = "rule"
DECIDED_BY_MODEL = "model"
DECIDED_BY_STEWARD = "steward"

STEWARD_APPROVE = "APPROVE"
STEWARD_REJECT = "REJECT"
STEWARD_HOLD = "HOLD"
