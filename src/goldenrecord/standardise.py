"""Tier 1: deterministic standardisation and validation rules (Bronze -> Silver).

Every rule is a pure Spark ``Column`` expression so it can be unit tested on a tiny
DataFrame and runs fully distributed at scale. The code is ANSI-mode safe (no casts or
array lookups that throw on bad input), so it behaves the same on Fabric Runtime 1.3
(Spark 3.5) and Runtime 2.0 (Spark 4.x).

Rules fix what they can and record what they did in ``dq_flags``. Records with no
usable identity are quarantined with a reason code instead of being dropped.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812 - conventional alias

from goldenrecord.config import ReferenceData, Settings, SourceSpec
from goldenrecord.schema import (
    CONSENT_COLUMNS,
    CONSENT_NO,
    CONSENT_UNKNOWN,
    CONSENT_YES,
    RECORD_ID,
    SOURCE_RECORD_ID,
    SOURCE_SYSTEM,
    SOURCE_UPDATED_AT,
)

EMAIL_PATTERN = r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"
SALUTATIONS = r"^(mr|mrs|ms|miss|dr|prof|shri|smt|sri|mx)\.?\s+"

FIX_FLAGS = frozenset({"email_typo_fixed", "phone_reformatted", "country_mapped", "name_split"})
INVALID_FLAGS = frozenset(
    {
        "email_invalid",
        "email_placeholder",
        "phone_invalid",
        "phone_placeholder",
        "country_unknown",
        "name_placeholder",
    }
)

YES_VALUES = ("y", "yes", "true", "1", "opt-in", "optin", "opted in", "subscribed")
NO_VALUES = ("n", "no", "false", "0", "opt-out", "optout", "opted out", "unsubscribed")


def _map_literal(mapping: Mapping[str, str]) -> Column:
    """Build a literal MapType column from a small Python dict (reference data)."""
    if not mapping:
        return F.create_map().cast("map<string,string>")
    items: list[Column] = []
    for key, value in sorted(mapping.items()):
        items.extend([F.lit(key), F.lit(value)])
    return F.create_map(*items)


def _in_set(col: Column, values: Iterable[str]) -> Column:
    values = sorted(values)
    return col.isin(values) if values else F.lit(False)


def blank_to_null(col: Column) -> Column:
    trimmed = F.trim(col.cast("string"))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


# ----------------------------------------------------------------------------- names
def clean_name_text(col: Column) -> Column:
    """Lower-case, strip salutations and anything that is not a letter, space, hyphen or apostrophe."""
    lowered = F.lower(blank_to_null(col))
    no_salutation = F.regexp_replace(lowered, SALUTATIONS, "")
    letters = F.regexp_replace(no_salutation, r"[^a-z\s,'\-]", " ")
    return blank_to_null(F.regexp_replace(letters, r"\s+", " "))


def split_full_name(full: Column) -> tuple[Column, Column]:
    """Split a full name into (first, last).

    ``"Sharma, Priya"`` -> (priya, sharma); ``"Priya K Sharma"`` -> (priya, sharma);
    a single token is treated as the last name.
    """
    cleaned = clean_name_text(full)
    has_comma = F.instr(cleaned, ",") > 0
    comma_last = F.trim(F.substring_index(cleaned, ",", 1))
    comma_first = F.trim(F.substring_index(cleaned, ",", -1))
    no_comma = F.regexp_replace(cleaned, ",", " ")
    tokens = F.split(F.trim(F.regexp_replace(no_comma, r"\s+", " ")), " ")
    n = F.size(tokens)
    first = F.when(has_comma, comma_first).when(n > 1, F.try_element_at(tokens, F.lit(1)))
    last = F.when(has_comma, comma_last).otherwise(F.try_element_at(tokens, F.lit(-1)))
    first_token = F.try_element_at(F.split(F.trim(first), " "), F.lit(1))
    return blank_to_null(first_token), blank_to_null(last)


def display_case(col: Column) -> Column:
    return F.initcap(col)


# ----------------------------------------------------------------------------- email
def standardise_email(raw: Column, ref: ReferenceData) -> dict[str, Column]:
    """Return email, email_domain and flags for one raw email column."""
    email = F.lower(F.regexp_replace(blank_to_null(raw), r"\s+", ""))
    local = F.substring_index(email, "@", 1)
    domain = F.substring_index(email, "@", -1)
    fixed_domain = F.coalesce(F.try_element_at(_map_literal(ref.email_domain_typos), domain), domain)
    typo_fixed = email.isNotNull() & (fixed_domain != domain)
    candidate = F.when(email.isNotNull(), F.concat_ws("@", local, fixed_domain))
    is_valid = candidate.rlike(EMAIL_PATTERN)
    is_placeholder = _in_set(local, ref.placeholder_email_locals) | _in_set(
        fixed_domain, ref.placeholder_email_domains
    )
    final = F.when(is_valid & ~is_placeholder, candidate)
    return {
        "email": final,
        "email_domain": F.when(final.isNotNull(), fixed_domain),
        "flag_email_typo_fixed": F.when(typo_fixed & final.isNotNull(), F.lit("email_typo_fixed")),
        "flag_email_invalid": F.when(email.isNotNull() & ~is_valid, F.lit("email_invalid")),
        "flag_email_placeholder": F.when(
            email.isNotNull() & is_valid & is_placeholder, F.lit("email_placeholder")
        ),
    }


# ----------------------------------------------------------------------------- country
def standardise_country(raw: Column, ref: ReferenceData) -> dict[str, Column]:
    lowered = F.lower(blank_to_null(raw))
    mapped = F.try_element_at(_map_literal(ref.country_aliases), lowered)
    return {
        "country": mapped,
        "flag_country_mapped": F.when(
            mapped.isNotNull() & (F.upper(F.trim(raw)) != mapped), F.lit("country_mapped")
        ),
        "flag_country_unknown": F.when(lowered.isNotNull() & mapped.isNull(), F.lit("country_unknown")),
    }


# ----------------------------------------------------------------------------- phone
def standardise_phone(
    raw: Column,
    country_iso2: Column,
    ref: ReferenceData,
    default_calling_code: str,
    min_digits: int,
    max_digits: int,
) -> dict[str, Column]:
    """Normalise a phone number to E.164 (``+<country code><number>``)."""
    text = blank_to_null(raw)
    has_plus = F.trim(text).startswith("+")
    digits = F.regexp_replace(text, r"[^0-9]", "")
    intl_00 = digits.startswith("00")
    digits_no00 = F.when(intl_00, F.regexp_replace(digits, r"^00", "")).otherwise(digits)
    calling = F.coalesce(
        F.try_element_at(_map_literal(ref.calling_codes), country_iso2), F.lit(default_calling_code)
    )
    national_len = F.length(digits)
    e164_digits = (
        F.when(has_plus | intl_00, digits_no00)
        .when(national_len == 10, F.concat(calling, digits))
        .when((national_len == 11) & digits.startswith("0"), F.concat(calling, F.substring(digits, 2, 10)))
        .when(digits.startswith(calling) & (national_len == F.length(calling) + 10), digits)
    )
    n = F.length(e164_digits)
    valid_len = (n >= min_digits) & (n <= max_digits)
    national = F.substring(e164_digits, -10, 10)
    is_placeholder = _in_set(national, ref.placeholder_phones) | national.rlike(r"^(\d)\1+$")
    final = F.when(
        valid_len & ~is_placeholder & ~e164_digits.startswith("0"), F.concat(F.lit("+"), e164_digits)
    )
    return {
        "phone_e164": final,
        "flag_phone_reformatted": F.when(
            final.isNotNull() & (F.trim(text) != final), F.lit("phone_reformatted")
        ),
        "flag_phone_invalid": F.when(
            text.isNotNull() & (e164_digits.isNull() | ~valid_len), F.lit("phone_invalid")
        ),
        "flag_phone_placeholder": F.when(
            text.isNotNull() & valid_len & is_placeholder, F.lit("phone_placeholder")
        ),
    }


# ----------------------------------------------------------------------------- simple fields
def standardise_postcode(raw: Column) -> Column:
    return blank_to_null(F.upper(F.regexp_replace(raw.cast("string"), r"\s+", "")))


def standardise_city(raw: Column) -> Column:
    return display_case(blank_to_null(F.regexp_replace(F.lower(raw.cast("string")), r"\s+", " ")))


def standardise_address(raw: Column) -> Column:
    return blank_to_null(F.regexp_replace(F.trim(raw.cast("string")), r"\s+", " "))


def standardise_code(raw: Column) -> Column:
    """Tax IDs and loyalty IDs: upper-case alphanumerics only."""
    return blank_to_null(F.upper(F.regexp_replace(raw.cast("string"), r"[^A-Za-z0-9]", "")))


def standardise_consent(raw: Column) -> Column:
    value = F.lower(blank_to_null(raw))
    return (
        F.when(value.isin(*YES_VALUES), F.lit(CONSENT_YES))
        .when(value.isin(*NO_VALUES), F.lit(CONSENT_NO))
        .otherwise(F.lit(CONSENT_UNKNOWN))
    )


def parse_timestamp(raw: Column) -> Column:
    """Parse the timestamp formats seen in the extracts without failing on bad values."""
    text = blank_to_null(raw)
    return F.coalesce(
        F.try_to_timestamp(text),
        F.try_to_timestamp(text, F.lit("yyyyMMdd")),
        F.try_to_timestamp(text, F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(text, F.lit("dd-MM-yyyy")),
        F.try_to_timestamp(text, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
    )


# ----------------------------------------------------------------------------- per-source
def _source_col(df: DataFrame, mapping: Mapping[str, str], attribute: str) -> Column:
    """The mapped source column, or NULL when the source does not provide the attribute."""
    column = mapping.get(attribute)
    if column is None or column not in df.columns:
        return F.lit(None).cast("string")
    return F.col(f"`{column}`").cast("string")


def standardise_source(bronze: DataFrame, spec: SourceSpec, settings: Settings) -> DataFrame:
    """Map one Bronze source onto the canonical model and apply Tier 1 rules."""
    ref = settings.reference
    std = settings.section("standardisation")
    m = spec.mapping

    def src(attr: str) -> Column:
        return _source_col(bronze, m, attr)

    split_first, split_last = split_full_name(src("full_name"))
    first_raw = clean_name_text(src("first_name"))
    last_raw = clean_name_text(src("last_name"))
    first = F.coalesce(first_raw, split_first)
    last = F.coalesce(last_raw, split_last)
    name_key = F.trim(F.concat_ws(" ", first, last))
    name_is_placeholder = _in_set(name_key, ref.placeholder_names) | _in_set(last, ref.placeholder_names)

    country = standardise_country(src("country"), ref)
    email = standardise_email(src("email"), ref)
    phone = standardise_phone(
        src("phone"),
        country["country"],
        ref,
        str(std.get("default_calling_code", "91")),
        int(std.get("min_phone_digits", 8)),
        int(std.get("max_phone_digits", 15)),
    )

    flags = [
        F.when(first_raw.isNull() & last_raw.isNull() & split_last.isNotNull(), F.lit("name_split")),
        F.when(name_is_placeholder, F.lit("name_placeholder")),
        email["flag_email_typo_fixed"],
        email["flag_email_invalid"],
        email["flag_email_placeholder"],
        phone["flag_phone_reformatted"],
        phone["flag_phone_invalid"],
        phone["flag_phone_placeholder"],
        country["flag_country_mapped"],
        country["flag_country_unknown"],
    ]
    consent_cols = {c: standardise_consent(src(c)) for c in CONSENT_COLUMNS}
    updated_col = spec.updated_at_column
    source_updated = (
        parse_timestamp(F.col(f"`{updated_col}`"))
        if updated_col and updated_col in bronze.columns
        else F.lit(None).cast("timestamp")
    )

    return bronze.select(
        F.col(SOURCE_SYSTEM),
        F.col(SOURCE_RECORD_ID),
        F.concat_ws("|", F.col(SOURCE_SYSTEM), F.col(SOURCE_RECORD_ID)).alias(RECORD_ID),
        F.col("batch_id"),
        F.col("row_hash"),
        source_updated.alias(SOURCE_UPDATED_AT),
        F.lit(spec.trust).alias("source_trust"),
        F.when(~name_is_placeholder, display_case(first)).alias("first_name"),
        F.when(~name_is_placeholder, display_case(last)).alias("last_name"),
        email["email"].alias("email"),
        email["email_domain"].alias("email_domain"),
        phone["phone_e164"].alias("phone_e164"),
        standardise_address(src("address_line")).alias("address_line"),
        standardise_city(src("city")).alias("city"),
        standardise_postcode(src("postcode")).alias("postcode"),
        country["country"].alias("country"),
        standardise_code(src("tax_id")).alias("tax_id"),
        standardise_code(src("loyalty_id")).alias("loyalty_id"),
        *[col.alias(name) for name, col in consent_cols.items()],
        F.filter(F.array(*flags), lambda x: x.isNotNull()).alias("dq_flags"),
    )


def quarantine_reason(df: DataFrame) -> Column:
    """Reason code for records that cannot be used for matching, else NULL."""
    has_contact = F.col("email").isNotNull() | F.col("phone_e164").isNotNull()
    has_strong_id = F.col("loyalty_id").isNotNull() | F.col("tax_id").isNotNull()
    placeholder = F.array_contains(F.col("dq_flags"), "name_placeholder")
    return (
        F.when(placeholder & ~has_contact & ~has_strong_id, F.lit("placeholder_record"))
        .when(F.col("last_name").isNull() & ~has_contact & ~has_strong_id, F.lit("no_identity"))
        .when(F.col("last_name").isNull() & F.col("first_name").isNull() & ~has_strong_id, F.lit("no_name"))
    )


def dq_status(flags: Column) -> Column:
    has_invalid = F.size(F.filter(flags, lambda f: f.isin(*sorted(INVALID_FLAGS)))) > 0
    has_fix = F.size(F.filter(flags, lambda f: f.isin(*sorted(FIX_FLAGS)))) > 0
    return F.when(has_invalid, F.lit("flagged")).when(has_fix, F.lit("fixed")).otherwise(F.lit("clean"))
