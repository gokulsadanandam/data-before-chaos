"""Silver build: latest version of every source record, standardised, tokenised and split
into clean records and quarantine.

Silver is recomputed from Bronze on every run (snapshot recompute). That keeps it
idempotent and lets a rule change be applied to all history in one run.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import Settings
from goldenrecord.schema import INGESTED_AT, SOURCE_RECORD_ID
from goldenrecord.standardise import dq_status, quarantine_reason, standardise_source
from goldenrecord.store import TableStore
from goldenrecord.tokenise import token_column


def latest_versions(bronze: DataFrame) -> DataFrame:
    """Keep the most recently ingested version of each source record."""
    window = Window.partitionBy(SOURCE_RECORD_ID).orderBy(F.col(INGESTED_AT).desc(), F.col("row_hash"))
    return bronze.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def build_silver(store: TableStore, settings: Settings, token_key: bytes) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantine) DataFrames for all configured sources."""
    prefix = settings.table("bronze_prefix")
    standardised: list[DataFrame] = []
    for spec in settings.sources:
        table = f"{prefix}{spec.name}"
        if not store.exists(table):
            continue
        standardised.append(standardise_source(latest_versions(store.read(table)), spec, settings))
    if not standardised:
        raise RuntimeError("No Bronze tables found: run the ingest stage first")

    unioned = reduce(lambda a, b: a.unionByName(b), standardised)
    enriched = (
        unioned.withColumn("email_token", token_column(F.col("email"), token_key))
        .withColumn("phone_token", token_column(F.col("phone_e164"), token_key))
        .withColumn("quarantine_reason", quarantine_reason(unioned))
        .withColumn("dq_status", dq_status(F.col("dq_flags")))
        .withColumn("updated_at", F.current_timestamp())
    )
    clean = enriched.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    quarantine = enriched.filter(F.col("quarantine_reason").isNotNull()).withColumn(
        "dq_status", F.lit("quarantined")
    )
    return clean, quarantine
