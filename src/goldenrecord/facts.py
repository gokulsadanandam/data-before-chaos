"""Gold facts keyed by master key: orders (spend) and campaign engagement.

These power the executive spend report: spend and reach are counted once per golden
customer, and sends to duplicates or to customers without consent are made visible.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import Settings
from goldenrecord.standardise import parse_timestamp
from goldenrecord.store import TableStore


def _latest(df: DataFrame, key: str) -> DataFrame:
    window = Window.partitionBy("source_system", key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def build_orders(store: TableStore, settings: Settings, xref: DataFrame) -> DataFrame | None:
    prefix = settings.table("bronze_orders_prefix")
    parts: list[DataFrame] = []
    for spec in settings.sources:
        if spec.orders is None or not store.exists(f"{prefix}{spec.name}"):
            continue
        o = spec.orders
        raw = _latest(store.read(f"{prefix}{spec.name}"), "source_record_id")
        parts.append(
            raw.select(
                F.lit(spec.name).alias("source_system"),
                F.col(f"`{o.id_column}`").alias("order_id"),
                F.col(f"`{o.customer_column}`").alias("source_customer_id"),
                parse_timestamp(F.col(f"`{o.date_column}`")).alias("order_ts"),
                F.expr(f"try_cast(`{o.amount_column}` as double)").alias("amount"),
                F.lit(o.channel).alias("channel"),
            )
        )
    if not parts:
        return None
    orders = reduce(lambda a, b: a.unionByName(b), parts)
    keys = xref.select("source_system", F.col("source_record_id").alias("source_customer_id"), "master_key")
    return (
        orders.join(keys, ["source_system", "source_customer_id"], "left")
        .withColumn("is_unmatched", F.col("master_key").isNull())
        .withColumn("order_date", F.to_date("order_ts"))
    )


def build_engagement(
    store: TableStore, settings: Settings, xref: DataFrame, golden: DataFrame
) -> DataFrame | None:
    table = settings.table("bronze_engagement")
    if not store.exists(table):
        return None
    raw = _latest(store.read(table), "source_record_id")
    sends = raw.select(
        F.col("send_id"),
        F.col("customer_source").alias("source_system"),
        F.col("customer_source_id").alias("source_customer_id"),
        F.col("campaign"),
        F.lower(F.col("channel")).alias("channel"),
        F.expr("try_cast(cost as double)").alias("cost"),
        parse_timestamp(F.col("sent_at")).alias("sent_ts"),
        F.col("opened").isin("1", "true", "yes").alias("opened"),
        F.col("clicked").isin("1", "true", "yes").alias("clicked"),
        F.col("converted").isin("1", "true", "yes").alias("converted"),
    )
    keys = xref.select("source_system", F.col("source_record_id").alias("source_customer_id"), "master_key")
    consent = golden.filter("is_current").select(
        "master_key", "consent_email", "consent_sms", "consent_whatsapp"
    )
    joined = sends.join(keys, ["source_system", "source_customer_id"], "left").join(
        consent, "master_key", "left"
    )
    consent_for_channel = (
        F.when(F.col("channel") == "email", F.col("consent_email"))
        .when(F.col("channel") == "sms", F.col("consent_sms"))
        .when(F.col("channel") == "whatsapp", F.col("consent_whatsapp"))
    )
    dup_window = Window.partitionBy("master_key", "campaign", "channel").orderBy("sent_ts", "send_id")
    return (
        joined.withColumn("sent_without_consent", F.coalesce(consent_for_channel, F.lit("no")) != "yes")
        .withColumn(
            "duplicate_send",
            F.when(F.col("master_key").isNotNull(), F.row_number().over(dup_window) > 1).otherwise(
                F.lit(False)
            ),
        )
        .withColumn("sent_date", F.to_date("sent_ts"))
        .drop("consent_email", "consent_sms", "consent_whatsapp")
    )
