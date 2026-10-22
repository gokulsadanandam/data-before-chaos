"""Ingestion: land source extracts in Bronze exactly as received.

Bronze is append-only. Every row gets its source system, source record id, batch id,
ingestion time and a SHA-256 ``row_hash`` of its full payload. A re-delivered row with
the same hash is skipped, so a pipeline can be re-run safely (idempotent ingestion).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from goldenrecord.config import EngagementSpec, OrderSpec, SourceSpec
from goldenrecord.schema import BATCH_ID, INGESTED_AT, PAYLOAD, ROW_HASH, SOURCE_RECORD_ID, SOURCE_SYSTEM
from goldenrecord.store import TableStore, append_or_create


def read_files(
    spark: SparkSession, landing_root: str, fmt: str, path: str, options: dict[str, str]
) -> DataFrame:
    reader = spark.read.format(fmt).options(**options)
    if fmt == "csv":
        # Keep every value as text in Bronze; typing happens in Silver where bad values are flagged.
        reader = reader.option("inferSchema", "false").option("mode", "PERMISSIVE")
    return reader.load(f"{landing_root.rstrip('/')}/{path}")


def to_bronze(raw: DataFrame, source_system: str, id_column: str, batch_id: str) -> DataFrame:
    """Add lineage metadata and a payload hash to a raw extract."""
    columns = sorted(c for c in raw.columns if not c.startswith("_corrupt"))
    as_text = raw.select(*[F.col(f"`{c}`").cast("string").alias(c) for c in columns])
    payload = F.to_json(F.struct(*[F.col(f"`{c}`") for c in columns]))
    return (
        as_text.withColumn(PAYLOAD, payload)
        .withColumn(ROW_HASH, F.sha2(F.col(PAYLOAD), 256))
        .withColumn(SOURCE_SYSTEM, F.lit(source_system))
        .withColumn(SOURCE_RECORD_ID, F.col(f"`{id_column}`").cast("string"))
        .withColumn(BATCH_ID, F.lit(batch_id))
        .withColumn(INGESTED_AT, F.current_timestamp())
        .dropDuplicates([ROW_HASH])
    )


def append_new_rows(store: TableStore, table: str, bronze: DataFrame) -> int:
    """Append rows whose hash is not yet in the table; returns the number appended."""
    if store.exists(table):
        existing = store.read(table).select(ROW_HASH).distinct()
        new_rows = bronze.join(existing, on=ROW_HASH, how="left_anti")
    else:
        new_rows = bronze
    new_rows = new_rows.cache()
    count = new_rows.count()
    if count:
        append_or_create(store, table, new_rows)
    new_rows.unpersist()
    return count


def ingest_customers(
    spark: SparkSession, store: TableStore, landing_root: str, spec: SourceSpec, table: str, batch_id: str
) -> int:
    raw = read_files(spark, landing_root, spec.format, spec.path, spec.options)
    if spec.id_column not in raw.columns:
        raise ValueError(f"Source '{spec.name}' is missing its id column '{spec.id_column}'")
    return append_new_rows(store, table, to_bronze(raw, spec.name, spec.id_column, batch_id))


def ingest_orders(
    spark: SparkSession,
    store: TableStore,
    landing_root: str,
    source: str,
    fmt: str,
    options: dict[str, str],
    orders: OrderSpec,
    table: str,
    batch_id: str,
) -> int:
    raw = read_files(spark, landing_root, fmt, orders.path, options)
    return append_new_rows(store, table, to_bronze(raw, source, orders.id_column, batch_id))


def ingest_engagement(
    spark: SparkSession, store: TableStore, landing_root: str, spec: EngagementSpec, table: str, batch_id: str
) -> int:
    raw = read_files(spark, landing_root, spec.format, spec.path, spec.options)
    return append_new_rows(store, table, to_bronze(raw, "engagement", "send_id", batch_id))
