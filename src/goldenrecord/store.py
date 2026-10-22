"""Table storage abstraction.

Jobs never call ``spark.read`` / ``df.write`` directly. They go through a ``TableStore``
so the same code runs:

* in Fabric, against Lakehouse Delta tables (:class:`LakehouseTableStore`), and
* locally / in CI, against folders of Parquet or Delta files (:class:`PathTableStore`).

Writes are whole-table overwrites or appends. Overwrites in Delta are atomic and keep
history (time travel), which makes every stage idempotent: re-running a stage with the
same inputs produces the same table (see docs/adr/0003-snapshot-recompute.md).
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession


class TableStore(Protocol):
    """Minimal storage contract used by every job."""

    def exists(self, name: str) -> bool: ...

    def read(self, name: str) -> DataFrame: ...

    def snapshot(self, name: str) -> DataFrame:
        """Read a table as a stable snapshot that later overwrites of it cannot disturb."""
        ...

    def overwrite(self, name: str, df: DataFrame) -> None: ...

    def append(self, name: str, df: DataFrame) -> None: ...


class LakehouseTableStore:
    """Delta tables in the notebook's default Fabric Lakehouse (or a qualified schema)."""

    def __init__(self, spark: SparkSession, schema: str | None = None) -> None:
        self._spark = spark
        self._schema = schema

    def _qualified(self, name: str) -> str:
        return f"{self._schema}.{name}" if self._schema else name

    def exists(self, name: str) -> bool:
        return bool(self._spark.catalog.tableExists(self._qualified(name)))

    def read(self, name: str) -> DataFrame:
        return self._spark.read.table(self._qualified(name))

    def snapshot(self, name: str) -> DataFrame:
        # Delta reads are pinned to the table version at analysis time (snapshot isolation),
        # so a later overwrite of the same table does not affect this DataFrame.
        return self.read(name)

    def overwrite(self, name: str, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self._qualified(name))
        )

    def append(self, name: str, df: DataFrame) -> None:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            self._qualified(name)
        )


class PathTableStore:
    """Tables stored as folders under ``base_path`` (local development and CI).

    ``fmt`` is ``parquet`` by default so tests need no extra jars; pass ``delta`` when
    the delta-spark jars are available.
    """

    def __init__(self, spark: SparkSession, base_path: str | Path, fmt: str = "parquet") -> None:
        self._spark = spark
        self._base = Path(base_path)
        self._fmt = fmt
        self._base.mkdir(parents=True, exist_ok=True)

    def _path(self, name: str) -> Path:
        return self._base / name

    def exists(self, name: str) -> bool:
        path = self._path(name)
        return path.exists() and any(path.iterdir())

    def read(self, name: str) -> DataFrame:
        return self._spark.read.format(self._fmt).load(str(self._path(name)))

    def snapshot(self, name: str) -> DataFrame:
        # Plain Parquet has no snapshot isolation: materialise the rows before the folder
        # can be replaced by a later overwrite in the same stage.
        return self.read(name).localCheckpoint(eager=True)

    def overwrite(self, name: str, df: DataFrame) -> None:
        # Write to a temp folder first: the input DataFrame may lazily read the table
        # being replaced, so the old files must survive until the write has finished.
        target = self._path(name)
        tmp = self._base / f".{name}.tmp-{uuid.uuid4().hex}"
        df.write.format(self._fmt).mode("overwrite").save(str(tmp))
        if target.exists():
            shutil.rmtree(target)
        tmp.rename(target)

    def append(self, name: str, df: DataFrame) -> None:
        df.write.format(self._fmt).mode("append").option("mergeSchema", "true").save(str(self._path(name)))


def append_or_create(store: TableStore, name: str, df: DataFrame) -> None:
    """Append to ``name``, creating the table on first write (log-style tables)."""
    if store.exists(name):
        store.append(name, df)
    else:
        store.overwrite(name, df)
