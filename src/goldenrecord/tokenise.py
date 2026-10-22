"""Consistent tokens for identifiers (email, phone).

A token is ``HMAC-SHA256(key, normalised value)``. The same email always yields the same
token, so equality matching keeps working, but nothing readable leaves the notebook: AI
functions, review screens and logs see tokens, never raw contact details.

The key is a secret. In Fabric it is read from Azure Key Vault (see jobs.resolve_token_key);
rotating it changes every token, so rotation means re-tokenising Silver in one run.
"""

from __future__ import annotations

import hashlib
import hmac

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StringType


def hmac_token(key: bytes, value: str | None) -> str | None:
    """Pure-Python token function (used by the Spark UDF and directly in tests)."""
    if value is None:
        return None
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def token_column(col: Column, key: bytes) -> Column:
    """Spark column that tokenises ``col`` with a vectorised (Arrow) UDF."""

    def _token(values: pd.Series) -> pd.Series:
        return values.map(lambda v: hmac_token(key, v) if isinstance(v, str) else None)

    token_udf = pandas_udf(_token, StringType())  # type: ignore[call-overload]
    return F.when(col.isNotNull(), token_udf(col))
