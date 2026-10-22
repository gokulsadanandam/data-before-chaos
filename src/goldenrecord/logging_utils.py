"""Structured logging helpers.

Log lines carry the run id and stage so a Fabric job's output can be correlated with the
``ops_run_metrics`` table. Personal data must never be logged: log counts and ids only.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

_CONFIGURED = False


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED  # noqa: PLW0603 - module-level one-time setup is intentional
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger("goldenrecord")
    root.setLevel(level)
    root.addHandler(handler)
    root.propagate = False
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(f"goldenrecord.{name}")


def kv(event: str, **fields: Any) -> str:
    """Render an event and its fields as one JSON line (easy to search in job logs)."""
    return json.dumps({"event": event, **fields}, default=str, sort_keys=True)
