"""Helpers used by the Fabric notebooks (fabric/notebooks/*.ipynb).

Notebooks stay thin: a parameters cell, one call into this module, and an exit value.
All logic lives in the tested library, so a notebook never has to be debugged on its own.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession

from goldenrecord.ai import FabricAIAdapter, HeuristicAIAdapter
from goldenrecord.config import load_settings
from goldenrecord.jobs import RunContext, resolve_token_key
from goldenrecord.logging_utils import configure_logging
from goldenrecord.store import LakehouseTableStore

DEFAULT_CONFIG_DIR = "/lakehouse/default/Files/config"
DEFAULT_LANDING_ROOT = "Files/landing"


def fabric_context(
    spark: SparkSession,
    config_dir: str = DEFAULT_CONFIG_DIR,
    landing_root: str = DEFAULT_LANDING_ROOT,
    use_key_vault: bool = True,
    use_fabric_ai: bool = True,
    run_id: str | None = None,
    schema: str | None = None,
) -> RunContext:
    """Build the run context for a Fabric notebook attached to the pipeline Lakehouse.

    ``run_id`` should be passed down from the orchestrator so every stage of one run
    writes the same id into ``ops_run_metrics`` and the Gold tables.
    """
    configure_logging()
    settings = load_settings(config_dir)
    ai_on = use_fabric_ai and settings.matching.ai_enabled
    ctx = RunContext(
        spark=spark,
        store=LakehouseTableStore(spark, schema=schema),
        settings=settings,
        landing_root=landing_root,
        token_key=resolve_token_key(settings, use_key_vault=use_key_vault),
        ai=FabricAIAdapter() if ai_on else HeuristicAIAdapter(),
    )
    if run_id:
        ctx.run_id = run_id
    return ctx


def exit_with(result: dict[str, Any]) -> None:
    """Return a stage result to the caller (pipeline activity or runMultiple)."""
    import notebookutils  # noqa: PLC0415 - only available inside Fabric

    notebookutils.notebook.exit(json.dumps(result, default=str))
