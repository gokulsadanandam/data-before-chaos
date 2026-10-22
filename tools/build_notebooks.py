"""Generate the thin Fabric notebooks in fabric/notebooks from one definition.

Each notebook has: a markdown header, a *parameters* cell (overridable by pipelines and
``runMultiple``), and a call into the tested ``goldenrecord`` library. Edit this file and
run ``make notebooks``; do not hand-edit the generated .ipynb files.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "fabric" / "notebooks"

PARAMETERS = """# Parameters (overridden by the pipeline / orchestrator)
run_id = ""                                      # same id for every stage of one run
config_dir = "/lakehouse/default/Files/config"   # config/ folder uploaded to the Lakehouse
landing_root = "Files/landing"                   # where source extracts land
use_key_vault = True                             # False only in a sandbox without Key Vault
use_fabric_ai = True                             # False -> deterministic heuristic (no AI calls)
"""

CONTEXT = """from goldenrecord.fabric_entry import exit_with, fabric_context

ctx = fabric_context(
    spark,
    config_dir=config_dir,
    landing_root=landing_root,
    use_key_vault=use_key_vault,
    use_fabric_ai=use_fabric_ai,
    run_id=run_id or None,
)
print("run_id:", ctx.run_id)
"""

STAGES: list[dict[str, str]] = [
    {
        "name": "01_ingest_bronze",
        "title": "01 · Ingest landing files into Bronze",
        "about": "Appends new rows from every source in `config/sources.yaml` to `brz_<source>` "
        "(append-only, de-duplicated by row hash). Safe to re-run.",
        "code": "from goldenrecord.jobs import run_ingest\n\nresult = run_ingest(ctx)\nresult",
    },
    {
        "name": "02_cleanse_silver",
        "title": "02 · Standardise and validate (Tier 1) into Silver",
        "about": "Maps each source onto the canonical model, fixes what can be fixed, tokenises email "
        "and phone (HMAC key from Key Vault) and quarantines records with no usable identity.",
        "code": "from goldenrecord.jobs import run_cleanse\n\nresult = run_cleanse(ctx)\n"
        "display(spark.table(ctx.table('silver_quarantine')).groupBy('quarantine_reason').count())\nresult",
    },
    {
        "name": "03_match_candidates",
        "title": "03 · Profile identifiers, block, score and call AI functions (Tiers 2-3)",
        "about": "Writes `slv_identifier_profile` and `slv_match_candidate` with features, model score, "
        "AI signal, final score, band and explanation. Nothing is merged here.",
        "code": "from goldenrecord.jobs import run_match\n\nresult = run_match(ctx)\n"
        "display(spark.table(ctx.table('silver_candidates')).groupBy('band').count())\nresult",
    },
    {
        "name": "04_publish_review_queue",
        "title": "04 · Publish the steward review outbox",
        "about": "LOW-band pairs and MEDIUM spot checks (masked contact details only) go to "
        "`rvw_review_queue_outbox`. The pipeline then copies the outbox to the SQL database.",
        "code": "from goldenrecord.jobs import run_publish_review\n\n"
        "result = run_publish_review(ctx)\nresult",
    },
    {
        "name": "05_resolve_publish_gold",
        "title": "05 · Resolve links, assign master keys, survivorship, DQ gate, publish Gold",
        "about": "Reads steward decisions from `rvw_decision_log` (shortcut to the mirrored SQL "
        "database), builds clusters, applies the DQ gate and publishes Gold as SCD2. If a blocking "
        "rule fails, Gold is not touched and the notebook fails so the pipeline alerts.",
        "code": "from goldenrecord.jobs import run_resolve\n\nresult = run_resolve(ctx)\n"
        "if not result['gate_passed']:\n"
        "    raise RuntimeError(f\"DQ gate failed, Gold not published: {result['failed_rules']}\")\nresult",
    },
    {
        "name": "06_evaluate_matching",
        "title": "06 · Evaluate matching quality (precision / recall) and log to MLflow",
        "about": "Uses `ops_labelled_pairs` when present, otherwise steward decisions.",
        "code": "from goldenrecord.jobs import run_evaluate\n\nresult = run_evaluate(ctx)\nresult",
    },
    {
        "name": "07_retrain_weights",
        "title": "07 · Propose new match weights from steward labels",
        "about": "Fits the logistic weights on labelled pairs. The proposal is **not applied**: "
        "review it and update `config/pipeline.yaml` through a pull request.",
        "code": "import json\nfrom goldenrecord.jobs import run_retrain\n\nproposal = run_retrain(ctx)\n"
        "print(json.dumps(proposal, indent=2))\n"
        "result = {k: proposal[k] for k in ('labelled_pairs', 'log_loss')}",
    },
]

ORCHESTRATOR = """import uuid
from datetime import datetime, timezone

run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
args = {
    "run_id": run_id,
    "config_dir": config_dir,
    "landing_root": landing_root,
    "use_key_vault": use_key_vault,
    "use_fabric_ai": use_fabric_ai,
}


def activity(name, depends_on=()):
    return {
        "name": name,
        "path": name,
        "timeoutPerCellInSeconds": 3600,
        "args": args,
        "dependencies": list(depends_on),
    }


dag = {
    "activities": [
        activity("01_ingest_bronze"),
        activity("02_cleanse_silver", ["01_ingest_bronze"]),
        activity("03_match_candidates", ["02_cleanse_silver"]),
        activity("04_publish_review_queue", ["03_match_candidates"]),
        activity("05_resolve_publish_gold", ["04_publish_review_queue"]),
        activity("06_evaluate_matching", ["05_resolve_publish_gold"]),
    ],
    "timeoutInSeconds": 4 * 3600,
    "concurrency": 2,
}
results = notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": False})
results
"""

SETUP = """# One-time checks for a new workspace. Run interactively, cell by cell.
import os

import goldenrecord
from goldenrecord.config import load_settings

print("goldenrecord", goldenrecord.__version__)
settings = load_settings(config_dir)
print("sources:", [s.name for s in settings.sources])
print("landing folders:", os.listdir("/lakehouse/default/Files/landing"))
"""

SETUP_KEY_VAULT_CHECK = """# Key Vault: the identity running the notebook must be able to read the token key.
from goldenrecord.jobs import resolve_token_key

key = resolve_token_key(settings, use_key_vault=use_key_vault)
print("token key loaded:", bool(key), "(value never printed)")
"""

SETUP_AI = """# AI functions smoke test (needs F2+ capacity and the Copilot / Azure OpenAI tenant setting).
df = spark.createDataFrame([("Priya Sharma, Bengaluru", "Priya S., Bengaluru 560001")], ["a", "b"])
display(df.ai.similarity(input_col="a", other_col="b", output_col="sim", error_col="err"))
"""


def _source(text: str) -> list[str]:
    lines = text.strip("\n").split("\n")
    return [line + "\n" for line in lines[:-1]] + [lines[-1]]


def md(text: str) -> dict[str, object]:
    return {"cell_type": "markdown", "metadata": {}, "source": _source(text)}


def code(text: str, parameters: bool = False) -> dict[str, object]:
    meta: dict[str, object] = {"tags": ["parameters"]} if parameters else {}
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": meta,
        "outputs": [],
        "source": _source(text),
    }


def notebook(cells: list[dict[str, object]]) -> dict[str, object]:
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "name": "synapse_pyspark",
                "display_name": "Synapse PySpark",
                "language": "Python",
            },
            "language_info": {"name": "python"},
            "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
        },
        "cells": cells,
    }


def build() -> list[Path]:
    OUT.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    header = (
        "\n\nAttach the **pipeline Lakehouse** as default and the **goldenrecord Environment** "
        "(wheel + config). All logic is in the `goldenrecord` library; this notebook only calls it."
    )
    books = {
        "00_setup_checks": notebook(
            [
                md("# 00 · Workspace setup checks" + header),
                code(PARAMETERS, parameters=True),
                code(SETUP),
                code(SETUP_KEY_VAULT_CHECK),
                code(SETUP_AI),
            ]
        ),
        "99_orchestrate_run": notebook(
            [
                md(
                    "# 99 · Orchestrate one full run\n\nRuns stages 01-06 as a DAG with `notebookutils."
                    "notebook.runMultiple`, passing one `run_id` to every stage." + header
                ),
                code(PARAMETERS, parameters=True),
                code(ORCHESTRATOR),
            ]
        ),
    }
    for stage in STAGES:
        books[stage["name"]] = notebook(
            [
                md(f"# {stage['title']}\n\n{stage['about']}" + header),
                code(PARAMETERS, parameters=True),
                code(CONTEXT),
                code(stage["code"]),
                code("exit_with(result)"),
            ]
        )
    for name, book in sorted(books.items()):
        path = OUT / f"{name}.ipynb"
        path.write_text(json.dumps(book, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        written.append(path)
    return written


if __name__ == "__main__":
    for p in build():
        print(p.relative_to(OUT.parents[1]))
