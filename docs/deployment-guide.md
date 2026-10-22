# Deployment guide (Fabric workspace)

First deployment takes about two hours. Every step lists how to check it worked.
Names in `code` are the defaults used by the config and docs; keep them unless you have a
reason not to.

## 0. Prerequisites

| Need | Why | Check |
|---|---|---|
| Fabric capacity **F2 or higher** (or trial) | AI functions need paid / trial capacity | Workspace settings → License info |
| Tenant setting *Copilot and Azure OpenAI Service* enabled for the workspace | AI functions | Admin portal → Tenant settings |
| Tenant settings for *SQL database*, *User data functions*, *Translytical task flows* | write-back | items appear under **New item** |
| An Azure Key Vault you can add secrets to | token key | Azure portal |
| Python 3.10+ and Java 17 on your laptop | build the wheel, run tests | `python --version`, `java -version` |

If the hackathon sandbox does not allow Key Vault, set `use_key_vault = false` in the
pipeline parameters for the demo (the dev key in `config/pipeline.yaml` is then used) and
say so in the submission: it is a sandbox-only shortcut.

## 1. Build the release

```bash
make setup && source .venv/bin/activate
make ci            # must be green
make wheel         # -> dist/goldenrecord-0.1.0-py3-none-any.whl
make data          # optional: synthetic landing files for the demo -> local_data/landing
```

## 2. Workspace and Lakehouse

1. Create workspace `ws-data-before-chaos` on the capacity.
2. **New item → Lakehouse** → `lh_golden_record` (schemas *off*, so table names match the
   config; with schemas on, pass `schema="dbo"` to `fabric_context`).
3. In the Lakehouse **Files**: create `config/` and `landing/`.
4. Upload the repo's `config/` folder content into `Files/config/` (keep the `reference/`
   subfolder).
5. Upload the source extracts into `Files/landing/<source>/` (for the demo: the folders
   from `local_data/landing/`, **without** `_ground_truth/`, which is uploaded to
   `Files/ground_truth/` only if you want to evaluate on it).

Check: `Files/config/pipeline.yaml` and `Files/landing/crm/contacts_001.csv` exist.

## 3. Key Vault secret

1. In Key Vault create secret `goldenrecord-token-key` with a random value
   (`python -c "import secrets; print(secrets.token_urlsafe(48))"`).
2. Give the identities that run notebooks **Key Vault Secrets User** on the vault
   (your user for interactive runs; the pipeline runs as the user who owns it).
3. Put the vault URI into `Files/config/pipeline.yaml` → `tokenisation.key_vault_uri`.

Check: notebook `00_setup_checks` prints `token key loaded: True`.

## 4. Environment with the wheel

1. **New item → Environment** → `env_goldenrecord`.
2. **Custom libraries → Upload** `dist/goldenrecord-0.1.0-py3-none-any.whl`.
   (PyYAML, pandas and pyarrow are already in the runtime.)
3. Runtime: **1.3** (Spark 3.5) or **2.0**; the code supports both.
4. **Publish** (takes a few minutes).
5. Workspace settings → Data Engineering → *Default environment* → `env_goldenrecord`
   (or attach it on each notebook).

Check: in a notebook, `import goldenrecord; goldenrecord.__version__` → `0.1.0`.

## 5. Notebooks

1. Workspace → **Import → Notebook → From this computer** → select all files in
   `fabric/notebooks/`.
2. Open each notebook once and attach **lh_golden_record** as the default Lakehouse
   (and `env_goldenrecord` if it is not the workspace default). Save.
3. Run `00_setup_checks` cell by cell.

Check: all three check cells succeed (config loads, key loads, AI similarity returns a
number between -1 and 1).

## 6. First run without stewards (proves Bronze → Gold)

Run notebooks `01` → `05` in order (or `99_orchestrate_run`).

Check:
* `05` ends with `gate_passed: True` and a `golden_records` count;
* tables `gld_customer`, `gld_customer_current`, `gld_customer_xref`, `ops_run_metrics`
  are in the Lakehouse.

This first run is the **baseline** that alerts compare against.

## 7. SQL database for reviews

1. **New item → SQL database** → `sqldb_review`.
2. Open the query editor and run, in order: `fabric/sql/01_review_schema.sql`,
   `02_merge_review_queue.sql`, `03_seed_stewards.sql` (edit the UPNs first),
   `04_views.sql`.
3. Mirroring is on by default for Fabric SQL databases. Check in the database's
   **Replication status** that `review.decision_log` is replicating.

Check: `SELECT * FROM review.steward` lists your stewards.

## 8. Decision log shortcut into the Lakehouse

1. Lakehouse → **Tables → New shortcut → Microsoft OneLake** → select `sqldb_review` →
   `review` → `decision_log`.
2. Name the shortcut **`rvw_decision_log`** (the name in `config/pipeline.yaml`).

Check: `spark.table("rvw_decision_log").printSchema()` shows `pair_id, decision, reason,
decided_by, decided_at`.

## 9. User data function for write-back

Follow `fabric/user_data_functions/review_writeback/README.md`: create the item
`review_writeback`, add the SQL connection with alias `ReviewDB`, paste
`function_app.py`, publish, test with a `pair_id` from `review.v_open_queue`.

Check: after a test call, `review.decision_log` has a row and the queue item is `DECIDED`.

## 10. Pipeline

Build `pl_golden_record_run` as described in `fabric/pipelines/README.md` (notebook
activities, Copy outbox → `review.review_queue_stage`, stored procedure, semantic model
refresh, failure notification). Run it once.

Check: `SELECT status, COUNT(*) FROM review.review_queue GROUP BY status` shows PENDING
items.

## 11. Semantic model and reports

1. From the Lakehouse: **New semantic model** `sm_golden_customer` with the tables in
   `fabric/powerbi/semantic_model.md`, relationships as listed, measures from
   `fabric/powerbi/measures.dax`.
2. Build the executive report (`executive_report.md`) on the model.
3. Build the steward review report on the SQL database in DirectQuery
   (`review_page.md`) and wire the three buttons to `submit_decision`.
4. Apply the *Confidential* sensitivity label to the Lakehouse, model and reports.

Check: approve a pair on the review page → it leaves the queue → run notebook `05` →
the two records share a master key in `gld_customer_xref`.

## 12. Governance, alerts, agent

* Purview data quality rules: `fabric/governance/purview_dq_rules.md`.
* Activator alerts: `fabric/governance/activator_alerts.md`.
* Fabric data agent for the marketing agent: `fabric/data_agent/README.md`.
* Task flow in the workspace: `fabric/powerbi/task_flow.md`.

## 13. Evaluate on ground truth (synthetic data only)

Upload `_ground_truth/labelled_pairs.csv` to `Files/ground_truth/`, then in a notebook:

```python
from pyspark.sql import functions as F
labelled = (spark.read.option("header", True).csv("Files/ground_truth/labelled_pairs.csv")
            .select("left_id", "right_id", (F.col("is_match") == "1").alias("is_match")))
labelled.write.mode("overwrite").saveAsTable("ops_labelled_pairs")
```

Notebook `06` then reports precision and recall against it and logs them to MLflow.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `ModuleNotFoundError: goldenrecord` | Environment not attached or not published |
| `ConfigError: Missing config file` | `config_dir` parameter does not point at `Files/config` (use the `/lakehouse/default/...` path) |
| Key Vault `Forbidden` | identity lacks *Key Vault Secrets User*; or use `use_key_vault=false` in a sandbox |
| `AttributeError: 'DataFrame' object has no attribute 'ai'` | capacity below F2 / AI tenant setting off; run with `use_fabric_ai=false` meanwhile |
| `05` fails with *DQ gate failed* | by design: see `ops_dq_results` for the failing rule; Gold is unchanged |
| `rvw_decision_log` not found | step 8 not done; stage 05 then runs without steward decisions (first run is fine) |
| Power BI button greyed out | user lacks *Execute* on the functions item, or no row selected |
