# Pipeline: `pl_golden_record_run`

One Data Factory pipeline runs the whole flow. Build it in the Fabric portal
(**New item → Data pipeline**) with the activities below. The notebooks are thin; all the
logic is in the `goldenrecord` wheel attached through the Environment.

## Pipeline parameters

| Name | Type | Default | Used for |
|---|---|---|---|
| `landing_root` | String | `Files/landing` | where source extracts land in the Lakehouse |
| `config_dir` | String | `/lakehouse/default/Files/config` | config folder uploaded to the Lakehouse |
| `use_fabric_ai` | Bool | `true` | `false` runs the deterministic heuristic (no AI cost) |
| `use_key_vault` | Bool | `true` | `false` only in a sandbox without Key Vault |

Every notebook activity passes the same base parameters:

| Notebook parameter | Value (dynamic content) |
|---|---|
| `run_id` | `@pipeline().RunId` |
| `config_dir` | `@pipeline().parameters.config_dir` |
| `landing_root` | `@pipeline().parameters.landing_root` |
| `use_key_vault` | `@pipeline().parameters.use_key_vault` |
| `use_fabric_ai` | `@pipeline().parameters.use_fabric_ai` |

## Activities (in order, each "On success" of the previous)

| # | Activity | Type | Settings |
|---|---|---|---|
| 1 | `Ingest Bronze` | Notebook | `01_ingest_bronze` |
| 2 | `Cleanse Silver` | Notebook | `02_cleanse_silver` |
| 3 | `Match candidates` | Notebook | `03_match_candidates` |
| 4 | `Publish review outbox` | Notebook | `04_publish_review_queue` |
| 5 | `Copy outbox to SQL` | Copy data | Source: Lakehouse table `rvw_review_queue_outbox`. Destination: SQL database table `review.review_queue_stage`, **Pre-copy script** `TRUNCATE TABLE review.review_queue_stage`. Mapping: by name. |
| 6 | `Merge review queue` | Stored procedure | Database: review SQL database. Procedure `review.usp_merge_review_queue`, parameter `run_id` (String) = `@pipeline().RunId` |
| 7 | `Resolve and publish Gold` | Notebook | `05_resolve_publish_gold` (fails if the DQ gate fails, so Gold is never half-published) |
| 8 | `Evaluate matching` | Notebook | `06_evaluate_matching` |
| 9 | `Refresh semantic model` | Semantic model refresh | `sm_golden_customer` (Direct Lake: reframes to the new Delta versions) |
| 10 | `Notify on failure` | Teams / Outlook | Connected **On failure** from activities 1-8. Message: `@{pipeline().Pipeline} run @{pipeline().RunId} failed in @{activity('...').error.message}` |

> Activity 7 reads steward decisions through the Lakehouse shortcut `rvw_decision_log`,
> which points at the SQL database's mirrored `review.decision_log` (see the deployment
> guide). Decisions made since the previous run are therefore applied on this run.

## Schedule and retries

* **Schedule:** daily 02:00 (workspace time zone), plus on demand for the demo.
* **Retries:** notebook activities retry 1 time after 5 minutes (transient Spark session
  failures). The stages are idempotent, so a retry is always safe.
* **Timeout:** 2 hours per notebook activity.

## Alternative: notebook orchestration

`99_orchestrate_run` runs stages 01-06 as a DAG with `notebookutils.notebook.runMultiple`
in one Spark session (faster for development). It does **not** copy the outbox to the
SQL database, so use the pipeline for anything a steward will see.

## Source ingestion (optional, production)

For the hackathon, extracts are uploaded to `Files/landing/<source>/`. In production add a
Copy activity per source in front of activity 1 (or a Dataflow Gen2 for sources that need
shaping), landing files in the same folders. `config/sources.yaml` already describes
where each source lands.
