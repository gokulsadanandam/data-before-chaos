# Runbook

## Normal operation

* `pl_golden_record_run` runs daily at 02:00. Stewards work the review page during the
  day; their decisions are applied by the next run (or trigger the pipeline on demand).
* Purview scans Gold at 03:00. Activator watches `ops_run_metrics`.

## When something fails

| Alert / symptom | What it means | Action |
|---|---|---|
| Pipeline failed in `Ingest Bronze` | file missing or unreadable | check `Files/landing/<source>`; fix the file; re-run the pipeline (ingest skips rows already loaded) |
| `DQ gate failed` | a blocking rule is below threshold; **Gold was not changed** | open `ops_dq_results` for the run → rule id → inspect `slv_customer_clean` for the pattern (often a new source format). Fix the rule / mapping in config, PR, redeploy config, re-run |
| Quarantine spike alert | many more records quarantined than at baseline | `slv_quarantine` grouped by `quarantine_reason` and `source_system`; usually a source changed a column |
| Oversized block alert | a value (e.g. a store phone) is shared by > 200 records | add it to `config/reference/placeholders.csv`; re-run from stage 02 |
| Held cluster alert | a cluster has two tax IDs or > 25 records | `slv_cluster_issue` → find the link that chains them in `slv_match_link` → steward rejects it |
| Precision below 0.95 | matching got worse | compare `ops_eval_results` across runs; check recent config changes; roll back weights |
| Review backlog growing | stewards cannot keep up | check `review.v_queue_summary`; consider retraining (notebook 07) to shrink the LOW band |

## Re-processing

* Every stage is idempotent: re-running with the same inputs gives the same tables.
* **Re-run from Silver** (after a rule change): run notebooks `02` → `05`.
* **Restore Gold** to a previous state: `RESTORE TABLE gld_customer TO VERSION AS OF <n>`
  (also `gld_customer_xref`, `gld_master_key_registry`) — use `DESCRIBE HISTORY` to find `n`.
* **Undo a steward decision:** insert the opposite decision for the pair in
  `review.decision_log` (latest decision wins; the log stays append-only), then re-run `05`.

## Rotating the token key

Changing the HMAC key changes every token, so it is done as one run:
1. add the new secret version in Key Vault;
2. run `02` → `05` in one pipeline run (Silver is rebuilt with new tokens; Gold keys are
   unaffected because master keys do not depend on tokens);
3. disable the old secret version.

## Changing configuration

All tuning is in `config/`. Change it in a pull request (CI validates it), then upload
the changed files to `Files/config/` in the Lakehouse. Weight changes come from
notebook `07` proposals; apply them only with the evaluation numbers in the PR.
