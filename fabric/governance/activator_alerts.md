# Activator: alerts on run metrics

Every stage appends to `ops_run_metrics` (`run_id, run_ts, stage, metric, value,
baseline, alert, alert_reason`). Rates alert on change against the **baseline** (the
first, raw-data run), not on a fixed number, because the raw data is expected to be poor.

## Setup

1. Workspace → **New item → Activator** → name `act_goldenrecord_alerts`.
2. **Get data → OneLake / Lakehouse table** `ops_run_metrics` (poll every 15 minutes), or
   build the rules from a Power BI visual on the scorecard page (*Set alert*).
3. Object: **metric**, identified by `stage` + `metric`; property: `value`.

## Rules

| Rule | Condition | Action |
|---|---|---|
| DQ gate failed | `metric = gate_passed` and `value = 0` | Teams message to the data owner + email |
| Quarantine spike | `metric = quarantine_rate_pct` and `alert = true` | Teams message (includes `alert_reason`) |
| Review backlog | `metric = review_rate_pct` and `alert = true` | Teams message to stewards |
| Oversized blocks | `metric = blocks_skipped_oversized` and `value > 0` | Email the engineer on duty (a placeholder value is flooding a blocking key) |
| Held clusters | `metric = held_clusters` and `value > 0` | Teams message to stewards (see `slv_cluster_issue`) |
| Matching precision drop | `stage = evaluate`, `metric = precision` and `value < 0.95` | Email the data owner |

Pipeline failures themselves are notified by the pipeline's *On failure* activity.
