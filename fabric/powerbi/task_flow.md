# Task flow (workspace view)

Fabric task flows show the solution's shape in the workspace. Create one from the
**Medallion** template and assign items:

| Task | Items |
|---|---|
| Get data | `pl_golden_record_run` (Copy activities, when sources are connected) |
| Store: Bronze / Silver / Gold | `lh_golden_record` (one Lakehouse; tables carry the `brz_`, `slv_`, `gld_` prefix) |
| Prepare | notebooks `01`-`05`, Environment `env_goldenrecord` |
| Analyze and train | `06_evaluate_matching`, `07_retrain_weights`, MLflow experiment `goldenrecord-matching` |
| Track | Activator `act_goldenrecord_alerts` |
| Visualize | `sm_golden_customer`, executive report, steward review report |
| Develop (write-back) | SQL database `sqldb_review`, user data functions `review_writeback` |
