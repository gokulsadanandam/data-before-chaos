# Team workstreams until submission (11 November 2026)

Proposed split by strength; swap owners freely, keep one owner per row.

| Workstream | Owner | Builds | Done when |
|---|---|---|---|
| Platform and pipeline | Gokul | workspace, Lakehouse, Environment, notebooks, pipeline, Key Vault | `pl_golden_record_run` green end to end |
| Matching and AI | Volodymyr | tune weights / bands on sandbox data, AI functions tier, notebook 07 | precision ≥ 0.95 on labelled pairs, AI tier live |
| Write-back | Sujata | SQL database scripts, user data function, review page buttons | approve on Power BI → applied by next run |
| Reports and scorecard | Deepika | semantic model, measures, executive + scorecard pages | before/after numbers on one page |
| Governance | Yasmin | Purview DQ rules, labels, Activator, security write-up | scorecard visible in Purview, one alert demonstrated |
| Story, demo, submission | Shravan | business case, demo script, video, submission form | demo rehearsed twice under 5 minutes |

## Milestones

| Date | Milestone |
|---|---|
| 22 Oct | Day 1 checklist done; first Bronze → Gold run on the sandbox |
| 27 Oct | Review loop working (queue → decision → Gold) |
| 31 Oct | Reports and DQ scorecard on real run data; AI tier live |
| 4 Nov | Feature freeze; Purview + Activator; data agent answers 5 questions |
| 7 Nov | Demo recorded; documentation final |
| 10 Nov | Submission dry run |
| 11 Nov | Submit |

## Ways of working

* One branch per change, pull request, CI green, one reviewer.
* Config changes need the evaluation numbers (`ops_eval_results`) in the PR description.
* Data contracts in `docs/data-contracts.md` are the interface between workstreams.
