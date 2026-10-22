# Changelog

## 0.1.0 - 2026-10-20

First complete version for the Solution Build stage.

* Library `goldenrecord`: ingestion, Tier 1 standardisation, tokenisation, identifier
  profiling, blocking, features, logistic scoring, AI tier adapter, review outbox and
  decisions, clustering with guards, master key lifecycle, survivorship, DQ gate, SCD2
  promotion, audit, facts, metrics, evaluation, weight retraining.
* Fabric assets: notebooks 00-07 + orchestrator, SQL review schema and MERGE procedure,
  write-back user data function, pipeline guide, Power BI model / measures / page specs,
  Purview, Activator, data agent, security guides.
* Synthetic multi-source data generator with ground truth; local runner.
* Tests: unit, Spark and end-to-end; CI on Spark 3.5 and 4.0.
