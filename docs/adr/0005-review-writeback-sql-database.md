# ADR 0005: Steward write-back through a Fabric SQL database and user data functions

**Status:** accepted · **Date:** 2026-10-19

## Context
Stewards need to decide inside the tool they already use (Power BI), with their identity
recorded and no double decisions. Lakehouse Delta tables are not built for many small
concurrent writes from reports.

## Decision
Queue and decisions live in a Fabric SQL database. A Power BI button (translytical task
flow) calls the `submit_decision` user data function: role check, conditional UPDATE
(first click wins), append to `decision_log` and `approval`, in one transaction. The
database's mirroring to OneLake plus a Lakehouse shortcut lets Spark read the log with no
export job. The Lakehouse → SQL direction uses an outbox table and a pipeline Copy + MERGE.

## Consequences
* Decisions are transactional, attributable and auditable.
* Decisions take effect on the next pipeline run (minutes to a day), which is acceptable
  for master data; the queue updates immediately.
