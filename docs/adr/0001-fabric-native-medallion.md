# ADR 0001: Fabric-native medallion in one Lakehouse

**Status:** accepted · **Date:** 2026-10-19

## Context
The hackathon judges Fabric usage, and the team has three weeks. Data must flow from
raw extracts to a governed golden record and on to reports and an agent.

## Decision
One Lakehouse `lh_golden_record` with `brz_` / `slv_` / `gld_` / `ops_` prefixed Delta
tables, Spark notebooks for processing, a Data Factory pipeline for orchestration, a
Fabric SQL database only for the transactional review loop, Direct Lake for reporting,
Purview for governance.

## Consequences
* One place to secure and label; shortcuts and Direct Lake need no copies.
* Separate Lakehouses per layer would allow per-layer permissions; we can split later
  without code changes because table names come from `config/pipeline.yaml`.
