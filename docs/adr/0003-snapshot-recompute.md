# ADR 0003: Recompute Silver and Gold as snapshots each run

**Status:** accepted · **Date:** 2026-10-19

## Context
Incremental entity resolution (MERGE per changed record) is complex: one new record can
join two existing customers, a steward reject can split one. Bugs there corrupt Gold.

## Decision
Bronze is append-only. Silver, candidates, links, clusters and the golden snapshot are
**recomputed from Bronze on every run** and written with whole-table overwrites. Stability
across runs comes from the master key registry (ADR 0004) and SCD2 change detection by
`attr_hash`, not from incremental updates.

## Consequences
* Every stage is idempotent and simple to test; re-running fixes any partial failure.
* Delta time travel gives history and restore for free.
* Cost grows with total volume rather than change volume. At hackathon and mid-size
  scale (millions of records) this is fine; beyond that, partition blocking by country
  or switch Silver to incremental while keeping the same contracts.
