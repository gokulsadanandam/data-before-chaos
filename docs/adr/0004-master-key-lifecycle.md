# ADR 0004: Master key lifecycle

**Status:** accepted · **Date:** 2026-10-19

## Context
Downstream systems (campaign tool, agent memory, reports) store the master key. If keys
change when clusters change, links break silently.

## Decision
* Keys are issued from a growing sequence (`C-000001`), recorded in `gld_master_key_registry`.
* A cluster whose records already have a key keeps it.
* A merge keeps the **oldest** key; the others are retired with `replaced_by`.
* A split gives the key to the side holding **most of its records**; the other side
  gets a new key.
* Retired keys are never issued again.

## Consequences
* Consumers can follow `replaced_by` to update stored keys after a merge.
* The DQ gate rule DQ03 (unique master key) guards the invariant on every run.
