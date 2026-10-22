# ADR 0002: Tiered matching (rules → model → AI → human)

**Status:** accepted · **Date:** 2026-10-19

## Context
Matching must be accurate, explainable and affordable. LLM calls on every pair are slow,
costly and hard to audit; rules alone miss typos and nicknames.

## Decision
Tier 1 deterministic standardisation; Tier 2 blocking + interpretable logistic score with
config weights; Tier 3 Fabric AI functions only for scores in [0.50, 0.90), blended into
the score with a fixed weight; humans review the LOW band and a 10% sample of MEDIUM.
A pair without any shared identifier never auto-merges.

## Consequences
* Every decision has a rule-based explanation; AI adds a sentence but never the final say.
* AI cost scales with ambiguity, not volume.
* Weights must be calibrated on labelled pairs (notebook 07); day-1 weights are expert-set.
