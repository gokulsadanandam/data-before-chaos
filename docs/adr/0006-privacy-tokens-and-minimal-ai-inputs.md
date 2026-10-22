# ADR 0006: HMAC tokens for identifiers and minimal AI inputs

**Status:** accepted · **Date:** 2026-10-19

## Context
Matching needs equality on emails and phones; AI prompts, logs and review screens should
not expose them. The mentor review flagged that "masking" alone is not a matching method.

## Decision
Silver stores `HMAC-SHA256(key, normalised value)` tokens next to the values; blocking and
features use tokens. The key is in Key Vault. AI functions receive only names, city,
postcode, email domain and yes/no comparison facts. Review screens show masked values.
Logs contain counts and ids only.

## Consequences
* Equality matching works on tokens; raw values stay inside Silver / Gold with labels.
* Key rotation means one full re-run from Silver (runbook).
