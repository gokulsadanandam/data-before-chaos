# Data contracts

The interfaces between stages (and between team workstreams). Changing a column here is
a breaking change: update the producer, the consumers, the tests and this page in one PR.

## Silver: `slv_customer_clean` (one row per source record)

| Column | Type | Notes |
|---|---|---|
| `record_id` | string | `<source_system>|<source_record_id>`, unique |
| `source_system`, `source_record_id` | string | lineage |
| `batch_id`, `row_hash` | string | Bronze lineage |
| `source_updated_at` | timestamp | from the source's updated-at column, may be null |
| `source_trust` | int | from `config/sources.yaml` |
| `first_name`, `last_name` | string | display case, placeholders removed |
| `email`, `email_domain` | string | lower-case, valid, typo-fixed |
| `email_token`, `phone_token` | string | HMAC-SHA256 hex |
| `phone_e164` | string | `+<cc><number>` |
| `address_line`, `city`, `postcode`, `country` | string | country ISO alpha-2 |
| `tax_id`, `loyalty_id` | string | upper-case alphanumerics |
| `consent_email`, `consent_sms`, `consent_whatsapp` | string | `yes` / `no` / `unknown` |
| `dq_flags` | array<string> | what Tier 1 fixed or flagged |
| `dq_status` | string | `clean` / `fixed` / `flagged` |

`slv_quarantine` has the same columns plus `quarantine_reason`.

## Silver: `slv_match_candidate` (one row per pair, `left_id < right_id`)

`pair_id`, `left_id`, `right_id`, `l_source_system`, `r_source_system`, `block_keys`
(key *types* only), the feature columns (`goldenrecord.features.FEATURE_COLUMNS`),
`first_name_jw`, `last_name_jw`, `address_jaccard`, `model_score`, `ai_similarity`,
`ai_verdict`, `ai_error`, `final_score`, `band` (`HIGH`/`MEDIUM`/`LOW`), `decision`
(`match`/`review`), `spot_check`, `rule_explanation`, `ai_explanation`, `run_id`.

## Review: outbox → `review.review_queue_stage` (SQL)

`pair_id, review_type (MATCH_REVIEW|SPOT_CHECK), left_id, right_id,
left_/right_ source, name, email_masked, phone_masked, city, postcode, confidence, band,
explanation, evidence, priority, created_at, run_id`. **No unmasked contact data.**

## Review: `review.decision_log` → `rvw_decision_log`

`decision_id, pair_id, decision (APPROVE|REJECT|HOLD), reason, decided_by (UPN),
decided_by_oid, decided_at (UTC), invocation_id, channel`. Append-only; the latest
decision per pair wins.

## Gold: `gld_customer` (SCD2) / `gld_customer_current`

| Column | Notes |
|---|---|
| `master_key` | `C-000001`; stable, never reused |
| golden attributes | `first_name, last_name, email, phone_e164, address_line, city, postcode, country, tax_id, loyalty_id, consent_email, consent_sms, consent_whatsapp` |
| `source_record_count`, `source_systems` | how many / which systems contributed |
| `confidence_band` | weakest link: `HIGH`/`MEDIUM`/`LOW`; `SINGLE` = one record; steward-approved links count as HIGH |
| `attr_hash` | change detection |
| `valid_from`, `valid_to`, `is_current` | SCD2 |
| `run_id` | run that created this version |

## Gold: `gld_customer_xref`

`record_id, source_system, source_record_id, master_key, linked_by (model|steward|singleton), linked_at`.

## Gold: `gld_dq_audit`

`master_key, attribute, old_value, new_value, rule, winning_record_id, changed_at, run_id`.

## Gold facts

* `gld_order`: `source_system, order_id, source_customer_id, order_ts, order_date, amount, channel, master_key, is_unmatched`
* `gld_engagement`: `send_id, source_system, source_customer_id, campaign, channel, cost, sent_ts, sent_date, opened, clicked, converted, master_key, sent_without_consent, duplicate_send`

## Ops

* `ops_run_metrics`: `run_id, run_ts, stage, metric, value, baseline, alert, alert_reason`
* `ops_dq_results`: `run_id, run_ts, table_name, rule_id, rule_name, passed, total, pass_rate, threshold, ok, blocking`
* `ops_eval_results`: `run_id, run_ts` + precision, recall, f1 and counts
* `ops_labelled_pairs` (optional input): `left_id, right_id, is_match`
