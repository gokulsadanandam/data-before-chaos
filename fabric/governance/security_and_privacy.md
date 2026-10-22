# Security and privacy controls

| Control | Where | How |
|---|---|---|
| Secrets out of code | Azure Key Vault | HMAC token key `goldenrecord-token-key`, read with `notebookutils.credentials.getSecret`; never logged or printed |
| Least privilege | Workspace roles | Engineers: Contributor. Stewards: Viewer + Execute on `review_writeback`. Marketing: Viewer on reports only |
| Tokenised identifiers | Silver | `email_token`, `phone_token` (HMAC-SHA256). Matching, AI prompts and logs use tokens or comparison facts, not raw values |
| Masking in review | Review outbox / SQL DB | `p***@gmail.com`, `+91******3210`; stewards never see full contact details |
| AI data minimisation | `goldenrecord.ai` | AI sees name, city, postcode, email domain and yes/no facts; no email, phone or tax ID |
| Sensitivity labels | Lakehouse, semantic model, reports | *Confidential* (labels flow to exports) |
| DLP | Purview DLP for Fabric | policy on Power BI / Lakehouse for personal data patterns (email, phone, PAN) |
| DSPM for AI | Purview | monitors prompts to Copilot / AI functions for sensitive data |
| Lineage and audit | `gld_dq_audit`, decision log | every golden value traces to a source record and rule; every steward decision is logged with user and time |
| Consent | Survivorship | most restrictive wins; missing consent = no |
| Logging | `goldenrecord.logging_utils` | structured key=value logs with counts and ids only, no personal data |

## Data retention

* Bronze keeps raw extracts (append-only) for audit; apply Delta `VACUUM` with the
  retention agreed with the data owner (default 30 days of file history).
* Quarantined records are kept with their reason code until fixed at source.

## Right to be forgotten

Delete the person's source records in Bronze by `source_record_id` (Delta `DELETE`), add
the pairs to `slv_do_not_match` if needed, and re-run from stage 02: Silver and Gold are
recomputed snapshots, so the person disappears everywhere in one run.
