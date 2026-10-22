# Semantic model: `sm_golden_customer` (Direct Lake)

Create from the pipeline Lakehouse: **Lakehouse → New semantic model**, select the tables
below. Direct Lake reads the Delta tables in OneLake directly, so the model needs no
import refresh; the pipeline's *semantic model refresh* activity just reframes it.

## Tables

| Table | Grain | Notes |
|---|---|---|
| `gld_customer_current` | one row per golden customer | current versions; relationships point here |
| `gld_customer` | one row per golden record version | SCD2 history (valid_from / valid_to) |
| `gld_customer_xref` | one row per source record | which master key each source record belongs to |
| `gld_order` | one row per order | `master_key` null = unmatched order |
| `gld_engagement` | one row per campaign send | flags `duplicate_send`, `sent_without_consent` |
| `gld_dq_audit` | one row per changed golden attribute | lineage: rule and winning source record |
| `ops_dq_results` | one row per DQ rule per run | scorecard trend |
| `ops_run_metrics` | one row per metric per run | baseline vs current, alert flags |
| `slv_quarantine` | one row per quarantined record | reason codes |
| `dim_date` | calendar | create with the DAX in `measures.dax` or a Lakehouse table |

## Relationships (single direction, many-to-one)

| From (many) | To (one) | Note |
|---|---|---|
| `gld_customer_xref[master_key]` | `gld_customer_current[master_key]` | |
| `gld_order[master_key]` | `gld_customer_current[master_key]` | |
| `gld_engagement[master_key]` | `gld_customer_current[master_key]` | |
| `gld_order[order_date]` | `dim_date[date]` | |
| `gld_engagement[sent_date]` | `dim_date[date]` | |

`gld_customer` holds history (SCD2), so `master_key` is unique only among current rows.
The pipeline therefore also writes **`gld_customer_current`** (current versions only, a
real Delta table, so the model stays in pure Direct Lake mode). Use it for relationships
and the measures; keep `gld_customer` in the model only for history visuals.

## Security

* **Sensitivity label:** *Confidential* on the model and reports (inherits downstream).
* **Object-level security:** hide `email`, `phone_e164`, `tax_id` from the *Marketing
  viewer* role; reports use counts and masked values only.
* **Row-level security (optional):** role per country: `[country] = "IN"`.
* Mark the model **Certified** once the DQ gate has passed on a full run (endorsement).
