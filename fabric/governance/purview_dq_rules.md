# Purview Data Quality: the six published rules

The pipeline evaluates `config/dq_rules.yaml` as a **gate** before Gold is published.
The same six rules are configured in **Microsoft Purview Unified Catalog → Data quality**
on the Gold table, so the business sees an independent, governed scorecard.

## Setup

1. Purview portal → **Unified Catalog → Governance domains** → create domain
   `Customer` → data product `Golden Customer` → add data asset `gld_customer_current`
   (Fabric Lakehouse, scanned through the Fabric data map source).
2. **Data quality → connections**: add a connection to the Fabric workspace (managed
   identity of Purview needs *Contributor* on the workspace).
3. **Data quality → rules** on `gld_customer_current`: create the rules below.
4. **Schedule** a scan daily at 03:00 (after the pipeline) and enable **alerts** to the
   governance lead when the overall score drops below 95.

## Rules

| ID | Purview rule type | Column(s) | Setting | Threshold |
|---|---|---|---|---|
| DQ01 | Custom (regular expression) | `email` | `^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`, empty allowed | 99% |
| DQ02 | Custom (regular expression) | `phone_e164` | `^\+[1-9][0-9]{7,14}$`, empty allowed | 97% |
| DQ03 | Unique values | `master_key` | | 100% |
| DQ04 | Custom (expression) | `last_name`, `email`, `phone_e164` | `last_name` not empty AND (`email` not empty OR `phone_e164` not empty) | 95% |
| DQ05 | Empty / blank fields | `consent_email`, `consent_sms`, `consent_whatsapp` | no empty values | 100% |
| DQ06 | Custom (allowed values) | `country` | ISO codes from `config/reference/countries.csv`, empty allowed | 99% |

Keep `config/dq_rules.yaml` and these rules in step: the YAML is the source of truth and
the README of the repo explains how to change a threshold (pull request + note here).

## Why both

* The **gate** (pipeline) stops a bad batch before anyone sees it.
* **Purview** gives the governed score, history and ownership that auditors expect,
  and runs independently of the pipeline code.
