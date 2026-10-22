# Architecture

## 1. Shape of the solution

```
                       ┌──────────────────────── Fabric workspace ───────────────────────────────────────┐
 ERP A, ERP B, CRM,    │  Lakehouse lh_golden_record                                                       │
 Web, POS, Mobile  ──▶ │  Files/landing/<source>/   ──01──▶  brz_<source>   (append-only, row_hash)          │
 + orders, sends       │                             ──02──▶  slv_customer_clean / slv_quarantine            │
                       │                             ──03──▶  slv_identifier_profile, slv_match_candidate    │
                       │                             ──04──▶  rvw_review_queue_outbox ──copy──┐              │
                       │                                                                       ▼              │
                       │  SQL database sqldb_review:  review.review_queue ◀── MERGE proc                     │
                       │        ▲  Power BI review page ── button ──▶ UDF submit_decision ──▶ decision_log   │
                       │        └──────────── mirroring to OneLake ──▶ shortcut rvw_decision_log ─┐           │
                       │                             ──05──▶  slv_match_link → clusters → master keys        │
                       │                                      → survivorship → DQ gate → gld_* (SCD2)        │
                       │                             ──06──▶  ops_eval_results (+ MLflow)                    │
                       │  Direct Lake model ─▶ executive / scorecard reports      Activator ◀─ ops_run_metrics│
                       │  Fabric data agent (Gold only) ─▶ marketing agent in Azure AI Foundry               │
                       └─────────────────────────────────────────────────────────────────────────────────┘
                         Purview: DQ rules on Gold, labels, DLP, lineage · Key Vault: token key
```

Numbers are the notebooks in `fabric/notebooks`; the pipeline in `fabric/pipelines`
runs them in order with the copy and MERGE steps in between.

## 2. Stages

### 01 Ingest → Bronze
Each source in `config/sources.yaml` is read as text (no type inference) and written to
`brz_<source>` with `source_system`, `source_record_id`, `batch_id`, `ingested_at`, the
full `payload` as JSON and `row_hash = sha256(payload)`. A row whose hash is already
there is skipped, so re-delivered files and re-runs add nothing. Orders and campaign
sends land the same way in `brz_orders_<source>` and `brz_engagement`.

### 02 Cleanse → Silver (Tier 1 rules)
The latest version of every source record is mapped onto one canonical model and
standardised with deterministic, unit-tested Spark expressions:

| Field | Rule |
|---|---|
| names | strip salutations and symbols, split "Last, First" and "First M Last", display case, placeholder names removed |
| email | trim, lower-case, fix domain typos (`gmial.com`), validate, drop placeholders (`test@test.com`) |
| phone | E.164 using the record's country calling code (`098761 43210` → `+919876143210`), placeholders and repeated digits removed |
| country | alias table → ISO alpha-2 (`India`, `IND`, `Bharat` → `IN`) |
| consent | many spellings → `yes` / `no` / `unknown` |
| dates | several formats, never failing (`try_to_timestamp`) |

Every fix is recorded in `dq_flags`; `dq_status` is `clean`, `fixed` or `flagged`.
Records with no usable identity are moved to `slv_quarantine` with a reason code.
Email and phone get **HMAC-SHA256 tokens** (`email_token`, `phone_token`) with a key from
Key Vault; matching uses the tokens.

### 03 Match (Tier 2 model + Tier 3 AI)
1. **Identifier profiling** measures uniqueness instead of assuming it: each email /
   phone / loyalty / tax value is labelled `unique` (one first name), `household`
   (2-4 first names, compared phonetically) or `shared` (5+; ignored for matching).
2. **Blocking** generates candidate pairs from six keys (email token, phone token,
   loyalty id, tax id, surname Soundex + postcode, email domain + initial + city).
   Oversized blocks (> 200, usually placeholder values) are skipped and reported.
3. **Features** per pair: identifier agreement weighted by label, identifier conflicts,
   Jaro-Winkler on nickname-normalised first names (initials handled), surname
   Jaro-Winkler and Soundex, address token Jaccard, postcode / city equality, email
   local-part similarity, household and tax conflicts.
4. **Score** = logistic model with weights in config (`matching.weights`), refit from
   steward labels by notebook 07. **Bands:** HIGH ≥ 0.90 auto-merge; MEDIUM ≥ 0.75
   auto-merge with a 10% spot check; otherwise LOW → steward review.
   **Rule:** a pair with no shared identifier never auto-merges (name + postcode alone
   is not proof).
5. **AI tier** only for scores in [0.50, 0.90): `ai.similarity` on minimal descriptions,
   `ai.classify` into "same person" / "different person", blended into the score with
   weight 0.3. LOW pairs get a one-sentence explanation from `ai.generate_response`.
   AI sees names, city, postcode, email domain and yes/no facts, never raw contacts.
   If an AI call fails, the pair keeps its model score.

### 04 Review outbox → SQL database
LOW pairs and MEDIUM spot checks not yet decided are written to
`rvw_review_queue_outbox` with masked contacts. The pipeline copies them into
`review.review_queue_stage` and `review.usp_merge_review_queue` merges them into the
live queue (decisions are never overwritten; stale items become SUPERSEDED).

### Steward write-back (translytical task flow)
On the Power BI review page a steward selects a pair and clicks Approve / Reject / Hold.
The `submit_decision` user data function checks the steward role, claims the item with
a conditional UPDATE (first click wins), and appends to `review.decision_log` and
`review.approval` in one transaction. The SQL database mirrors to OneLake; the Lakehouse
shortcut `rvw_decision_log` makes the log readable by Spark.

### 05 Resolve → Gold
1. **Links** = auto-matches + steward approvals − steward rejects / holds. Rejected pairs
   go to `slv_do_not_match` and are never proposed again.
2. **Clusters** = connected components of the links (min-label propagation in Spark).
   Guards hold a cluster larger than 25 records or with two different tax IDs
   (`slv_cluster_issue`); its records stay on their previous golden record.
3. **Master keys** (ADR 0004): a cluster keeps its existing key; a merge keeps the
   oldest; a split gives the key to the side with most of its records; new clusters
   get the next number (`C-000123`). Keys are never reused (`gld_master_key_registry`).
4. **Survivorship** per attribute from `config/survivorship.yaml`: most trusted, most
   recent, most frequent, longest complete, source of record; consent is **most
   restrictive** (any explicit no wins, missing = no). The winning source record of
   each value is kept.
5. **DQ gate**: the six rules in `config/dq_rules.yaml` run on the candidate golden
   records. If a blocking rule fails, nothing in Gold changes and the run fails.
6. **Publish**: `gld_customer` as SCD2 (changed records close the old version and open a
   new one), `gld_customer_current`, `gld_customer_xref`, `gld_dq_audit` (old → new
   value, rule, winning record), `gld_order` and `gld_engagement` keyed by master key
   (with `duplicate_send` and `sent_without_consent` flags).

### 06 Evaluate, 07 Retrain
Precision / recall / F1 of "same master key" against labelled pairs (steward decisions,
or generator ground truth), logged to MLflow and `ops_eval_results`. Notebook 07 refits
the weights and prints a proposal; a person applies it through a pull request.

## 3. Table catalogue

| Layer | Table | Key | Purpose |
|---|---|---|---|
| Bronze | `brz_<source>`, `brz_orders_<source>`, `brz_engagement` | row_hash | raw extracts, append-only |
| Silver | `slv_customer_clean` | record_id | standardised, tokenised records |
| | `slv_quarantine` | record_id | unusable records + reason |
| | `slv_identifier_profile` | id_type, id_value | uniqueness labels |
| | `slv_match_candidate` | pair_id | scored pairs with explanation |
| | `slv_match_link` | pair_id | accepted links |
| | `slv_do_not_match` | left_id, right_id | steward rejects (permanent) |
| | `slv_cluster_issue` | cluster_id | held clusters |
| Review | `rvw_review_queue_outbox` | pair_id | to SQL DB |
| | `rvw_decision_log` (shortcut) | decision_id | from SQL DB |
| Gold | `gld_customer` | master_key, valid_from | golden record, SCD2 |
| | `gld_customer_current` | master_key | current versions |
| | `gld_customer_xref` | record_id | source record → master key |
| | `gld_master_key_registry` | master_key | key lifecycle |
| | `gld_dq_audit` | master_key, attribute, changed_at | value lineage |
| | `gld_order`, `gld_engagement` | order_id / send_id | facts by master key |
| Ops | `ops_run_metrics`, `ops_dq_results`, `ops_eval_results`, `ops_labelled_pairs` | run_id | monitoring and evaluation |

Column-level contracts: `docs/data-contracts.md`.

## 4. Code structure

`goldenrecord` is a plain Python package. Each module owns one step and exposes pure
functions from DataFrames to DataFrames (`Column` expressions where possible), so steps
are unit-tested on tiny frames. `jobs.py` wires the steps into stages over a
`TableStore` (Lakehouse Delta tables in Fabric, Parquet folders locally) and an
`AIAdapter` (Fabric AI functions in Fabric, a deterministic heuristic locally).
`fabric_entry.py` builds the Fabric context for notebooks; `local_runner.py` builds the
local one.

## 5. Non-functional notes

* **Scale:** blocking keeps comparisons near-linear; the max block size caps the worst
  case. Clustering converges in a few iterations (chains are short). The AI tier touches
  only the ambiguous middle (about 2% of candidate pairs in the synthetic data).
* **Cost:** AI functions are the main variable cost; `use_fabric_ai=false` or a narrower
  `[ai.lower, ai.upper)` reduces it. F2 capacity is the minimum for AI functions.
* **Recovery:** every stage is idempotent; Delta time travel restores any Gold table
  (`RESTORE TABLE gld_customer TO VERSION AS OF n`).
