# Adapting: a new source, or vendor records instead of customers

## Add a source (config only)

1. Land its extract in `Files/landing/<name>/`.
2. Add a block to `config/sources.yaml`: `format`, `path`, `options`, `trust`,
   `id_column`, `updated_at_column` and a `mapping` from canonical attributes
   (`schema.SOURCE_ATTRIBUTES`) to the source's column names. Unmapped attributes are null.
3. If the source is the system of record for an attribute, set it in
   `config/survivorship.yaml` (`strategy: source_of_record, source: <name>`).
4. Run `make test` (config validation catches unknown attributes or sources), commit,
   upload the config, run the pipeline. No code change.

Tip: run stage 02 and look at `dq_flags` / `slv_quarantine` for the new source first:
new formats show up there before they affect matching.

## Vendor master data (track variant)

The pipeline is entity-agnostic except for the canonical attributes. For vendors:

| Customer concept | Vendor equivalent |
|---|---|
| first / last name | legal name / trading name (map legal name to `last_name`, leave `first_name` empty, and drop the first-name weight to 0) |
| email, phone | accounts-payable contact email / phone |
| tax_id | GSTIN / VAT number: the strongest identifier (`tax_exact` weight high, `tax_conflict` blocks) |
| loyalty_id | supplier number in the procurement system |
| consent | not used (map nothing; the DQ rule DQ05 can be set `blocking: false`) |

Set `entity: vendor` and `master_key_prefix: "V-"` in `config/pipeline.yaml`, adjust the
weights (name similarity on company names needs suffix normalisation such as
"Pvt Ltd" / "Private Limited": add those to the placeholders or a small mapping), and
keep the review loop as is.
