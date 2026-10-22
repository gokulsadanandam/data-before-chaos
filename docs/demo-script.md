# Demo script (5 minutes)

Prepared state: pipeline has run once (baseline) on the synthetic data; the review queue
has items; the executive report and review page are open in two tabs.

| Time | Show | Say |
|---|---|---|
| 0:00 | Slide: six systems, one customer appearing four times | "Priya is in four systems with three spellings, two phone formats and conflicting consent. Marketing sends her four messages, one of them without consent." |
| 0:30 | Lakehouse: `brz_crm` → `slv_customer_clean` row for Priya, `dq_flags` | "Tier 1 rules fix what is certain: the gmial.com typo, the phone to E.164, India → IN. Every fix is recorded." |
| 1:15 | `slv_match_candidate` filtered to her pairs: score, band, `rule_explanation` | "Tier 2 scores every candidate pair and says why. Household members share a phone but the model sees different first names and does not merge them." |
| 2:00 | A LOW pair with `ai_explanation` | "Only the ambiguous middle goes to Fabric AI functions, with names and yes/no facts, never raw contacts. AI refines the score; it cannot block the pipeline." |
| 2:30 | Power BI review page: select the pair → **Approve** | "The steward decides inside Power BI. The click runs a user data function that writes to the SQL database in one transaction, with the steward's identity." |
| 3:15 | Run notebook `05` (or pipeline); `gld_customer_current` for her master key; `gld_dq_audit` | "Next run applies the decision. One golden record, consent resolved most-restrictively, and every value traced to its source record." |
| 4:00 | Executive report: duplicate reduction, wasted spend, scorecard | "Before vs after: N source records became M customers; X% of campaign spend was going to duplicates or people without consent. The DQ gate passed all six rules." |
| 4:30 | Data agent question: "How many customers can we reach on WhatsApp in Bengaluru?" | "The marketing agent asks the Fabric data agent, which only sees certified Gold." |

Backup: record the full demo once it works (screen recording) and keep the local run
report (`local_data/report.json`); if AI functions or the write-back fail live, switch to them.
