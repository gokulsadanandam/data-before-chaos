# Report: Customer 360 and campaign waste (executive)

Built on `sm_golden_customer` (Direct Lake). Measures are in `measures.dax`.

## Page 1 - "One customer, one record"

* KPI cards: **Golden Customers**, **Source Records**, **Duplicate Reduction %**,
  **High Confidence Share %**, **Consent Coverage %**.
* Bar chart: golden customers by number of source systems (`source_record_count`).
* Matrix: source system x golden customers (how much each system overlaps).
* Before / after table from `ops_run_metrics`: baseline vs latest for quarantine rate,
  review rate, DQ pass rates (use **Metric Baseline**, **Metric Latest**, **Metric Change**).

## Page 2 - "Where the money leaks"

* KPI cards: **Campaign Cost**, **Wasted Spend**, **Wasted Spend %**,
  **Reach Overstatement %**, **Sends Without Consent**.
* Clustered bar by campaign: Duplicate Send Cost vs Campaign Cost.
* Line chart by week: Sends vs Reach (People).
* Card: **Revenue per Golden Customer**, **Unmatched Order Revenue**.

## Page 3 - "Data quality scorecard"

* Table from `ops_dq_results` for the latest run: rule, pass rate, threshold, status icon.
* Line chart: pass rate per rule across runs (trend).
* Bar chart: quarantined records by `quarantine_reason`.
* Card: **Open Alerts** (from `ops_run_metrics`).

## Page 4 - "Trace any value" (drill-through)

Drill-through on `master_key`: golden record fields, the source records behind it
(`gld_customer_xref`) and the audit trail (`gld_dq_audit`: attribute, old value, new
value, rule, winning record). This answers "where did this phone number come from?".
