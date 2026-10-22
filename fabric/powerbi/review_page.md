# Report page: Steward review (translytical write-back)

The review page lets a steward approve, reject or hold a suggested match **inside Power
BI**. The button calls the `submit_decision` user data function, which writes to the
Fabric SQL database in one transaction.

## Data for the page

Connect the report (or a second semantic model) to the **SQL database** in
**DirectQuery** so the queue updates as soon as a decision is written:

* `review.v_open_queue` - open items (masked contact details only)
* `review.v_queue_summary` - counts for the header cards
* `review.v_steward_activity` - decisions per steward per day

## Layout

| Area | Visual | Fields |
|---|---|---|
| Header | 3 cards | open items, spot checks, items older than 48 h (`v_queue_summary`) |
| Left | Table (single select) | `priority`, `band`, `confidence`, `left_name`, `right_name`, `review_type` sorted by `priority` desc |
| Right top | Two multi-row cards | Record A / Record B: name, masked email, masked phone, city, postcode, source |
| Right middle | Card | `explanation` (AI or rule-based sentence) and `evidence` |
| Right bottom | Text slicer `Reason` + 3 buttons | **Approve**, **Reject**, **Hold** |

Rows with `confidence` under 0.6 can be highlighted with conditional formatting so the
steward reads the evidence before deciding.

## Wire the buttons (Data function action)

For each button: **Format → Action → Type: Data function**

| Setting | Value |
|---|---|
| Workspace / item | the workspace / `review_writeback` |
| Function | `submit_decision` |
| `pairId` | *Conditional formatting →* field value: `v_open_queue[pair_id]` (selected row; use `SELECTEDVALUE` in a measure `Selected Pair = SELECTEDVALUE ( v_open_queue[pair_id] )`) |
| `decision` | constant `APPROVE` / `REJECT` / `HOLD` (one per button) |
| `reason` | the text slicer `Reason` |
| Refresh the report after a successful outcome | **On** |

Disable the buttons when nothing is selected: button *Disabled* state with the
condition `ISBLANK ( [Selected Pair] )`.

## Permissions checklist

* Stewards: **Read** on the report, **Execute** on the `review_writeback` item, and a row
  in `review.steward` (role `steward` or `lead`).
* The SQL database connection in the functions item uses the item's identity; stewards
  do not need direct SQL permissions.
