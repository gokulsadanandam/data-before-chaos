# review_writeback (Fabric User Data Functions)

`function_app.py` is the code of the **User data functions** item that the Power BI
review page calls. It has one function, `submit_decision(pairId, decision, reason)`.

## Deploy

1. Workspace → **New item** → **User data functions** → name it `review_writeback`.
2. **Manage connections** → add the Fabric SQL database that holds the `review` schema,
   with alias **`ReviewDB`** (the alias must match `@udf.connection(alias="ReviewDB")`).
3. Replace the template code with `function_app.py` → **Publish**.
4. Test in the portal with a `pairId` from `review.v_open_queue` and decision `APPROVE`.
5. Give report users the **Execute** permission on the item; register them in
   `review.steward` (`fabric/sql/03_seed_stewards.sql`).

## Wire to Power BI

See `fabric/powerbi/review_page.md` (button → Data function action, parameters bound to
the selected pair and slicers / text inputs).

## Tested

`tests/unit/test_review_writeback.py` runs this file with a stub of `fabric.functions`
and a fake DB connection: validation, role check, the concurrent-click case and rollback.
