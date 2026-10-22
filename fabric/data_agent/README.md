# Fabric data agent: `da_golden_customer`

The marketing agent never queries raw sources. It asks the **Fabric data agent**, which
answers over the certified Gold tables only. Create it in the workspace
(**New item → Data agent**), add the Lakehouse `lh_golden_record` as a source and select
only these tables:

* `gld_customer_current`, `gld_order`, `gld_engagement`, `gld_customer_xref`

Paste `instructions.md` into **Agent instructions** and add the pairs in
`example_queries.md` under **Example queries** for the Lakehouse source. Publish, then
connect it from Azure AI Foundry (the Fabric tool for Foundry agents uses the end
user's identity, so the agent only sees what that user may see).
