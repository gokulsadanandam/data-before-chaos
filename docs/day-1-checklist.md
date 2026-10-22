# Day 1 on the sandbox (first 2-3 hours)

Goal of the first session: prove the full path Bronze → Gold on Fabric with the
synthetic data, before building reports.

- [ ] Confirm capacity (F2+ or trial) and that **New item** offers Lakehouse, Notebook,
      Environment, SQL database, User data functions, Data agent.
- [ ] Ask the organisers whether Key Vault is available. If not, plan `use_key_vault=false`.
- [ ] On a laptop: `make ci` green, `make wheel`, `make data`.
- [ ] Create `lh_golden_record`; upload `config/` and `landing/` (deployment guide §2).
- [ ] Create `env_goldenrecord` with the wheel; publish (§4).
- [ ] Import notebooks; attach Lakehouse + Environment; run `00_setup_checks` (§5).
- [ ] Run `01` → `05`; note the numbers from each notebook's output (baseline).
- [ ] If AI functions fail: re-run `03` with `use_fabric_ai=false`, raise it with the
      organisers, carry on.
- [ ] Create `sqldb_review` and run the four SQL scripts (§7).
- [ ] Share the workspace with the team (Contributor) and the stewards (Viewer).

Record anything that differs from the guide in `docs/deployment-guide.md` (pull request),
so the next person does not hit it.
