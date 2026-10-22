# Contributing

1. `make setup` once; `pre-commit` then runs ruff, formatting and `nbstripout` on commit.
2. Branch from `main` (`feature/<short-name>`), keep changes small, open a pull request.
3. CI must be green: `make ci` locally runs the same lint, type check and tests.
4. New logic goes into `src/goldenrecord` with a test in `tests/unit` (Spark tests are
   marked `@pytest.mark.spark`). Notebooks only call the library; change them through
   `tools/build_notebooks.py` and `make notebooks`.
5. Config changes (weights, thresholds, rules) include before/after evaluation numbers in
   the PR description.
6. Never commit real personal data, secrets, or notebook outputs.

Commit messages: imperative mood, one line summary (`Add phone placeholder for store numbers`).
