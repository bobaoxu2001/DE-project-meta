# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

Python-based product analytics data pipeline in `meta-product-analytics-pipeline/`. No external services required — DuckDB is embedded, data is synthetic. See `meta-product-analytics-pipeline/README.md` for full documentation.

### Commands

All commands run from `meta-product-analytics-pipeline/`:

- **Install deps:** `pip install -r requirements.txt` (note: `great-expectations==1.2.1` conflicts with `pandas==2.2.3`; install it with `--no-deps` separately, same for `apache-airflow==2.10.4`)
- **Run pipeline:** `python3 run_pipeline.py --users 1000 --days 7` (or `--skip-viz`)
- **Run tests:** `python3 -m pytest tests/ -v`
- **Lint:** No linter configured in the repo

### Known issues

- **Probability bug in `src/data_generation/generate_events.py`:** The `_hour_weight()` function returns weights that sum to ~1.10 instead of 1.0, causing `ValueError: probabilities do not sum to 1` when running the pipeline or most tests. This is a pre-existing code bug, not an environment issue.
- **Schema initialization failures in tests:** `WarehouseSchema.initialize()` silently skips DDL/index creation statements that reference tables not yet created, causing cascading test failures.

### Environment notes

- Python 3.12.3 is available system-wide as `python3` (not `python`).
- Pip installs to `~/.local/` — ensure `$HOME/.local/bin` is on `PATH`.
- `great-expectations` and `apache-airflow` are not imported by the core pipeline; they are only used by the optional Airflow DAG and are listed as development/production dependencies.
