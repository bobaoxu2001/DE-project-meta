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

### Environment notes

- Python 3.12.3 is available system-wide as `python3` (not `python`).
- Pip installs to `~/.local/` — ensure `$HOME/.local/bin` is on `PATH`.
- `great-expectations` and `apache-airflow` are not imported by the core pipeline; they are only used by the optional Airflow DAG and are listed as development/production dependencies.

### Skills

Skill files in `.cursor/skills/` document SOPs for common tasks:

| Skill | File | Use when |
|-------|------|----------|
| Run Pipeline | `run_pipeline.md` | Running the full ETL pipeline end-to-end |
| Run Tests | `run_tests.md` | Running or writing tests |
| Debug Data Quality | `debug_data_quality.md` | Investigating DQ failures or data issues |
| ETL Development | `etl_development.md` | Modifying the ETL pipeline or data model |
| Analytics Queries | `analytics_queries.md` | Running analytics or building dashboards |

### DuckDB patterns

- **DataFrame registration**: Always register DataFrames before using them in DuckDB SQL: `conn.register("name", df)` → use in SQL → `conn.unregister("name")`.
- **Schema initialization**: The DDL parser in `schema.py` strips `--` comment lines before executing each statement block. If you add new SQL files, keep comments on their own lines (not inline with SQL).
