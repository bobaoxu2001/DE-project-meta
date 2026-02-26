# Skill: Run the Product Analytics Pipeline

## When to use
Use this skill when you need to run the full ETL pipeline, generate synthetic data, or test pipeline changes end-to-end.

## Prerequisites
- Python 3.10+ available as `python3`
- Dependencies installed: `pip install -r meta-product-analytics-pipeline/requirements.txt`
- Working directory: `meta-product-analytics-pipeline/`

## Commands

### Full pipeline (quick demo)
```bash
cd meta-product-analytics-pipeline
python3 run_pipeline.py --users 500 --days 7 --skip-viz
```

### Full pipeline with visualizations
```bash
python3 run_pipeline.py --users 1000 --days 14
```

### Production-scale test
```bash
python3 run_pipeline.py --users 5000 --days 30
```

## What the pipeline does
1. **Data Generation** — Creates synthetic users and events using Faker + NumPy
2. **ETL Pipeline** — Extract from Parquet → Transform (clean, build dims/facts) → Load into DuckDB
3. **Data Quality** — Runs 17+ automated checks (completeness, uniqueness, freshness, RI)
4. **Analytics** — Engagement (DAU/WAU/MAU), Growth (funnels, Quick Ratio), Retention (cohorts)
5. **Visualization** (optional) — Generates Plotly charts to `data/processed/charts/`

## Outputs
- **DuckDB warehouse**: `data/warehouse/product_analytics.duckdb`
- **Raw data**: `data/raw/users/` and `data/raw/events/dt=YYYY-MM-DD/`
- **Charts** (if not skipped): `data/processed/charts/`

## Key tables in the warehouse
| Table | Description |
|-------|-------------|
| `analytics.fct_events` | Fact table — one row per user event |
| `analytics.dim_users` | SCD-2 user dimension |
| `analytics.dim_date` | Calendar dimension |
| `analytics.dim_platform` | Platform dimension (5 platforms) |
| `analytics.dim_event_type` | Event type dimension (15 types) |
| `analytics.agg_daily_metrics` | Pre-computed daily KPIs per platform |
| `analytics.agg_user_engagement` | Per-user engagement scores |
| `analytics.agg_retention_cohorts` | Weekly cohort retention matrix |

## Verifying results
After pipeline completes, you can query the warehouse:
```python
import duckdb
conn = duckdb.connect("data/warehouse/product_analytics.duckdb", read_only=True)
print(conn.execute("SELECT table_name, estimated_size FROM information_schema.tables WHERE table_schema='analytics'").fetchdf())
conn.close()
```
