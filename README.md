# Product Analytics Data Pipeline

> Full project is inside [`meta-product-analytics-pipeline/`](meta-product-analytics-pipeline/) — see the [detailed README](meta-product-analytics-pipeline/README.md).

A production-grade data engineering project that builds an end-to-end analytics pipeline for social media product metrics across a family of applications (Facebook, Instagram, Messenger, WhatsApp, Threads).

## Quick Start

```bash
cd meta-product-analytics-pipeline

# Install dependencies
pip install -r requirements.txt

# Production scale (10K users × 90 days → 5.7M events, 1.4 GB, ~2 min)
python3 run_pipeline.py

# Quick demo (~1.3s)
python3 run_pipeline.py --users 500 --days 7

# Run tests (60 tests)
python3 -m pytest tests/ -v
```

## What This Project Demonstrates

| Skill | Implementation |
|-------|---------------|
| **ETL Pipeline Design** | Custom Extract → Transform → Load framework with full + incremental modes |
| **Dimensional Modeling** | Kimball star schema: `fct_events` + 4 dimensions + 3 aggregate tables |
| **SQL Analytics** | Window functions, CTEs, cohort analysis, funnel analysis, growth accounting |
| **Data Quality Engineering** | 17+ automated checks (completeness, uniqueness, freshness, RI, ranges) |
| **Data Visualization** | 7 interactive Plotly charts (DAU trends, funnels, retention heatmaps, geo) |
| **Performance Engineering** | Vectorized NumPy data generation, pandas `groupby().agg()` aggregation |
| **Configuration Management** | Centralized YAML config with `src/config.py` loader module |
| **Testing** | 60 pytest tests: unit, integration, edge cases (empty inputs, bounds, idempotency) |
| **Orchestration** | Production-ready Apache Airflow DAG with quality gates |

## Tech Stack

`Python 3.10+` · `DuckDB` · `pandas` · `NumPy` · `PyArrow / Parquet` · `Plotly / Dash` · `Faker` · `pytest` · `Apache Airflow` · `YAML`

## License

MIT
