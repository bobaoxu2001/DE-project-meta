# Meta-Style Product Analytics Data Pipeline

## What Is This?

This project simulates what a **Data Engineer at Meta (Facebook)** would build: a production-grade analytics pipeline that processes **millions of user interaction events** across Meta's family of apps — Facebook, Instagram, Messenger, WhatsApp, and Threads.

The pipeline ingests raw event data → cleans and transforms it into an analytical data warehouse → computes business-critical metrics like DAU/MAU stickiness, retention cohorts, growth accounting, and engagement funnels → generates interactive dashboards.

## The Business Problem

Product teams at social media companies need answers to questions like:

- **How many users are active daily?** (DAU/WAU/MAU and stickiness ratios)
- **Are we growing?** (New vs. retained vs. resurrected vs. churned users)
- **Where do users drop off?** (View → Like → Comment → Share → Create funnels)
- **Do users come back?** (D1/D7/D30 retention by cohort, segment, platform)
- **Who is at risk of leaving?** (Churn risk scoring with ML-ready features)
- **How do users engage across apps?** (Cross-platform usage patterns)

This pipeline answers all of them at scale.

## Data Source

The pipeline generates **realistic synthetic event data** that mirrors real social media usage patterns:

| Attribute | Detail |
|-----------|--------|
| **Users** | 10,000 synthetic user profiles with demographics (16 countries, 7 age groups, 4 device types) |
| **Platforms** | Facebook, Instagram, Messenger, WhatsApp, Threads |
| **Events** | 15 event types: `app_open`, `content_view`, `like`, `comment`, `share`, `content_create`, `message_sent`, `story_view`, `ad_impression`, etc. |
| **Time range** | 90 days of daily event data |
| **Volume** | **5.7 million events** (~64K/day) |
| **Realism** | Weighted hourly distributions (evening peaks), day-of-week seasonality, segment-based activity levels (power/active/casual/dormant), 70/30 primary-platform preference |

The data is generated using **vectorized NumPy operations** for performance — no slow row-by-row loops.

## Architecture

```
   Synthetic Data          Data Lake            ETL Pipeline           Star Schema
  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │ 10K users    │    │ 90 Parquet   │    │ Extract          │    │ fct_events       │
  │ 5.7M events  │───▶│ partitions   │───▶│ Transform        │───▶│ (5.7M rows)      │
  │ 15 event     │    │ (305 MB)     │    │ Load             │    │                  │
  │ types        │    │ dt=YYYY-MM-DD│    │ Quality Check    │    │ dim_users (SCD-2)│
  └──────────────┘    └──────────────┘    │ (17+ checks)     │    │ dim_date (730)   │
                                          └──────────────────┘    │ dim_platform (5) │
                                                                  │ dim_event_type   │
                                                   │              │ (15)             │
                                                   ▼              └────────┬─────────┘
                                           Config-driven                   │
                                           thresholds                      ▼
                                           (YAML)               ┌──────────────────┐
                                                                │ Analytics Layer  │
                                                                │                  │
                                                                │ • Engagement     │
                                                                │ • Growth         │
                                                                │ • Retention      │
                                                                │ • 7 Dashboards   │
                                                                └──────────────────┘
```

## Key Results (from 10K users × 90 days)

```
Pipeline Output:
  Total events processed:    5,725,239
  Data quality:              100.0% (17/17 checks passed)
  DuckDB warehouse:          1.1 GB
  Pipeline execution time:   ~2 minutes
```

**Engagement**: DAU stickiness ratio (DAU/MAU), cross-platform usage distribution, Pareto power user analysis

**Growth**: Daily growth accounting (new/retained/resurrected), Quick Ratio, engagement funnels per platform

**Retention**: D1/D3/D7/D14/D30 retention by cohort, retention by segment (power/active/casual/dormant), churn risk scoring

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Generation | Python, NumPy (vectorized), Faker |
| Data Lake | Apache Parquet (date-partitioned) |
| Data Warehouse | DuckDB (Kimball star schema, SCD-2) |
| ETL | Custom Python framework (full + incremental) |
| Data Quality | Custom framework (17+ config-driven checks) |
| Analytics | SQL (window functions, CTEs, cohort analysis) + Python |
| Visualization | Plotly / Dash (7 interactive charts) |
| Orchestration | Apache Airflow (daily DAG with quality gates) |
| Configuration | YAML (`pipeline_config.yaml`) + cached loader |
| Testing | pytest (60 tests: unit, integration, edge cases) |

## Quick Start

```bash
cd meta-product-analytics-pipeline
pip install -r requirements.txt

# Production scale (10K users × 90 days → 5.7M events, ~2 min)
python3 run_pipeline.py

# Quick demo (500 users × 7 days → 21K events, ~1.3s)
python3 run_pipeline.py --users 500 --days 7

# Run all 60 tests
python3 -m pytest tests/ -v
```

## Project Structure

```
meta-product-analytics-pipeline/
├── run_pipeline.py              # One-command pipeline runner
├── config/pipeline_config.yaml  # Centralized configuration
├── src/
│   ├── config.py                # YAML config loader (cached)
│   ├── data_generation/         # Vectorized synthetic data generator
│   ├── etl/                     # Extract, Transform, Load, Pipeline orchestrator
│   ├── models/                  # DuckDB schema manager (DDL + seeding)
│   ├── data_quality/            # 17+ automated checks (config-driven)
│   ├── analytics/               # Engagement, Growth, Retention modules
│   └── visualization/           # 7 Plotly dashboard panels
├── sql/                         # Star schema DDL + analytical queries
├── airflow/dags/                # Production Airflow DAG
├── notebooks/                   # Interactive Jupyter exploration
├── tests/                       # 60 pytest tests
└── docs/                        # Architecture + data model docs
```

See the [detailed README](meta-product-analytics-pipeline/README.md) for full documentation.

## License

MIT
