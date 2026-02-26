# Product Analytics Data Pipeline

A production-grade data engineering project that builds an end-to-end analytics pipeline for social media product metrics across a family of applications (Facebook, Instagram, Messenger, WhatsApp, Threads).

**Demonstrates**: ETL pipeline design, dimensional data modeling (star schema), advanced SQL analytics, data quality engineering, cohort analysis, and interactive visualization — the core skills of a Data Engineer in Product Analytics.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      Product Analytics Pipeline                              │
│                                                                              │
│   Synthetic Data ──▶ Data Lake ──▶ ETL Pipeline ──▶ Star Schema (DuckDB)    │
│   (Faker + NumPy)    (Parquet)     (Extract /       (Kimball Model)          │
│   100K+ users        Partitioned    Transform /      Fact + Dimensions       │
│   Millions events    by date        Load + DQ)       + Aggregates            │
│                                        │                    │                │
│                                        ▼                    ▼                │
│                                   Data Quality         Analytics Layer       │
│                                   17+ checks           ┌──────────────┐     │
│                                   (completeness,       │ Engagement   │     │
│                                    uniqueness,         │ Growth       │     │
│                                    freshness,          │ Retention    │     │
│                                    RI, ranges)         └──────┬───────┘     │
│                                                               ▼             │
│                                                        7 Plotly Charts      │
│                                                        (DAU, Funnels,       │
│                                                         Heatmaps, Geo)      │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Generation** | Python, Faker, NumPy (vectorized) | Realistic synthetic event data for 5 platforms |
| **Data Lake** | Apache Parquet | Date-partitioned columnar storage |
| **ETL Pipeline** | Python (custom framework) | Extract, Transform, Load with quality gates |
| **Data Warehouse** | DuckDB (star schema) | Kimball dimensional model with fact + dim tables |
| **Data Quality** | Custom DQ framework (config-driven) | 17+ automated checks with configurable thresholds |
| **Analytics** | SQL + Python | Engagement, Growth, Retention analytics modules |
| **Visualization** | Plotly / Dash | 7 interactive dashboard panels |
| **Configuration** | YAML + config loader | Centralized pipeline settings (`pipeline_config.yaml`) |
| **Orchestration** | Apache Airflow DAG | Production-ready daily pipeline with quality gates |
| **Testing** | pytest | 60 unit/integration/edge-case tests |

---

## Quick Start

### Prerequisites
- Python 3.10+
- pip

### Setup

```bash
# Clone the repository
git clone https://github.com/bobaoxu2001/DE-project-meta.git
cd DE-project-meta/meta-product-analytics-pipeline

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

> **Note**: `great-expectations` and `apache-airflow` have a version conflict with `pandas==2.2.3`. If you encounter installation errors, install them separately:
> ```bash
> pip install duckdb==1.1.3 pandas==2.2.3 numpy==1.26.4 plotly==5.24.1 dash==2.18.2 pyyaml==6.0.2 faker==30.8.2 pyarrow==18.1.0 pytest==8.3.4 scipy==1.14.1 kaleido==0.2.1
> pip install great-expectations==1.2.1 --no-deps
> pip install apache-airflow==2.10.4 --no-deps
> ```

### Run the Full Pipeline

```bash
# Production scale (10K users, 90 days, ~5.7M events, ~2 min)
python3 run_pipeline.py

# Quick demo (500 users, 7 days, ~21K events, ~1.3s)
python3 run_pipeline.py --users 500 --days 7

# Medium scale with visualizations
python3 run_pipeline.py --users 5000 --days 30

# Skip visualization generation
python3 run_pipeline.py --users 10000 --days 90 --skip-viz
```

### Run Tests

```bash
# Full test suite (60 tests)
python3 -m pytest tests/ -v

# Run specific test modules
python3 -m pytest tests/test_etl.py -v           # ETL pipeline tests
python3 -m pytest tests/test_data_quality.py -v   # Data quality tests
python3 -m pytest tests/test_analytics.py -v      # Analytics module tests
python3 -m pytest tests/test_edge_cases.py -v     # Edge case tests
```

### Explore in Jupyter

```bash
jupyter notebook notebooks/product_analytics_exploration.ipynb
```

---

## Project Structure

```
meta-product-analytics-pipeline/
│
├── README.md                          # This file
├── requirements.txt                   # Python dependencies (13 packages)
├── run_pipeline.py                    # One-command pipeline runner
│
├── config/
│   └── pipeline_config.yaml           # Centralized pipeline configuration
│
├── src/
│   ├── config.py                      # Configuration loader (cached YAML access)
│   │
│   ├── data_generation/
│   │   └── generate_events.py         # Vectorized synthetic data generator
│   │
│   ├── etl/
│   │   ├── extract.py                 # Extract from data lake (Parquet)
│   │   ├── transform.py               # Clean, validate, build dimensions & facts
│   │   ├── load.py                    # Load into DuckDB warehouse (register-based)
│   │   └── pipeline.py                # End-to-end orchestrator (full + incremental)
│   │
│   ├── models/
│   │   └── schema.py                  # Warehouse schema manager (DDL + seeding)
│   │
│   ├── data_quality/
│   │   └── checks.py                  # 17+ automated DQ checks (config-driven)
│   │
│   ├── analytics/
│   │   ├── engagement.py              # DAU/WAU/MAU, stickiness, cross-platform
│   │   ├── growth.py                  # Growth accounting, funnels, Quick Ratio
│   │   └── retention.py               # Cohort retention, churn prediction
│   │
│   └── visualization/
│       └── dashboards.py              # 7 Plotly dashboard panels (config-driven)
│
├── sql/
│   ├── create_tables.sql              # Star schema DDL (8 tables + indexes)
│   ├── etl_queries.sql                # Dimension seeding + analytical transforms
│   └── analytics_queries.sql          # Advanced analytical queries
│
├── airflow/
│   └── dags/
│       └── product_analytics_dag.py   # Production Airflow DAG
│
├── notebooks/
│   └── product_analytics_exploration.ipynb  # Interactive analysis
│
├── tests/
│   ├── test_etl.py                    # ETL pipeline tests (23 tests)
│   ├── test_data_quality.py           # DQ framework tests (7 tests)
│   ├── test_analytics.py              # Analytics module tests (10 tests)
│   └── test_edge_cases.py             # Edge case + boundary tests (20 tests)
│
└── docs/
    ├── architecture.md                # System architecture
    └── data_model.md                  # Dimensional model documentation
```

---

## Data Model

### Star Schema Design

The warehouse uses a **Kimball-style star schema** with:

- **Fact table**: `fct_events` — one row per user interaction event (millions of rows)
- **Dimensions**: `dim_users` (SCD-2), `dim_date`, `dim_platform`, `dim_event_type`
- **Aggregates**: `agg_daily_metrics`, `agg_user_engagement`, `agg_retention_cohorts`

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (730 rows) │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌────────────────┐
│ dim_platform │    │  fct_events  │    │ dim_event_type │
│  (5 rows)    │◄───│  (millions)  │───►│  (15 rows)     │
└──────────────┘    └──────┬───────┘    └────────────────┘
                           │
                    ┌──────┴──────┐
                    │  dim_users  │
                    │  (SCD-2)    │
                    └─────────────┘
```

See [docs/data_model.md](docs/data_model.md) for the full schema documentation.

---

## Analytics Highlights

### 1. Engagement Metrics
- **DAU / WAU / MAU** with moving averages
- **DAU/MAU ratio** (stickiness) — the metric Meta watches most closely
- **Cross-platform usage** — how users engage across the app family
- **Power user analysis** — Pareto distribution of engagement

### 2. Growth Analytics
- **Growth accounting**: New / Retained / Resurrected / Churned
- **Quick Ratio**: (New + Resurrected) / Churned — health indicator
- **Engagement funnel**: View → Like → Comment → Share → Create
- **Geographic and demographic breakdowns**

### 3. Retention Analytics
- **N-day retention curves** (D1, D3, D7, D14, D30)
- **Weekly cohort retention matrix** with heatmap
- **Retention by segment and platform**
- **Churn risk scoring** with ML-ready features

### 4. Data Quality
- **17+ automated checks** with configurable thresholds (`pipeline_config.yaml`):
  - Completeness (null rates)
  - Uniqueness (duplicate detection)
  - Freshness (data recency)
  - Referential integrity (FK validation)
  - Value ranges (anomaly detection)
  - Volume (row count thresholds)

---

## Dashboard Panels

7 interactive Plotly charts generated to `data/processed/charts/`:

| Panel | Chart Type | Description |
|-------|------------|-------------|
| DAU Trend | Bar + Line | Daily active users with 7-day moving average |
| Platform Comparison | Bar + Donut | Side-by-side metrics across 5 platforms |
| Engagement Funnel | Funnel | View → Like → Comment → Share → Create |
| Retention Heatmap | Heatmap | Weekly cohort retention matrix (configurable platform) |
| Growth Accounting | Stacked Area | DAU composition (new / retained / resurrected) |
| Geographic Map | Choropleth | User distribution by country |
| Engagement Distribution | Histogram | Score distribution by user segment |

---

## Configuration

The pipeline is centrally configured via `config/pipeline_config.yaml`:

| Section | Key settings |
|---------|-------------|
| `database` | DuckDB warehouse path and schema name |
| `data_generation` | Default user count, days, platforms, event types |
| `etl` | Batch size, parallel workers, retry settings |
| `data_quality` | Null threshold, duplicate threshold, freshness window, min row counts |
| `analytics` | Retention windows, cohort period, growth metrics |
| `visualization` | Dashboard port, Plotly theme, refresh interval |
| `logging` | Log level, format, file path |

Access config values programmatically:
```python
from src import config as cfg

db_path = cfg.get("database", "path")
dq_threshold = cfg.get("data_quality", "null_threshold", default=0.01)
retention_days = cfg.get("analytics", "retention_windows")
```

---

## SQL Highlights

The `sql/` directory contains production-quality analytical queries:

- **Window functions**: Moving averages, LAG/LEAD for growth rates
- **CTEs**: Complex multi-step transformations
- **Cohort analysis**: Retention matrices with date arithmetic
- **Funnel analysis**: Step-by-step conversion rates
- **Pareto analysis**: PERCENT_RANK for power user concentration
- **Growth accounting**: User classification (new/retained/resurrected)

---

## Performance & Scale

| Scale | Users | Days | Events | Raw Data | Warehouse | Time |
|-------|-------|------|--------|----------|-----------|------|
| **Quick demo** | 500 | 7 | 21K | 1.3 MB | 10 MB | ~1.3s |
| **Medium** | 5,000 | 30 | 1M+ | 45 MB | 160 MB | ~15s |
| **Production** | 10,000 | 90 | 5.7M | 305 MB | 1.1 GB | ~2 min |
| **Large** | 50,000 | 90 | 30M+ | 1.5 GB | 6+ GB | ~15 min |

- **Event generation**: Vectorized NumPy (no `iterrows()`) — 64K events/day/sec
- **Daily aggregates**: Vectorized `groupby().agg()` instead of manual loops
- **DuckDB**: Columnar storage with automatic query optimization
- **Test suite**: 60 tests in ~5.5s

---

## Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **DuckDB** over Spark/BigQuery | Zero-config analytical DB; demonstrates SQL skills without infra overhead |
| **Parquet** data lake | Columnar, compressed, schema-aware; industry standard |
| **Star schema** over flat tables | Optimized for analytical queries; industry-standard dimensional modeling |
| **SCD Type 2** for users | Tracks historical changes; demonstrates data modeling depth |
| **Custom DQ framework** | Shows understanding of data quality principles beyond just using a library |
| **Incremental ETL** | Partition-based idempotent loads; production-ready pattern |
| **Vectorized data gen** | NumPy array ops instead of row-by-row loops for scalability |
| **Config-driven thresholds** | DQ checks and pipeline defaults from YAML, not hardcoded |
| **Airflow DAG** | Industry-standard orchestration with quality gates and alerting |

---

## Sample Pipeline Output

```
============================================================
  PRODUCT ANALYTICS PIPELINE — COMPLETE
============================================================
  Users generated:    10,000
  Days of data:       90
  Total events:       5,725,239
  Data quality:       100.0% (17/17 checks passed)
  Total time:         114.06s
  Warehouse:          data/warehouse/product_analytics.duckdb
============================================================
```

---

## License

MIT
