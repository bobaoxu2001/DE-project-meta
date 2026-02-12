# Product Analytics Data Pipeline

A production-grade data engineering project that builds an end-to-end analytics pipeline for social media product metrics across a family of applications (Facebook, Instagram, Messenger, WhatsApp, Threads).

**Demonstrates**: ETL pipeline design, dimensional data modeling (star schema), advanced SQL analytics, data quality engineering, cohort analysis, and interactive visualization — the core skills of a Data Engineer in Product Analytics.

---

## Architecture

```
Data Lake (Parquet)  →  ETL Pipeline  →  Star Schema (DuckDB)  →  Analytics & Dashboards
     ↑                      ↑                    ↑                        ↑
 Synthetic data       Extract/Transform     Dimensional Model      Plotly Interactive
 100K+ users          /Load/Validate        Fact + Dimensions      Charts & Notebooks
 Millions of events   Incremental loads     SCD Type 2             7+ Dashboard Panels
```

### Key Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Generation** | Python, Faker, NumPy | Realistic synthetic event data for 5 platforms |
| **Data Lake** | Apache Parquet | Date-partitioned columnar storage |
| **ETL Pipeline** | Python (custom framework) | Extract, Transform, Load with quality gates |
| **Data Warehouse** | DuckDB (star schema) | Kimball dimensional model with fact + dim tables |
| **Data Quality** | Custom DQ framework | 17+ automated checks (completeness, uniqueness, freshness, RI) |
| **Analytics** | SQL + Python | Engagement, Growth, Retention analytics modules |
| **Visualization** | Plotly / Dash | 7 interactive dashboard panels |
| **Orchestration** | Apache Airflow DAG | Production-ready daily pipeline with quality gates |
| **Testing** | pytest | 30+ unit/integration tests |

---

## Quick Start

### Prerequisites
- Python 3.10+
- pip

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/meta-product-analytics-pipeline.git
cd meta-product-analytics-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt
```

### Run the Full Pipeline

```bash
# Generate data + ETL + Analytics + Dashboards (default: 5K users, 30 days)
python run_pipeline.py

# Smaller demo for quick testing
python run_pipeline.py --users 1000 --days 7

# Skip visualization generation
python run_pipeline.py --skip-viz
```

### Run Tests

```bash
pytest tests/ -v
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
├── requirements.txt                   # Python dependencies
├── run_pipeline.py                    # One-command pipeline runner
│
├── config/
│   └── pipeline_config.yaml           # Pipeline configuration
│
├── src/
│   ├── data_generation/
│   │   └── generate_events.py         # Synthetic data generator (users + events)
│   │
│   ├── etl/
│   │   ├── extract.py                 # Extract from data lake (Parquet)
│   │   ├── transform.py               # Clean, validate, build dimensions & facts
│   │   ├── load.py                    # Load into DuckDB warehouse
│   │   └── pipeline.py                # End-to-end orchestrator
│   │
│   ├── models/
│   │   └── schema.py                  # Warehouse schema manager
│   │
│   ├── data_quality/
│   │   └── checks.py                  # 17+ automated DQ checks
│   │
│   ├── analytics/
│   │   ├── engagement.py              # DAU/WAU/MAU, stickiness, cross-platform
│   │   ├── growth.py                  # Growth accounting, funnels, Quick Ratio
│   │   └── retention.py               # Cohort retention, churn prediction
│   │
│   └── visualization/
│       └── dashboards.py              # Interactive Plotly dashboards
│
├── sql/
│   ├── create_tables.sql              # Star schema DDL
│   ├── etl_queries.sql                # ETL transformation SQL
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
│   ├── test_etl.py                    # ETL pipeline tests
│   ├── test_data_quality.py           # DQ framework tests
│   └── test_analytics.py             # Analytics module tests
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
- **17+ automated checks** covering:
  - Completeness (null rates)
  - Uniqueness (duplicate detection)
  - Freshness (data recency)
  - Referential integrity (FK validation)
  - Value ranges (anomaly detection)
  - Volume (row count thresholds)

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

## Dashboard Panels

| Panel | Description |
|-------|-------------|
| DAU Trend | Daily active users with 7-day moving average |
| Platform Comparison | Side-by-side metrics across 5 platforms |
| Engagement Funnel | View → Like → Comment → Share → Create |
| Retention Heatmap | Weekly cohort retention matrix |
| Growth Accounting | DAU composition (new/retained/resurrected) |
| Geographic Map | User distribution by country |
| Engagement Distribution | Score histogram by user segment |

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
| **Airflow DAG** | Industry-standard orchestration with quality gates and alerting |

---

## License

MIT
