# Architecture Overview

## System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      Product Analytics Pipeline                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌───────────────────────┐    │
│  │   Data Lake      │    │   ETL Layer      │    │ Analytical Warehouse  │    │
│  │   (Parquet)      │───▶│                  │───▶│ (DuckDB Star Schema)  │    │
│  │                  │    │  Extract         │    │                       │    │
│  │  • Events        │    │  Transform       │    │ Fact:                 │    │
│  │    (partitioned) │    │  Load            │    │  • fct_events (5.7M+) │    │
│  │  • Users         │    │  (vectorized)    │    │                       │    │
│  │                  │    │                  │    │ Dimensions:           │    │
│  │  305 MB Parquet  │    │                  │    │  • dim_users (SCD-2)  │    │
│  └─────────────────┘    └────────┬─────────┘    │  • dim_date           │    │
│                                   │              │  • dim_platform       │    │
│  ┌─────────────────┐      ┌──────▼──────┐       │  • dim_event_type     │    │
│  │ Configuration    │      │  Data       │       │                       │    │
│  │ (YAML + loader)  │─────▶│  Quality    │       │ Aggregates:           │    │
│  │                  │      │  17+ checks │       │  • agg_daily_metrics  │    │
│  │ pipeline_config  │      │  (config-   │       │  • agg_engagement     │    │
│  │   .yaml          │      │   driven)   │       │  • agg_retention      │    │
│  └─────────────────┘      └─────────────┘       └───────────┬───────────┘    │
│                                                              │                │
│  ┌───────────────────────────────────────────────────────────▼────────────┐   │
│  │                       Analytics Layer                                  │   │
│  │                                                                        │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌──────────────┐             │   │
│  │   │ Engagement  │    │   Growth    │    │  Retention   │             │   │
│  │   │ Analytics   │    │  Analytics  │    │  Analytics   │             │   │
│  │   │             │    │             │    │              │             │   │
│  │   │ DAU/WAU/MAU │    │ Growth Acct │    │ N-day curves │             │   │
│  │   │ Stickiness  │    │ Funnels     │    │ Cohort matrix│             │   │
│  │   │ Cross-plat  │    │ Quick Ratio │    │ Churn risk   │             │   │
│  │   │ Power users │    │ Demographics│    │ By segment   │             │   │
│  │   └──────┬──────┘    └──────┬──────┘    └──────┬───────┘             │   │
│  │          └──────────────────┼──────────────────┘                      │   │
│  └─────────────────────────────┼─────────────────────────────────────────┘   │
│                                │                                              │
│  ┌─────────────────────────────▼─────────────────────────────────────────┐   │
│  │               Visualization Layer (Plotly / Dash)                      │   │
│  │   • DAU Trends  • Platform Comparison  • Engagement Funnel            │   │
│  │   • Retention Heatmaps  • Growth Accounting  • Geo Distribution       │   │
│  │   • Engagement Score Distribution                                      │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │               Orchestration (Apache Airflow)                          │   │
│  │   Daily DAG: extract → transform+load → quality_gate → aggregates    │   │
│  │   Config-driven  •  Idempotent  •  Quality gates  •  Alerting        │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │               Testing (pytest — 60 tests)                             │   │
│  │   ETL (23) • Data Quality (7) • Analytics (10) • Edge Cases (20)     │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Scale

| Metric | Demo | Production |
|--------|------|------------|
| Users | 500 | 10,000–100,000 |
| Days | 7 | 90 |
| Events | 21K | 5.7M+ |
| Events/day | ~3K | ~64K |
| Raw data (Parquet) | 1.3 MB | 305 MB |
| DuckDB warehouse | 10 MB | 1.1 GB |
| Pipeline time | 1.3s | ~2 min |

## Data Model (Star Schema)

```
                    ┌──────────────────┐
                    │    dim_date      │
                    │    (730 rows)    │
                    │──────────────────│
                    │ date_key PK      │
                    │ year, quarter    │
                    │ month, week      │
                    │ day_of_week      │
                    │ is_weekend       │
                    │ fiscal_quarter   │
                    └────────┬─────────┘
                             │
┌────────────────┐    ┌──────┴──────────┐    ┌──────────────────┐
│ dim_platform   │    │   fct_events    │    │ dim_event_type   │
│ (5 rows)       │    │   (5.7M+ rows)  │    │ (15 rows)        │
│────────────────│    │─────────────────│    │──────────────────│
│platform_key PK │◄───│event_id PK      │───►│event_type_key PK │
│platform_name   │    │event_timestamp  │    │event_type_name   │
│platform_family │    │date_key FK      │    │event_category    │
└────────────────┘    │user_key FK      │    │is_active_event   │
                      │platform_key FK  │    └──────────────────┘
                      │event_type_key FK│
                      │session_id       │
                      │country          │
                      │device_type      │
                      │event_count      │
                      │_partition_date  │
                      └────────┬────────┘
                               │
                      ┌────────┴────────┐
                      │   dim_users     │
                      │   (10K+ rows)   │
                      │   SCD Type 2    │
                      │─────────────────│
                      │ user_key PK     │
                      │ user_id         │
                      │ country         │
                      │ age_group       │
                      │ device_type     │
                      │ user_segment    │
                      │ signup_date     │
                      │ primary_platform│
                      │ effective_from  │
                      │ effective_to    │
                      │ is_current      │
                      └─────────────────┘
```

## Key Design Decisions

### 1. DuckDB as Analytical Engine
- **Why**: Columnar, in-process, zero-config, excellent for analytics
- **Scale**: Handles 5.7M+ events in a 1.1 GB warehouse
- **Trade-off**: Not distributed — production would use Presto/Spark/BigQuery
- **Benefit**: Demonstrates SQL skills without infrastructure overhead

### 2. Parquet Data Lake
- **Why**: Columnar format, compression, schema evolution support
- **Pattern**: Date-partitioned (`dt=YYYY-MM-DD/events.parquet`)
- **Scale**: 305 MB across 90 daily partitions
- **Benefit**: Efficient incremental processing

### 3. Vectorized Data Generation
- **Why**: NumPy array operations instead of row-by-row loops
- **Pattern**: Batch user attribute expansion with `np.repeat`, vectorized platform selection with `np.where`
- **Benefit**: Generates 64K+ events/day in <1s (was 3-5x slower with `iterrows`)

### 4. Star Schema Dimensional Model
- **Why**: Optimized for analytical queries, simple JOINs
- **Features**: SCD Type 2 for user dimension, surrogate keys
- **Benefit**: Industry-standard approach for data warehousing

### 5. Configuration-Driven Pipeline
- **Why**: Centralized, easy to tune for different environments
- **Pattern**: `pipeline_config.yaml` → `src/config.py` loader (cached)
- **Benefit**: DQ thresholds, defaults, and paths are not hardcoded

### 6. Incremental + Full-Refresh ETL
- **Why**: Supports both backfill and daily incremental processing
- **Pattern**: Partition-based idempotent loads with FK-safe table clearing
- **Benefit**: Production-ready pipeline architecture

### 7. Data Quality as First-Class Citizen
- **Why**: Critical for trustworthy analytics
- **Checks**: 17+ automated (completeness, uniqueness, freshness, RI, ranges)
- **Thresholds**: Configurable via YAML
- **Benefit**: Quality gates prevent bad data from reaching stakeholders

### 8. Comprehensive Testing
- **Why**: 60 tests covering ETL, DQ, analytics, and edge cases
- **Coverage**: Empty inputs, probability bounds, schema idempotency, config loading
- **Benefit**: Confidence in code correctness at scale
