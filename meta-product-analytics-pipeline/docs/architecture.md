# Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Product Analytics Pipeline                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────────┐    │
│  │  Data Lake   │    │   ETL Layer  │    │  Analytical Warehouse│    │
│  │  (Parquet)   │───▶│              │───▶│   (DuckDB)          │    │
│  │             │    │  Extract     │    │                     │    │
│  │  • Events   │    │  Transform   │    │  Star Schema:       │    │
│  │  • Users    │    │  Load        │    │  • fct_events       │    │
│  │             │    │  Validate    │    │  • dim_users        │    │
│  └─────────────┘    └──────────────┘    │  • dim_date         │    │
│                            │            │  • dim_platform     │    │
│                     ┌──────▼──────┐     │  • dim_event_type   │    │
│                     │  Data       │     │                     │    │
│                     │  Quality    │     │  Aggregates:        │    │
│                     │  Checks     │     │  • agg_daily_metrics│    │
│                     └─────────────┘     │  • agg_engagement   │    │
│                                         │  • agg_retention    │    │
│                                         └──────────┬──────────┘    │
│                                                     │              │
│  ┌──────────────────────────────────────────────────▼──────────┐   │
│  │                   Analytics Layer                            │   │
│  │                                                              │   │
│  │  ┌───────────┐  ┌───────────┐  ┌────────────┐              │   │
│  │  │Engagement │  │  Growth   │  │ Retention  │              │   │
│  │  │Analytics  │  │ Analytics │  │ Analytics  │              │   │
│  │  └─────┬─────┘  └─────┬─────┘  └──────┬─────┘              │   │
│  │        └───────────────┼───────────────┘                    │   │
│  └────────────────────────┼────────────────────────────────────┘   │
│                           │                                        │
│  ┌────────────────────────▼────────────────────────────────────┐   │
│  │              Visualization Layer (Plotly/Dash)               │   │
│  │  • DAU Trends  • Funnels  • Retention Heatmaps  • Geo Maps │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Orchestration (Apache Airflow)                  │   │
│  │  Daily DAG: extract → transform → load → validate → notify  │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Model (Star Schema)

```
                    ┌─────────────┐
                    │  dim_date   │
                    │─────────────│
                    │ date_key PK │
                    │ year        │
                    │ quarter     │
                    │ month       │
                    │ day_of_week │
                    │ is_weekend  │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌────────────────┐
│ dim_platform │    │  fct_events  │    │ dim_event_type │
│──────────────│    │──────────────│    │────────────────│
│platform_key  │◄───│event_id   PK │───►│event_type_key  │
│platform_name │    │event_timestamp│    │event_type_name │
│platform_family│   │date_key   FK │    │event_category  │
└──────────────┘    │user_key   FK │    │is_active_event │
                    │platform_key FK│    └────────────────┘
                    │event_type_key │
                    │session_id    │
                    │event_count   │
                    └──────┬───────┘
                           │
                    ┌──────┴──────┐
                    │  dim_users  │
                    │─────────────│
                    │ user_key PK │
                    │ user_id     │
                    │ country     │
                    │ age_group   │
                    │ device_type │
                    │ user_segment│
                    │ signup_date │
                    │ SCD-2 cols  │
                    └─────────────┘
```

## Key Design Decisions

### 1. DuckDB as Analytical Engine
- **Why**: Columnar, in-process, zero-config, excellent for analytics
- **Trade-off**: Not distributed — production would use Presto/Spark/BigQuery
- **Benefit**: Demonstrates SQL skills without infrastructure overhead

### 2. Parquet Data Lake
- **Why**: Columnar format, compression, schema evolution support
- **Pattern**: Date-partitioned (`dt=YYYY-MM-DD/events.parquet`)
- **Benefit**: Efficient incremental processing

### 3. Star Schema Dimensional Model
- **Why**: Optimized for analytical queries, simple JOINs
- **Features**: SCD Type 2 for user dimension, surrogate keys
- **Benefit**: Industry-standard approach for data warehousing

### 4. Incremental + Full-Refresh ETL
- **Why**: Supports both backfill and daily incremental processing
- **Pattern**: Partition-based idempotent loads
- **Benefit**: Production-ready pipeline architecture

### 5. Data Quality as First-Class Citizen
- **Why**: Critical for trustworthy analytics
- **Checks**: Completeness, uniqueness, freshness, referential integrity
- **Benefit**: Quality gates prevent bad data from reaching stakeholders
