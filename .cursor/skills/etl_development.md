# Skill: ETL Development Guide

## When to use
Use this skill when modifying the ETL pipeline, adding new data sources, or extending the data model.

## Architecture overview
```
data/raw/ (Parquet)  →  Extract  →  Transform  →  Load  →  DuckDB warehouse
                         ↓            ↓            ↓
                    Extractor    Transformer    Loader
                    extract.py   transform.py   load.py
```

## Key modules

### `src/data_generation/generate_events.py`
- `UserGenerator` — Creates synthetic user profiles
- `EventGenerator` — Creates event streams with realistic distributions
- `generate_demo_dataset()` — One-call function to generate a complete dataset

### `src/etl/extract.py`
- `Extractor` — Reads Parquet files from the data lake
- Supports date-range filtering via partition paths (`dt=YYYY-MM-DD/`)

### `src/etl/transform.py`
- `Transformer.clean_events()` — Deduplication, null handling, type casting, validation
- `Transformer.build_user_dimension()` — SCD-2 user dimension with surrogate keys
- `Transformer.build_fact_events()` — Fact table with resolved dimension keys
- `Transformer.compute_daily_aggregates()` — Pre-aggregated daily KPIs
- `Transformer.compute_engagement_scores()` — Per-user L1/L7/L28 engagement scores

### `src/etl/load.py`
- `Loader.load_dimension()` — Supports replace, append, and upsert modes
- `Loader.load_facts()` — Supports full and incremental (partition-based) loading
- `Loader.load_aggregates()` — Full replace for aggregate tables

### `src/models/schema.py`
- `WarehouseSchema` — Manages DuckDB schema lifecycle
- `initialize()` — Runs DDL from `sql/create_tables.sql`
- `seed_dimensions()` — Populates static dimensions from `sql/etl_queries.sql`

### `src/etl/pipeline.py`
- `ProductAnalyticsPipeline.run_full_pipeline()` — Full refresh: schema → extract → transform → load → aggregate → DQ
- `ProductAnalyticsPipeline.run_incremental()` — Single-date incremental load

## DuckDB DataFrame registration pattern
When loading DataFrames into DuckDB via SQL, always register first:
```python
conn.register("__temp_df", df)
conn.execute("INSERT INTO table_name SELECT * FROM __temp_df")
conn.unregister("__temp_df")
```

## Adding a new dimension
1. Add `CREATE TABLE` to `sql/create_tables.sql`
2. Add seed `INSERT` to `sql/etl_queries.sql`
3. Add foreign key to `fct_events` if needed
4. Update `Transformer` to resolve the new dimension key
5. Update `Loader` if custom load logic is needed
6. Add DQ checks in `src/data_quality/checks.py`
7. Add tests in `tests/`

## Adding a new analytics module
1. Create `src/analytics/new_module.py` implementing queries against the warehouse
2. Add to `run_pipeline.py` Step 3 section
3. Add tests in `tests/test_analytics.py`

## SQL files
- `sql/create_tables.sql` — Star schema DDL (schema + tables + indexes)
- `sql/etl_queries.sql` — Dimension seeding + analytical query templates
- `sql/analytics_queries.sql` — Standalone analytical queries
