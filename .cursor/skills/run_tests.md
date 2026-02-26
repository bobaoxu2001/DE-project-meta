# Skill: Run and Write Tests

## When to use
Use this skill when you need to run the test suite, add new tests, or debug test failures.

## Running tests

### Full test suite
```bash
cd meta-product-analytics-pipeline
python3 -m pytest tests/ -v
```

### Run specific test files
```bash
python3 -m pytest tests/test_etl.py -v          # ETL pipeline tests
python3 -m pytest tests/test_data_quality.py -v  # Data quality framework tests
python3 -m pytest tests/test_analytics.py -v     # Analytics module tests
```

### Run a single test
```bash
python3 -m pytest tests/test_etl.py::TestDataGeneration::test_user_generation_count -v
```

### Run with output
```bash
python3 -m pytest tests/ -v -s  # Show print/log output
```

## Test structure

### `tests/test_etl.py` (23 tests)
- **TestDataGeneration** — Validates user/event generation (counts, columns, uniqueness)
- **TestExtract** — Tests Parquet extraction with date filtering
- **TestTransform** — Tests cleaning, dimension building, fact table construction, aggregates
- **TestLoad** — Tests dimension loading, fact loading, incremental loads, verification
- **TestSchema** — Tests warehouse schema initialization and dimension seeding

### `tests/test_data_quality.py` (7 tests)
- Null rate checks, uniqueness checks, row counts, referential integrity, value ranges
- Uses a `populated_warehouse` fixture that creates a full test warehouse with data

### `tests/test_analytics.py` (10 tests)
- **TestEngagementAnalytics** — DAU/WAU/MAU, trends, cross-platform, power users
- **TestGrowthAnalytics** — Growth accounting, funnels, demographics
- **TestRetentionAnalytics** — N-day retention, by-segment, by-platform

## Key fixtures
- `sample_users` — 100 generated users (fast, no I/O)
- `sample_events` — Events for one day for 100 users
- `warehouse` — Temporary DuckDB with schema + seeded dimensions
- `populated_warehouse` — Full warehouse with users, events, and aggregates
- `analytics_warehouse` — 200 users, 7 days of events, with all aggregates

## Writing new tests
1. Place test files in `meta-product-analytics-pipeline/tests/`
2. Use existing fixtures (`sample_users`, `warehouse`, etc.) when possible
3. For tests requiring loaded data, use `populated_warehouse` or `analytics_warehouse`
4. DuckDB connections in fixtures are temporary (auto-cleaned after test)

## Common gotchas
- All tests use temporary directories and in-memory/temp DuckDB instances
- The `warehouse` fixture creates a temp DuckDB at a temp path with schema initialized
- DataFrame registration is required when using DataFrames in DuckDB SQL (`conn.register("name", df)`)
- Hour probability weights are normalized in `EventGenerator.__init__()` — raw weights in `_hour_weight()` sum to 1.10
