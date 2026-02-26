# Skill: Debug Data Quality Issues

## When to use
Use this skill when data quality checks fail, data looks incorrect, or you need to investigate data integrity issues in the warehouse.

## Quick data quality check
```bash
cd meta-product-analytics-pipeline
python3 -c "
import duckdb
from src.data_quality.checks import DataQualityChecker
conn = duckdb.connect('data/warehouse/product_analytics.duckdb', read_only=True)
checker = DataQualityChecker(conn)
results = checker.run_all_checks()
for d in results['details']:
    icon = '✅' if d['status'] == 'passed' else '❌'
    print(f\"{icon} [{d['severity']}] {d['check']}: {d['message']}\")
conn.close()
"
```

## Common data quality issues

### 1. Schema not initialized (tables missing)
**Symptom**: `CatalogException: Table with name X does not exist!`
**Root cause**: The `WarehouseSchema.initialize()` method parses SQL by splitting on `;`. Comment lines before SQL statements must be stripped line-by-line (not checked at block level).
**Fix**: Ensure `initialize()` strips `--` comment lines from each statement block before execution.

### 2. DataFrame not registered with DuckDB
**Symptom**: `CatalogException: Table with name df does not exist!`
**Root cause**: DuckDB SQL cannot reference Python variables directly. DataFrames must be registered first.
**Fix**: Use `conn.register("name", df)` before SQL and `conn.unregister("name")` after.

### 3. Probability normalization
**Symptom**: `ValueError: probabilities do not sum to 1`
**Root cause**: Hardcoded probability arrays may have floating-point errors or sum != 1.0.
**Fix**: Always normalize probability arrays: `probs = probs / probs.sum()`

### 4. Referential integrity failures
**Symptom**: Orphaned foreign keys in fact table
**Debug query**:
```sql
SELECT COUNT(DISTINCT f.user_key)
FROM analytics.fct_events f
LEFT JOIN analytics.dim_users d ON f.user_key = d.user_key
WHERE d.user_key IS NULL;
```

### 5. Freshness check failures
**Symptom**: Data is stale (latest timestamp too old)
**Debug query**:
```sql
SELECT MAX(event_timestamp) as latest,
       NOW() - MAX(event_timestamp) as age
FROM analytics.fct_events;
```

## Inspecting the warehouse
```python
import duckdb
conn = duckdb.connect("data/warehouse/product_analytics.duckdb", read_only=True)

# Table stats
tables = conn.execute("""
    SELECT table_name, estimated_size
    FROM information_schema.tables
    WHERE table_schema = 'analytics'
""").fetchdf()
print(tables)

# Sample data
print(conn.execute("SELECT * FROM analytics.fct_events LIMIT 5").fetchdf())
print(conn.execute("SELECT * FROM analytics.dim_users LIMIT 5").fetchdf())

conn.close()
```
