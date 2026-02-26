# Skill: Analytics and Querying

## When to use
Use this skill when running analytics queries, building dashboards, or extending the analytics modules.

## Running analytics interactively
```python
import duckdb
conn = duckdb.connect("data/warehouse/product_analytics.duckdb", read_only=True)
```

## Key analytics modules

### Engagement (`src/analytics/engagement.py`)
```python
from src.analytics.engagement import EngagementAnalytics
ea = EngagementAnalytics(conn)

# DAU/WAU/MAU metrics
metrics = ea.get_dau_wau_mau("2025-11-07")

# DAU trend with 7-day moving average
trend = ea.get_dau_trend("2025-11-01", "2025-11-07")

# Cross-platform usage distribution
cross = ea.get_cross_platform_usage()

# Power user Pareto analysis
power = ea.get_power_user_analysis()
```

### Growth (`src/analytics/growth.py`)
```python
from src.analytics.growth import GrowthAnalytics
ga = GrowthAnalytics(conn)

# Growth accounting (new/retained/resurrected/churned)
growth = ga.get_growth_accounting("2025-11-01", "2025-11-07")

# Engagement funnel (view → like → comment → share → create)
funnel = ga.get_funnel_analysis("2025-11-07")

# Demographic breakdown
demo = ga.get_demographic_breakdown("2025-11-07")
```

### Retention (`src/analytics/retention.py`)
```python
from src.analytics.retention import RetentionAnalytics
ra = RetentionAnalytics(conn)

# N-day retention curves
retention = ra.get_nday_retention([1, 3, 7, 14, 30])

# Retention by user segment
by_segment = ra.get_retention_by_segment(day_n=7)

# Retention by platform
by_platform = ra.get_retention_by_platform(day_n=7)

# Churn risk features for ML
churn = ra.get_churn_risk_features()
```

## Useful SQL queries

### DAU/WAU/MAU stickiness
```sql
WITH dau AS (
    SELECT date_key, COUNT(DISTINCT user_key) AS dau
    FROM analytics.fct_events
    GROUP BY date_key
),
wau AS (
    SELECT d.date_key,
           COUNT(DISTINCT f.user_key) AS wau
    FROM analytics.dim_date d
    JOIN analytics.fct_events f
      ON f.date_key BETWEEN d.date_key - INTERVAL '6 days' AND d.date_key
    GROUP BY d.date_key
)
SELECT dau.date_key, dau.dau, wau.wau,
       ROUND(dau.dau * 100.0 / NULLIF(wau.wau, 0), 2) AS stickiness
FROM dau JOIN wau ON dau.date_key = wau.date_key
ORDER BY dau.date_key;
```

### Platform breakdown
```sql
SELECT platform_key,
       COUNT(DISTINCT user_key) AS unique_users,
       COUNT(*) AS total_events,
       ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_key), 1) AS events_per_user
FROM analytics.fct_events
GROUP BY platform_key
ORDER BY total_events DESC;
```

### Cohort retention matrix
```sql
SELECT cohort_week, weeks_since_signup,
       cohort_size, retained_users,
       ROUND(retention_rate * 100, 1) AS retention_pct
FROM analytics.agg_retention_cohorts
WHERE platform_key = 'facebook'
ORDER BY cohort_week, weeks_since_signup;
```
