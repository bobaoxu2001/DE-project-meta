# Data Model Documentation

## Overview

This document describes the dimensional data model for the Product Analytics warehouse. The model follows a **Kimball star schema** design optimized for analytical query patterns typical of social media product analytics.

## Fact Table

### `fct_events`
**Grain**: One row per user interaction event

| Column | Type | Description |
|--------|------|-------------|
| event_id | VARCHAR (PK) | Unique event identifier |
| event_timestamp | TIMESTAMP | When the event occurred |
| date_key | DATE (FK) | Reference to dim_date |
| user_key | VARCHAR (FK) | Reference to dim_users |
| platform_key | VARCHAR (FK) | Reference to dim_platform |
| event_type_key | VARCHAR (FK) | Reference to dim_event_type |
| session_id | VARCHAR | User session identifier |
| country | VARCHAR | User's country at event time |
| device_type | VARCHAR | Device used (ios/android/web/tablet) |
| event_count | INT | Always 1 (for SUM aggregation) |
| _partition_date | DATE | ETL partition key |

## Dimension Tables

### `dim_users` (SCD Type 2)
| Column | Type | Description |
|--------|------|-------------|
| user_key | VARCHAR (PK) | Surrogate key |
| user_id | VARCHAR | Natural key (anonymized) |
| country | VARCHAR | ISO-3 country code |
| age_group | VARCHAR | Age bucket (13-17, 18-24, etc.) |
| device_type | VARCHAR | Primary device |
| user_segment | VARCHAR | power / active / casual / dormant |
| signup_date | DATE | Account creation date |
| primary_platform | VARCHAR | Most-used platform |
| effective_from | DATE | SCD-2 validity start |
| effective_to | DATE | SCD-2 validity end (NULL=current) |
| is_current | BOOLEAN | Current record flag |

### `dim_date`
| Column | Type | Description |
|--------|------|-------------|
| date_key | DATE (PK) | Calendar date |
| year | INT | Year |
| quarter | INT | Quarter (1-4) |
| month | INT | Month (1-12) |
| month_name | VARCHAR | Full month name |
| week_of_year | INT | ISO week number |
| day_of_week | INT | 0=Monday, 6=Sunday |
| day_name | VARCHAR | Full day name |
| is_weekend | BOOLEAN | Weekend flag |
| fiscal_quarter | VARCHAR | Fiscal quarter (e.g., FY26-Q1) |

### `dim_platform`
| Column | Type | Description |
|--------|------|-------------|
| platform_key | VARCHAR (PK) | Platform identifier |
| platform_name | VARCHAR | Display name |
| platform_family | VARCHAR | social / messaging / media |

### `dim_event_type`
| Column | Type | Description |
|--------|------|-------------|
| event_type_key | VARCHAR (PK) | Event type identifier |
| event_type_name | VARCHAR | Display name |
| event_category | VARCHAR | engagement / content / monetization / session |
| is_active_event | BOOLEAN | Counts toward DAU |

## Aggregate Tables

### `agg_daily_metrics`
Pre-computed daily KPIs per platform.

| Metric | Description |
|--------|-------------|
| dau | Daily Active Users |
| new_users | New signups |
| total_events | Event count |
| total_sessions | Session count |
| content_creates | UGC volume |
| likes, comments, shares | Engagement volume |
| ad_impressions, ad_clicks | Monetization metrics |
| avg_session_events | Average events per session |

### `agg_user_engagement`
Per-user engagement scoring with L7/L28 windows.

### `agg_retention_cohorts`
Weekly cohort retention matrix.

## Key Metrics Glossary

| Metric | Definition |
|--------|------------|
| DAU | Users with ≥1 active event in a day |
| WAU | Users with ≥1 active event in trailing 7 days |
| MAU | Users with ≥1 active event in trailing 28 days |
| DAU/MAU | Stickiness ratio (target: >50%) |
| L7 | Number of active days in last 7 |
| Quick Ratio | (New + Resurrected) / Churned |
| D-N Retention | % of cohort active on day N after signup |
