-- =============================================================================
-- Product Analytics Data Warehouse — Star Schema DDL
-- =============================================================================
-- Designed for a social media platform analytics warehouse.
-- Uses a Kimball-style dimensional model with:
--   • Fact table  : fct_events  (user interaction events)
--   • Dimensions  : dim_users, dim_date, dim_platform, dim_event_type
--   • Aggregates  : agg_daily_metrics, agg_user_engagement, agg_retention_cohorts
--
-- Target engine : DuckDB (compatible with any ANSI SQL engine)
-- =============================================================================

-- -------------------------
-- Schema
-- -------------------------
CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================================================
-- DIMENSION TABLES
-- =========================================================================

-- dim_date — calendar dimension with fiscal and ISO attributes
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key        DATE PRIMARY KEY,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR NOT NULL,
    week_of_year    INT NOT NULL,
    day_of_month    INT NOT NULL,
    day_of_week     INT NOT NULL,       -- 0=Monday … 6=Sunday
    day_name        VARCHAR NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    fiscal_quarter  VARCHAR NOT NULL     -- e.g. 'FY26-Q1'
);

-- dim_users — Slowly Changing Dimension Type 2 (SCD-2)
CREATE TABLE IF NOT EXISTS analytics.dim_users (
    user_key        VARCHAR PRIMARY KEY,  -- surrogate key
    user_id         VARCHAR NOT NULL,
    country         VARCHAR NOT NULL,
    age_group       VARCHAR NOT NULL,
    device_type     VARCHAR NOT NULL,
    user_segment    VARCHAR NOT NULL,     -- power / active / casual / dormant
    signup_date     DATE NOT NULL,
    primary_platform VARCHAR NOT NULL,
    effective_from  DATE NOT NULL,
    effective_to    DATE,                 -- NULL = current record
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_platform
CREATE TABLE IF NOT EXISTS analytics.dim_platform (
    platform_key    VARCHAR PRIMARY KEY,
    platform_name   VARCHAR NOT NULL,
    platform_family VARCHAR NOT NULL      -- 'social', 'messaging', 'media'
);

-- dim_event_type
CREATE TABLE IF NOT EXISTS analytics.dim_event_type (
    event_type_key  VARCHAR PRIMARY KEY,
    event_type_name VARCHAR NOT NULL,
    event_category  VARCHAR NOT NULL,     -- 'engagement', 'content', 'monetization', 'session'
    is_active_event BOOLEAN NOT NULL      -- counts toward DAU
);

-- =========================================================================
-- FACT TABLE
-- =========================================================================

-- fct_events — grain: one row per user interaction event
CREATE TABLE IF NOT EXISTS analytics.fct_events (
    event_id            VARCHAR PRIMARY KEY,
    event_timestamp     TIMESTAMP NOT NULL,
    date_key            DATE NOT NULL,
    user_key            VARCHAR NOT NULL,
    platform_key        VARCHAR NOT NULL,
    event_type_key      VARCHAR NOT NULL,
    session_id          VARCHAR,
    country             VARCHAR,
    device_type         VARCHAR,

    -- Measures
    event_count         INT DEFAULT 1,

    -- Partitioning / clustering hint
    _partition_date     DATE NOT NULL,

    -- Foreign keys (logical — DuckDB does not enforce FK by default)
    FOREIGN KEY (date_key)       REFERENCES analytics.dim_date(date_key),
    FOREIGN KEY (user_key)       REFERENCES analytics.dim_users(user_key),
    FOREIGN KEY (platform_key)   REFERENCES analytics.dim_platform(platform_key),
    FOREIGN KEY (event_type_key) REFERENCES analytics.dim_event_type(event_type_key)
);

-- =========================================================================
-- AGGREGATE / SUMMARY TABLES
-- =========================================================================

-- agg_daily_metrics — pre-computed daily KPIs per platform
CREATE TABLE IF NOT EXISTS analytics.agg_daily_metrics (
    date_key            DATE NOT NULL,
    platform_key        VARCHAR NOT NULL,
    dau                 BIGINT,          -- Daily Active Users
    new_users           BIGINT,          -- Users whose signup_date = date_key
    total_events        BIGINT,
    total_sessions      BIGINT,
    content_creates     BIGINT,
    likes               BIGINT,
    comments            BIGINT,
    shares              BIGINT,
    messages_sent       BIGINT,
    ad_impressions      BIGINT,
    ad_clicks           BIGINT,
    avg_session_events  DOUBLE,
    PRIMARY KEY (date_key, platform_key)
);

-- agg_user_engagement — per-user rolling engagement scores
CREATE TABLE IF NOT EXISTS analytics.agg_user_engagement (
    user_key            VARCHAR NOT NULL,
    date_key            DATE NOT NULL,
    l1_active           BOOLEAN,   -- active in last 1 day
    l7_active           BOOLEAN,   -- active in last 7 days
    l28_active          BOOLEAN,   -- active in last 28 days
    l7_days_active      INT,       -- # of active days in last 7
    l28_days_active     INT,       -- # of active days in last 28
    total_events_l7     BIGINT,
    total_events_l28    BIGINT,
    platforms_used_l7   INT,       -- cross-platform usage
    engagement_score    DOUBLE,    -- composite score (0-100)
    PRIMARY KEY (user_key, date_key)
);

-- agg_retention_cohorts — weekly cohort retention matrix
CREATE TABLE IF NOT EXISTS analytics.agg_retention_cohorts (
    cohort_week         DATE NOT NULL,    -- ISO week start (signup week)
    platform_key        VARCHAR NOT NULL,
    weeks_since_signup  INT NOT NULL,     -- 0, 1, 2 … N
    cohort_size         BIGINT NOT NULL,
    retained_users      BIGINT NOT NULL,
    retention_rate      DOUBLE NOT NULL,
    PRIMARY KEY (cohort_week, platform_key, weeks_since_signup)
);

-- =========================================================================
-- INDEXES for query performance
-- =========================================================================

CREATE INDEX IF NOT EXISTS idx_fct_events_date     ON analytics.fct_events(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_events_user     ON analytics.fct_events(user_key);
CREATE INDEX IF NOT EXISTS idx_fct_events_platform ON analytics.fct_events(platform_key);
CREATE INDEX IF NOT EXISTS idx_fct_events_type     ON analytics.fct_events(event_type_key);
